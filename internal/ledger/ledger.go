// Package ledger owns money correctness: every movement is an immutable
// double-entry posting, balances are materialized in the same transaction,
// and holds are two-phase reservations that never live outside PostgreSQL.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInsufficientFunds = errors.New("insufficient available funds")
	ErrHoldNotActive     = errors.New("hold is not active")
	ErrNotFound          = errors.New("not found")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
	ErrAmountExceedsHold = errors.New("capture amount exceeds held amount")
)

// System counterparty accounts created by the initial migration.
var (
	settlementAccounts = map[string]uuid.UUID{
		"USD": uuid.MustParse("00000000-0000-0000-0000-00000000c001"),
		"EUR": uuid.MustParse("00000000-0000-0000-0000-00000000c002"),
		"GBP": uuid.MustParse("00000000-0000-0000-0000-00000000c003"),
		"JPY": uuid.MustParse("00000000-0000-0000-0000-00000000c004"),
	}
	merchantPayableAccounts = map[string]uuid.UUID{
		"USD": uuid.MustParse("00000000-0000-0000-0000-00000000d001"),
		"EUR": uuid.MustParse("00000000-0000-0000-0000-00000000d002"),
		"GBP": uuid.MustParse("00000000-0000-0000-0000-00000000d003"),
		"JPY": uuid.MustParse("00000000-0000-0000-0000-00000000d004"),
	}
)

// Entry is one leg of a posting.
type Entry struct {
	AccountID uuid.UUID
	Direction string // debit | credit
	Amount    int64
	Currency  string
}

// Facade is the single write-path into the ledger (Facade pattern): gRPC,
// consumers and the sweeper all post through it. Every public method runs a
// serializable transaction with automatic retry on serialization failures.
type Facade struct {
	pool   *pgxpool.Pool
	outbox OutboxWriter
}

// OutboxWriter records a domain event inside the caller's transaction.
type OutboxWriter interface {
	Write(ctx context.Context, tx pgx.Tx, topic, aggregateType, aggregateID string, payload map[string]any) error
}

func NewFacade(pool *pgxpool.Pool, outbox OutboxWriter) *Facade {
	return &Facade{pool: pool, outbox: outbox}
}

// InTx runs fn in a serializable transaction, retrying on 40001/40P01.
func (f *Facade) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := pgx.BeginTxFunc(ctx, f.pool, pgx.TxOptions{IsoLevel: pgx.Serializable}, fn)
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			lastErr = err
			time.Sleep(time.Duration(1<<attempt) * 5 * time.Millisecond)
			continue
		}
		return err
	}
	return fmt.Errorf("serialization retries exhausted: %w", lastErr)
}

// post writes one balanced ledger transaction. SUM(debits) must equal
// SUM(credits) per currency — enforced before any row is written.
func post(ctx context.Context, tx pgx.Tx, txnID uuid.UUID, kind string, paymentID *uuid.UUID, idempotencyKey *string, entries []Entry) error {
	if err := ValidateBalanced(entries); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`insert into ledger_transactions (id, idempotency_key, kind, payment_id) values ($1,$2,$3,$4)`,
		txnID, idempotencyKey, kind, paymentID); err != nil {
		return fmt.Errorf("insert ledger_transaction: %w", err)
	}
	for _, e := range entries {
		if _, err := tx.Exec(ctx,
			`insert into ledger_entries (transaction_id, account_id, direction, amount, currency)
			 values ($1,$2,$3,$4,$5)`,
			txnID, e.AccountID, e.Direction, e.Amount, e.Currency); err != nil {
			return fmt.Errorf("insert ledger_entry: %w", err)
		}
	}
	return nil
}

type balanceRow struct {
	Available int64
	Held      int64
	Version   int64
	Currency  string
}

// lockBalance locks the balance row (and returns the account currency).
func lockBalance(ctx context.Context, tx pgx.Tx, accountID uuid.UUID) (balanceRow, error) {
	var b balanceRow
	err := tx.QueryRow(ctx,
		`select b.available, b.held, b.version, a.currency
		   from account_balances b join accounts a on a.id = b.account_id
		  where b.account_id = $1 for update of b`, accountID).
		Scan(&b.Available, &b.Held, &b.Version, &b.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNotFound
	}
	return b, err
}

func bumpBalance(ctx context.Context, tx pgx.Tx, accountID uuid.UUID, dAvailable, dHeld int64) error {
	tag, err := tx.Exec(ctx,
		`update account_balances
		    set available = available + $2, held = held + $3, version = version + 1
		  where account_id = $1`, accountID, dAvailable, dHeld)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}
