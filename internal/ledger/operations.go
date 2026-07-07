package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// HoldResult mirrors the gRPC response shape.
type HoldResult struct {
	HoldID         uuid.UUID
	Status         string
	AvailableAfter int64
	Currency       string
}

// Hold reserves funds: available -= amount, held += amount, plus a holds row.
// No ledger entries — nothing moved yet (two-phase pending transfer).
func (f *Facade) Hold(ctx context.Context, tx pgx.Tx, accountID, paymentID uuid.UUID, amount int64, currency string, ttl time.Duration) (HoldResult, error) {
	b, err := lockBalance(ctx, tx, accountID)
	if err != nil {
		return HoldResult{}, err
	}
	if b.Currency != currency {
		return HoldResult{}, fmt.Errorf("%w: account is %s, request is %s", ErrCurrencyMismatch, b.Currency, currency)
	}
	if b.Available < amount {
		return HoldResult{}, ErrInsufficientFunds
	}
	holdID := uuid.New()
	expiresAt := time.Now().Add(ttl)
	if _, err := tx.Exec(ctx,
		`insert into holds (id, account_id, payment_id, amount, currency, status, expires_at)
		 values ($1,$2,$3,$4,$5,'active',$6)`,
		holdID, accountID, paymentID, amount, currency, expiresAt); err != nil {
		return HoldResult{}, fmt.Errorf("insert hold: %w", err)
	}
	if err := bumpBalance(ctx, tx, accountID, -amount, +amount); err != nil {
		return HoldResult{}, err
	}
	if err := f.outbox.Write(ctx, tx, "accounts.funds.held.v1", "account", accountID.String(), map[string]any{
		"hold_id":           holdID.String(),
		"account_id":        accountID.String(),
		"payment_id":        paymentID.String(),
		"amount_minor_units": amount,
		"currency_code":     currency,
		"expires_at":        expiresAt.UTC().Format(time.RFC3339),
	}); err != nil {
		return HoldResult{}, err
	}
	return HoldResult{HoldID: holdID, Status: "active", AvailableAfter: b.Available - amount, Currency: currency}, nil
}

type holdRow struct {
	AccountID uuid.UUID
	PaymentID uuid.UUID
	Amount    int64
	Currency  string
	Status    string
}

func lockHold(ctx context.Context, tx pgx.Tx, holdID uuid.UUID) (holdRow, error) {
	var h holdRow
	err := tx.QueryRow(ctx,
		`select account_id, payment_id, amount, currency, status from holds where id = $1 for update`,
		holdID).Scan(&h.AccountID, &h.PaymentID, &h.Amount, &h.Currency, &h.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return h, ErrNotFound
	}
	return h, err
}

// Capture converts a hold into a real posting: debit the user's liability
// account, credit merchant-payable. A partial capture releases the remainder
// (single-capture semantics, processor-style).
func (f *Facade) Capture(ctx context.Context, tx pgx.Tx, holdID uuid.UUID, amount int64) (uuid.UUID, error) {
	h, err := lockHold(ctx, tx, holdID)
	if err != nil {
		return uuid.Nil, err
	}
	if h.Status != "active" {
		return uuid.Nil, fmt.Errorf("%w (status=%s)", ErrHoldNotActive, h.Status)
	}
	if amount <= 0 || amount > h.Amount {
		return uuid.Nil, ErrAmountExceedsHold
	}
	payable, ok := merchantPayableAccounts[h.Currency]
	if !ok {
		return uuid.Nil, fmt.Errorf("no merchant payable account for %s", h.Currency)
	}

	txnID := uuid.New()
	idem := "capture:" + holdID.String()
	if err := post(ctx, tx, txnID, "capture", &h.PaymentID, &idem, []Entry{
		{AccountID: h.AccountID, Direction: "debit", Amount: amount, Currency: h.Currency},
		{AccountID: payable, Direction: "credit", Amount: amount, Currency: h.Currency},
	}); err != nil {
		return uuid.Nil, err
	}

	remainder := h.Amount - amount
	// user account: held decreases by the full hold; remainder returns to available
	if err := bumpBalance(ctx, tx, h.AccountID, +remainder, -h.Amount); err != nil {
		return uuid.Nil, err
	}
	// merchant payable account gains available funds
	if err := bumpBalance(ctx, tx, payable, +amount, 0); err != nil {
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `update holds set status='captured' where id=$1`, holdID); err != nil {
		return uuid.Nil, err
	}

	if err := f.outbox.Write(ctx, tx, "accounts.funds.captured.v1", "account", h.AccountID.String(), map[string]any{
		"hold_id":               holdID.String(),
		"account_id":            h.AccountID.String(),
		"ledger_transaction_id": txnID.String(),
		"amount_minor_units":    amount,
		"currency_code":         h.Currency,
	}); err != nil {
		return uuid.Nil, err
	}
	if remainder > 0 {
		if err := f.outbox.Write(ctx, tx, "accounts.funds.released.v1", "account", h.AccountID.String(), map[string]any{
			"hold_id":            holdID.String(),
			"account_id":         h.AccountID.String(),
			"amount_minor_units": remainder,
			"currency_code":      h.Currency,
			"reason":             "partial_capture_remainder",
		}); err != nil {
			return uuid.Nil, err
		}
	}
	return txnID, nil
}

// Release frees a hold without moving money (compensation / expiry).
func (f *Facade) Release(ctx context.Context, tx pgx.Tx, holdID uuid.UUID, reason string) error {
	h, err := lockHold(ctx, tx, holdID)
	if err != nil {
		return err
	}
	if h.Status != "active" {
		return fmt.Errorf("%w (status=%s)", ErrHoldNotActive, h.Status)
	}
	newStatus := "released"
	if reason == "expiry" {
		newStatus = "expired"
	}
	if err := bumpBalance(ctx, tx, h.AccountID, +h.Amount, -h.Amount); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `update holds set status=$2 where id=$1`, holdID, newStatus); err != nil {
		return err
	}
	return f.outbox.Write(ctx, tx, "accounts.funds.released.v1", "account", h.AccountID.String(), map[string]any{
		"hold_id":            holdID.String(),
		"account_id":         h.AccountID.String(),
		"amount_minor_units": h.Amount,
		"currency_code":      h.Currency,
		"reason":             reason,
	})
}

// ReleaseByPayment releases the active hold attached to a payment, if any —
// consumed from payments.payment.failed.v1 (compensation path). Idempotent:
// no active hold is a no-op.
func (f *Facade) ReleaseByPayment(ctx context.Context, tx pgx.Tx, paymentID uuid.UUID, reason string) (bool, error) {
	var holdID uuid.UUID
	err := tx.QueryRow(ctx,
		`select id from holds where payment_id = $1 and status = 'active' for update`, paymentID).
		Scan(&holdID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, f.Release(ctx, tx, holdID, reason)
}

// Deposit credits a user account from the settlement asset account
// (demo welcome seed; also the shape a real top-up would take).
func (f *Facade) Deposit(ctx context.Context, tx pgx.Tx, accountID uuid.UUID, amount int64, currency, idemKey string) error {
	settlement, ok := settlementAccounts[currency]
	if !ok {
		return fmt.Errorf("no settlement account for %s", currency)
	}
	txnID := uuid.New()
	if err := post(ctx, tx, txnID, "deposit", nil, &idemKey, []Entry{
		{AccountID: settlement, Direction: "debit", Amount: amount, Currency: currency},
		{AccountID: accountID, Direction: "credit", Amount: amount, Currency: currency},
	}); err != nil {
		return err
	}
	if err := bumpBalance(ctx, tx, settlement, +amount, 0); err != nil {
		return err
	}
	return bumpBalance(ctx, tx, accountID, +amount, 0)
}

// SweepExpiredHolds releases all expired active holds (called on interval).
func (f *Facade) SweepExpiredHolds(ctx context.Context) (int, error) {
	released := 0
	err := f.InTx(ctx, func(tx pgx.Tx) error {
		released = 0 // reset on retry
		rows, err := tx.Query(ctx,
			`select id from holds where status='active' and expires_at < now() for update skip locked limit 100`)
		if err != nil {
			return err
		}
		ids := []uuid.UUID{}
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			if err := f.Release(ctx, tx, id, "expiry"); err != nil {
				return err
			}
			released++
		}
		return nil
	})
	return released, err
}
