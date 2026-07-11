// Package consumer handles the async facts this service reacts to:
//   - identity.user.registered.v1 → create the user's default account + demo
//     welcome deposit (so fresh users can pay immediately)
//   - payments.payment.failed.v1  → release any orphaned hold (compensation)
//
// Consumption is idempotent: each envelope's event_id is recorded in
// processed_events inside the same transaction as the effect. Poison
// messages go to the per-group DLQ after bounded attempts.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/peikonpurekkusu/account-service/internal/events"
	"github.com/peikonpurekkusu/account-service/internal/ledger"
)

const (
	group            = "account-service"
	topicRegistered  = "identity.user.registered.v1"
	topicPayFailed   = "payments.payment.failed.v1"
	maxAttempts      = 3
	defaultCurrency  = "USD"
)

type Consumer struct {
	pool      *pgxpool.Pool
	facade    *ledger.Facade
	client    *kgo.Client
	producer  *kgo.Client // for DLQ publishing
	validator *events.Validator
	log       *slog.Logger
	seedAmt   int64
}

func New(pool *pgxpool.Pool, facade *ledger.Facade, bootstrap []string, producer *kgo.Client,
	validator *events.Validator, log *slog.Logger, seedAmt int64) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topicRegistered, topicPayFailed),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{pool: pool, facade: facade, client: client, producer: producer, validator: validator, log: log, seedAmt: seedAmt}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.log.Error("kafka fetch error", "topic", e.Topic, "error", e.Err)
			}
			time.Sleep(time.Second)
			continue
		}
		settled := true
		fetches.EachRecord(func(rec *kgo.Record) {
			if !c.handleWithRetry(ctx, rec) {
				settled = false
			}
		})
		// Only advance past records that are processed or durably dead-lettered.
		// Handling is idempotent, so redelivering the batch is always safe.
		if !settled {
			c.log.Error("batch not settled — offsets left uncommitted for redelivery")
			time.Sleep(time.Second)
			continue
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("offset commit failed", "error", err)
		}
	}
}

// handleWithRetry reports whether the record is settled (processed, or durably
// dead-lettered). False means the offset must not advance.
func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) bool {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.handle(ctx, rec)
		if err == nil {
			return true
		}
		if poison := (*poisonError)(nil); errors.As(err, &poison) {
			return c.deadLetter(ctx, rec, poison.cause)
		}
		lastErr = err
		c.log.Warn("event handling failed", "topic", rec.Topic, "attempt", attempt, "error", err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
	return c.deadLetter(ctx, rec, lastErr)
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	// Validate against the exact schema the producer framed with, BEFORE any
	// field is read. Registry outage blocks HERE: franz-go commits per batch,
	// so returning "unsettled" would let the next successful batch commit past
	// this record — blocking inside the handler is what holds the offset.
	for {
		verr := c.validator.Validate(ctx, rec.Value)
		if verr == nil {
			break
		}
		if errors.Is(verr, events.ErrRegistryUnavailable) {
			c.log.Warn("schema registry unavailable — holding", "topic", rec.Topic, "offset", rec.Offset)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}
		return poison(verr)
	}

	env, err := events.Unframe(rec.Value)
	if err != nil {
		// Poison by construction — no retry will fix it.
		return poison(err)
	}
	eventID, err := uuid.Parse(env.EventID)
	if err != nil {
		return poison(fmt.Errorf("bad event_id: %w", err))
	}

	return c.facade.InTx(ctx, func(tx pgx.Tx) error {
		// Idempotency gate: ON CONFLICT DO NOTHING + RowsAffected==0 → seen.
		tag, err := tx.Exec(ctx,
			`insert into processed_events (event_id) values ($1) on conflict do nothing`, eventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already processed
		}
		switch env.EventType {
		case topicRegistered:
			return c.onUserRegistered(ctx, tx, env)
		case topicPayFailed:
			return c.onPaymentFailed(ctx, tx, env)
		default:
			return nil // not ours; ack silently
		}
	})
}

func (c *Consumer) onUserRegistered(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	userID, err := uuid.Parse(str(env.Payload["user_id"]))
	if err != nil {
		return fmt.Errorf("user_id: %w", err)
	}
	accountID := uuid.New()
	if _, err := tx.Exec(ctx,
		`insert into accounts (id, user_id, currency, type) values ($1,$2,$3,'liability')`,
		accountID, userID, defaultCurrency); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`insert into account_balances (account_id) values ($1)`, accountID); err != nil {
		return err
	}
	if c.seedAmt > 0 {
		if err := c.facade.Deposit(ctx, tx, accountID, c.seedAmt, defaultCurrency, "welcome:"+userID.String()); err != nil {
			return err
		}
	}
	c.log.Info("account provisioned", "user_id", userID, "account_id", accountID, "seed_minor_units", c.seedAmt)
	return nil
}

func (c *Consumer) onPaymentFailed(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	paymentID, err := uuid.Parse(str(env.Payload["payment_id"]))
	if err != nil {
		return fmt.Errorf("payment_id: %w", err)
	}
	released, err := c.facade.ReleaseByPayment(ctx, tx, paymentID, "compensation")
	if err != nil {
		return err
	}
	if released {
		c.log.Info("orphaned hold released", "payment_id", paymentID)
	}
	return nil
}

// deadLetter reports whether the record reached the DLQ. A failed DLQ publish
// must not settle the record — dropping it is the silent loss the DLQ prevents.
func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error) bool {
	dlq := fmt.Sprintf("%s.%s.dlq", group, rec.Topic)
	headers := append(rec.Headers,
		kgo.RecordHeader{Key: "x-exception", Value: []byte(fmt.Sprint(cause))},
		kgo.RecordHeader{Key: "x-original-topic", Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: "x-original-partition", Value: []byte(fmt.Sprint(rec.Partition))},
		kgo.RecordHeader{Key: "x-original-offset", Value: []byte(fmt.Sprint(rec.Offset))},
		kgo.RecordHeader{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		kgo.RecordHeader{Key: "x-consumer-group", Value: []byte(group)},
	)
	err := c.producer.ProduceSync(ctx, &kgo.Record{
		Topic:   dlq,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: headers,
	}).FirstErr()
	if err != nil {
		c.log.Error("DLQ publish failed — offset held for redelivery", "dlq", dlq, "cause", cause, "error", err)
		return false
	}
	c.log.Warn("message dead-lettered", "dlq", dlq, "cause", cause)
	return true
}

// poisonError marks an event that can never succeed on redelivery — it goes
// straight to the DLQ instead of burning the retry budget.
type poisonError struct{ cause error }

func (p *poisonError) Error() string { return p.cause.Error() }

func poison(cause error) error { return &poisonError{cause: cause} }

func str(v any) string {
	s, _ := v.(string)
	return s
}
