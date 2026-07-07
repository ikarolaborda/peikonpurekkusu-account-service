# account-service

The money-truth service: append-only double-entry ledger, materialized
balances, two-phase holds. Go 1.26 · pgx 5 · goose (embedded) · franz-go.

## Interfaces

- **gRPC :9090** (`peikon.account.v1.AccountService`) — Hold / Capture /
  Release / GetBalance. Every mutating RPC is idempotent on `request_id`
  (serialized response stored in the same transaction as the effect).
- **REST :8080** — `GET /accounts`, `GET /accounts/{id}/balance`
  (cache-aside, display-only, labeled `source: ledger|cache`),
  `GET /accounts/{id}/entries` (cursor-paginated). Identity via ForwardAuth
  `X-User-Id`. Health at `/health/live` + `/health/ready`.
- **Kafka in:** `identity.user.registered.v1` (provision account + demo
  welcome deposit), `payments.payment.failed.v1` (release orphaned holds).
  Idempotent via `processed_events`; poison → `account-service.<topic>.dlq`.
- **Kafka out (outbox):** `accounts.funds.{held,captured,released}.v1`.

## Correctness model

- `ledger_transactions` + `ledger_entries` are **immutable** — enforced by
  BEFORE UPDATE/DELETE triggers (grants can't bind the table owner).
- Every posting balances: per currency, SUM(debits) == SUM(credits)
  (`ValidateBalanced`, property-tested) — plus non-negative balance CHECKs.
- Holds are reservations, not postings: `available -= x, held += x` and a
  `holds` row. Capture posts the real entries (debit user liability, credit
  merchant-payable) and releases any remainder; release/expiry restores
  `available`. An expiry sweeper runs every 30 s.
- All decisions happen inside **serializable transactions** with retry on
  40001/40P01. Redis is display-cache only; no authorization ever reads it.
- Reconciliation re-derives every balance from entries + active holds on an
  interval and logs drift loudly (never patches silently).

## Patterns map

- **Facade** — `ledger.Facade`: the single write path (gRPC, consumers,
  sweeper all post through it)
- **Strategy** — balance read path (authoritative gRPC vs cached REST display)
- **Factory** — posting construction in `Capture`/`Deposit` (balanced entry sets)
- **Transactional Outbox** — `outbox.Writer` (same-tx) + `outbox.Relay`
  (`FOR UPDATE SKIP LOCKED`, Confluent wire format via Apicurio ccompat)
- **Idempotency-Key** — `grpc_idempotency` replay + `ledger_transactions.idempotency_key`
  unique constraint as defense-in-depth

## Notes

- gRPC stubs are pre-generated into `contracts/gen/go` (repo-root build
  context; `replace` directive in go.mod).
- Deviation from plan §0: repositories are hand-written pgx SQL rather than
  sqlc codegen — equivalent type safety at this scale, one less build step.
