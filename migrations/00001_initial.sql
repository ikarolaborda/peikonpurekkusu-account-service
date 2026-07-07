-- +goose Up

create table accounts (
  id         uuid primary key,
  user_id    uuid not null,
  currency   char(3) not null,
  type       text not null check (type in ('asset','liability','revenue','expense','equity')),
  status     text not null default 'active' check (status in ('active','frozen','closed')),
  created_at timestamptz not null default now()
);
create index accounts_user_idx on accounts (user_id);

-- One immutable transaction per money movement; entries must balance.
create table ledger_transactions (
  id              uuid primary key,
  idempotency_key text unique,
  kind            text not null check (kind in ('deposit','capture','release_adjust','reversal','seed')),
  payment_id      uuid,
  created_at      timestamptz not null default now()
);

create table ledger_entries (
  id             bigint generated always as identity primary key,
  transaction_id uuid not null references ledger_transactions(id),
  account_id     uuid not null references accounts(id),
  direction      text not null check (direction in ('debit','credit')),
  amount         bigint not null check (amount > 0),
  currency       char(3) not null,
  created_at     timestamptz not null default now()
);
create index ledger_entries_account_idx on ledger_entries (account_id, id desc);

-- Append-only enforcement: the service role owns these tables, so GRANTs
-- alone cannot protect them — triggers make mutation impossible for everyone.
-- +goose StatementBegin
create function forbid_mutation() returns trigger as $$
begin
  raise exception 'ledger rows are immutable (append-only): % on %', tg_op, tg_table_name;
end
$$ language plpgsql;
-- +goose StatementEnd
create trigger ledger_entries_immutable
  before update or delete on ledger_entries
  for each row execute function forbid_mutation();
create trigger ledger_transactions_immutable
  before update or delete on ledger_transactions
  for each row execute function forbid_mutation();

-- Materialized balances: derived from entries + active holds, rebuildable by
-- reconciliation. version = optimistic concurrency / audit ordering.
create table account_balances (
  account_id uuid primary key references accounts(id),
  available  bigint not null default 0 check (available >= 0),
  held       bigint not null default 0 check (held >= 0),
  version    bigint not null default 0
);

-- Two-phase reservations (TigerBeetle-style pending transfers). Mutable
-- lifecycle state — intentionally NOT part of the immutable ledger.
create table holds (
  id         uuid primary key,
  account_id uuid not null references accounts(id),
  payment_id uuid not null,
  amount     bigint not null check (amount > 0),
  currency   char(3) not null,
  status     text not null default 'active' check (status in ('active','captured','released','expired')),
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);
create unique index holds_active_payment_uq on holds (payment_id) where status = 'active';
create index holds_expiry_idx on holds (expires_at) where status = 'active';

-- Stripe-style idempotency for the gRPC money API: replay returns the stored
-- serialized response; a request_id can never execute twice.
create table grpc_idempotency (
  request_id text primary key,
  method     text not null,
  response   bytea not null,
  created_at timestamptz not null default now()
);

-- Transactional outbox (Debezium Outbox Event Router-compatible columns).
create table outbox (
  id            uuid primary key,
  aggregatetype text not null,
  aggregateid   text not null,
  type          text not null,
  payload       jsonb not null,
  created_at    timestamptz not null default now(),
  processed_at  timestamptz
);
create index outbox_unprocessed_idx on outbox (id) where processed_at is null;

-- Idempotent Kafka consumption (at-least-once → exactly-once effect).
create table processed_events (
  event_id     uuid primary key,
  processed_at timestamptz not null default now()
);

-- System accounts (fixed UUIDs; counterparties for deposits and captures).
insert into accounts (id, user_id, currency, type, status) values
  ('00000000-0000-0000-0000-00000000c001', '00000000-0000-0000-0000-000000000000', 'USD', 'asset',     'active'),
  ('00000000-0000-0000-0000-00000000c002', '00000000-0000-0000-0000-000000000000', 'EUR', 'asset',     'active'),
  ('00000000-0000-0000-0000-00000000c003', '00000000-0000-0000-0000-000000000000', 'GBP', 'asset',     'active'),
  ('00000000-0000-0000-0000-00000000c004', '00000000-0000-0000-0000-000000000000', 'JPY', 'asset',     'active'),
  ('00000000-0000-0000-0000-00000000d001', '00000000-0000-0000-0000-000000000000', 'USD', 'liability', 'active'),
  ('00000000-0000-0000-0000-00000000d002', '00000000-0000-0000-0000-000000000000', 'EUR', 'liability', 'active'),
  ('00000000-0000-0000-0000-00000000d003', '00000000-0000-0000-0000-000000000000', 'GBP', 'liability', 'active'),
  ('00000000-0000-0000-0000-00000000d004', '00000000-0000-0000-0000-000000000000', 'JPY', 'liability', 'active');
insert into account_balances (account_id) select id from accounts;

-- +goose Down
drop table if exists processed_events, outbox, grpc_idempotency, holds,
  account_balances, ledger_entries, ledger_transactions, accounts cascade;
drop function if exists forbid_mutation cascade;
