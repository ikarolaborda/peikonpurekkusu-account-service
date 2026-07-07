package ledger

import (
	"context"
	"log/slog"
)

// Reconcile re-derives every balance from first principles and compares it to
// the materialized account_balances row. Drift means a bug — it is reported,
// never silently patched.
//
// Sign convention by account type (available):
//   asset:                 debits - credits
//   liability/equity/rev:  credits - debits, minus active holds
// held must equal the sum of active holds on the account.
func Reconcile(ctx context.Context, f *Facade, log *slog.Logger) (drifted int, err error) {
	rows, err := f.pool.Query(ctx, `
		with entry_sums as (
			select e.account_id,
			       coalesce(sum(e.amount) filter (where e.direction = 'debit'), 0)  as debits,
			       coalesce(sum(e.amount) filter (where e.direction = 'credit'), 0) as credits
			  from ledger_entries e group by e.account_id
		), hold_sums as (
			select h.account_id, coalesce(sum(h.amount), 0) as active_held
			  from holds h where h.status = 'active' group by h.account_id
		)
		select a.id, a.type,
		       coalesce(es.debits, 0), coalesce(es.credits, 0),
		       coalesce(hs.active_held, 0),
		       b.available, b.held
		  from accounts a
		  join account_balances b on b.account_id = a.id
		  left join entry_sums es on es.account_id = a.id
		  left join hold_sums hs on hs.account_id = a.id`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id                       string
			typ                      string
			debits, credits          int64
			activeHeld               int64
			available, held          int64
		)
		if err := rows.Scan(&id, &typ, &debits, &credits, &activeHeld, &available, &held); err != nil {
			return drifted, err
		}
		var expectedAvailable int64
		switch typ {
		case "asset", "expense":
			expectedAvailable = debits - credits
		default: // liability, equity, revenue
			expectedAvailable = credits - debits - activeHeld
		}
		if available != expectedAvailable || held != activeHeld {
			drifted++
			log.Error("BALANCE DRIFT detected",
				"account_id", id, "type", typ,
				"materialized_available", available, "derived_available", expectedAvailable,
				"materialized_held", held, "derived_held", activeHeld)
		}
	}
	if drifted == 0 {
		log.Info("reconciliation clean — ledger and balances agree")
	}
	return drifted, rows.Err()
}
