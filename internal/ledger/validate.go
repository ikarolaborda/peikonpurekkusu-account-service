package ledger

import "fmt"

// ValidateBalanced enforces the double-entry invariant: per currency,
// SUM(debits) == SUM(credits), every amount strictly positive, directions
// well-formed. Pure function — the posting path calls it before any write.
func ValidateBalanced(entries []Entry) error {
	if len(entries) < 2 {
		return fmt.Errorf("a posting needs at least two entries")
	}
	sums := map[string]int64{}
	for _, e := range entries {
		if e.Amount <= 0 {
			return fmt.Errorf("non-positive entry amount %d", e.Amount)
		}
		switch e.Direction {
		case "debit":
			sums[e.Currency] += e.Amount
		case "credit":
			sums[e.Currency] -= e.Amount
		default:
			return fmt.Errorf("invalid direction %q", e.Direction)
		}
	}
	for cur, s := range sums {
		if s != 0 {
			return fmt.Errorf("unbalanced posting for %s: debits-credits=%d", cur, s)
		}
	}
	return nil
}
