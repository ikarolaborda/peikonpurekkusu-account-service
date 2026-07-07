package ledger

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func entry(dir string, amount int64, currency string) Entry {
	return Entry{AccountID: uuid.New(), Direction: dir, Amount: amount, Currency: currency}
}

func TestValidateBalanced(t *testing.T) {
	cases := []struct {
		name    string
		entries []Entry
		wantErr string
	}{
		{
			name:    "balanced pair",
			entries: []Entry{entry("debit", 500, "USD"), entry("credit", 500, "USD")},
		},
		{
			name: "balanced multi-leg",
			entries: []Entry{
				entry("debit", 300, "USD"), entry("debit", 200, "USD"),
				entry("credit", 500, "USD"),
			},
		},
		{
			name: "balanced per currency independently",
			entries: []Entry{
				entry("debit", 500, "USD"), entry("credit", 500, "USD"),
				entry("debit", 900, "JPY"), entry("credit", 900, "JPY"),
			},
		},
		{
			name:    "unbalanced",
			entries: []Entry{entry("debit", 500, "USD"), entry("credit", 499, "USD")},
			wantErr: "unbalanced",
		},
		{
			name: "cross-currency smuggling is unbalanced",
			entries: []Entry{
				entry("debit", 500, "USD"), entry("credit", 500, "EUR"),
			},
			wantErr: "unbalanced",
		},
		{
			name:    "zero amount",
			entries: []Entry{entry("debit", 0, "USD"), entry("credit", 0, "USD")},
			wantErr: "non-positive",
		},
		{
			name:    "negative amount",
			entries: []Entry{entry("debit", -5, "USD"), entry("credit", -5, "USD")},
			wantErr: "non-positive",
		},
		{
			name:    "bad direction",
			entries: []Entry{entry("debit", 5, "USD"), entry("withdraw", 5, "USD")},
			wantErr: "invalid direction",
		},
		{
			name:    "single leg",
			entries: []Entry{entry("debit", 5, "USD")},
			wantErr: "at least two",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBalanced(tc.entries)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
			}
		})
	}
}
