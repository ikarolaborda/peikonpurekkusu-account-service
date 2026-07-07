// Package httpapi serves the read-side REST API (behind Traefik ForwardAuth —
// identity arrives as X-User-Id, trustworthy only because this container is
// reachable exclusively through the gateway) and the health probes.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	pool     *pgxpool.Pool
	cache    *redis.Client
	cacheTTL time.Duration
	log      *slog.Logger
	kafkaOK  func() bool
}

func New(pool *pgxpool.Pool, cache *redis.Client, cacheTTL time.Duration, kafkaOK func() bool, log *slog.Logger) *Server {
	return &Server{pool: pool, cache: cache, cacheTTL: cacheTTL, kafkaOK: kafkaOK, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("GET /accounts", s.listAccounts)
	mux.HandleFunc("GET /accounts/{id}/balance", s.getBalance)
	mux.HandleFunc("GET /accounts/{id}/entries", s.listEntries)
	return mux
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	checks := map[string]string{"postgres": "up", "redis-cache": "up", "kafka": "up"}
	status := http.StatusOK
	if err := s.pool.Ping(ctx); err != nil {
		checks["postgres"], status = "down", http.StatusServiceUnavailable
	}
	if err := s.cache.Ping(ctx).Err(); err != nil {
		checks["redis-cache"], status = "down", http.StatusServiceUnavailable
	}
	if !s.kafkaOK() {
		checks["kafka"], status = "down", http.StatusServiceUnavailable
	}
	writeJSON(w, status, checks)
}

func callerID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(r.Header.Get("X-User-Id"))
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	userID, err := callerID(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	rows, err := s.pool.Query(r.Context(),
		`select a.id, a.currency, a.type, a.status, a.created_at, b.available, b.held
		   from accounts a join account_balances b on b.account_id = a.id
		  where a.user_id = $1 order by a.created_at`, userID)
	if err != nil {
		s.fail(w, "list accounts", err)
		return
	}
	defer rows.Close()

	type account struct {
		ID        string    `json:"account_id"`
		Currency  string    `json:"currency_code"`
		Type      string    `json:"type"`
		Status    string    `json:"status"`
		CreatedAt time.Time `json:"created_at"`
		Available int64     `json:"available_minor_units"`
		Held      int64     `json:"held_minor_units"`
	}
	out := []account{}
	for rows.Next() {
		var a account
		if err := rows.Scan(&a.ID, &a.Currency, &a.Type, &a.Status, &a.CreatedAt, &a.Available, &a.Held); err != nil {
			s.fail(w, "scan account", err)
			return
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"accounts": out, "as_of": time.Now().UTC()})
}

type balanceDTO struct {
	AccountID string    `json:"account_id"`
	Available int64     `json:"available_minor_units"`
	Held      int64     `json:"held_minor_units"`
	Currency  string    `json:"currency_code"`
	AsOf      time.Time `json:"as_of"`
	Source    string    `json:"source"` // ledger | cache
}

// getBalance is cache-aside for DISPLAY only (5s TTL, delete-on-write from
// the ledger paths is future work — TTL bounds staleness). Authorization
// decisions never read this endpoint; they use gRPC GetBalance / locked rows.
func (s *Server) getBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := callerID(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	accountID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid account id")
		return
	}

	cacheKey := "balance:" + accountID.String()
	if cached, err := s.cache.Get(r.Context(), cacheKey).Bytes(); err == nil {
		var dto balanceDTO
		if json.Unmarshal(cached, &dto) == nil {
			dto.Source = "cache"
			writeJSON(w, http.StatusOK, dto)
			return
		}
	}

	var dto balanceDTO
	err = s.pool.QueryRow(r.Context(),
		`select b.account_id, b.available, b.held, a.currency
		   from account_balances b join accounts a on a.id = b.account_id
		  where b.account_id = $1 and a.user_id = $2`, accountID, userID).
		Scan(&dto.AccountID, &dto.Available, &dto.Held, &dto.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		httpError(w, http.StatusNotFound, "account not found")
		return
	}
	if err != nil {
		s.fail(w, "balance read", err)
		return
	}
	dto.AsOf = time.Now().UTC()
	dto.Source = "ledger"
	if body, err := json.Marshal(dto); err == nil {
		// best-effort cache; never block or fail the response on it
		_ = s.cache.Set(r.Context(), cacheKey, body, s.cacheTTL).Err()
	}
	writeJSON(w, http.StatusOK, dto)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	userID, err := callerID(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	accountID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid account id")
		return
	}
	var owned bool
	if err := s.pool.QueryRow(r.Context(),
		`select exists(select 1 from accounts where id=$1 and user_id=$2)`, accountID, userID).
		Scan(&owned); err != nil || !owned {
		httpError(w, http.StatusNotFound, "account not found")
		return
	}

	cursor := int64(1<<62 - 1)
	if c := r.URL.Query().Get("cursor"); c != "" {
		if n, err := strconv.ParseInt(c, 10, 64); err == nil {
			cursor = n
		}
	}
	rows, err := s.pool.Query(r.Context(),
		`select e.id, e.transaction_id, t.kind, e.direction, e.amount, e.currency, e.created_at
		   from ledger_entries e join ledger_transactions t on t.id = e.transaction_id
		  where e.account_id = $1 and e.id < $2
		  order by e.id desc limit 50`, accountID, cursor)
	if err != nil {
		s.fail(w, "list entries", err)
		return
	}
	defer rows.Close()

	type entry struct {
		ID          int64     `json:"id"`
		Transaction string    `json:"transaction_id"`
		Kind        string    `json:"kind"`
		Direction   string    `json:"direction"`
		Amount      int64     `json:"amount_minor_units"`
		Currency    string    `json:"currency_code"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := []entry{}
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.Transaction, &e.Kind, &e.Direction, &e.Amount, &e.Currency, &e.CreatedAt); err != nil {
			s.fail(w, "scan entry", err)
			return
		}
		out = append(out, e)
	}
	next := ""
	if len(out) == 50 {
		next = fmt.Sprint(out[len(out)-1].ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out, "next_cursor": next})
}

func (s *Server) fail(w http.ResponseWriter, op string, err error) {
	s.log.Error(op, "error", err)
	httpError(w, http.StatusInternalServerError, "internal error")
}

func httpError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]any{"title": http.StatusText(status), "status": status, "detail": detail})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
