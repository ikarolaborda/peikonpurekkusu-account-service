// Package grpcapi implements peikon.account.v1.AccountService — the
// synchronous money-decision API. Every mutating RPC is idempotent on the
// caller-supplied request_id: the serialized response is stored in the same
// transaction as the effect, and replays return it byte-for-byte.
package grpcapi

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	accountv1 "github.com/peikonpurekkusu/contracts/gen/go/account/v1"
	"github.com/peikonpurekkusu/account-service/internal/ledger"
)

type Server struct {
	accountv1.UnimplementedAccountServiceServer
	pool    *pgxpool.Pool
	facade  *ledger.Facade
	holdTTL time.Duration
}

func New(pool *pgxpool.Pool, facade *ledger.Facade, holdTTL time.Duration) *Server {
	return &Server{pool: pool, facade: facade, holdTTL: holdTTL}
}

// replay returns a previously stored response for request_id, if any.
func replay[T proto.Message](ctx context.Context, tx pgx.Tx, requestID string, out T) (bool, error) {
	var body []byte
	err := tx.QueryRow(ctx,
		`select response from grpc_idempotency where request_id = $1`, requestID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, proto.Unmarshal(body, out)
}

func store(ctx context.Context, tx pgx.Tx, requestID, method string, resp proto.Message) error {
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`insert into grpc_idempotency (request_id, method, response) values ($1,$2,$3)`,
		requestID, method, body)
	return err
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, "insufficient available funds")
	case errors.Is(err, ledger.ErrHoldNotActive):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ledger.ErrAmountExceedsHold):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	default:
		return status.Error(codes.Internal, "ledger operation failed")
	}
}

func parseIDs(ids ...string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(ids))
	for i, s := range ids {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid uuid %q", s)
		}
		out[i] = id
	}
	return out, nil
}

func (s *Server) HoldFunds(ctx context.Context, req *accountv1.HoldFundsRequest) (*accountv1.HoldFundsResponse, error) {
	if req.GetRequestId() == "" || req.GetAmount() == nil {
		return nil, status.Error(codes.InvalidArgument, "request_id and amount are required")
	}
	ids, err := parseIDs(req.GetAccountId(), req.GetPaymentId())
	if err != nil {
		return nil, err
	}
	ttl := s.holdTTL
	if req.GetExpiresInSeconds() > 0 {
		ttl = time.Duration(req.GetExpiresInSeconds()) * time.Second
	}

	resp := &accountv1.HoldFundsResponse{}
	opErr := s.facade.InTx(ctx, func(tx pgx.Tx) error {
		if hit, err := replay(ctx, tx, req.GetRequestId(), resp); err != nil || hit {
			return err
		}
		result, err := s.facade.Hold(ctx, tx, ids[0], ids[1], req.GetAmount().GetMinorUnits(), req.GetAmount().GetCurrencyCode(), ttl)
		if err != nil {
			return err
		}
		resp.HoldId = result.HoldID.String()
		resp.Status = accountv1.HoldStatus_HOLD_STATUS_ACTIVE
		resp.AvailableAfter = &accountv1.Money{
			MinorUnits:   result.AvailableAfter,
			CurrencyCode: result.Currency,
		}
		return store(ctx, tx, req.GetRequestId(), "HoldFunds", resp)
	})
	if opErr != nil {
		return nil, mapErr(opErr)
	}
	return resp, nil
}

func (s *Server) CaptureFunds(ctx context.Context, req *accountv1.CaptureFundsRequest) (*accountv1.CaptureFundsResponse, error) {
	if req.GetRequestId() == "" || req.GetAmount() == nil {
		return nil, status.Error(codes.InvalidArgument, "request_id and amount are required")
	}
	ids, err := parseIDs(req.GetHoldId())
	if err != nil {
		return nil, err
	}

	resp := &accountv1.CaptureFundsResponse{}
	opErr := s.facade.InTx(ctx, func(tx pgx.Tx) error {
		if hit, err := replay(ctx, tx, req.GetRequestId(), resp); err != nil || hit {
			return err
		}
		txnID, err := s.facade.Capture(ctx, tx, ids[0], req.GetAmount().GetMinorUnits())
		if err != nil {
			return err
		}
		resp.LedgerTransactionId = txnID.String()
		resp.Status = accountv1.HoldStatus_HOLD_STATUS_CAPTURED
		return store(ctx, tx, req.GetRequestId(), "CaptureFunds", resp)
	})
	if opErr != nil {
		return nil, mapErr(opErr)
	}
	return resp, nil
}

func (s *Server) ReleaseFunds(ctx context.Context, req *accountv1.ReleaseFundsRequest) (*accountv1.ReleaseFundsResponse, error) {
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	ids, err := parseIDs(req.GetHoldId())
	if err != nil {
		return nil, err
	}
	reason := req.GetReason()
	if reason == "" {
		reason = "compensation"
	}

	resp := &accountv1.ReleaseFundsResponse{}
	opErr := s.facade.InTx(ctx, func(tx pgx.Tx) error {
		if hit, err := replay(ctx, tx, req.GetRequestId(), resp); err != nil || hit {
			return err
		}
		if err := s.facade.Release(ctx, tx, ids[0], reason); err != nil {
			return err
		}
		resp.Status = accountv1.HoldStatus_HOLD_STATUS_RELEASED
		return store(ctx, tx, req.GetRequestId(), "ReleaseFunds", resp)
	})
	if opErr != nil {
		return nil, mapErr(opErr)
	}
	return resp, nil
}

// GetBalance reads the authoritative materialized balance (never a cache).
func (s *Server) GetBalance(ctx context.Context, req *accountv1.GetBalanceRequest) (*accountv1.GetBalanceResponse, error) {
	ids, err := parseIDs(req.GetAccountId())
	if err != nil {
		return nil, err
	}
	var available, held, version int64
	var currency string
	err = s.pool.QueryRow(ctx,
		`select b.available, b.held, b.version, a.currency
		   from account_balances b join accounts a on a.id = b.account_id
		  where b.account_id = $1`, ids[0]).Scan(&available, &held, &version, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "account not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "balance read failed")
	}
	return &accountv1.GetBalanceResponse{
		AccountId: req.GetAccountId(),
		Available: &accountv1.Money{MinorUnits: available, CurrencyCode: currency},
		Held:      &accountv1.Money{MinorUnits: held, CurrencyCode: currency},
		Version:   version,
	}, nil
}
