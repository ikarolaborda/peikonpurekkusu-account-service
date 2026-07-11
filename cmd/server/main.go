// account-service — double-entry ledger, holds, balances.
// gRPC :9090 (money decisions) · HTTP :8080 (reads + health).
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	accountv1 "github.com/ikarolaborda/peikonpurekkusu-contracts/gen/go/account/v1"
	account "github.com/peikonpurekkusu/account-service"
	"github.com/peikonpurekkusu/account-service/internal/consumer"
	"github.com/peikonpurekkusu/account-service/internal/events"
	"github.com/peikonpurekkusu/account-service/internal/grpcapi"
	"github.com/peikonpurekkusu/account-service/internal/httpapi"
	"github.com/peikonpurekkusu/account-service/internal/ledger"
	"github.com/peikonpurekkusu/account-service/internal/outbox"
	"github.com/peikonpurekkusu/account-service/internal/platform"
)

func main() {
	// `account-service healthcheck` — self-probe for the distroless image
	// (no shell/curl available): exits 0 iff /health/ready returns 200.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(selfProbe())
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func selfProbe() int {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://localhost:" + port + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		return 1
	}
	resp.Body.Close()
	return 0
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := platform.Load()
	if err != nil {
		return err
	}

	// ── PostgreSQL + embedded migrations (advisory-lock guarded) ────────────
	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		return fmt.Errorf("pgx pool: %w", err)
	}
	defer pool.Close()
	if err := migrate(ctx, cfg); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	log.Info("migrations applied")

	// ── Redis (display cache only) ──────────────────────────────────────────
	cache := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisCacheHost, cfg.RedisCachePort),
		Password: cfg.RedisCachePassword,
	})
	defer cache.Close()

	// ── Kafka producer (outbox relay + DLQ) ─────────────────────────────────
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(cfg.KafkaBootstrap)...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	registry := events.NewRegistry(cfg.SchemaRegistryURL)
	facade := ledger.NewFacade(pool, outbox.Writer{})

	// ── Background workers ───────────────────────────────────────────────────
	relay := outbox.NewRelay(pool, producer, registry, log)
	go relay.Run(ctx)

	validator := events.NewValidator(cfg.SchemaRegistryURL)
	cons, err := consumer.New(pool, facade, splitCSV(cfg.KafkaBootstrap), producer, validator, log, cfg.WelcomeSeedMinor)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer cons.Close()
	go cons.Run(ctx)

	go every(ctx, cfg.SweepInterval, func() {
		if n, err := facade.SweepExpiredHolds(ctx); err != nil {
			log.Error("hold sweep failed", "error", err)
		} else if n > 0 {
			log.Info("expired holds released", "count", n)
		}
	})
	go every(ctx, cfg.ReconcileEvery, func() {
		if _, err := ledger.Reconcile(ctx, facade, log); err != nil && ctx.Err() == nil {
			log.Error("reconciliation failed", "error", err)
		}
	})

	// ── gRPC server ───────────────────────────────────────────────────────────
	grpcSrv := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(grpcSrv, grpcapi.New(pool, facade, cfg.HoldDefaultTTL))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return err
	}
	go func() {
		log.Info("gRPC listening", "port", cfg.GRPCPort)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Error("grpc serve", "error", err)
		}
	}()

	// ── HTTP server ───────────────────────────────────────────────────────────
	kafkaOK := func() bool {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return producer.Ping(pingCtx) == nil
	}
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           httpapi.New(pool, cache, cfg.BalanceCacheTTL, kafkaOK, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("HTTP listening", "port", cfg.HTTPPort)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http serve", "error", err)
		}
	}()

	// ── Graceful shutdown: stop intake, drain, close deps ────────────────────
	<-ctx.Done()
	log.Info("shutting down")
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	grpcSrv.GracefulStop()
	return nil
}

func migrate(ctx context.Context, cfg platform.Config) error {
	sqlDB, err := goose.OpenDBWithDriver("pgx", cfg.DSN())
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	migrations, err := fs.Sub(account.MigrationsFS, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations,
		goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

func every(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
