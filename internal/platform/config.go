package platform

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the full environment contract (names match the repo .env).
type Config struct {
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string

	RedisCacheHost     string
	RedisCachePort     int
	RedisCachePassword string

	KafkaBootstrap    string
	SchemaRegistryURL string
	JWKSURL           string

	HTTPPort int
	GRPCPort int

	HoldDefaultTTL   time.Duration
	SweepInterval    time.Duration
	ReconcileEvery   time.Duration
	BalanceCacheTTL  time.Duration
	WelcomeSeedMinor int64 // demo deposit granted on user registration
}

func Load() (Config, error) {
	cfg := Config{
		DBHost:             getenv("ACCOUNT_DB_HOST", "account-db"),
		DBPort:             getint("ACCOUNT_DB_PORT", 5432),
		DBUser:             os.Getenv("ACCOUNT_DB_USER"),
		DBPassword:         os.Getenv("ACCOUNT_DB_PASSWORD"),
		DBName:             os.Getenv("ACCOUNT_DB_NAME"),
		RedisCacheHost:     getenv("REDIS_CACHE_HOST", "redis-cache"),
		RedisCachePort:     getint("REDIS_CACHE_PORT", 6379),
		RedisCachePassword: os.Getenv("REDIS_CACHE_PASSWORD"),
		KafkaBootstrap:     getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
		SchemaRegistryURL:  getenv("SCHEMA_REGISTRY_URL", "http://apicurio-registry:8080/apis/ccompat/v7"),
		JWKSURL:            getenv("GATEWAY_JWKS_URL", "http://user-service:8080/.well-known/jwks.json"),
		HTTPPort:           getint("HTTP_PORT", 8080),
		GRPCPort:           getint("GRPC_PORT", 9090),
		HoldDefaultTTL:     getdur("HOLD_DEFAULT_TTL", 7*24*time.Hour),
		SweepInterval:      getdur("HOLD_SWEEP_INTERVAL", 30*time.Second),
		ReconcileEvery:     getdur("RECONCILE_INTERVAL", 10*time.Minute),
		BalanceCacheTTL:    getdur("BALANCE_CACHE_TTL", 5*time.Second),
		WelcomeSeedMinor:   getint64("WELCOME_SEED_MINOR_UNITS", 100_000),
	}
	if cfg.DBUser == "" || cfg.DBPassword == "" || cfg.DBName == "" {
		return cfg, fmt.Errorf("ACCOUNT_DB_USER/PASSWORD/NAME are required")
	}
	return cfg, nil
}

func (c Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getint64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
