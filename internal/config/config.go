// config.go — YAML-конфіг xsink.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config — весь конфіг.
type Config struct {
	Log      LogConfig      `yaml:"log"`
	Proxy    ProxyConfig    `yaml:"proxy"`
	Redis    []RedisConfig  `yaml:"redis"` // список Redis-джерел; має бути >= 1
	Postgres PostgresConfig `yaml:"postgres"`
	Pipeline PipelineConfig `yaml:"pipeline"`
	Mapping  MappingConfig  `yaml:"mapping"`
	HTTP     HTTPConfig     `yaml:"http"`
	Reclaim  ReclaimConfig  `yaml:"reclaim"`
	Breaker  BreakerConfig  `yaml:"breaker"`
}

// HTTPConfig — /healthz /readyz /metrics.
type HTTPConfig struct {
	Addr string `yaml:"addr"` // "" = вимкнено
}

// ReclaimConfig — XAUTOCLAIM цикл для повернення stuck-повідомлень з PEL.
type ReclaimConfig struct {
	Enabled     bool          `yaml:"enabled"`
	Interval    time.Duration `yaml:"interval"`      // раз на скільки запускати
	MinIdleTime time.Duration `yaml:"min_idle_time"` // повідомлення старші за це — забираємо
	BatchSize   int           `yaml:"batch_size"`
}

// BreakerConfig — параметри circuit-breaker для Postgres/Redis.
type BreakerConfig struct {
	// Максимальна серія збоїв перш ніж breaker відкриється.
	MaxConsecutiveFailures uint32        `yaml:"max_consecutive_failures"`
	// Час у Open-стані перш ніж дати HalfOpen пробу.
	OpenTimeout            time.Duration `yaml:"open_timeout"`
}

// LogConfig — рівень і призначення виводу.
type LogConfig struct {
	Level string `yaml:"level"`
	Sink  string `yaml:"sink"` // journal | stdout
}

// ProxyConfig — параметри SOCKS5 proxy-пула.
type ProxyConfig struct {
	ListPath    string        `yaml:"list_path"`
	Strategy    string        `yaml:"strategy"`    // random | round_robin | sticky
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

// RedisConfig — параметри одного Redis-джерела.
// Multi-source: у config може бути кілька таких блоків; поле `name` — унікальне.
type RedisConfig struct {
	Name              string        `yaml:"name"` // унікальне ім'я джерела; посилання з mapping.stream.source
	Addr              string        `yaml:"addr"`
	TLS               RedisTLS      `yaml:"tls"`
	Password          string        `yaml:"password"`
	DB                int           `yaml:"db"`
	MaxRetries        int           `yaml:"max_retries"`
	PollRatePerSecond int           `yaml:"poll_rate_per_second"`
	DialTimeout       time.Duration `yaml:"dial_timeout"`
}

// RedisTLS — mTLS для Redis.
type RedisTLS struct {
	CACertPath     string `yaml:"ca_cert_path"`
	ClientCertPath string `yaml:"client_cert_path"`
	ClientKeyPath  string `yaml:"client_key_path"`
	ServerName     string `yaml:"server_name"`
}

// PostgresConfig — Postgres через pgbouncer.
type PostgresConfig struct {
	DSN            string        `yaml:"dsn"`
	MaxOpenConns   int32         `yaml:"max_open_conns"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	AutoMigrate    bool          `yaml:"auto_migrate"`
}

// PipelineConfig — паралелізм і retry-політика.
type PipelineConfig struct {
	Workers            int           `yaml:"workers"`
	InsertRetries      int           `yaml:"insert_retries"`
	InsertRetryBackoff time.Duration `yaml:"insert_retry_backoff"` // база для backoff+jitter
	DeadLetterTable    string        `yaml:"dead_letter_table"`
	// DeadLetterRetention — TTL записів dead_letters; старші видаляються фоново.
	// 0 (default) — вимкнено (зберігати безстроково).
	DeadLetterRetention time.Duration `yaml:"dead_letter_retention"`
	// RedactDeadLetterPayload — зберігати у raw_fields лише SHA-256+розмір
	// замість самих значень (для чутливих даних). Default false.
	RedactDeadLetterPayload bool `yaml:"redact_dead_letter_payload"`
}

// MappingConfig — де лежить JSON-mapping.
type MappingConfig struct {
	Path string `yaml:"path"`
}

// Load — YAML → *Config + Validate + Defaults.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	// Секрет поза файлом: env XSINK_POSTGRES_DSN має пріоритет над
	// postgres.dsn з config.yaml (щоб не тримати пароль у файлі на диску).
	if v := os.Getenv("XSINK_POSTGRES_DSN"); v != "" {
		c.Postgres.DSN = v
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyDefaults — sensible defaults для полів які не задані.
func (c *Config) applyDefaults() {
	if c.Proxy.DialTimeout == 0 {
		c.Proxy.DialTimeout = 10 * time.Second
	}
	for i := range c.Redis {
		if c.Redis[i].MaxRetries == 0 {
			c.Redis[i].MaxRetries = 3
		}
		if c.Redis[i].PollRatePerSecond == 0 {
			c.Redis[i].PollRatePerSecond = 500
		}
		if c.Redis[i].DialTimeout == 0 {
			c.Redis[i].DialTimeout = 10 * time.Second
		}
	}
	if c.Postgres.MaxOpenConns == 0 {
		c.Postgres.MaxOpenConns = 16
	}
	if c.Postgres.ConnectTimeout == 0 {
		c.Postgres.ConnectTimeout = 10 * time.Second
	}
	if c.Pipeline.Workers == 0 {
		c.Pipeline.Workers = 4
	}
	if c.Pipeline.InsertRetries == 0 {
		c.Pipeline.InsertRetries = 3
	}
	if c.Pipeline.InsertRetryBackoff == 0 {
		c.Pipeline.InsertRetryBackoff = 500 * time.Millisecond
	}
	if c.Pipeline.DeadLetterTable == "" {
		c.Pipeline.DeadLetterTable = "dead_letters"
	}
	if c.Reclaim.Interval == 0 {
		c.Reclaim.Interval = 60 * time.Second
	}
	if c.Reclaim.MinIdleTime == 0 {
		c.Reclaim.MinIdleTime = 5 * time.Minute
	}
	if c.Reclaim.BatchSize == 0 {
		c.Reclaim.BatchSize = 100
	}
	if c.Breaker.MaxConsecutiveFailures == 0 {
		c.Breaker.MaxConsecutiveFailures = 5
	}
	if c.Breaker.OpenTimeout == 0 {
		c.Breaker.OpenTimeout = 30 * time.Second
	}
}

// Validate — обов'язкові поля і адекватність значень.
func (c *Config) Validate() error {
	if c.Proxy.ListPath == "" {
		return fmt.Errorf("config: proxy.list_path is required")
	}
	if len(c.Redis) == 0 {
		return fmt.Errorf("config: redis[] must have at least 1 source")
	}
	names := map[string]struct{}{}
	for i, r := range c.Redis {
		prefix := fmt.Sprintf("config: redis[%d]", i)
		if r.Name == "" {
			return fmt.Errorf("%s.name is required (unique source name)", prefix)
		}
		if _, dup := names[r.Name]; dup {
			return fmt.Errorf("%s.name %q duplicated", prefix, r.Name)
		}
		names[r.Name] = struct{}{}
		if r.Addr == "" {
			return fmt.Errorf("%s.addr is required", prefix)
		}
		if r.TLS.CACertPath == "" || r.TLS.ClientCertPath == "" || r.TLS.ClientKeyPath == "" {
			return fmt.Errorf("%s.tls.{ca_cert_path,client_cert_path,client_key_path} required (mTLS)", prefix)
		}
	}
	if c.Postgres.DSN == "" {
		return fmt.Errorf("config: postgres.dsn is required")
	}
	if c.Mapping.Path == "" {
		return fmt.Errorf("config: mapping.path is required")
	}
	if c.Pipeline.Workers < 1 {
		return fmt.Errorf("config: pipeline.workers must be >= 1")
	}
	return nil
}

// RedisSourceNames — imена усіх джерел (для валідації mapping.stream.source).
func (c *Config) RedisSourceNames() map[string]struct{} {
	out := make(map[string]struct{}, len(c.Redis))
	for _, r := range c.Redis {
		out[r.Name] = struct{}{}
	}
	return out
}
