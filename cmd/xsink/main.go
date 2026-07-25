// main.go — точка входу xsink.
//
// Multi-source: config.redis[] — список Redis-джерел; для кожного окремий
// *redis.Client, спільний SOCKS5-пул, спільний Postgres-store.
//
// Життєвий цикл:
//   1. Config.Load + Validate.
//   2. Logger.
//   3. Proxylist.Load.
//   4. Redisclient.OpenAll (mTLS через SOCKS5, Ping всі source'и).
//   5. Storage.Open (pgxpool через pgbouncer, Ping).
//   6. Mapping.Load + ValidateAgainstSources + optional EnsureSchema.
//   7. HTTP admin (/healthz /readyz /metrics) + pool metrics collector.
//   8. Reclaim loop (XAUTOCLAIM) + pending observer.
//   9. sd_notify READY=1 + readyz OK.
//   10. Pipeline.Run (блокуюча).
//   11. SIGINT/SIGTERM → sd_notify STOPPING → cancel ctx → drain → close.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"

	"xsink/internal/config"
	"xsink/internal/httpapi"
	"xsink/internal/logging"
	"xsink/internal/mapping"
	"xsink/internal/metrics"
	"xsink/internal/pipeline"
	"xsink/internal/proxylist"
	"xsink/internal/reclaim"
	"xsink/internal/redisclient"
	"xsink/internal/storage"
)

// main — вхідна точка бінарника.
func main() {
	cfgPath := flag.String("config", "config.yaml", "path to YAML config")
	showVersion := flag.Bool("version", false, "print version and exit")
	printDDL := flag.Bool("print-ddl", false, "print DDL from mapping and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("xsink dev")
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "xsink: config:", err)
		os.Exit(1)
	}
	log := logging.New(cfg.Log.Level, cfg.Log.Sink)

	if *printDDL {
		m, err := mapping.Load(cfg.Mapping.Path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "xsink: mapping:", err)
			os.Exit(1)
		}
		if err := m.ValidateAgainstSources(cfg.RedisSourceNames()); err != nil {
			fmt.Fprintln(os.Stderr, "xsink: mapping/sources:", err)
			os.Exit(1)
		}
		for _, s := range m.Streams {
			fmt.Printf("-- %s (source=%s)\n", s.Table, s.Source)
			fmt.Println(s.CreateTableSQL() + ";")
			for _, f := range s.Fields {
				fmt.Println(s.AddColumnSQL(f) + ";")
			}
			fmt.Println()
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutdown_signal")
		_, _ = daemon.SdNotify(false, daemon.SdNotifyStopping)
		cancel()
	}()

	pool, err := proxylist.Load(cfg.Proxy.ListPath, cfg.Proxy.Strategy)
	if err != nil {
		log.Error("proxylist_load_failed", "err", err.Error())
		os.Exit(1)
	}
	log.Info("proxylist_loaded", "count", pool.Size(), "strategy", cfg.Proxy.Strategy)

	clients, err := redisclient.OpenAll(ctx, cfg.Redis, pool, cfg.Proxy.DialTimeout)
	if err != nil {
		log.Error("redis_open_failed", "err", err.Error())
		os.Exit(1)
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()
	names := make([]string, 0, len(clients))
	for n := range clients {
		names = append(names, n)
	}
	log.Info("redis_connected", "sources", names)

	store, err := storage.Open(ctx, cfg.Postgres, log)
	if err != nil {
		log.Error("postgres_open_failed", "err", err.Error())
		os.Exit(1)
	}
	defer store.Close()
	log.Info("postgres_connected")

	m, err := mapping.Load(cfg.Mapping.Path)
	if err != nil {
		log.Error("mapping_load_failed", "err", err.Error())
		os.Exit(1)
	}
	if err := m.ValidateAgainstSources(cfg.RedisSourceNames()); err != nil {
		log.Error("mapping_sources_invalid", "err", err.Error())
		os.Exit(1)
	}
	log.Info("mapping_loaded", "streams", len(m.Streams))

	if cfg.Postgres.AutoMigrate {
		ddlCtx, ddlCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := store.EnsureSchema(ddlCtx, m, cfg.Pipeline.DeadLetterTable); err != nil {
			ddlCancel()
			log.Error("ensure_schema_failed", "err", err.Error())
			os.Exit(1)
		}
		ddlCancel()
		log.Info("schema_ensured")
	} else {
		log.Info("auto_migrate_disabled",
			"note", "INSERT-помилки при відсутніх таблицях/колонках → dead_letters")
		ddlCtx, ddlCancel := context.WithTimeout(ctx, 10*time.Second)
		_ = store.EnsureSchema(ddlCtx, &mapping.Mapping{}, cfg.Pipeline.DeadLetterTable)
		ddlCancel()
	}

	http := httpapi.New(cfg.HTTP.Addr, clients, store.Pool, log)
	http.Start()
	defer http.Shutdown(context.Background())

	go poolMetricsLoop(ctx, store, 5*time.Second)

	if cfg.Pipeline.DeadLetterRetention > 0 {
		go deadLetterPurgeLoop(ctx, store, cfg.Pipeline.DeadLetterTable, cfg.Pipeline.DeadLetterRetention, log)
		log.Info("dead_letter_retention_enabled", "retention", cfg.Pipeline.DeadLetterRetention.String())
	}

	pl := pipeline.New(cfg, m, clients, store, log)

	var bgWg sync.WaitGroup
	bgWg.Add(2)
	go func() { defer bgWg.Done(); reclaim.Loop(ctx, clients, m.Streams, cfg.Reclaim, log, pl.ProcessClaimed) }()
	go func() { defer bgWg.Done(); reclaim.PendingObserver(ctx, clients, m.Streams, 15*time.Second, log) }()

	if sent, err := daemon.SdNotify(false, daemon.SdNotifyReady); err != nil {
		log.Warn("sd_notify_error", "err", err.Error())
	} else if sent {
		log.Info("sd_notify_ready")
	}
	http.SetReady(true)

	go watchdogPinger(ctx)

	if err := pl.Run(ctx); err != nil {
		log.Error("pipeline_error", "err", err.Error())
		os.Exit(1)
	}
	http.SetReady(false)
	bgWg.Wait()
	log.Info("shutdown_complete")
}

// poolMetricsLoop — оновлення gauge'ів по пулу pgxpool.
func poolMetricsLoop(ctx context.Context, s *storage.Store, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		stat := s.Pool.Stat()
		metrics.PostgresPoolInUse.Set(float64(stat.AcquiredConns()))
		metrics.PostgresPoolIdle.Set(float64(stat.IdleConns()))
	}
}

// deadLetterPurgeLoop — фонове видалення записів dead_letters, старших за
// retention (TTL). Інтервал опитування = retention/4 у межах [1m, 1h].
func deadLetterPurgeLoop(ctx context.Context, s *storage.Store, table string, retention time.Duration, log *slog.Logger) {
	interval := retention / 4
	if interval > time.Hour {
		interval = time.Hour
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cutoff := time.Now().Add(-retention)
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		n, err := s.PurgeDeadLetters(pctx, table, cutoff)
		cancel()
		if err != nil {
			log.Error("dead_letter_purge_error", "err", err.Error())
			continue
		}
		if n > 0 {
			metrics.DeadLettersPurged.Add(float64(n))
			log.Info("dead_letter_purged", "count", n, "older_than", retention.String())
		}
	}
}

// watchdogPinger — sd_notify WATCHDOG=1 кожні WatchdogSec/2. Якщо systemd
// не задав WATCHDOG_USEC — no-op.
func watchdogPinger(ctx context.Context) {
	period, err := daemon.SdWatchdogEnabled(false)
	if err != nil || period <= 0 {
		return
	}
	tick := period / 2
	if tick <= 0 {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = daemon.SdNotify(false, daemon.SdNotifyWatchdog)
		}
	}
}
