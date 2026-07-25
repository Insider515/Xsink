// redisclient.go — Redis-клієнти з mTLS через SOCKS5-proxy пул.
//
// Multi-source: OpenAll конструює *redis.Client для кожного config.RedisConfig
// і повертає map[name]*redis.Client. Один SOCKS5-пул на всі джерела (з кешем
// дилерів усередині Pool — тому створення proxy.Dialer тільки один раз на
// унікальну (proxy_addr,auth)).
//
// Dial-flow одного conn:
//   1. proxy.Dialer (кешований) → TCP до SOCKS5-сервера → CONNECT до Redis addr.
//   2. tls.Client wrap над TCP-conn з mTLS-конфігом.
//   3. Готовий net.Conn → go-redis.
package redisclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"xsink/internal/config"
	"xsink/internal/proxylist"
)

// OpenAll — конструює *redis.Client для кожного config.RedisConfig.
// Ключ map — RedisConfig.Name. Кожен клієнт Ping'ується. Якщо будь-який
// Ping невдалий — усі попередньо відкриті закриваються, повертається помилка.
func OpenAll(ctx context.Context, sources []config.RedisConfig, pool *proxylist.Pool, dialTimeout time.Duration) (map[string]*redis.Client, error) {
	out := make(map[string]*redis.Client, len(sources))
	for _, src := range sources {
		cli, err := Open(ctx, src, pool, dialTimeout)
		if err != nil {
			for _, c := range out {
				_ = c.Close()
			}
			return nil, fmt.Errorf("redisclient: source %q: %w", src.Name, err)
		}
		out[src.Name] = cli
	}
	return out, nil
}

// Open — один Redis-клієнт для одного джерела.
func Open(ctx context.Context, cfg config.RedisConfig, pool *proxylist.Pool, dialTimeout time.Duration) (*redis.Client, error) {
	tlsCfg, err := loadClientTLS(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("redisclient: tls: %w", err)
	}
	if dialTimeout == 0 {
		dialTimeout = 10 * time.Second
	}
	if cfg.DialTimeout > 0 {
		dialTimeout = cfg.DialTimeout
	}
	dialer := func(dctx context.Context, network, addr string) (net.Conn, error) {
		return dialThroughSOCKS5(dctx, network, addr, pool, tlsCfg, cfg.TLS.ServerName, dialTimeout)
	}
	opts := &redis.Options{
		Addr:        cfg.Addr,
		DB:          cfg.DB,
		Password:    cfg.Password,
		MaxRetries:  cfg.MaxRetries,
		Dialer:      dialer,
		DialTimeout: dialTimeout,
	}
	cli := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout+5*time.Second)
	defer cancel()
	if err := cli.Ping(pingCtx).Err(); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("redisclient: ping: %w", err)
	}
	return cli, nil
}

// loadClientTLS — client cert+key, CA. tls.Config з ServerName для SNI+verify.
func loadClientTLS(cfg config.RedisTLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.ClientCertPath, cfg.ClientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("client keypair: %w", err)
	}
	caPEM, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("ca cert: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("ca cert: no PEM blocks")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ServerName:   cfg.ServerName,
	}, nil
}

// unhealthyCooldown — на скільки позначати проксі непрацездатним після збою
// dial/handshake, щоб тимчасово не обирати його знову.
const unhealthyCooldown = 30 * time.Second

// dialThroughSOCKS5 — dial через SOCKS5-проксі з failover: пробує до 3 різних
// проксі. Мертву проксі позначає unhealthy і переходить на наступну, а не
// висить на одній (критично для стратегії sticky). Кешований dialer береться з
// proxylist.Pool.
func dialThroughSOCKS5(ctx context.Context, network, addr string, pool *proxylist.Pool,
	tlsCfg *tls.Config, serverName string, timeout time.Duration) (net.Conn, error) {
	attempts := pool.Size()
	if attempts > 3 {
		attempts = 3
	}
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pr := pool.PickHealthy()
		d, err := pool.DialerFor(pr, timeout)
		if err != nil {
			lastErr = err
			continue
		}
		tcpConn, err := d.Dial(network, addr)
		if err != nil {
			lastErr = fmt.Errorf("socks5 dial (%s → %s): %w", pr.Address(), addr, err)
			pool.MarkUnhealthy(pr, unhealthyCooldown)
			pool.Rotate()
			continue
		}
		dl := time.Now().Add(timeout)
		if dd, ok := ctx.Deadline(); ok && dd.Before(dl) {
			dl = dd
		}
		_ = tcpConn.SetDeadline(dl)
		tlsConn := tls.Client(tcpConn, tlsCfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = tcpConn.Close()
			lastErr = fmt.Errorf("tls handshake (server=%s): %w", serverName, err)
			pool.MarkUnhealthy(pr, unhealthyCooldown)
			pool.Rotate()
			continue
		}
		_ = tcpConn.SetDeadline(time.Time{})
		return tlsConn, nil
	}
	return nil, lastErr
}
