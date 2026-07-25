// proxylist.go — завантаження списку SOCKS5 проксі з файла.
//
// Формат файла: по 1 рядку. Порожні рядки і рядки що починаються з '#' — пропуск.
//   host:port
//   host:port:user:pass
package proxylist

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/proxy"
)

// Proxy — розпарсений SOCKS5 endpoint.
type Proxy struct {
	Host string
	Port string
	User string // може бути порожнім
	Pass string // може бути порожнім
}

// URL — для golang.org/x/net/proxy.SOCKS5 передається як address ("host:port")
// + окремо auth. Тут повертаємо тільки address.
func (p Proxy) Address() string { return p.Host + ":" + p.Port }

// Pool — обертається над списком проксі, дає одну за викликом Pick.
type Pool struct {
	strategy string  // random | round_robin | sticky
	proxies  []Proxy // read-only після Load
	rr       atomic.Uint64
	stickyMu sync.Mutex
	sticky   *Proxy
	stickyAt time.Time

	// dialerCache — кеш proxy.Dialer'ів за address+auth. Створення дилера
	// не безкоштовне (парсинг auth і т.д.); кешуємо щоб не перестворювати
	// на кожен новий TCP-connect.
	dialerCache sync.Map // key: dialerKey → value: proxy.Dialer

	// unhealthy — проксі, позначені непрацездатними після помилки dial.
	// key: address(string) → value: time.Time (коли знову вважати робочим).
	unhealthy sync.Map
}

// dialerKey — унікальний ключ для кешу.
type dialerKey struct {
	addr string
	user string
	pass string
}

// Load — читає файл, парсить, повертає непорожній Pool.
func Load(path, strategy string) (*Pool, error) {
	switch strategy {
	case "random", "round_robin", "sticky":
	default:
		return nil, fmt.Errorf("proxylist: unknown strategy %q", strategy)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("proxylist: open %s: %w", path, err)
	}
	defer f.Close()

	var out []Proxy
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		p, err := parseLine(s)
		if err != nil {
			return nil, fmt.Errorf("proxylist: line %d: %w", line, err)
		}
		out = append(out, p)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("proxylist: scan: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("proxylist: empty (no proxies loaded)")
	}
	return &Pool{strategy: strategy, proxies: out}, nil
}

// parseLine — "host:port" або "host:port:user:pass".
func parseLine(s string) (Proxy, error) {
	parts := strings.Split(s, ":")
	switch len(parts) {
	case 2:
		return Proxy{Host: parts[0], Port: parts[1]}, nil
	case 4:
		return Proxy{Host: parts[0], Port: parts[1], User: parts[2], Pass: parts[3]}, nil
	default:
		return Proxy{}, fmt.Errorf("bad format %q (expected host:port or host:port:user:pass)", s)
	}
}

// Size — кількість проксі у пулі.
func (p *Pool) Size() int { return len(p.proxies) }

// Pick — повертає одну проксі за поточною стратегією.
// sticky: та ж сама на процес (перший виклик обирає rand).
func (p *Pool) Pick() Proxy {
	switch p.strategy {
	case "round_robin":
		i := p.rr.Add(1) - 1
		return p.proxies[int(i%uint64(len(p.proxies)))]
	case "sticky":
		p.stickyMu.Lock()
		defer p.stickyMu.Unlock()
		if p.sticky == nil {
			pr := p.proxies[rand.IntN(len(p.proxies))]
			p.sticky = &pr
			p.stickyAt = time.Now()
		}
		return *p.sticky
	default: // random
		return p.proxies[rand.IntN(len(p.proxies))]
	}
}

// DialerFor — повертає закешований proxy.Dialer для проксі pr. Створення
// відбувається один раз на унікальну (addr, user, pass) — далі беремо з
// sync.Map. baseTimeout використовується для встановлення TCP-з'єднання
// до самого проксі-сервера.
func (p *Pool) DialerFor(pr Proxy, baseTimeout time.Duration) (proxy.Dialer, error) {
	k := dialerKey{addr: pr.Address(), user: pr.User, pass: pr.Pass}
	if v, ok := p.dialerCache.Load(k); ok {
		return v.(proxy.Dialer), nil
	}
	var auth *proxy.Auth
	if pr.User != "" {
		auth = &proxy.Auth{User: pr.User, Password: pr.Pass}
	}
	base := &net.Dialer{Timeout: baseTimeout}
	d, err := proxy.SOCKS5("tcp", pr.Address(), auth, base)
	if err != nil {
		return nil, fmt.Errorf("proxylist: socks5 dialer %s: %w", pr.Address(), err)
	}
	actual, _ := p.dialerCache.LoadOrStore(k, d)
	return actual.(proxy.Dialer), nil
}

// Rotate — примусова зміна поточного sticky (наприклад після мережевої помилки).
// Для random/round_robin — no-op.
func (p *Pool) Rotate() {
	if p.strategy != "sticky" {
		return
	}
	p.stickyMu.Lock()
	defer p.stickyMu.Unlock()
	pr := p.proxies[rand.IntN(len(p.proxies))]
	p.sticky = &pr
	p.stickyAt = time.Now()
}

// MarkUnhealthy — позначає проксі непрацездатним на cooldown (після помилки
// dial/handshake), щоб тимчасово не обирати його знову.
func (p *Pool) MarkUnhealthy(pr Proxy, cooldown time.Duration) {
	p.unhealthy.Store(pr.Address(), time.Now().Add(cooldown))
}

// healthy — чи проксі зараз вважається робочим (cooldown минув або не було
// помилок).
func (p *Pool) healthy(pr Proxy) bool {
	v, ok := p.unhealthy.Load(pr.Address())
	if !ok {
		return true
	}
	return time.Now().After(v.(time.Time))
}

// PickHealthy — як Pick, але намагається оминути проксі в стані unhealthy.
// Робить до Size() спроб; якщо всі позначені unhealthy — повертає звичайний
// Pick (краще спробувати проблемний, ніж зупинити збір). Для sticky між
// спробами викликає Rotate, щоб змінити поточну.
func (p *Pool) PickHealthy() Proxy {
	for i := 0; i < len(p.proxies); i++ {
		pr := p.Pick()
		if p.healthy(pr) {
			return pr
		}
		p.Rotate()
	}
	return p.Pick()
}
