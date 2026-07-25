# Xsink

Універсальний конвеєр **Redis Streams → Postgres** із конфігурованим mapping.
Одна програма — багато різних даних: що і куди писати задається у `mapping.json`,
без зміни коду.

---

## Зміст

- [Що робить](#що-робить)
- [Гарантії надійності](#гарантії-надійності)
- [Файли конфігурації](#файли-конфігурації)
- [mapping.json — архітектура БД](#mappingjson--архітектура-бд)
- [auto_migrate](#auto_migrate)
- [dead_letters](#dead_letters)
- [Секрети](#секрети)
- [HTTP та метрики](#http-та-метрики)
- [Збірка](#збірка)
- [Запуск локально](#запуск-локально-dev)
- [Запуск як systemd-сервіс](#запуск-як-systemd-сервіс-linux)
- [Прапорці CLI](#прапорці-cli)
- [Безпека / що не комітити](#безпека--що-не-комітити)

---

## Що робить

1. Читає SOCKS5 проксі-ліст (файл `host:port` або `host:port:user:pass` по рядку).
2. Відкриває mTLS-з'єднання до одного або кількох Redis-джерел через проксі
   (з failover: мертва проксі позначається unhealthy, dial переходить на іншу).
3. У N паралельних worker'ах на КОЖЕН stream:
   - `XREADGROUP GROUP <cg> <consumer_id> COUNT <batch> BLOCK <ms> STREAMS <stream> >`;
   - конвертує field-и entry за схемою з `mapping.json`;
   - `INSERT INTO <table> ... ON CONFLICT ...` (один multi-row INSERT на batch);
   - при успіху → `XACK` + `XDEL` (видалення з Redis);
   - при **невиправній** помилці (битий формат/порушення обмежень) → у `dead_letters`;
   - при **тимчасовому** збої (мережа, Postgres впав) → лишає в Redis для повтору.
4. Continuous poll з rate-limit (`poll_rate_per_second` на джерело).
5. Фонові процеси: `XAUTOCLAIM`-reclaim stuck-повідомлень, TTL-очистка `dead_letters`,
   збір метрик пулу.
6. Працює як systemd-сервіс: `Type=notify`, `WatchdogSec`, journal-логи, hardening.

**Multi-source:** `config.redis[]` — список Redis-серверів; кожен stream у mapping
прив'язаний до джерела через `source`. Один спільний Postgres-store.

---

## Гарантії надійності

- **At-least-once.** Повідомлення видаляється з Redis (`XACK`+`XDEL`) ЛИШЕ після
  того як воно надійно збережене — вставлене в таблицю або записане в `dead_letters`.
  Дублікати гасяться через `ON CONFLICT` (див. `on_conflict`).
- **Тимчасові vs невиправні помилки.** Тимчасовий збій (обрив мережі, дедлок,
  недоступність Postgres, ще не створена схема) НЕ кидає дані в брак — batch
  лишається в Redis PEL і повторюється. У `dead_letters` потрапляють лише реально
  биті дані (SQLSTATE класів 22/23) та помилки конверсії.
- **Circuit breaker.** Окремий на кожне Redis-джерело і на Postgres: після N
  поспіль збоїв → Open на `open_timeout`, щоб не роздовбувати впалу залежність.
- **Backoff + jitter.** Повтори INSERT ідуть з експоненційним ростом і випадковим
  розкидом — worker'и не повторюють синхронно.
- **Reclaim.** `XAUTOCLAIM` підбирає stuck-повідомлення з PEL мертвого consumer'а
  (краш worker'а) і **переобробляє** їх тим самим шляхом (INSERT + XACK/XDEL).
- **Proxy failover.** Dial пробує до 3 проксі; збійну позначає unhealthy на 30s.
- **Graceful shutdown.** SIGTERM → `sd_notify STOPPING` → скасування ctx → drain →
  вихід. Незбережене лишається в Redis (не втрачається).

---

## Файли конфігурації

| Файл | Призначення | У git |
|---|---|---|
| `config.yaml` | інфраструктура: Redis+mTLS, проксі, Postgres DSN, worker'и, ліміти | **ні** (.gitignore) |
| `mapping.json` | опис stream'ів і мапінг field → column (архітектура БД) | **ні** (.gitignore) |
| `config.yaml.example` | шаблон config.yaml | так |
| `mapping.json.example` | шаблон mapping.json | так |

Реальні `config.yaml` і `mapping.json` у `.gitignore` — містять критичну
інформацію (паролі, IP, SNI, структуру таблиць). У репозиторії — лише `*.example`.

```
cp config.yaml.example config.yaml
cp mapping.json.example mapping.json
# відредагувати під своє середовище
```

### Основні секції config.yaml

- `log` — `level` (DEBUG|INFO|WARN|ERROR), `sink` (`journal`|`stdout`).
- `proxy` — `list_path`, `strategy` (`random`|`round_robin`|`sticky`), `dial_timeout`.
- `redis[]` — список джерел: `name` (унікальне), `addr`, `tls.{ca,client_cert,client_key,server_name}`,
  `password`, `db`, `max_retries`, `poll_rate_per_second`, `dial_timeout`.
- `postgres` — `dsn` (через pgbouncer), `max_open_conns`, `connect_timeout`, `auto_migrate`.
- `pipeline` — `workers`, `insert_retries`, `insert_retry_backoff`, `dead_letter_table`,
  `dead_letter_retention`, `redact_dead_letter_payload`.
- `mapping.path` — шлях до mapping.json.
- `http.addr` — адреса адмін-HTTP (`""` — вимкнути).
- `reclaim` — `enabled`, `interval`, `min_idle_time`, `batch_size`.
- `breaker` — `max_consecutive_failures`, `open_timeout`.

---

## mapping.json — архітектура БД

Це головний файл, де ти описуєш **що читати з Redis і в яку таблицю/колонки писати**.
Корінь — масив `streams`; кожен елемент = один Redis-stream → одна таблиця.

### Поля stream'а

| Поле | Тип | Опис |
|---|---|---|
| `source` | string | ім'я Redis-джерела (`config.redis[].name`). Якщо джерело одне — можна не вказувати. |
| `name` | string | ім'я Redis stream (напр. `queue:orders`). Унікальне. |
| `consumer_group` | string | consumer group для `XREADGROUP` (створюється автоматично). |
| `batch_size` | int | `COUNT` у XREADGROUP (default 100). |
| `block_ms` | int | `BLOCK` у XREADGROUP, мс (default 5000). |
| `table` | string | таблиця Postgres. Унікальна. Ідентифікатор `[a-z_][a-z0-9_]*`. |
| `id_column` | string | колонка, куди писати Redis stream id (напр. `1701-0`). Опційно. |
| `primary_key` | []string | колонки PK — для `ON CONFLICT`. |
| `on_conflict` | string | `do_nothing` \| `update` \| `error` (default `do_nothing`). |
| `fields` | []Field | мапінг redis-field → postgres-column. |

### Поле field

| Ключ | Опис |
|---|---|
| `redis` | ім'я field у `XADD`. |
| `column` | ім'я колонки Postgres (`[a-z_][a-z0-9_]*`). |
| `type` | SQL-тип (див. нижче). |
| `not_null` | (опц.) `NOT NULL` у DDL при CREATE TABLE. |
| `encoding` | (опц., лише `bytea`) `hex` (default) \| `base64`. |

### Підтримувані типи

| type | Postgres | Прийом значення з Redis (рядок) |
|---|---|---|
| `text` | TEXT | як є |
| `uuid` | UUID | як є |
| `integer` | INTEGER | ціле, int32 |
| `bigint` | BIGINT | ціле, int64 |
| `real` | REAL | float32 |
| `double` | DOUBLE PRECISION | float64 |
| `boolean` | BOOLEAN | `true/false/1/0/t/f` |
| `jsonb` | JSONB | валідний JSON |
| `timestamptz` | TIMESTAMPTZ | RFC3339 / RFC3339Nano / unix-секунди |
| `bytea` | BYTEA | hex (default) або base64 (`encoding`) |

Порожній рядок → SQL `NULL` (якщо `not_null=false`). Кожна таблиця отримує
автоматичну колонку `ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`.

### on_conflict

- `do_nothing` — `ON CONFLICT DO NOTHING` (retry-safe, ідемпотентно).
- `update` — `ON CONFLICT (<primary_key>) DO UPDATE SET ...` (upsert). Вимагає `primary_key`.
- `error` — без `ON CONFLICT`; дублікат → SQL-помилка → `dead_letters`.

Приклад — див. [`mapping.json.example`](mapping.json.example).

---

## auto_migrate

`postgres.auto_migrate: true` — при старті програма створює відсутні таблиці й
додає відсутні колонки: `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN
IF NOT EXISTS`. Старі колонки не прибирає.

> **Важливо:** `NOT NULL` застосовується лише при первинному `CREATE TABLE`
> (порожня таблиця). При додаванні колонки в **існуючу** таблицю вона додається
> **nullable** (бо `ALTER ADD COLUMN NOT NULL` падає на непорожній таблиці).
> Якщо потрібен `NOT NULL` на нову колонку існуючої таблиці — робиться окремою
> контрольованою міграцією вручну (backfill + `SET NOT NULL`).

`false` — програма просто пробує INSERT; якщо таблиці/колонки немає, entry
лишається в Redis і повторюється (не втрачається), помилка — в лог.

Подивитись DDL без підключення до БД:
```
make print-ddl
```

---

## dead_letters

Таблиця для entries, які не вдалося записати з **невиправної** причини:

```sql
CREATE TABLE dead_letters (
  id          BIGSERIAL PRIMARY KEY,
  stream      TEXT NOT NULL,
  redis_id    TEXT NOT NULL,
  raw_fields  JSONB NOT NULL,
  error       TEXT NOT NULL,
  failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- Тексти в `error` НЕ містять сирих значень (лише ім'я поля/тип; для Postgres —
  SQLSTATE+Message без Detail).
- `pipeline.dead_letter_retention` — TTL: фонове видалення записів, старших за
  цей термін (`0s` — зберігати безстроково).
- `pipeline.redact_dead_letter_payload: true` — у `raw_fields` зберігається лише
  `{"_redacted":true,"sha256":...,"size":...}` замість самих значень (для
  чутливих даних).

---

## Секрети

Пароль Postgres краще НЕ тримати в `config.yaml`. Env-змінна перекриває `postgres.dsn`:

```
export XSINK_POSTGRES_DSN='postgres://xsink_user:REAL_PASS@127.0.0.1:6432/xsink?sslmode=disable&default_query_exec_mode=simple_protocol'
```

Під systemd — через `EnvironmentFile=-/etc/xsink/xsink.env` (chmod 600):
```
XSINK_POSTGRES_DSN=postgres://xsink_user:REAL_PASS@127.0.0.1:6432/xsink?sslmode=disable&default_query_exec_mode=simple_protocol
```

---

## HTTP та метрики

Адмін-HTTP (`http.addr`, за замовчуванням `127.0.0.1:9090`):

- `GET /healthz` — процес живий (не залежить від залежностей).
- `GET /readyz` — 200 лише коли pipeline готовий + ping усіх Redis + Postgres.
- `GET /metrics` — Prometheus.

Ключові метрики:

| Метрика | Сенс |
|---|---|
| `xsink_messages_read_total` | прочитано з XREADGROUP |
| `xsink_messages_inserted_total` | успішно вставлено |
| `xsink_messages_dead_letter_total` | у dead_letters (reason=convert\|insert) |
| `xsink_messages_deferred_total` | відкладено в Redis (reason=transient\|pg_open) |
| `xsink_messages_reclaimed_total` | підібрано XAUTOCLAIM |
| `xsink_dead_letters_purged_total` | видалено по TTL |
| `xsink_pending_count` | довжина PEL (XPENDING) |
| `xsink_stream_length` | XLEN (backlog) |
| `xsink_breaker_state` | 0=Closed 1=HalfOpen 2=Open |
| `xsink_insert_duration_seconds` / `xsink_ack_duration_seconds` | латентність |
| `xsink_postgres_pool_in_use` / `_idle` | пул pgx |

Алерти варто ставити на різкий ріст `dead_letter`, `deferred` і `stream_length`.

---

## Збірка

Залежності: Go 1.25+. Без CGO.

```
make build         # bin/xsink (під поточну ОС)
make build-linux   # bin/linux/xsink (крос-компіляція для прод-деплою)
make print-ddl     # надрукувати DDL з mapping без підключення до БД
make test          # go test ./...
make tidy          # go mod tidy
make clean         # видалити bin/
```

---

## Запуск локально (dev)

```
cp config.yaml.example config.yaml     # заповнити Redis addr, mTLS-шляхи, DSN
cp mapping.json.example mapping.json    # описати свої stream'и/таблиці
# створити keys/ з CA+client cert/key і proxies.txt зі SOCKS5

./bin/xsink --config config.yaml
```

Для локального dev зручно `log.sink: stdout`.

---

## Запуск як systemd-сервіс (Linux)

```
sudo cp deploy/systemd/xsink.service /etc/systemd/system/
sudo cp bin/linux/xsink /usr/local/bin/xsink
sudo mkdir -p /etc/xsink/keys
sudo cp config.yaml mapping.json /etc/xsink/
sudo cp keys/*.crt keys/*.key /etc/xsink/keys/
sudo touch /etc/xsink/proxies.txt   # заповнити SOCKS5
# (опц.) секрет DSN:
echo 'XSINK_POSTGRES_DSN=postgres://...' | sudo tee /etc/xsink/xsink.env
sudo chmod 600 /etc/xsink/xsink.env
sudo useradd --system --no-create-home --shell /usr/sbin/nologin xsink
sudo chown -R xsink:xsink /etc/xsink
sudo systemctl daemon-reload
sudo systemctl enable --now xsink.service
sudo journalctl -u xsink -f
```

Юніт делегує systemd: перезапуск (`Restart=on-failure`), живість
(`Type=notify` + `WatchdogSec=60s`), захист від шторму рестартів
(`StartLimitIntervalSec=300`/`StartLimitBurst=10`), вікно на drain
(`TimeoutStopSec=120s`), ліміт дескрипторів (`LimitNOFILE=65536`), journal-логи,
порядок старту, сервісний користувач і повний sandbox/hardening.

**Ліміти пам'яті/CPU** (`MemoryMax`/`CPUQuota`) свідомо НЕ задані в universal-юніті
(залежать від конкретного заліза). За потреби — пер-хостовий drop-in:
```
# /etc/systemd/system/xsink.service.d/limits.conf
[Service]
MemoryMax=2G
CPUQuota=200%
```

---

## Прапорці CLI

- `--config <path>` — YAML-конфіг (default `config.yaml`).
- `--print-ddl` — надрукувати `CREATE TABLE`/`ALTER TABLE` з mapping і вийти.
- `--version` — версія.

---

## Безпека / що не комітити

`.gitignore` виключає з git: `config.yaml`, `mapping.json` (структура таблиць),
`keys/` та `*.crt`/`*.key`/`*.pem`, `proxies.txt`, `*.env`, `bin/`, `data/`,
бекапи (`*.tar.gz`). У репозиторії лишаються лише `*.example`-шаблони.

Додатково:
- mTLS до Redis (client cert + CA verify + SNI).
- SQL-ідентифікатори перевіряються (`[a-z_][a-z0-9_]*`), значення — через
  параметри pgx (захист від інʼєкцій).
- Пароль БД — через `XSINK_POSTGRES_DSN`, не у файлі.
- Тексти помилок і `dead_letters.error` — без сирих значень; опційна редакція payload.
