# Finance

> **Archived and no longer maintained.**
> This is the Rust implementation, kept as a read-only snapshot. It was renamed from
> `finance` to `finance-rust` when development moved on, and it receives no further
> updates, fixes, or security patches. Use it as a reference, not as running software.


A self-hosted, real-timeish market watcher for stocks, ETFs, indexes, and futures: live charts, key stats, fundamentals, and SEC filings. One axum binary with sqlx + SQLite, minijinja templates, and a Vite frontend.

Single-operator, no auth, no accounts. It is for *watching* the market, not tracking holdings: there is no portfolio, no cost basis, no money in it at all.

## Features

- Curated universe of ~150 stocks, ETFs, indexes, and commodity/index futures, extendable from the Search page
- Deep daily OHLCV history (decades) plus 15-minute intraday bars and live quotes
- Symbol pages with candlestick charts, SMA 50/200 + EMA 21 overlays, an RSI pane, a volume histogram, and a drag-to-measure tool
- Skimmable key stats: the day's range, the 52-week range, and volume vs average, all drawn as range meters rather than a flat card grid
- SEC fundamentals: nine graded ratios with plain-English readings, an annual/quarterly financials table, and a recent-filings list
- An opinionated home dashboard: index + commodity sparkline cards over the day's biggest movers
- Live prices over Server-Sent Events, polled only for the symbols actually being viewed, and only during market hours
- A `/health` page that lays the background machinery open: every job, every endpoint guard, and a tail of the fetch log
- A persistent per-endpoint guard (circuit breaker + hourly budget + request pacing) so an upstream rate limit can never be hit
- Single-binary deploy via `git push server master`

## Data sources

All free, no account, no key:

| Source | Used for | Auth |
|---|---|---|
| Yahoo Finance | Deep daily history + live quotes + 15-minute intraday bars | None (a browser User-Agent) |
| SEC EDGAR | Fundamentals (XBRL) + filing history | None (a contact email, `SEC_CONTACT_EMAIL`, in the User-Agent) |

Everything fetched is cached in SQLite; the network is touched only for increments. Yahoo's chart endpoint serves a symbol's entire daily history in one call, so the per-symbol backfill runs once; thereafter the daily-close snapshot appends each day's bar and intraday quotes are polled only for watched symbols during market hours. P/E and dividend yield are computed from SEC data plus the latest price, never stored.

## System dependencies

Local dev needs these on your `PATH`:

| Tool | Why | Version |
|---|---|---|
| `rustc` / `cargo` | Build the axum binary | 2021 edition, current stable (1.70+) |
| `bun` | Frontend deps + Vite build | 1.x |
| `make` | Run the dev/build targets | any |
| A C toolchain + OpenSSL headers | Linked at build time on Linux | `build-essential pkg-config libssl-dev` (Debian/Ubuntu), `musl-dev pkgconfig openssl-dev` (Alpine) |

The Docker build (see `Dockerfile`) reproduces this on `rust:alpine` + `alpine:3.23`. If you only care about Docker, you do not need any of the above on the host.

## Quickstart

```sh
cp samplefiles/env.sample .env
# edit .env: set SEC_CONTACT_EMAIL (and BASE_URL for prod)
make
```

`make` (alias `make run`) installs frontend deps if needed, then runs Vite watch and `cargo run` concurrently on port 8000. Visit http://localhost:8000.

On first boot the scheduler seeds the curated universe and backfills its deep daily history from Yahoo (resumable, paced, guarded). No API key is needed; Yahoo serves history, live quotes, and intraday bars from the same endpoint.

## Configuration

All config comes from `.env` (loaded via `dotenvy`). The full set:

| Variable | Required | Purpose |
|---|---|---|
| `SEC_CONTACT_EMAIL` | for fundamentals | Appended to the User-Agent on SEC requests so SEC can identify the caller. Empty disables the SEC job |
| `BASE_URL` | yes for prod | Absolute origin used in the sitemap and og tags. No trailing slash |
| `PORT` | no (default `8000`) | HTTP listen port |
| `FINANCE_DATA_DIR` | no (default `./data`) | Where `db.sqlite3` lives. Production sets this to `/data` |
| `FINANCE_ROOT` | no (default `.`) | Override the project root (where `templates/`, `dist/`, `migrations/`, `universe/` are read from) |
| `FINANCE_USER_AGENT` | no | Browser-like User-Agent sent on every outbound data request |
| `FINANCE_QUOTE_PROVIDER` | no (default `yahoo`) | Which `QuoteProvider` impl to use for live data |
| `FINANCE_TITLE` | no (default `Finance`) | Title shown in the header and `<title>` |

## Make targets

| Target | What it does |
|---|---|
| `make run` (default) | Vite watch + `cargo run` on port 8000 |
| `make build` | Vite assets + release binary (`target/release/finance`) |
| `make start` | Run the release binary (after `make build`) |
| `make seed` | Re-run the universe seed (curated symbols + bulk daily history). Idempotent |
| `make push` | `git push` to every configured remote |
| `make clean` | Remove build output, frontend deps, and the local `data/` dir |

There are no tests or linters configured.

## Deploy

Production runs on Docker. The standard flow is `git push server master` to a remote whose post-receive hook runs `docker compose up --build --detach`. Sample files in `samplefiles/`:

- `env.sample`: the `.env` shown above
- `Caddyfile.sample`: reverse proxy with TLS
- `post-receive.sample`: the git hook

Data persists to `/srv/data/finance/` on the host (mounted into the container at `/data`).

## Stack

- **Backend:** axum 0.8, sqlx 0.8 against SQLite (WAL), single binary
- **Templates:** minijinja 2 with a Jinja2-faithful HTML formatter
- **Frontend:** Vite 6, SCSS, lightweight-charts; Source Serif 4 / Inter / JetBrains Mono, self-hosted via `@fontsource`
- **Scheduler:** one long-lived tokio loop running market-hours-aware background jobs
- **Real-time:** a `tokio::sync::broadcast` hub feeding a `/stream` SSE endpoint
