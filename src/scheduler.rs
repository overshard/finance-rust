//! Background job scheduler (demand-only).
//!
//! One long-lived tokio task wakes on a fixed tick. Since the demand-only
//! refocus (2026-06-03) the home dashboard's instruments are the **only**
//! timed network fetching (the active home sweep, a 2026-06-10 user call);
//! everything else is demand-driven. Each tick it:
//!  - broadcasts a market-session change so open pages update their pill (and
//!    re-pushes the dashboard summary, a local DB read, on a session flip);
//!  - runs the demand-driven intraday quote poll — Yahoo quotes for just the
//!    symbols a browser is currently viewing (the stream hub's interest
//!    registry), so nothing is polled when nobody is watching;
//!  - runs the active home sweep when due (every 15 minutes): re-quotes the
//!    dashboard's overview instruments + all watchlist symbols, viewer or not,
//!    so the home page always opens fresh — session-aware and guard-routed;
//!  - prunes aged `intraday_bars` and `fetch_log` rows (~daily, local only).
//!
//! All the old timed sweeps (daily-close, SEC, dividends, fund metadata, NAV,
//! earnings, asset profile, periodic history) were removed in Phase A. The data
//! they fetched is now pulled **on demand** when a symbol's page is viewed and
//! its stored copy is stale — see `backfill_symbol` (the synchronous per-symbol
//! pull the add-symbol route and the on-demand refresh use) and Phase B.
//!
//! The boot seed only reconciles the universe rows from the curated CSV (local,
//! no network); a symbol's history and deep data fill in the first time it is
//! viewed. Outbound calls still pass through the persistent `EndpointGuard`
//! (see `src/guard.rs`), which paces requests and trips on rate limits.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tokio::task::JoinHandle;

use crate::db::now_ms;
use crate::guard::{EndpointGuard, Permit};
use crate::market;
use crate::providers::sec::SecProvider;
use crate::providers::yahoo::YahooProvider;
use crate::providers::{
    self, DividendEvent, Fact, FilingRecord, FundId, FundMetadata,
    FundShape, FundamentalsProvider, HistoryProvider, IntradayBar, OwnershipPerson,
    PortfolioData, Quote, QuoteProvider,
};
use crate::stream::{Hub, QuoteUpdate, StreamEvent};
use crate::{seed, Config};

/// How often the loop wakes to check whether a job is due. The jobs themselves
/// run hours apart; a one-minute tick is plenty responsive and nearly free
/// (two small SELECTs per wake).
const TICK: Duration = Duration::from_secs(60);

/// Minimum seconds between intraday polls of the same viewed symbol. With the
/// 60s tick this yields a ~5-minute per-symbol cadence: the
/// dashboard's watchlist and an open symbol page refresh about every five
/// minutes while watched, not every minute. Just under 5 min so a symbol due at
/// the 5-minute mark isn't skipped to the next tick.
const INTRADAY_MIN_INTERVAL_SECS: i64 = 4 * 60 + 45;

/// Active home-dashboard sweep cadence (a user call, 2026-06-10): the home
/// page's instruments — the market overview, the VIX / volume reads, and every
/// watchlist symbol across all sessions — are re-quoted every 15 minutes even
/// with nobody on the site, so the dashboard always opens fresh instead of
/// waiting on the on-open refresh's round trip to Yahoo. This is the one
/// deliberate exception to the demand-only model; it stays session-aware
/// (off-hours only the ~24h instruments are polled) and guard-routed, so the
/// worst case is a few dozen requests per hour against the 1000/hr budget.
const HOME_SWEEP_INTERVAL_SECS: i64 = 15 * 60;

/// Prune cadence and the two retention windows it enforces.
const PRUNE_INTERVAL_SECS: i64 = 24 * 3600;
const INTRADAY_RETENTION_DAYS: i64 = 14;
const FETCH_LOG_RETENTION_DAYS: i64 = 30;
/// ~13 months: one month past the fin_sid cookie's 12-month lifetime, so only
/// sids that can no longer return are pruned.
const WATCHLIST_STALE_DAYS: i64 = 396;

/// Per-hour request ceiling for the Yahoo endpoint guard. Higher than Stooq's
/// default 200: an intraday tick sweeps every viewed symbol, and a daily-close
/// run touches the whole ~144-symbol universe at once. Still a hard cap that
/// stops a runaway loop well short of anything Yahoo would refuse.
/// `pub(crate)` so the add-symbol route builds its Yahoo guard with the same
/// ceiling (see `routes::symbols`).
pub(crate) const YAHOO_BUDGET: i64 = 1000;

/// Per-hour request ceiling for the SEC endpoint guard. A first-run full sweep
/// is one bulk ticker-map call plus two calls per stock (~220 for the starter
/// universe); 600 clears that in a single budget hour while still capping a
/// runaway loop well short of anything SEC's fair-access policy would refuse.
const SEC_BUDGET: i64 = 600;

/// Company-leadership refresh (Phase 14). Leadership changes slowly, so the
/// roster is rebuilt monthly rather than on the weekly SEC cadence above. Each
/// sweep parses at most this many of a company's most recent ownership filings
/// (one HTTP request each).
///
/// At 10 filings per sweep, a first-time backfill of one company captures the
/// recently-filing officers and board (a Form 3/4/5 from each active director
/// + a handful of officer trades), while the steady-state monthly refresh is
/// tiny — only the filings since `leadership_synced_at` are pulled. A higher
/// chunk eagerly grabs more history but, multiplied by the few-hundred-stock
/// universe, churned through the SEC endpoint's hourly budget during the
/// Phase 14 backfill (markets-closed weekend burn). Smaller chunks spread
/// that initial fill across more sweeps without changing the eventual roster.
const LEADERSHIP_MAX_FILINGS: usize = 10;

/// Spawn the scheduler. The returned handle is normally dropped: dropping it
/// detaches the task, which then runs for the lifetime of the process.
pub fn spawn(pool: SqlitePool, config: Arc<Config>, hub: Arc<Hub>) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!("[scheduler] started");

        if let Err(e) = reset_states_on_boot(&pool).await {
            tracing::warn!("[scheduler] reset states: {e}");
        }
        if let Err(e) = register_endpoints(&pool).await {
            tracing::warn!("[scheduler] register endpoints: {e}");
        }

        // Reconcile the universe rows from the curated CSV (local, no network).
        // No history is fetched here any more — a symbol's data fills in on
        // demand the first time its page is viewed (Phase B).
        if let Err(e) = run_boot_seed(&pool, &config).await {
            tracing::warn!("[scheduler] boot seed: {e:#}");
        }

        // Prune's last-run time is loop-local: a restart simply re-prunes once,
        // which is harmless (local-only DELETEs, no network).
        let mut last_prune: Option<i64> = None;
        // The home sweep's last-run time is loop-local too: a restart sweeps
        // once right away (the per-symbol throttle inside `refresh_quotes`
        // keeps a quick restart from re-hitting Yahoo for fresh quotes).
        let mut last_home_sweep: Option<i64> = None;
        // The session last broadcast, so a transition (e.g. into after-hours)
        // is pushed to connected clients exactly once.
        let mut last_session: Option<market::Session> = None;
        loop {
            // Broadcast a market-session change so open pages update their pill.
            let session = market::session_at(chrono::Utc::now());
            if last_session != Some(session) {
                if let Some(prev) = last_session {
                    tracing::info!(
                        "[scheduler] market {} -> {}",
                        prev.as_str(),
                        session.as_str()
                    );
                }
                hub.publish(StreamEvent::Market {
                    session: session.as_str().to_string(),
                });
                last_session = Some(session);
            }

            // Intraday quotes: demand-driven (only symbols a browser is
            // viewing). Inside a trading session every viewed symbol is
            // polled; outside it, only viewed futures, which trade nearly
            // around the clock. Does no network work when nobody is watching.
            if let Err(e) = run_intraday(&pool, &config, &hub, session).await {
                tracing::warn!("[scheduler] intraday: {e:#}");
            }

            // The active home-dashboard sweep: every 15 minutes, re-quote the
            // home page's instruments whether or not anyone is watching, so
            // the dashboard always opens fresh.
            if let Err(e) =
                run_home_sweep_if_due(&pool, &config, &hub, session, &mut last_home_sweep).await
            {
                tracing::warn!("[scheduler] home sweep: {e:#}");
            }

            if let Err(e) = run_prune_if_due(&pool, &mut last_prune, &hub).await {
                tracing::warn!("[scheduler] prune: {e:#}");
            }
            tokio::time::sleep(TICK).await;
        }
    })
}

/// Register the known data endpoints at startup, so the data-health page lists
/// each one — with its correct hourly budget — from the first boot, rather than
/// only once that endpoint's first request lazily creates its guard row. The
/// ids and budgets mirror how each job below constructs its `EndpointGuard`.
async fn register_endpoints(pool: &SqlitePool) -> anyhow::Result<()> {
    // Stooq was retired 2026-05-30 (Yahoo now serves history too). Drop its
    // stale guard row so it no longer lingers on the data-health page.
    sqlx::query("DELETE FROM endpoint_guard WHERE endpoint = 'stooq'")
        .execute(pool)
        .await?;
    // The demand-only refocus (Phase A) removed every timed sweep. Drop their
    // leftover `data_status` rows so `/health` lists only the jobs that still
    // run (the demand-driven intraday poll, the active home sweep, and the
    // local prune); a prod DB carries rows from the old jobs that would
    // otherwise show as stale.
    sqlx::query("DELETE FROM data_status WHERE job NOT IN ('intraday', 'home', 'prune')")
        .execute(pool)
        .await?;
    EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET)
        .ensure_registered()
        .await?;
    EndpointGuard::with_budget(pool.clone(), "sec", SEC_BUDGET)
        .ensure_registered()
        .await?;
    Ok(())
}

/// Clear any `fetching` state left behind by a crash mid-job. The owning task
/// did not survive the restart, so the row must not stay stuck `fetching`.
async fn reset_states_on_boot(pool: &SqlitePool) -> sqlx::Result<()> {
    sqlx::query("UPDATE data_status SET state = 'idle', updated_at = ? WHERE state = 'fetching'")
        .bind(now_ms())
        .execute(pool)
        .await?;
    Ok(())
}

/// Reconcile the universe rows from the curated CSV on every boot — local only,
/// no network. Upserts every listed symbol and prunes curated rows dropped from
/// the CSV, so a `starter.csv` edit takes effect on deploy without a manual
/// re-seed (user-added `is_seeded = 0` rows are never touched).
///
/// Unlike before the demand-only refocus, this no longer backfills any history:
/// a symbol's deep daily history and SEC data fill in the first time its page is
/// viewed and found stale (Phase B), or via an explicit `make seed` run.
async fn run_boot_seed(pool: &SqlitePool, config: &Config) -> anyhow::Result<()> {
    match seed::sync_universe(pool, config).await {
        Ok(r) => tracing::info!(
            "[scheduler] universe sync: {} symbols, {} pruned",
            r.total,
            r.pruned
        ),
        Err(e) => tracing::warn!("[scheduler] universe sync: {e:#}"),
    }
    Ok(())
}

/// Demand-driven intraday quote refresh.
///
/// Polls Yahoo only for the symbols a browser is currently viewing (the stream
/// hub's interest registry). With nobody watching, `hub.viewed()` is empty and
/// this returns at once having done no network work: the user's hard rule is
/// to poll only what is on screen.
///
/// Which viewed symbols are polled depends on the session. Inside any trading
/// session (pre, regular, post) every viewed symbol is fair game. Outside it,
/// only viewed symbols that trade ~around the clock are polled — index futures,
/// commodities, and crypto (BTC) — while indexes, stocks and ETFs sit frozen
/// until the next session, so polling them off-hours would only re-fetch a flat
/// quote. This is what keeps the dashboard's overview futures/commodity/BTC
/// lines live overnight.
///
/// A clean run is recorded only in `data_status` (plus each `quotes.fetched_at`
/// row); a `fetch_log` row is written only for a notable run, an error or a
/// guard stop, so the minute-cadence job does not bury the log.
async fn run_intraday(
    pool: &SqlitePool,
    config: &Config,
    hub: &Hub,
    session: market::Session,
) -> anyhow::Result<()> {
    let viewed = hub.viewed();
    if viewed.is_empty() {
        return Ok(());
    }
    // Inside a session, poll every viewed symbol; outside it, only the futures.
    let mut targets: Vec<String> = if session.is_open() {
        viewed
    } else {
        // Off-hours, only poll symbols that trade ~24h: index futures (and
        // commodities) plus crypto (BTC). Cash indexes / stocks / ETFs are
        // frozen, so polling them outside the session just burns budget.
        let around_clock: HashSet<String> =
            sqlx::query_scalar("SELECT ticker FROM symbols WHERE kind IN ('future', 'crypto')")
                .fetch_all(pool)
                .await?
                .into_iter()
                .collect();
        viewed
            .into_iter()
            .filter(|t| around_clock.contains(t))
            .collect()
    };
    if targets.is_empty() {
        return Ok(());
    }
    // Throttle to a ~5-minute per-symbol cadence: the loop
    // ticks every 60s, but a symbol quoted within the last few minutes is left
    // alone, so a dashboard left open polls each watchlist symbol about once
    // every five minutes rather than every minute — light on the budget, and
    // plenty "real-time" for delayed data. A symbol never quoted is always
    // eligible. The set is small (only viewed symbols), so the lookup is cheap.
    let throttle_cutoff = now_ms() - INTRADAY_MIN_INTERVAL_SECS * 1000;
    let recent: HashSet<String> = sqlx::query_scalar(
        "SELECT ticker FROM symbols WHERE last_quote_at IS NOT NULL AND last_quote_at >= ?",
    )
    .bind(throttle_cutoff)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();
    targets.retain(|t| !recent.contains(t));
    if targets.is_empty() {
        return Ok(());
    }
    let started = now_ms();
    mark_fetching(pool, "intraday").await?;
    notify_health(hub);

    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    let t0 = Instant::now();

    let mut ok = 0i64;
    let mut errors = 0i64;
    let mut stopped: Option<String> = None;

    for ticker in &targets {
        match guard.acquire().await? {
            Permit::Granted => {}
            Permit::Denied(why) => {
                stopped = Some(why);
                break;
            }
        }
        match yahoo.quote(ticker).await {
            Ok(data) => {
                guard.record_success().await?;
                store_quote(pool, ticker, &data.quote).await?;
                if !data.bars.is_empty() {
                    store_intraday(pool, ticker, &data.bars).await?;
                }
                hub.publish(StreamEvent::Quote(QuoteUpdate::new(
                    ticker.clone(),
                    data.quote.price,
                    data.quote.prev_close,
                    data.quote.market_state.clone(),
                )));
                ok += 1;
            }
            Err(e) => {
                guard.record_failure(&e).await?;
                errors += 1;
                tracing::warn!("[scheduler] intraday {ticker} failed: {e:#}");
            }
        }
    }

    let dur = t0.elapsed().as_millis() as i64;
    if let Some(why) = stopped {
        let detail = format!("stopped early ({why}); {ok} ok, {errors} errors");
        tracing::warn!("[scheduler] intraday: {detail}");
        log_fetch(pool, "intraday", "yahoo", "skipped", Some(&detail), Some(ok), dur, started)
            .await?;
        mark_ok(pool, "intraday", None).await?;
    } else if ok == 0 && errors > 0 {
        let detail = format!("all {errors} quotes failed");
        log_fetch(pool, "intraday", "yahoo", "error", Some(&detail), Some(0), dur, started).await?;
        mark_error(pool, "intraday", &detail, None).await?;
    } else {
        if errors > 0 {
            let detail = format!("{ok} ok, {errors} errors");
            log_fetch(pool, "intraday", "yahoo", "ok", Some(&detail), Some(ok), dur, started)
                .await?;
        }
        mark_ok(pool, "intraday", None).await?;
    }

    notify_health(hub);
    Ok(())
}

/// The active home-dashboard sweep (a user call, 2026-06-10; see
/// `HOME_SWEEP_INTERVAL_SECS`). Every 15 minutes it re-quotes the full
/// dashboard set — the market overview's cash + futures tickers, the VIX and
/// volume-proxy reads, and the union of every session's watchlist — without
/// requiring a viewer, so the home page always opens on current figures.
///
/// Session-aware like `run_intraday`: inside any trading session everything is
/// polled; outside it, only the instruments that trade ~around the clock
/// (futures, crypto), since a frozen stock/ETF/index quote would just burn
/// budget. Each pull rides `refresh_quotes`, so it is guard-routed, throttled
/// per symbol (a quote fresher than ~5 minutes is skipped), and published to
/// the hub so any open page live-ticks too.
async fn run_home_sweep_if_due(
    pool: &SqlitePool,
    config: &Config,
    hub: &Hub,
    session: market::Session,
    last: &mut Option<i64>,
) -> anyhow::Result<()> {
    let now = now_ms();
    if let Some(t) = *last {
        if (now - t) / 1000 < HOME_SWEEP_INTERVAL_SECS {
            return Ok(());
        }
    }
    *last = Some(now);

    let mut targets: Vec<String> = crate::routes::home::dashboard_tickers()
        .into_iter()
        .map(str::to_string)
        .collect();
    let watchlisted: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT ticker FROM watchlist ORDER BY ticker")
            .fetch_all(pool)
            .await?;
    for t in watchlisted {
        if !targets.contains(&t) {
            targets.push(t);
        }
    }
    if !session.is_open() {
        let around_clock: HashSet<String> =
            sqlx::query_scalar("SELECT ticker FROM symbols WHERE kind IN ('future', 'crypto')")
                .fetch_all(pool)
                .await?
                .into_iter()
                .collect();
        targets.retain(|t| around_clock.contains(t));
    }
    if targets.is_empty() {
        return Ok(());
    }

    mark_fetching(pool, "home").await?;
    notify_health(hub);
    let refreshed = refresh_quotes(pool, config, hub, &targets).await;
    if refreshed > 0 {
        tracing::info!(
            "[scheduler] home sweep: {refreshed}/{} refreshed",
            targets.len()
        );
    }
    mark_ok(pool, "home", Some(now + HOME_SWEEP_INTERVAL_SECS * 1000)).await?;
    notify_health(hub);
    Ok(())
}

/// Replace one stock's dividend history with what Yahoo returned. Yahoo serves
/// the canonical, corrected history each call, so a `DELETE` + `INSERT` keeps
/// the table honest if a payout is later retracted or restated. `pub(crate)`:
/// the add-symbol backfill reuses it.
pub(crate) async fn store_dividends(
    pool: &SqlitePool,
    ticker: &str,
    events: &[DividendEvent],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM dividends WHERE ticker = ?")
        .bind(ticker)
        .execute(&mut *tx)
        .await?;
    for e in events {
        sqlx::query(
            "INSERT INTO dividends (ticker, ex_date, amount) VALUES (?, ?, ?) \
             ON CONFLICT(ticker, ex_date) DO UPDATE SET amount = excluded.amount",
        )
        .bind(ticker)
        .bind(&e.ex_date)
        .bind(e.amount)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Stamp a stock as freshly dividend-synced.
async fn mark_dividends_synced(pool: &SqlitePool, ticker: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query("UPDATE symbols SET dividends_synced_at = ?, updated_at = ? WHERE ticker = ?")
        .bind(now)
        .bind(now)
        .bind(ticker)
        .execute(pool)
        .await?;
    Ok(())
}

/// Upsert one ETF's Yahoo `quoteSummary` metadata. Yahoo serves the full
/// current snapshot each call, so the row is replaced wholesale: a field
/// Yahoo no longer carries on a refresh becomes `NULL` here, rather than
/// keeping a stale value. `pub(crate)`: the add-symbol backfill reuses it.
pub(crate) async fn store_fund_metadata(
    pool: &SqlitePool,
    ticker: &str,
    m: &FundMetadata,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO fund_metadata \
           (ticker, expense_ratio, yield_pct, trailing_yield_pct, nav_price, \
            inception_date, category, fund_family, strategy_summary, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(ticker) DO UPDATE SET \
           expense_ratio = excluded.expense_ratio, \
           yield_pct = excluded.yield_pct, \
           trailing_yield_pct = excluded.trailing_yield_pct, \
           nav_price = excluded.nav_price, \
           inception_date = excluded.inception_date, \
           category = excluded.category, \
           fund_family = excluded.fund_family, \
           strategy_summary = excluded.strategy_summary, \
           updated_at = excluded.updated_at",
    )
    .bind(ticker)
    .bind(m.expense_ratio)
    .bind(m.yield_pct)
    .bind(m.trailing_yield_pct)
    .bind(m.nav_price)
    .bind(&m.inception_date)
    .bind(&m.category)
    .bind(&m.fund_family)
    .bind(&m.strategy_summary)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp an ETF as freshly fund-metadata-synced. ETFs only — the column is
/// `NULL` forever on every non-ETF row.
async fn mark_fund_metadata_synced(pool: &SqlitePool, ticker: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "UPDATE symbols SET fund_metadata_synced_at = ?, updated_at = ? WHERE ticker = ?",
    )
    .bind(now)
    .bind(now)
    .bind(ticker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Write one stock's next-earnings date and stamp it as freshly synced.
/// `next` is `None` when Yahoo has no upcoming date (the stored value is
/// cleared so the page falls back to a cadence estimate). `pub(crate)`:
/// the add-symbol backfill reuses it.
pub(crate) async fn store_earnings_next(
    pool: &SqlitePool,
    ticker: &str,
    next: Option<i64>,
) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "UPDATE symbols SET next_earnings_at = ?, earnings_synced_at = ?, \
                            updated_at = ? \
         WHERE ticker = ?",
    )
    .bind(next)
    .bind(now)
    .bind(now)
    .bind(ticker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Write one stock's sector / industry classification and stamp it freshly
/// synced. Either field may be `None` when Yahoo's coverage is partial; an
/// empty / whitespace value is dropped at parse time, not stored. `pub(crate)`:
/// the add-symbol backfill reuses it.
pub(crate) async fn store_asset_profile(
    pool: &SqlitePool,
    ticker: &str,
    profile: &crate::providers::AssetProfile,
) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "UPDATE symbols SET sector = ?, industry = ?, \
                            asset_profile_synced_at = ?, updated_at = ? \
         WHERE ticker = ?",
    )
    .bind(&profile.sector)
    .bind(&profile.industry)
    .bind(now)
    .bind(now)
    .bind(ticker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp a stock as freshly asset-profile-synced without overwriting its
/// stored sector / industry (used on a clean-empty Yahoo response so the
/// sweep does not re-fetch).
async fn mark_asset_profile_synced(pool: &SqlitePool, ticker: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "UPDATE symbols SET asset_profile_synced_at = ?, updated_at = ? WHERE ticker = ?",
    )
    .bind(now)
    .bind(now)
    .bind(ticker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fill in `symbols.cik` for any stock found in the bulk SEC ticker map.
/// Returns how many were newly resolved.
async fn resolve_ciks(pool: &SqlitePool, map: &HashMap<String, String>) -> sqlx::Result<i64> {
    let stocks: Vec<String> =
        sqlx::query_scalar("SELECT ticker FROM symbols WHERE kind = 'stock' AND cik IS NULL")
            .fetch_all(pool)
            .await?;
    let mut resolved = 0;
    for ticker in stocks {
        if let Some(cik) = map.get(&crate::providers::sec::normalize_ticker(&ticker)) {
            let now = now_ms();
            sqlx::query("UPDATE symbols SET cik = ?, updated_at = ? WHERE ticker = ?")
                .bind(cik)
                .bind(now)
                .bind(&ticker)
                .execute(pool)
                .await?;
            resolved += 1;
        }
    }
    Ok(resolved)
}

/// Fill in `symbols.cik` and `symbols.series_id` for any ETF found in the bulk
/// SEC mutual-fund ticker map. Returns how many were newly resolved.
async fn resolve_fund_ciks(
    pool: &SqlitePool,
    map: &HashMap<String, FundId>,
) -> sqlx::Result<i64> {
    let etfs: Vec<String> =
        sqlx::query_scalar("SELECT ticker FROM symbols WHERE kind = 'etf' AND cik IS NULL")
            .fetch_all(pool)
            .await?;
    let mut resolved = 0;
    for ticker in etfs {
        if let Some(id) = map.get(&crate::providers::sec::normalize_ticker(&ticker)) {
            let now = now_ms();
            sqlx::query(
                "UPDATE symbols SET cik = ?, series_id = ?, updated_at = ? WHERE ticker = ?",
            )
            .bind(&id.cik)
            .bind(&id.series_id)
            .bind(now)
            .bind(&ticker)
            .execute(pool)
            .await?;
            resolved += 1;
        }
    }
    Ok(resolved)
}

/// Stamp one of a symbol's SEC sync timestamps to now. `column` is one of a
/// few hardcoded literals (never user input), so interpolating it is safe.
async fn mark_sec_synced(pool: &SqlitePool, ticker: &str, column: &str) -> sqlx::Result<()> {
    let now = now_ms();
    let sql = format!("UPDATE symbols SET {column} = ?, updated_at = ? WHERE ticker = ?");
    sqlx::query(&sql)
        .bind(now)
        .bind(now)
        .bind(ticker)
        .execute(pool)
        .await?;
    Ok(())
}

/// Upsert one company's fundamental facts. Keyed on (ticker, metric, period),
/// so a later filing's restated figure overwrites the prior one.
async fn store_fundamentals(pool: &SqlitePool, ticker: &str, facts: &[Fact]) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    for f in facts {
        sqlx::query(
            "INSERT INTO fundamentals \
               (ticker, metric, period, fiscal_year, fiscal_qtr, period_end, \
                value, unit, form, filed_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(ticker, metric, period) DO UPDATE SET \
               fiscal_year = excluded.fiscal_year, fiscal_qtr = excluded.fiscal_qtr, \
               period_end = excluded.period_end, value = excluded.value, \
               unit = excluded.unit, form = excluded.form, filed_at = excluded.filed_at",
        )
        .bind(ticker)
        .bind(&f.metric)
        .bind(&f.period)
        .bind(f.fiscal_year)
        .bind(f.fiscal_qtr)
        .bind(&f.period_end)
        .bind(f.value)
        .bind(&f.unit)
        .bind(&f.form)
        .bind(&f.filed_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Upsert one company's filing history. Keyed on (ticker, accession).
async fn store_filings(
    pool: &SqlitePool,
    ticker: &str,
    filings: &[FilingRecord],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    for f in filings {
        sqlx::query(
            "INSERT INTO filings \
               (ticker, accession, form, filed_at, period_of_report, \
                primary_doc, url, description, items) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(ticker, accession) DO UPDATE SET \
               form = excluded.form, filed_at = excluded.filed_at, \
               period_of_report = excluded.period_of_report, \
               primary_doc = excluded.primary_doc, url = excluded.url, \
               description = excluded.description, items = excluded.items",
        )
        .bind(ticker)
        .bind(&f.accession)
        .bind(&f.form)
        .bind(&f.filed_at)
        .bind(&f.period_of_report)
        .bind(&f.primary_doc)
        .bind(&f.url)
        .bind(&f.description)
        .bind(&f.items)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Upsert a company's leadership roster from parsed ownership filings (Phase
/// 14). `roster` arrives newest-filing-first, so the first entry seen for a
/// person is their most recent filing and any later duplicate is skipped. The
/// upsert is guarded on `last_seen`, so a stale re-parse never overwrites a
/// person's role with an older filing's; departed insiders simply stop being
/// re-stamped and age out of the symbol page's recency window.
async fn store_leadership(
    pool: &SqlitePool,
    ticker: &str,
    roster: &[(OwnershipPerson, String)],
) -> sqlx::Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut tx = pool.begin().await?;
    for (person, filed_at) in roster {
        if !seen.insert(person.name.as_str()) {
            continue;
        }
        sqlx::query(
            "INSERT INTO leadership \
               (ticker, name, is_director, is_officer, officer_title, last_seen) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(ticker, name) DO UPDATE SET \
               is_director = excluded.is_director, is_officer = excluded.is_officer, \
               officer_title = excluded.officer_title, last_seen = excluded.last_seen \
             WHERE excluded.last_seen >= leadership.last_seen",
        )
        .bind(ticker)
        .bind(&person.name)
        .bind(person.is_director as i64)
        .bind(person.is_officer as i64)
        .bind(&person.officer_title)
        .bind(filed_at)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Upsert one ETF's N-PORT fund profile and replace its stored holdings. The
/// kept holdings are a small top slice, so they are deleted and re-inserted
/// wholesale on each refresh.
async fn store_fund_portfolio(
    pool: &SqlitePool,
    ticker: &str,
    p: &PortfolioData,
) -> anyhow::Result<()> {
    // Asset / sector / geography mixes are each variable-length, so they ride
    // in JSON columns rather than their own tables:
    // [["Equity", 99.8], ["Cash & equivalents", 0.2], ...].
    let asset_mix = serde_json::to_string(&p.asset_mix)?;
    let sector_mix = serde_json::to_string(&p.sector_mix)?;
    let geography_mix = serde_json::to_string(&p.geography_mix)?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO fund_profiles \
           (ticker, kind, net_assets, total_assets, holdings_count, report_date, \
            asset_mix, sector_mix, geography_mix, updated_at) \
         VALUES (?, 'portfolio', ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(ticker) DO UPDATE SET \
           kind = excluded.kind, net_assets = excluded.net_assets, \
           total_assets = excluded.total_assets, holdings_count = excluded.holdings_count, \
           report_date = excluded.report_date, asset_mix = excluded.asset_mix, \
           sector_mix = excluded.sector_mix, geography_mix = excluded.geography_mix, \
           updated_at = excluded.updated_at",
    )
    .bind(ticker)
    .bind(p.net_assets)
    .bind(p.total_assets)
    .bind(p.holdings_count)
    .bind(&p.report_date)
    .bind(&asset_mix)
    .bind(&sector_mix)
    .bind(&geography_mix)
    .bind(now_ms())
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM fund_holdings WHERE ticker = ?")
        .bind(ticker)
        .execute(&mut *tx)
        .await?;
    for (i, h) in p.top_holdings.iter().enumerate() {
        sqlx::query(
            "INSERT INTO fund_holdings (ticker, rank, name, pct, value_usd) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(ticker)
        .bind(i as i64 + 1)
        .bind(&h.name)
        .bind(h.pct)
        .bind(h.value_usd)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Record a physical-commodity grantor trust's profile: just the AUM read from
/// its 10-K. It holds bullion, not a securities portfolio, so there are no
/// holdings and no asset mix.
async fn store_fund_commodity(
    pool: &SqlitePool,
    ticker: &str,
    aum: Option<f64>,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO fund_profiles \
           (ticker, kind, net_assets, total_assets, holdings_count, report_date, \
            asset_mix, updated_at) \
         VALUES (?, 'commodity_trust', ?, NULL, NULL, NULL, NULL, ?) \
         ON CONFLICT(ticker) DO UPDATE SET \
           kind = excluded.kind, net_assets = excluded.net_assets, \
           updated_at = excluded.updated_at",
    )
    .bind(ticker)
    .bind(aum)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp an ETF's fund profile as freshly synced.
async fn mark_fund_synced(pool: &SqlitePool, ticker: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query("UPDATE symbols SET fund_synced_at = ?, updated_at = ? WHERE ticker = ?")
        .bind(now)
        .bind(now)
        .bind(ticker)
        .execute(pool)
        .await?;
    Ok(())
}

/// Upsert one symbol's live quote into `quotes` and refresh the denormalized
/// snapshot columns on `symbols` that the dashboard and SSE seeding read.
/// `pub(crate)`: the add-symbol route stores the quote its Yahoo lookup
/// already returned, rather than spending a second request.
pub(crate) async fn store_quote(pool: &SqlitePool, ticker: &str, q: &Quote) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO quotes \
           (ticker, price, prev_close, open, day_high, day_low, volume, \
            market_state, source, source_time, fetched_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'yahoo', ?, ?) \
         ON CONFLICT(ticker) DO UPDATE SET \
           price = excluded.price, prev_close = excluded.prev_close, \
           open = excluded.open, day_high = excluded.day_high, \
           day_low = excluded.day_low, volume = excluded.volume, \
           market_state = excluded.market_state, source = excluded.source, \
           source_time = excluded.source_time, fetched_at = excluded.fetched_at",
    )
    .bind(ticker)
    .bind(q.price)
    .bind(q.prev_close)
    .bind(q.open)
    .bind(q.day_high)
    .bind(q.day_low)
    .bind(q.volume)
    .bind(&q.market_state)
    .bind(q.source_time)
    .bind(now)
    .execute(pool)
    .await?;

    sqlx::query(
        "UPDATE symbols SET last_price = ?, prev_close = ?, last_quote_at = ?, \
         updated_at = ? WHERE ticker = ?",
    )
    .bind(q.price)
    .bind(q.prev_close)
    .bind(now)
    .bind(now)
    .bind(ticker)
    .execute(pool)
    .await?;
    Ok(())
}

/// Upsert one symbol's intraday bars in a single transaction. The prune job
/// trims `intraday_bars` to a rolling ~14-day window, so nothing here grows
/// without bound. `pub(crate)`: also called by the add-symbol route.
pub(crate) async fn store_intraday(
    pool: &SqlitePool,
    ticker: &str,
    bars: &[IntradayBar],
) -> sqlx::Result<()> {
    let mut tx = pool.begin().await?;
    for b in bars {
        sqlx::query(
            "INSERT INTO intraday_bars (ticker, ts, open, high, low, close, volume) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(ticker, ts) DO UPDATE SET \
               open = excluded.open, high = excluded.high, low = excluded.low, \
               close = excluded.close, volume = excluded.volume",
        )
        .bind(ticker)
        .bind(b.ts)
        .bind(b.open)
        .bind(b.high)
        .bind(b.low)
        .bind(b.close)
        .bind(b.volume)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

// ── synchronous backfill for a freshly-added symbol (Phase 21) ─────────────

/// Run one guarded outbound call: acquire a permit, await `call`, and feed the
/// outcome back to the guard. `None` when the guard denied the request (the
/// breaker is open or the hourly budget is spent) or the permit could not be
/// acquired; `Some` carries whatever the call itself returned.
async fn guarded<T>(
    guard: &EndpointGuard,
    call: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Option<anyhow::Result<T>> {
    match guard.acquire().await {
        Ok(Permit::Granted) => {
            let result = call.await;
            match &result {
                Ok(_) => {
                    let _ = guard.record_success().await;
                }
                Err(e) => {
                    let _ = guard.record_failure(e).await;
                }
            }
            Some(result)
        }
        Ok(Permit::Denied(why)) => {
            tracing::info!("[backfill] guard denied: {why}");
            None
        }
        Err(e) => {
            tracing::warn!("[backfill] guard error: {e:#}");
            None
        }
    }
}

/// Synchronously backfill everything for one just-added symbol: its deep daily
/// history from Stooq and, for a stock or ETF, its SEC data. The add-symbol
/// route (`routes::symbols`) calls this so a user-added symbol's page is
/// complete the moment the add returns, rather than filling in over later
/// scheduler cycles.
///
/// Best-effort and guard-routed: every outbound call passes through the same
/// `EndpointGuard` the background jobs use, and a guard denial or upstream
/// error for any one piece is logged and skipped. The symbol is already added;
/// the normal scheduler sweeps pick up whatever this run missed.
pub(crate) async fn backfill_symbol(pool: &SqlitePool, config: &Config, ticker: &str, kind: &str) {
    backfill_history(pool, config, ticker, kind).await;
    // Phase 26 + Phase 28: dividends covers stock dividends and ETF
    // distributions (same Yahoo event series). Rides the Yahoo guard, so it
    // runs independently of the SEC contact-email gate below.
    if kind == "stock" || kind == "etf" {
        backfill_dividends(pool, config, ticker).await;
    }
    // Phase 28: ETFs get their Yahoo fund_metadata pulled too — expense
    // ratio, yield, NAV, inception, category, family, strategy summary.
    if kind == "etf" {
        backfill_fund_metadata(pool, config, ticker).await;
    }
    // Phase 25: stocks get their Yahoo earnings calendar pulled too — so a
    // user-added stock's symbol page carries the next-expected earnings
    // date the moment the add returns, rather than waiting on the next
    // scheduler cycle.
    if kind == "stock" {
        backfill_earnings_calendar(pool, config, ticker).await;
    }
    // Phase 15: stocks get their Yahoo assetProfile pulled too — so a
    // user-added stock immediately shows up under its sector and industry
    // on /industries and carries the symbol-page header tag, rather than
    // waiting on the next monthly scheduler sweep.
    if kind == "stock" {
        backfill_asset_profile(pool, config, ticker).await;
    }

    // SEC data covers stocks and ETFs; indexes and futures do not file. The
    // whole SEC step is skipped with no contact email configured, as `run_sec`
    // skips itself.
    if config.sec_contact_email.is_empty() {
        return;
    }
    let sec = SecProvider::new(providers::http::build_sec_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), sec.name(), SEC_BUDGET);
    match kind {
        "stock" => backfill_stock_sec(pool, &sec, &guard, ticker).await,
        "etf" => backfill_etf_sec(pool, &sec, &guard, ticker).await,
        _ => {}
    }
}

/// Pull and store a freshly-added ETF's Yahoo `quoteSummary` metadata (Phase
/// 28). Mirrors `backfill_dividends`: same `yahoo` guard, best-effort, no
/// failure propagated to the add-symbol response.
async fn backfill_fund_metadata(pool: &SqlitePool, config: &Config, ticker: &str) {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.fund_metadata(ticker)).await {
        Some(Ok(Some(meta))) => match store_fund_metadata(pool, ticker, &meta).await {
            Ok(()) => {
                let _ = mark_fund_metadata_synced(pool, ticker).await;
                tracing::info!("[backfill] {ticker} <- fund_metadata");
            }
            Err(e) => tracing::warn!("[backfill] store fund_metadata {ticker}: {e:#}"),
        },
        // Yahoo answered cleanly but had no fund modules for this ETF — stamp
        // it checked so the next sweep does not re-fetch the same empty.
        Some(Ok(None)) => {
            let _ = mark_fund_metadata_synced(pool, ticker).await;
        }
        Some(Err(e)) => tracing::warn!("[backfill] fund_metadata {ticker}: {e:#}"),
        None => {}
    }
}

/// Pull and store a freshly-added stock's sector / industry classification
/// (Phase 15). Mirrors `backfill_earnings_calendar`: same `yahoo` guard,
/// best-effort, no failure propagated to the add-symbol response.
async fn backfill_asset_profile(pool: &SqlitePool, config: &Config, ticker: &str) {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.asset_profile(ticker)).await {
        Some(Ok(Some(profile))) => match store_asset_profile(pool, ticker, &profile).await {
            Ok(()) => {
                tracing::info!(
                    "[backfill] {ticker} <- asset_profile ({} / {})",
                    profile.sector.as_deref().unwrap_or("—"),
                    profile.industry.as_deref().unwrap_or("—"),
                );
            }
            Err(e) => tracing::warn!("[backfill] store asset_profile {ticker}: {e:#}"),
        },
        // Yahoo answered cleanly but had no profile for this stock — stamp
        // it so the next sweep does not re-fetch the same empty.
        Some(Ok(None)) => {
            let _ = mark_asset_profile_synced(pool, ticker).await;
        }
        Some(Err(e)) => tracing::warn!("[backfill] asset_profile {ticker}: {e:#}"),
        None => {}
    }
}

/// Pull and store a freshly-added stock's next-expected earnings date
/// (Phase 25). Mirrors `backfill_dividends`: same `yahoo` guard,
/// best-effort, no failure propagated to the add-symbol response.
async fn backfill_earnings_calendar(pool: &SqlitePool, config: &Config, ticker: &str) {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.earnings_calendar(ticker)).await {
        Some(Ok(next)) => match store_earnings_next(pool, ticker, next).await {
            Ok(()) => {
                tracing::info!(
                    "[backfill] {ticker} <- earnings_calendar ({})",
                    next.map(|_| "next set").unwrap_or("no upcoming date"),
                );
            }
            Err(e) => tracing::warn!("[backfill] store earnings {ticker}: {e:#}"),
        },
        Some(Err(e)) => tracing::warn!("[backfill] earnings_calendar {ticker}: {e:#}"),
        None => {}
    }
}

/// Pull and store a freshly-added symbol's dividend / distribution history
/// (Phase 26 + Phase 28: stocks and ETFs both use this path). Routed through
/// the same `yahoo` guard the dividends sweep uses. Best-effort: a guard
/// denial or upstream error leaves the symbol for the next normal sweep.
async fn backfill_dividends(pool: &SqlitePool, config: &Config, ticker: &str) {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.dividends(ticker)).await {
        Some(Ok(events)) => match store_dividends(pool, ticker, &events).await {
            Ok(()) => {
                let _ = mark_dividends_synced(pool, ticker).await;
                tracing::info!("[backfill] {ticker} <- {} dividends", events.len());
            }
            Err(e) => tracing::warn!("[backfill] store dividends {ticker}: {e:#}"),
        },
        Some(Err(e)) => tracing::warn!("[backfill] dividends {ticker}: {e:#}"),
        None => {}
    }
}

/// Pull and store one symbol's deep daily history from Yahoo (one
/// `interval=1d&range=max` call). Used by the add-symbol backfill; all kinds
/// are eligible since Yahoo serves `=F` futures history too.
async fn backfill_history(pool: &SqlitePool, config: &Config, ticker: &str, _kind: &str) {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.daily(ticker, None)).await {
        Some(Ok(bars)) if !bars.is_empty() => match seed::store_daily(pool, ticker, &bars).await {
            Ok(()) => tracing::info!("[backfill] {ticker} <- {} daily bars", bars.len()),
            Err(e) => tracing::warn!("[backfill] store history {ticker}: {e:#}"),
        },
        // A valid but empty response (a historyless symbol): stamp it checked
        // so the history job does not immediately re-fetch it.
        Some(Ok(_)) => {
            let _ = mark_history_checked(pool, ticker).await;
        }
        _ => {}
    }
}

/// Backfill a stock's SEC data: resolve its CIK, then pull fundamentals,
/// filings, and the officer/board roster.
async fn backfill_stock_sec(
    pool: &SqlitePool,
    sec: &SecProvider,
    guard: &EndpointGuard,
    ticker: &str,
) {
    let cik = match resolve_one_cik(pool, sec, guard, ticker, false).await {
        CikResolution::Found(c) => c,
        CikResolution::Absent => {
            // Not in SEC's company map: a non-filer, a foreign issuer, or a
            // delisted/renamed ticker. Stamp the sections checked so the page
            // shows an honest "no data" rather than a perpetual pending note.
            let _ = mark_sec_synced(pool, ticker, "fundamentals_synced_at").await;
            let _ = mark_sec_synced(pool, ticker, "filings_synced_at").await;
            tracing::info!("[backfill] {ticker}: not in SEC company map, marked checked");
            return;
        }
        CikResolution::Unavailable => {
            tracing::info!("[backfill] {ticker}: SEC CIK map unavailable, leaving it for the sec job");
            return;
        }
    };
    if let Some(Ok(facts)) = guarded(guard, sec.facts(&cik)).await {
        match store_fundamentals(pool, ticker, &facts).await {
            Ok(()) => {
                let _ = mark_sec_synced(pool, ticker, "fundamentals_synced_at").await;
            }
            Err(e) => tracing::warn!("[backfill] store facts {ticker}: {e:#}"),
        }
    }
    if let Some(Ok(filings)) = guarded(guard, sec.filings(&cik)).await {
        match store_filings(pool, ticker, &filings).await {
            Ok(()) => {
                let _ = mark_sec_synced(pool, ticker, "filings_synced_at").await;
            }
            Err(e) => tracing::warn!("[backfill] store filings {ticker}: {e:#}"),
        }
    }
    backfill_leadership(pool, sec, guard, ticker, &cik).await;
}

/// Backfill a stock's officer/board roster from a window of its most recent
/// Form 3/4/5 ownership filings, mirroring the leadership sweep in `run_sec`.
async fn backfill_leadership(
    pool: &SqlitePool,
    sec: &SecProvider,
    guard: &EndpointGuard,
    ticker: &str,
    cik: &str,
) {
    let Some(Ok(index)) = guarded(guard, sec.ownership_index(cik)).await else {
        return;
    };
    let to_parse: Vec<_> = index.into_iter().take(LEADERSHIP_MAX_FILINGS).collect();

    let mut roster: Vec<(OwnershipPerson, String)> = Vec::new();
    let mut complete = true;
    for f in &to_parse {
        match guarded(guard, sec.ownership_doc(cik, &f.accession, &f.primary_doc)).await {
            Some(Ok(people)) => {
                for p in people {
                    if p.is_director || p.is_officer {
                        roster.push((p, f.filed_at.clone()));
                    }
                }
            }
            // A parse or network error for one filing: skip it and build the
            // roster from the rest, exactly as `run_sec` does.
            Some(Err(e)) => tracing::warn!("[backfill] ownership_doc {ticker}: {e:#}"),
            // A guard denial leaves the roster only partial: leave it unsynced
            // so the next `sec` cycle finishes it.
            None => complete = false,
        }
    }
    let _ = store_leadership(pool, ticker, &roster).await;
    if complete {
        let _ = mark_sec_synced(pool, ticker, "leadership_synced_at").await;
    }
}

/// Backfill an ETF's fund profile: resolve its fund CIK, pull the filing list,
/// then either the N-PORT portfolio or a commodity trust's AUM.
async fn backfill_etf_sec(pool: &SqlitePool, sec: &SecProvider, guard: &EndpointGuard, ticker: &str) {
    let cik = match resolve_one_cik(pool, sec, guard, ticker, true).await {
        CikResolution::Found(c) => c,
        CikResolution::Absent => {
            // Not in SEC's mutual-fund map: a delisted/renamed fund (e.g. SPCX,
            // which renamed to SPCK) or one that does not file N-PORT. Stamp it
            // checked so the page shows an honest "no fund profile available"
            // rather than a pending note Refresh can never clear.
            let _ = mark_fund_synced(pool, ticker).await;
            tracing::info!("[backfill] {ticker}: not in SEC fund map (delisted/renamed?), marked checked");
            return;
        }
        CikResolution::Unavailable => {
            tracing::info!("[backfill] {ticker}: SEC fund map unavailable, leaving it for the sec job");
            return;
        }
    };
    // `resolve_fund_ciks` stored the series id alongside the CIK.
    let series_id: Option<String> =
        sqlx::query_scalar("SELECT series_id FROM symbols WHERE ticker = ?")
            .bind(ticker)
            .fetch_one(pool)
            .await
            .ok()
            .flatten();
    let id = FundId {
        cik: cik.clone(),
        series_id,
    };

    let Some(Ok(ff)) = guarded(guard, sec.fund_filings(&id)).await else {
        return;
    };
    let _ = store_filings(pool, ticker, &ff.filings).await;
    match ff.shape {
        FundShape::Portfolio { nport_href } => {
            if let Some(Ok(portfolio)) = guarded(guard, sec.fund_portfolio(&nport_href)).await {
                if store_fund_portfolio(pool, ticker, &portfolio).await.is_ok() {
                    let _ = mark_fund_synced(pool, ticker).await;
                }
            }
        }
        FundShape::CommodityTrust => {
            if let Some(Ok(aum)) = guarded(guard, sec.fund_aum(&cik)).await {
                if store_fund_commodity(pool, ticker, aum).await.is_ok() {
                    let _ = mark_fund_synced(pool, ticker).await;
                }
            }
        }
        FundShape::Unknown => {
            let _ = mark_fund_synced(pool, ticker).await;
        }
    }
}

/// Outcome of resolving a symbol's SEC CIK from the bulk ticker map.
enum CikResolution {
    /// A CIK is on file for this symbol (resolved now or on a prior run).
    Found(String),
    /// The bulk map was fetched successfully but does not list this ticker:
    /// the company/fund genuinely has no SEC entry (delisted, renamed, a
    /// foreign issuer, or a non-filer). The caller stamps the affected section
    /// *checked* so the page shows an honest "no data available" instead of a
    /// perpetual "not synced yet, hit Refresh".
    Absent,
    /// The map could not be fetched (guard denied / network error): nothing was
    /// learned, so the caller leaves the section unsynced for a later retry.
    Unavailable,
}

/// Resolve and store a freshly-added symbol's SEC CIK from the bulk ticker map.
/// `fund` selects the mutual-fund map (ETFs) over the operating-company map
/// (stocks). Distinguishes a genuinely-absent ticker from an unreachable map so
/// the caller can render an honest empty state (see [`CikResolution`]).
async fn resolve_one_cik(
    pool: &SqlitePool,
    sec: &SecProvider,
    guard: &EndpointGuard,
    ticker: &str,
    fund: bool,
) -> CikResolution {
    // Whether the bulk map was actually fetched this call (vs guard-denied /
    // errored). A symbol may already carry a CIK from a prior run regardless.
    let fetched = if fund {
        match guarded(guard, sec.fund_ticker_map()).await {
            Some(Ok(map)) => {
                let _ = resolve_fund_ciks(pool, &map).await;
                true
            }
            _ => false,
        }
    } else {
        match guarded(guard, sec.cik_map()).await {
            Some(Ok(map)) => {
                let _ = resolve_ciks(pool, &map).await;
                true
            }
            _ => false,
        }
    };
    let cik: Option<String> =
        sqlx::query_scalar::<_, Option<String>>("SELECT cik FROM symbols WHERE ticker = ?")
            .bind(ticker)
            .fetch_one(pool)
            .await
            .ok()
            .flatten();
    match (cik, fetched) {
        (Some(c), _) => CikResolution::Found(c),
        (None, true) => CikResolution::Absent,
        (None, false) => CikResolution::Unavailable,
    }
}

/// Prune aged rows once per `PRUNE_INTERVAL_SECS`. `intraday_bars` keeps a
/// rolling ~14-day window; `fetch_log` keeps ~30 days. `daily_prices` is
/// permanent and never touched here.
async fn run_prune_if_due(
    pool: &SqlitePool,
    last: &mut Option<i64>,
    hub: &Hub,
) -> anyhow::Result<()> {
    let now = now_ms();
    if let Some(t) = *last {
        if (now - t) / 1000 < PRUNE_INTERVAL_SECS {
            return Ok(());
        }
    }

    let t0 = Instant::now();
    let intraday_cutoff = now - INTRADAY_RETENTION_DAYS * 86_400 * 1000;
    let log_cutoff = now - FETCH_LOG_RETENTION_DAYS * 86_400 * 1000;

    let bars = sqlx::query("DELETE FROM intraday_bars WHERE ts < ?")
        .bind(intraday_cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    let logs = sqlx::query("DELETE FROM fetch_log WHERE started_at < ?")
        .bind(log_cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    // Watchlist sids whose newest row is >13 months old belong to expired
    // cookies: fin_sid lives 12 months from mint and is never refreshed, and
    // every added_at falls within that lifetime, so 13 months of silence means
    // the sid can never come back. Without this, every visitor ever leaves 5
    // starter rows behind forever.
    let wl_cutoff = now - WATCHLIST_STALE_DAYS * 86_400 * 1000;
    let sids = sqlx::query(
        "DELETE FROM watchlist WHERE sid IN \
           (SELECT sid FROM watchlist GROUP BY sid HAVING MAX(added_at) < ?)",
    )
    .bind(wl_cutoff)
    .execute(pool)
    .await?
    .rows_affected();

    let dur = t0.elapsed().as_millis() as i64;
    let detail =
        format!("{bars} intraday bars, {logs} fetch_log rows, {sids} stale watchlist rows");
    tracing::info!("[scheduler] prune: removed {detail}");
    log_fetch(pool, "prune", "-", "ok", Some(&detail), Some((bars + logs) as i64), dur, now).await?;

    *last = Some(now);
    notify_health(hub);
    Ok(())
}

// ── data_status / fetch_log helpers ───────────────────────────────────────

/// Nudge any connected `/health` page to pull a fresh snapshot. Sent whenever a
/// job changes state or appends a `fetch_log` row, so the data-health page
/// tracks the worker in near real time. Carries no payload (see `StreamEvent`).
fn notify_health(hub: &Hub) {
    hub.publish(StreamEvent::Health);
}

/// Move a job's `data_status` row to the `fetching` state.
async fn mark_fetching(pool: &SqlitePool, job: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO data_status (job, state, updated_at) VALUES (?, 'fetching', ?) \
         ON CONFLICT(job) DO UPDATE SET state = 'fetching', updated_at = excluded.updated_at",
    )
    .bind(job)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a job finished-OK, recording when it next falls due (`None` for
/// one-shot jobs like the seed).
async fn mark_ok(pool: &SqlitePool, job: &str, next_run_at: Option<i64>) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO data_status (job, state, last_ok_at, next_run_at, updated_at) \
         VALUES (?, 'ok', ?, ?, ?) \
         ON CONFLICT(job) DO UPDATE SET \
           state = 'ok', last_ok_at = excluded.last_ok_at, \
           next_run_at = excluded.next_run_at, updated_at = excluded.updated_at",
    )
    .bind(job)
    .bind(now)
    .bind(next_run_at)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a job failed, recording the error and when it should be retried.
async fn mark_error(
    pool: &SqlitePool,
    job: &str,
    msg: &str,
    next_run_at: Option<i64>,
) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO data_status (job, state, last_error, last_error_at, next_run_at, updated_at) \
         VALUES (?, 'error', ?, ?, ?, ?) \
         ON CONFLICT(job) DO UPDATE SET \
           state = 'error', last_error = excluded.last_error, \
           last_error_at = excluded.last_error_at, next_run_at = excluded.next_run_at, \
           updated_at = excluded.updated_at",
    )
    .bind(job)
    .bind(msg)
    .bind(now)
    .bind(next_run_at)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp a symbol as history-checked without storing bars: used when the
/// upstream returned a valid response that simply held nothing new.
async fn mark_history_checked(pool: &SqlitePool, ticker: &str) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query("UPDATE symbols SET history_synced_at = ?, updated_at = ? WHERE ticker = ?")
        .bind(now)
        .bind(now)
        .bind(ticker)
        .execute(pool)
        .await?;
    Ok(())
}

/// Append one `fetch_log` row. `ticker` is left NULL: these are bulk jobs, so
/// a run logs once rather than once per symbol.
#[allow(clippy::too_many_arguments)]
async fn log_fetch(
    pool: &SqlitePool,
    job: &str,
    provider: &str,
    status: &str,
    detail: Option<&str>,
    rows: Option<i64>,
    duration_ms: i64,
    started_at: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO fetch_log \
           (job, provider, ticker, status, detail, rows, duration_ms, started_at, finished_at) \
         VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)",
    )
    .bind(job)
    .bind(provider)
    .bind(status)
    .bind(detail)
    .bind(rows)
    .bind(duration_ms)
    .bind(started_at)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

// ───────────────────── on-demand refresh pipeline (Phase B) ─────────────────
//
// Since the demand-only refocus there are no timed sweeps: a viewed symbol's
// data is pulled here, on demand, when its page loads (the fast price steps
// always run; the slow SEC / metadata steps run only when their stored copy is
// stale) or when the user hits Refresh (`force`, which runs everything). The
// symbol-page SSE route (`routes::symbols::refresh_stream`) drives it: it asks
// `refresh_plan` which steps will run, then runs each via `refresh_step`,
// streaming progress to the page's loading bar. Each step reuses the same
// guarded `backfill_*` helpers the add-symbol flow already uses.

/// On-demand staleness windows for the gated (slow) steps. The always-run price
/// steps (quote + history) carry no window — they run on every load.
const REFRESH_SEC_STALE_SECS: i64 = 7 * 24 * 3600;
const REFRESH_LEADERSHIP_STALE_SECS: i64 = 30 * 24 * 3600;
const REFRESH_META_STALE_SECS: i64 = 7 * 24 * 3600;

/// One step in a symbol's refresh, shown on the page's loading bar.
pub(crate) struct RefreshStep {
    /// Stable key the route passes back to `refresh_step`.
    pub key: &'static str,
    /// Human label for the loading bar.
    pub label: &'static str,
    /// Whether this refreshes a server-rendered "deep" section — if any deep
    /// step ran, the page reloads to show it; a load that runs only the (live)
    /// price steps patches the price in place instead.
    pub deep: bool,
}

const fn step(key: &'static str, label: &'static str, deep: bool) -> RefreshStep {
    RefreshStep { key, label, deep }
}

/// Decide which steps a symbol's refresh will run. The two price steps always
/// run; the slow steps are included only when stale (or `force`). An index /
/// future / unknown kind gets just the price steps.
pub(crate) async fn refresh_plan(
    pool: &SqlitePool,
    config: &Config,
    ticker: &str,
    kind: &str,
    force: bool,
) -> Vec<RefreshStep> {
    let mut steps = vec![
        step("quote", "Live quote", false),
        step("history", "Daily history", false),
    ];
    let Some(s) =
        sqlx::query_as::<_, crate::models::SymbolRow>("SELECT * FROM symbols WHERE ticker = ?")
            .bind(ticker)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
    else {
        return steps;
    };
    let now = now_ms();
    let stale = |at: Option<i64>, secs: i64| force || at.map_or(true, |t| now - t > secs * 1000);
    let sec_ok = !config.sec_contact_email.is_empty();
    match kind {
        "stock" => {
            if sec_ok
                && (stale(s.fundamentals_synced_at, REFRESH_SEC_STALE_SECS)
                    || stale(s.filings_synced_at, REFRESH_SEC_STALE_SECS)
                    || stale(s.leadership_synced_at, REFRESH_LEADERSHIP_STALE_SECS))
            {
                steps.push(step("sec", "Fundamentals, filings & leadership", true));
            }
            if stale(s.earnings_synced_at, REFRESH_META_STALE_SECS) {
                steps.push(step("earnings", "Earnings date", true));
            }
            if stale(s.asset_profile_synced_at, REFRESH_META_STALE_SECS) {
                steps.push(step("profile", "Sector & industry", true));
            }
            if stale(s.dividends_synced_at, REFRESH_META_STALE_SECS) {
                steps.push(step("dividends", "Dividends", true));
            }
        }
        "etf" => {
            if sec_ok && stale(s.fund_synced_at, REFRESH_SEC_STALE_SECS) {
                steps.push(step("fund_sec", "Holdings & filings", true));
            }
            if stale(s.fund_metadata_synced_at, REFRESH_META_STALE_SECS) {
                steps.push(step("fund_meta", "Fund details & NAV", true));
            }
            if stale(s.dividends_synced_at, REFRESH_META_STALE_SECS) {
                steps.push(step("dividends", "Distributions", true));
            }
        }
        _ => {}
    }
    steps
}

/// Run one refresh step by key. Returns a short status for the loading bar:
/// "ok" when it ran, "skipped" when the guard denied it (breaker open / budget
/// spent). The backfill helpers are best-effort and swallow their own errors,
/// so the deep steps report "ok" once attempted; the price steps, which this
/// runs inline through the guard, distinguish a guard denial.
pub(crate) async fn refresh_step(
    pool: &SqlitePool,
    config: &Config,
    hub: &Hub,
    ticker: &str,
    kind: &str,
    key: &str,
) -> &'static str {
    let _ = kind;
    match key {
        "quote" => refresh_quote(pool, config, hub, ticker).await,
        "history" => refresh_history_incremental(pool, config, ticker).await,
        "sec" => refresh_sec(pool, config, ticker, false).await,
        "fund_sec" => refresh_sec(pool, config, ticker, true).await,
        "earnings" => {
            backfill_earnings_calendar(pool, config, ticker).await;
            "ok"
        }
        "profile" => {
            backfill_asset_profile(pool, config, ticker).await;
            "ok"
        }
        "dividends" => {
            backfill_dividends(pool, config, ticker).await;
            "ok"
        }
        "fund_meta" => refresh_fund_meta(pool, config, ticker).await,
        _ => "ok",
    }
}

/// Pull fresh quotes for a set of dashboard symbols on demand — the dashboard's
/// on-open refresh. A symbol quoted within the last few
/// minutes is skipped, so a reload (or the add/remove reload) does not re-hit
/// Yahoo, and overnight the gate keeps a re-open from re-polling the same frozen
/// close. Runs regardless of session: opening the dashboard after the close
/// should still confirm the latest (closing) prices rather than show a stale
/// snapshot. Each fetch publishes to the hub so open cards live-tick. Returns how
/// many symbols were actually refreshed.
pub(crate) async fn refresh_quotes(
    pool: &SqlitePool,
    config: &Config,
    hub: &Hub,
    tickers: &[String],
) -> usize {
    let cutoff = now_ms() - INTRADAY_MIN_INTERVAL_SECS * 1000;
    let mut refreshed = 0;
    for t in tickers {
        // Skip a symbol with a quote younger than the throttle window.
        let fresh: Option<i64> = sqlx::query_scalar(
            "SELECT last_quote_at FROM symbols WHERE ticker = ? AND last_quote_at >= ?",
        )
        .bind(t)
        .bind(cutoff)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if fresh.is_some() {
            continue;
        }
        if refresh_quote(pool, config, hub, t).await == "ok" {
            refreshed += 1;
        }
    }
    refreshed
}

/// Pull one live quote + its intraday bars and publish it to the hub so an open
/// page patches its price in place (mirrors `run_intraday`'s per-symbol body).
async fn refresh_quote(pool: &SqlitePool, config: &Config, hub: &Hub, ticker: &str) -> &'static str {
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.quote(ticker)).await {
        Some(Ok(data)) => {
            let _ = store_quote(pool, ticker, &data.quote).await;
            if !data.bars.is_empty() {
                let _ = store_intraday(pool, ticker, &data.bars).await;
            }
            hub.publish(StreamEvent::Quote(QuoteUpdate::new(
                ticker.to_string(),
                data.quote.price,
                data.quote.prev_close,
                data.quote.market_state.clone(),
            )));
            "ok"
        }
        Some(Err(e)) => {
            tracing::warn!("[refresh] quote {ticker}: {e:#}");
            "error"
        }
        None => "skipped",
    }
}

/// Intraday range that covers a whole trading week (Mon–Fri) of 15-minute bars
/// in one request — enough for the end-of-week dashboard view.
const INTRADAY_WEEK_RANGE: &str = "5d";

/// Backfill the whole trading week's 15-minute bars for `tickers` when the
/// stored bars don't already cover the early week. The routine intraday poll
/// only ever stores one day at a time (`range=1d`), so the end-of-week view is
/// missing any day the dashboard wasn't open. One guarded `range=5d` request per
/// still-incomplete symbol fills the gap; symbols whose stored bars already
/// reach the week's start are skipped, so a reload doesn't re-hit Yahoo.
pub(crate) async fn backfill_intraday_week(
    pool: &SqlitePool,
    config: &Config,
    tickers: &[String],
    week_start_ms: i64,
    week_end_ms: i64,
) -> usize {
    // "Already covered": the earliest in-window bar sits within ~36h of the
    // week's open. A normally-polled week starts at Monday's open; a
    // holiday-Monday week at Tuesday's — both inside this margin, so they're not
    // refetched. Only a week missing its first two days (the gap the user sees)
    // falls outside it.
    let covered_before = week_start_ms + 36 * 3_600 * 1000;
    let mut filled = 0;
    for t in tickers {
        let earliest: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(ts) FROM intraday_bars WHERE ticker = ? AND ts >= ? AND ts <= ?",
        )
        .bind(t)
        .bind(week_start_ms)
        .bind(week_end_ms)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .flatten();
        if matches!(earliest, Some(ms) if ms <= covered_before) {
            continue;
        }
        let yahoo = YahooProvider::new(providers::http::build_client(config));
        let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
        match guarded(&guard, yahoo.intraday_window(t, INTRADAY_WEEK_RANGE)).await {
            Some(Ok(data)) if !data.bars.is_empty() => {
                let _ = store_intraday(pool, t, &data.bars).await;
                filled += 1;
            }
            Some(Err(e)) => tracing::warn!("[week] intraday {t}: {e:#}"),
            _ => {}
        }
    }
    filled
}

/// Pull the daily history a viewed symbol is missing: the window since its last
/// stored bar (incremental) when it already has history, else a full
/// `range=max` backfill. Cheaper than the deep re-fetch on a routine load.
async fn refresh_history_incremental(
    pool: &SqlitePool,
    config: &Config,
    ticker: &str,
) -> &'static str {
    let last: Option<String> =
        sqlx::query_scalar("SELECT history_last_date FROM symbols WHERE ticker = ?")
            .bind(ticker)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    match guarded(&guard, yahoo.daily(ticker, last.as_deref())).await {
        Some(Ok(bars)) if !bars.is_empty() => {
            let _ = seed::store_daily(pool, ticker, &bars).await;
            "ok"
        }
        Some(Ok(_)) => {
            let _ = mark_history_checked(pool, ticker).await;
            "ok"
        }
        Some(Err(e)) => {
            tracing::warn!("[refresh] history {ticker}: {e:#}");
            "error"
        }
        None => "skipped",
    }
}

/// Backfill a viewed symbol's SEC data on demand (stock fundamentals/filings/
/// leadership, or an ETF's holdings/filings). Skipped cleanly with no contact
/// email configured, as the old sweep was.
async fn refresh_sec(pool: &SqlitePool, config: &Config, ticker: &str, fund: bool) -> &'static str {
    if config.sec_contact_email.is_empty() {
        return "skipped";
    }
    let sec = SecProvider::new(providers::http::build_sec_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), sec.name(), SEC_BUDGET);
    if fund {
        backfill_etf_sec(pool, &sec, &guard, ticker).await;
    } else {
        backfill_stock_sec(pool, &sec, &guard, ticker).await;
    }
    "ok"
}

/// Refresh an ETF's Yahoo fund metadata and its NAV (the price-vs-NAV premium
/// behind the quality read's tracking factor needs a fresh NAV; see the
/// hard-won lesson). Two cheap `quoteSummary` calls through the
/// Yahoo guard.
async fn refresh_fund_meta(pool: &SqlitePool, config: &Config, ticker: &str) -> &'static str {
    backfill_fund_metadata(pool, config, ticker).await;
    let yahoo = YahooProvider::new(providers::http::build_client(config));
    let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", YAHOO_BUDGET);
    if let Some(Ok(nav)) = guarded(&guard, yahoo.fund_nav(ticker)).await {
        let _ = store_fund_nav(pool, ticker, nav).await;
    }
    "ok"
}

/// Upsert an ETF's freshly-fetched NAV + its sync stamp, touching only the NAV
/// columns so the static fields stay intact. A `None` nav clears any prior NAV
/// (honest: no fresh value to read a premium against). Re-added for the Phase-B
/// on-demand NAV pull after the daily `fund_nav` job was removed in Phase A.
async fn store_fund_nav(pool: &SqlitePool, ticker: &str, nav: Option<f64>) -> sqlx::Result<()> {
    let now = now_ms();
    sqlx::query(
        "INSERT INTO fund_metadata (ticker, nav_price, nav_synced_at, updated_at) \
         VALUES (?, ?, ?, ?) \
         ON CONFLICT(ticker) DO UPDATE SET \
           nav_price = excluded.nav_price, \
           nav_synced_at = excluded.nav_synced_at, \
           updated_at = excluded.updated_at",
    )
    .bind(ticker)
    .bind(nav)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}
