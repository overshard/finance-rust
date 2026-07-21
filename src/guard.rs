//! Persistent, per-endpoint request guard.
//!
//! Every outbound call to a third-party data endpoint (today only Stooq) passes
//! through an `EndpointGuard`. The guard is this project's hard guarantee that a
//! third-party rate limit can never be hit: not by a burst, not by a buggy
//! loop, and not across process restarts.
//!
//! It combines three mechanisms, all backed by the `endpoint_guard` table so
//! they survive restarts and are shared by every job and every process (the
//! server and the `finance seed` subcommand both write the same row):
//!
//!  - **A reactive circuit breaker.** A request that returns an explicit
//!    rate-limit signal (HTTP 429/503) opens the breaker immediately; so does a
//!    streak of ordinary failures. While open, every request is refused. Each
//!    consecutive trip backs off longer (30m, 1h, 2h, 4h ... capped at 24h).
//!    Once the backoff elapses the breaker goes *half-open* and lets exactly
//!    one probe through: it closes on success, re-opens (longer) on failure.
//!
//!  - **A hard per-hour request budget.** At most `HOURLY_BUDGET` requests are
//!    let through per rolling clock hour. When the budget is spent, jobs are
//!    refused until the hour rolls. This caps a runaway loop even when the
//!    upstream never returns an error.
//!
//!  - **Pacing.** Consecutive requests are spaced at least `MIN_GAP` apart;
//!    `acquire` sleeps as needed before granting, so callers no longer pace
//!    themselves.
//!
//! For a Python reader: picture one row of `endpoint_guard` as a small state
//! machine persisted to disk. `acquire()` asks "may I send now?" (and blocks
//! for pacing); `record_success` / `record_failure` feed the outcome back in.
//!
//! Usage in a bulk loop:
//! ```ignore
//! let guard = EndpointGuard::with_budget(pool.clone(), "yahoo", 1000);
//! for ticker in tickers {
//!     match guard.acquire().await? {
//!         Permit::Granted => {}
//!         Permit::Denied(why) => break, // breaker open or budget spent: stop the run
//!     }
//!     match provider.daily(ticker, None).await {
//!         Ok(bars)  => guard.record_success().await?,
//!         Err(err)  => guard.record_failure(&err).await?,
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;

use sqlx::SqlitePool;

use crate::db::now_ms;
use crate::providers::RateLimited;

/// Minimum spacing between two requests to the same endpoint. Matches the
/// anti-spam policy (>= 1.5s per request). `acquire` enforces it.
const MIN_GAP: Duration = Duration::from_millis(1500);

/// Consecutive ordinary failures that trip the breaker while it is closed. An
/// explicit rate-limit signal (429/503) trips it immediately, regardless.
const FAIL_THRESHOLD: i64 = 4;

/// A half-open probe that records no result within this long is treated as
/// abandoned (its process likely crashed mid-probe), so a fresh probe is let
/// through rather than wedging the breaker half-open forever.
const STALE_PROBE_SECS: i64 = 10 * 60;

const HOUR_MS: i64 = 3600 * 1000;

/// One async mutex per endpoint name, shared by every `EndpointGuard`
/// instance in the process (instances are constructed cheaply all over, so
/// the lock cannot live on the struct).
fn endpoint_lock(endpoint: &str) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    let map = LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut m = map.lock().expect("endpoint lock map poisoned");
    m.entry(endpoint.to_string()).or_default().clone()
}

/// The guard's verdict for one request attempt.
pub enum Permit {
    /// Cleared to send. Pacing has already been applied (this call slept as
    /// needed) and the request has been counted against the hourly budget.
    Granted,
    /// The request must not be sent: the circuit breaker is open or the hourly
    /// budget is spent. The caller should stop its run. The string explains
    /// why and is suitable for a log line or a `fetch_log` detail.
    Denied(String),
}

/// A persistent guard over one outbound data endpoint. Cheap to construct
/// (`SqlitePool` is an `Arc` internally); all real state lives in the
/// `endpoint_guard` row, so separate instances for the same endpoint stay
/// consistent.
pub struct EndpointGuard {
    pool: SqlitePool,
    endpoint: String,
    /// This endpoint's hard per-hour request ceiling.
    hourly_budget: i64,
}

/// The subset of an `endpoint_guard` row the guard logic reads back. The table
/// carries more columns (`opened_at`, `last_ok_at`, `last_error`,
/// `hourly_budget`, ...) for the data-health page; they are written here but
/// not read here.
#[derive(sqlx::FromRow)]
struct GuardRow {
    state: String,
    fail_streak: i64,
    trip_count: i64,
    retry_at: Option<i64>,
    hour_start: Option<i64>,
    hour_count: i64,
    last_request_at: Option<i64>,
    updated_at: i64,
}

impl EndpointGuard {
    /// A guard with an explicit per-hour request budget. Each endpoint sets its
    /// own ceiling (e.g. 1000 for Yahoo, 600 for SEC) — see the constants in
    /// `scheduler.rs`.
    pub fn with_budget(pool: SqlitePool, endpoint: &str, hourly_budget: i64) -> Self {
        Self {
            pool,
            endpoint: endpoint.to_string(),
            hourly_budget,
        }
    }

    /// Ensure this endpoint's `endpoint_guard` row exists and its persisted
    /// `hourly_budget` matches this guard. Used at startup to register the
    /// known endpoints, so the data-health page shows each one with its right
    /// budget from boot rather than only after that endpoint's first request.
    pub async fn ensure_registered(&self) -> anyhow::Result<()> {
        self.load(now_ms()).await?;
        Ok(())
    }

    /// Ask permission to send one request.
    ///
    /// On `Permit::Granted` the call has already slept for pacing and counted
    /// the request against the hourly budget; the caller should send the
    /// request straight away. On `Permit::Denied` the caller must not send and
    /// should stop its run (the breaker is open or the budget is spent).
    pub async fn acquire(&self) -> anyhow::Result<Permit> {
        // Serialize the read-decide-sleep-commit sequence per endpoint. It is
        // not atomic against the shared row: two concurrent acquires could
        // both read the same `last_request_at`, sleep identical pacing, and
        // send simultaneously (or both claim the single half-open probe
        // slot). The DB still coordinates across processes (server vs seed);
        // this closes the race between tasks inside one process.
        let lock = endpoint_lock(&self.endpoint);
        let _serialized = lock.lock().await;
        let now = now_ms();
        let g = self.load(now).await?;

        // 1. Hard per-hour budget. Checked first: it holds even when nothing
        //    has failed, so it is the backstop against a runaway loop.
        if g.hour_count >= self.hourly_budget {
            let resets_in = human_secs((g.hour_start.unwrap_or(now) + HOUR_MS - now) / 1000);
            return Ok(Permit::Denied(format!(
                "{} hourly budget spent ({}/{}), resets in {resets_in}",
                self.endpoint, g.hour_count, self.hourly_budget
            )));
        }

        // 2. Circuit breaker. `probing` means this acquire is taking the single
        //    half-open probe slot.
        let probing = match g.state.as_str() {
            "open" => {
                let retry_at = g.retry_at.unwrap_or(now);
                if now < retry_at {
                    return Ok(Permit::Denied(format!(
                        "{} circuit breaker open, retry in {}",
                        self.endpoint,
                        human_secs((retry_at - now) / 1000)
                    )));
                }
                true // backoff elapsed: this caller becomes the half-open probe
            }
            "half_open" => {
                // A probe is in flight. Allow a fresh one only if the previous
                // probe looks abandoned (no result recorded for a long time).
                if now - g.updated_at <= STALE_PROBE_SECS * 1000 {
                    return Ok(Permit::Denied(format!(
                        "{} circuit breaker half-open, probe in flight",
                        self.endpoint
                    )));
                }
                true
            }
            _ => false, // closed
        };

        // 3. Granted. Pace, then commit the bookkeeping.
        if let Some(last) = g.last_request_at {
            let wait = MIN_GAP.as_millis() as i64 - (now - last);
            if wait > 0 {
                tokio::time::sleep(Duration::from_millis(wait as u64)).await;
            }
        }
        let sent = now_ms();
        let new_state = if probing { "half_open" } else { g.state.as_str() };
        sqlx::query(
            "UPDATE endpoint_guard SET \
               state = ?, hour_count = hour_count + 1, \
               last_request_at = ?, updated_at = ? WHERE endpoint = ?",
        )
        .bind(new_state)
        .bind(sent)
        .bind(sent)
        .bind(&self.endpoint)
        .execute(&self.pool)
        .await?;

        Ok(Permit::Granted)
    }

    /// Record that the last `acquire`d request succeeded. Closes the breaker
    /// and clears every failure counter.
    pub async fn record_success(&self) -> anyhow::Result<()> {
        let now = now_ms();
        sqlx::query(
            "UPDATE endpoint_guard SET \
               state = 'closed', fail_streak = 0, trip_count = 0, \
               opened_at = NULL, retry_at = NULL, \
               last_ok_at = ?, updated_at = ? WHERE endpoint = ?",
        )
        .bind(now)
        .bind(now)
        .bind(&self.endpoint)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record that the last `acquire`d request failed.
    ///
    /// The breaker trips (opens) when any of these holds: the error is an
    /// explicit rate-limit signal ([`RateLimited`]); a half-open probe failed;
    /// or the ordinary-failure streak reached `FAIL_THRESHOLD`. Otherwise the
    /// streak is just incremented. A trip backs off exponentially, honouring a
    /// `Retry-After` when it is longer than the computed backoff.
    pub async fn record_failure(&self, err: &anyhow::Error) -> anyhow::Result<()> {
        let now = now_ms();
        let rate_limited = err.downcast_ref::<RateLimited>();
        let g = self.load(now).await?;

        let streak = g.fail_streak + 1;
        let trip = rate_limited.is_some()      // explicit upstream rate-limit signal
            || g.state != "closed"             // a half-open probe failed
            || streak >= FAIL_THRESHOLD;       // too many ordinary failures in a row
        let msg = format!("{err:#}");

        if trip {
            let trip_count = g.trip_count + 1;
            let mut backoff = backoff_secs(trip_count);
            if let Some(ra) = rate_limited.and_then(|r| r.retry_after_secs) {
                backoff = backoff.max(ra);
            }
            let retry_at = now + backoff * 1000;
            sqlx::query(
                "UPDATE endpoint_guard SET \
                   state = 'open', fail_streak = 0, trip_count = ?, \
                   opened_at = ?, retry_at = ?, \
                   last_error = ?, last_error_at = ?, updated_at = ? WHERE endpoint = ?",
            )
            .bind(trip_count)
            .bind(now)
            .bind(retry_at)
            .bind(&msg)
            .bind(now)
            .bind(now)
            .bind(&self.endpoint)
            .execute(&self.pool)
            .await?;
            tracing::warn!(
                "[guard] {} breaker OPEN (trip #{trip_count}), backoff {}: {msg}",
                self.endpoint,
                human_secs(backoff)
            );
        } else {
            sqlx::query(
                "UPDATE endpoint_guard SET \
                   fail_streak = ?, last_error = ?, last_error_at = ?, updated_at = ? \
                 WHERE endpoint = ?",
            )
            .bind(streak)
            .bind(&msg)
            .bind(now)
            .bind(now)
            .bind(&self.endpoint)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Load the guard row, creating a default one on first use and rolling the
    /// per-hour budget window if the clock hour has elapsed. The hour roll is
    /// persisted here (not just held in memory) so a later `hour_count + 1` is
    /// always counting within the right hour.
    async fn load(&self, now: i64) -> anyhow::Result<GuardRow> {
        // Create the row on first use, and keep `hourly_budget` in step with
        // how this guard was constructed — it differs per endpoint (see
        // `with_budget`), and the data-health page reads it straight from here.
        // `updated_at` is left untouched on the correcting update: it tracks
        // state-machine changes, not routine bookkeeping.
        sqlx::query(
            "INSERT INTO endpoint_guard (endpoint, hourly_budget, updated_at) VALUES (?, ?, ?) \
             ON CONFLICT(endpoint) DO UPDATE SET hourly_budget = excluded.hourly_budget \
               WHERE endpoint_guard.hourly_budget <> excluded.hourly_budget",
        )
        .bind(&self.endpoint)
        .bind(self.hourly_budget)
        .bind(now)
        .execute(&self.pool)
        .await?;

        // Roll the budget window. `updated_at` is deliberately left untouched:
        // it tracks state-machine changes, and a half-open staleness check
        // depends on it not being bumped by routine budget bookkeeping.
        sqlx::query(
            "UPDATE endpoint_guard SET hour_start = ?, hour_count = 0 \
             WHERE endpoint = ? AND (hour_start IS NULL OR ? - hour_start >= ?)",
        )
        .bind(now)
        .bind(&self.endpoint)
        .bind(now)
        .bind(HOUR_MS)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query_as::<_, GuardRow>(
            "SELECT state, fail_streak, trip_count, retry_at, hour_start, \
                    hour_count, last_request_at, updated_at \
             FROM endpoint_guard WHERE endpoint = ?",
        )
        .bind(&self.endpoint)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }
}

/// Backoff for the n-th consecutive trip: 30m, 1h, 2h, 4h, ... capped at 24h.
fn backoff_secs(trip_count: i64) -> i64 {
    const BASE: i64 = 30 * 60;
    const CAP: i64 = 24 * 3600;
    // trip_count is >= 1; clamp the shift so `1 << shift` cannot overflow.
    let shift = (trip_count - 1).clamp(0, 16) as u32;
    BASE.saturating_mul(1_i64 << shift).min(CAP)
}

/// A coarse, human-readable duration for log lines and status messages.
fn human_secs(secs: i64) -> String {
    let s = secs.max(0);
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m", s / 60)
    } else {
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    }
}
