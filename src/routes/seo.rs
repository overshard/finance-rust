use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/favicon.ico", get(favicon))
        .route("/robots.txt", get(robots))
        .route("/sitemap.xml", get(sitemap))
}

/// The Paper Ledger mark: a rising figures line over the accountant's double
/// underline, ink-on-paper. Matches the topbar brand in `base.html`.
async fn favicon() -> Response {
    let mut h = HeaderMap::new();
    h.insert(header::CONTENT_TYPE, "image/svg+xml".parse().unwrap());
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<rect width="64" height="64" rx="14" fill="#211f1a"/>
<polyline points="14,38 26,28 37,33 52,16" fill="none" stroke="#f0ece1" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>
<circle cx="52" cy="16" r="4" fill="#f0ece1"/>
<line x1="14" y1="49" x2="50" y2="49" stroke="#f0ece1" stroke-width="4.2" stroke-linecap="round"/>
<line x1="14" y1="56" x2="50" y2="56" stroke="#f0ece1" stroke-width="4.2" stroke-linecap="round"/>
</svg>"##;
    (StatusCode::OK, h, svg).into_response()
}

async fn robots() -> Response {
    let mut h = HeaderMap::new();
    h.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
    // Keep crawlers off the JSON API and the SSE stream: both are
    // valueless to index and /stream holds a connection open per hit.
    (
        StatusCode::OK,
        h,
        "User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /stream\n",
    )
        .into_response()
}

async fn sitemap(State(state): State<AppState>) -> Response {
    let mut h = HeaderMap::new();
    h.insert(header::CONTENT_TYPE, "application/xml".parse().unwrap());
    let base = if state.config.base_url.is_empty() {
        "/".to_string()
    } else {
        format!("{}/", state.config.base_url.trim_end_matches('/'))
    };
    let now = chrono::Utc::now().format("%Y-%m-%d");

    // Static pages plus one entry per known symbol.
    let tickers: Vec<String> = sqlx::query_scalar("SELECT ticker FROM symbols ORDER BY ticker")
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

    let mut urls = String::new();
    for page in ["", "search"] {
        urls.push_str(&format!(
            "  <url><loc>{base}{page}</loc><lastmod>{now}</lastmod></url>\n"
        ));
    }
    for t in tickers {
        let enc = urlencoding::encode(&t);
        urls.push_str(&format!(
            "  <url><loc>{base}s/{enc}</loc><lastmod>{now}</lastmod></url>\n"
        ));
    }

    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n{urls}</urlset>\n"
    );
    (StatusCode::OK, h, body).into_response()
}
