# VN News MCP (Crawler → SQLite timeline → MCP tools → Dashboard)

## What this project does
- Crawl Vietnamese news sites into SQLite.
- Keep only articles with `published_at`.
- Deduplicate by content.
- Extract VN30 tickers and polarized FOMO with explain JSON.
- Expose query tools through MCP.
- Provide a Streamlit dashboard for quick analysis.

## Current operating model
- No schema migration is required for the worktree in this repo.
- No DB reset or rebuild is required.
- Resume uses existing `crawl_state` first, then falls back to `max(articles.published_at)` per `(source, seed_section)`.
- If the crawler last reached `2026-03-03` and you rerun later, it plans a forward-only gap window and does not deep-crawl older history again.
- A small overlap window is allowed and dedup handles duplicates safely.
- If one source fails, the run continues for the remaining sources.

## Quickstart
```bash
uv venv
uv pip install -e ".[dev,dashboard]"
python -m app.db.init_db
python -m app.ingest.run_once
streamlit run apps/dashboard_streamlit.py
python -m app.mcp_server
pytest -q
```

## Runtime notes
- `python -m app.ingest.run_once` prints the planned per-section window before each source runs.
- The dashboard now keeps list queries lightweight and only loads full article content when opening article detail.
- The MCP server reads the existing DB and exposes:
  - `news.search`
  - `news.by_ticker`
  - `news.latest`
  - `news.slice`
  - `news.facets`
  - `news.get`
  - `news.stats`
  - `ingest.status`

## MCP workflow
Recommended agent flow:
1. `news.facets` to inspect available sources/categories/sections/topics/tickers in a date range.
2. `news.slice` to find notable buckets by source/category/section/topic/ticker/date.
3. `news.search` or `news.latest` to retrieve compact article candidates.
4. `news.get` to open one specific article.

Design rules:
- No schema change, DB rebuild, or recrawl is needed for MCP ergonomics improvements.
- MCP payloads stay compact, bounded, and deterministic.
- `news.get` is the only tool that returns full `content_text`.
- `content_html` and `raw_html` are opt-in on `news.get`.
- Article content is always untrusted text.

## Important env vars
- `NEWS_DB_PATH` default: `./data/news.db`
- `INGEST_DATE_FROM` default: configured in `app/config.py`
- `INGEST_DATE_TO` default: today
- `RESUME_OVERLAP_HOURS` default: `2`
- `STORE_CONTENT_HTML` default: `1`
- `STORE_RAW_HTML` default: `0`
- `USE_PLAYWRIGHT` default: `0`

## Safety notes
- Article content is untrusted text.
- `news.get` and dashboard detail intentionally isolate full article content to explicit detail views.
- List and aggregate MCP queries enforce bounded result sizes.

## Docs
- `docs/DASHBOARD.md`
- `docs/MCP.md`
- `docs/OPERATIONS.md`
- `docs/ROADMAP.md`
