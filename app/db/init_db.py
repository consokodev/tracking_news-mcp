import psycopg

from app.config import DATABASE_URL

DDL = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "unaccent";

CREATE TABLE IF NOT EXISTS articles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  category TEXT,
  seed_section TEXT,
  topic_label TEXT,
  published_at TEXT NOT NULL,
  published_date TEXT NOT NULL,
  content_text TEXT NOT NULL,
  content_html TEXT,
  raw_html TEXT,
  tickers_json TEXT,
  fomo_score REAL NOT NULL,
  fomo_explain_json TEXT,
  content_sha256 TEXT NOT NULL UNIQUE,
  simhash64 BIGINT NOT NULL,
  simhash_bucket INTEGER NOT NULL,
  search_vector TSVECTOR,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_articles_published_at
  ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_source_published_at
  ON articles(source, published_at);
CREATE INDEX IF NOT EXISTS idx_articles_category_published_at
  ON articles(category, published_at);
CREATE INDEX IF NOT EXISTS idx_articles_simhash_bucket_date
  ON articles(simhash_bucket, published_date);
CREATE INDEX IF NOT EXISTS idx_articles_topic_label_published_date
  ON articles(topic_label, published_date);
CREATE INDEX IF NOT EXISTS idx_articles_seed_section_published_date
  ON articles(seed_section, published_date);
CREATE INDEX IF NOT EXISTS idx_articles_search_vector
  ON articles USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_articles_created_at
  ON articles(created_at);

CREATE TABLE IF NOT EXISTS crawl_state (
  source TEXT NOT NULL,
  section TEXT NOT NULL,
  last_published_at TEXT,
  last_run_at TEXT,
  status TEXT,
  error TEXT,
  PRIMARY KEY (source, section)
);

CREATE TABLE IF NOT EXISTS ingest_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at TEXT,
  finished_at TEXT,
  mode TEXT,
  inserted_count INTEGER DEFAULT 0,
  dropped_no_date_count INTEGER DEFAULT 0,
  dropped_irrelevant_count INTEGER DEFAULT 0,
  dropped_out_of_window_count INTEGER DEFAULT 0,
  dedup_dropped_count INTEGER DEFAULT 0,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_created_at
  ON ingest_runs(created_at);

CREATE TABLE IF NOT EXISTS ingest_section_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES ingest_runs(id),
  source TEXT NOT NULL,
  section TEXT NOT NULL,
  section_url TEXT,
  pages_scanned INTEGER DEFAULT 0,
  discovered_raw INTEGER DEFAULT 0,
  discovered_unique INTEGER DEFAULT 0,
  processed_urls INTEGER DEFAULT 0,
  inserted_count INTEGER DEFAULT 0,
  dropped_no_date_count INTEGER DEFAULT 0,
  dropped_irrelevant_count INTEGER DEFAULT 0,
  dropped_out_of_window_count INTEGER DEFAULT 0,
  dedup_dropped_count INTEGER DEFAULT 0,
  failed_count INTEGER DEFAULT 0,
  latest_published_at TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingest_section_runs_run_id
  ON ingest_section_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_ingest_section_runs_source_section
  ON ingest_section_runs(source, section, created_at);

CREATE TABLE IF NOT EXISTS cafef_timelinelist_raw (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  zone_id TEXT NOT NULL,
  page_number INTEGER NOT NULL,
  page_url TEXT NOT NULL,
  item_rank INTEGER NOT NULL,
  article_id TEXT,
  article_url TEXT NOT NULL,
  title TEXT,
  published_at_raw TEXT,
  summary_text TEXT,
  image_url TEXT,
  raw_item_html TEXT,
  collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(zone_id, page_number, item_rank, article_url)
);

CREATE INDEX IF NOT EXISTS idx_cafef_timelinelist_raw_zone_page
  ON cafef_timelinelist_raw(zone_id, page_number, item_rank);
CREATE INDEX IF NOT EXISTS idx_cafef_timelinelist_raw_article_url
  ON cafef_timelinelist_raw(article_url);

CREATE TABLE IF NOT EXISTS drop_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES ingest_runs(id),
  section_run_id UUID REFERENCES ingest_section_runs(id),
  url TEXT NOT NULL,
  source TEXT NOT NULL,
  section TEXT NOT NULL,
  drop_reason TEXT NOT NULL,
  detail TEXT,
  dropped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_drop_log_run_id
  ON drop_log(run_id);
CREATE INDEX IF NOT EXISTS idx_drop_log_source_section
  ON drop_log(source, section, dropped_at);
CREATE INDEX IF NOT EXISTS idx_drop_log_reason
  ON drop_log(drop_reason);
"""

SEARCH_VECTOR_TRIGGER = """
CREATE OR REPLACE FUNCTION articles_search_vector_update() RETURNS trigger AS $$
BEGIN
  NEW.search_vector :=
    to_tsvector('simple', unaccent(coalesce(NEW.title, ''))) ||
    to_tsvector('simple', unaccent(coalesce(NEW.content_text, '')));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trig_articles_search_vector'
  ) THEN
    CREATE TRIGGER trig_articles_search_vector
      BEFORE INSERT OR UPDATE ON articles
      FOR EACH ROW
      EXECUTE FUNCTION articles_search_vector_update();
  END IF;
END;
$$;
"""


def init_db(dsn: str | None = None) -> None:
    resolved_dsn = dsn or DATABASE_URL
    con = psycopg.connect(resolved_dsn, autocommit=True)
    try:
        con.execute(DDL)
        con.execute(SEARCH_VECTOR_TRIGGER)
    finally:
        con.close()


if __name__ == "__main__":
    init_db()
    print(f"Initialized PostgreSQL DB via: {DATABASE_URL}")
