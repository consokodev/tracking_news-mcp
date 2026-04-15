import os

from app.config import (
    CAFEF_ONLY_ARTICLE_RATE_LIMIT_SECONDS,
    DATABASE_URL,
    INGEST_DATE_FROM,
    INGEST_DATE_TO,
)
from app.db.conn import connect
from app.db.ingest_runs_repo import (
    IngestRunCounts,
    finish_ingest_run,
    insert_ingest_section_runs,
    start_ingest_run,
)
from app.db.init_db import init_db
from app.extract.http_client import build_client
from app.ingest.pipeline import CafeFRebuildPipeline

RESET_TABLES = (
    "drop_log",
    "ingest_section_runs",
    "ingest_runs",
    "articles",
    "crawl_state",
    "cafef_timelinelist_raw",
)


def reset_db_in_place() -> None:
    with connect() as con:
        for table_name in RESET_TABLES:
            con.execute(f"DELETE FROM {table_name}")
        con.commit()


def _print_rebuild_summary() -> None:
    with connect() as con:
        rows = con.execute(
            """
            SELECT
              section,
              pages_scanned,
              processed_urls,
              inserted_count,
              dropped_no_date_count,
              dropped_irrelevant_count,
              dropped_out_of_window_count,
              dedup_dropped_count,
              failed_count,
              latest_published_at
            FROM ingest_section_runs
            WHERE source = 'cafef'
            ORDER BY created_at DESC
            LIMIT 8
            """
        ).fetchall()
        print(
            "cafef rebuild completed:",
            f"date_from={INGEST_DATE_FROM}",
            f"date_to={INGEST_DATE_TO}",
        )
        for row in reversed(rows):
            print(
                f"[cafef:{row['section']}]",
                f"pages_scanned={row['pages_scanned']}",
                f"processed={row['processed_urls']}",
                f"inserted={row['inserted_count']}",
                f"dropped_no_date={row['dropped_no_date_count']}",
                f"dropped_irrelevant={row['dropped_irrelevant_count']}",
                f"dropped_out_of_window={row['dropped_out_of_window_count']}",
                f"dedup_dropped={row['dedup_dropped_count']}",
                f"failed={row['failed_count']}",
                f"latest_published_at={row['latest_published_at']}",
            )


def main() -> None:
    os.environ["CAFEF_ONLY_MODE"] = "1"
    os.environ["STORE_RAW_HTML"] = "0"
    os.environ["STORE_CONTENT_HTML"] = "0"

    init_db()
    reset_db_in_place()
    init_db()

    counts = IngestRunCounts()
    with connect() as con, build_client() as client:
        run_id = start_ingest_run(con, mode="rebuild_cafef")
        try:
            result = CafeFRebuildPipeline(
                client=client,
                article_rate_limit_seconds=CAFEF_ONLY_ARTICLE_RATE_LIMIT_SECONDS,
                run_id=run_id,
            ).run(con)
            counts = result.counts
            insert_ingest_section_runs(con, run_id, "cafef", result.section_stats)
            finish_ingest_run(con, run_id, counts)
        except Exception as exc:
            finish_ingest_run(con, run_id, counts, error=str(exc))
            raise

    _print_rebuild_summary()


if __name__ == "__main__":
    main()
