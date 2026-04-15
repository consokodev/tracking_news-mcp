import psycopg
from dataclasses import dataclass
from datetime import UTC, datetime

from app.sources import SectionDiscoveryStats


@dataclass(slots=True)
class IngestRunCounts:
    inserted_count: int = 0
    dropped_no_date_count: int = 0
    dropped_irrelevant_count: int = 0
    dropped_out_of_window_count: int = 0
    dedup_dropped_count: int = 0


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def start_ingest_run(con: psycopg.Connection, *, mode: str = "manual") -> str:
    row = con.execute(
        "INSERT INTO ingest_runs (started_at, mode) VALUES (%s, %s) RETURNING id",
        (_now_iso(), mode),
    ).fetchone()
    con.commit()
    return str(row["id"])


def finish_ingest_run(
    con: psycopg.Connection,
    run_id: str,
    counts: IngestRunCounts,
    *,
    error: str | None = None,
) -> None:
    con.execute(
        """
        UPDATE ingest_runs
        SET finished_at = %s,
            inserted_count = %s,
            dropped_no_date_count = %s,
            dropped_irrelevant_count = %s,
            dropped_out_of_window_count = %s,
            dedup_dropped_count = %s,
            error = %s
        WHERE id = %s
        """,
        (
            _now_iso(),
            counts.inserted_count,
            counts.dropped_no_date_count,
            counts.dropped_irrelevant_count,
            counts.dropped_out_of_window_count,
            counts.dedup_dropped_count,
            error,
            run_id,
        ),
    )
    con.commit()


def insert_ingest_section_runs(
    con: psycopg.Connection,
    run_id: str,
    source: str,
    section_stats: list[SectionDiscoveryStats],
) -> None:
    if not section_stats:
        return
    for item in section_stats:
        con.execute(
            """
            INSERT INTO ingest_section_runs (
                run_id,
                source,
                section,
                section_url,
                pages_scanned,
                discovered_raw,
                discovered_unique,
                processed_urls,
                inserted_count,
                dropped_no_date_count,
                dropped_irrelevant_count,
                dropped_out_of_window_count,
                dedup_dropped_count,
                failed_count,
                latest_published_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                source,
                item.section_name,
                item.section_url,
                item.pages_scanned,
                item.discovered_urls,
                item.unique_urls,
                item.processed_urls,
                item.inserted_count,
                item.dropped_no_date_count,
                item.dropped_irrelevant_count,
                item.dropped_out_of_window_count,
                item.dedup_dropped_count,
                item.failed_count,
                item.latest_published_at,
            ),
        )
    con.commit()
