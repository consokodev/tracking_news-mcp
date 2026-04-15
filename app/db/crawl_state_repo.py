import psycopg
from datetime import UTC, datetime


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def get_crawl_state_last_published_at(
    con: psycopg.Connection,
    *,
    source: str,
    section: str,
) -> str | None:
    row = con.execute(
        """
        SELECT last_published_at
        FROM crawl_state
        WHERE source = %s AND section = %s
        LIMIT 1
        """,
        (source, section),
    ).fetchone()
    if row is None:
        return None
    return row["last_published_at"]


def upsert_crawl_state(
    con: psycopg.Connection,
    *,
    source: str,
    section: str,
    status: str,
    error: str | None = None,
    last_published_at: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO crawl_state (source, section, last_published_at, last_run_at, status, error)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(source, section) DO UPDATE SET
            last_published_at = CASE
                WHEN excluded.last_published_at IS NULL THEN crawl_state.last_published_at
                WHEN crawl_state.last_published_at IS NULL THEN excluded.last_published_at
                WHEN excluded.last_published_at > crawl_state.last_published_at THEN excluded.last_published_at
                ELSE crawl_state.last_published_at
            END,
            last_run_at = excluded.last_run_at,
            status = excluded.status,
            error = excluded.error
        """,
        (source, section, last_published_at, _now_iso(), status, error),
    )
    con.commit()
