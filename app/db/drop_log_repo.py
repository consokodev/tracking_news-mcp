import psycopg


def log_drop(
    con: psycopg.Connection,
    *,
    run_id: str,
    section_run_id: str | None = None,
    url: str,
    source: str,
    section: str,
    drop_reason: str,
    detail: str | None = None,
) -> None:
    con.execute(
        """
        INSERT INTO drop_log (run_id, section_run_id, url, source, section, drop_reason, detail)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, section_run_id, url, source, section, drop_reason, detail),
    )


def log_drops_batch(
    con: psycopg.Connection,
    drops: list[tuple[str, str | None, str, str, str, str, str | None]],
) -> None:
    if not drops:
        return
    for drop in drops:
        con.execute(
            """
            INSERT INTO drop_log (run_id, section_run_id, url, source, section, drop_reason, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            drop,
        )
