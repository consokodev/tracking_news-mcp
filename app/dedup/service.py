import psycopg
from dataclasses import dataclass
from datetime import date, timedelta

from app.dedup.hashers import hamming_distance


@dataclass(frozen=True, slots=True)
class DedupDecision:
    is_duplicate: bool
    reason: str | None = None
    canonical_id: str | None = None


def find_duplicate(
    con: psycopg.Connection,
    *,
    published_date: str,
    content_sha256: str,
    simhash64: int,
    simhash_bucket: int,
    max_distance: int = 3,
) -> DedupDecision:
    exact_row = con.execute(
        "SELECT id FROM articles WHERE content_sha256 = %s LIMIT 1",
        (content_sha256,),
    ).fetchone()
    if exact_row:
        return DedupDecision(True, "exact_sha256", str(exact_row["id"]))

    anchor_date = date.fromisoformat(published_date)
    start_date = (anchor_date - timedelta(days=1)).isoformat()
    end_date = (anchor_date + timedelta(days=1)).isoformat()
    candidate_rows = con.execute(
        """
        SELECT id, simhash64
        FROM articles
        WHERE simhash_bucket = %s
          AND published_date BETWEEN %s AND %s
        """,
        (simhash_bucket, start_date, end_date),
    ).fetchall()

    for row in candidate_rows:
        if hamming_distance(simhash64, int(row["simhash64"])) <= max_distance:
            return DedupDecision(True, "near_simhash", str(row["id"]))

    return DedupDecision(False)
