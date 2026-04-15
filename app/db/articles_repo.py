import json
from dataclasses import dataclass

import psycopg

from app.dedup.service import find_duplicate


@dataclass(frozen=True, slots=True)
class ArticleRecord:
    title: str
    url: str
    source: str
    category: str | None
    seed_section: str | None
    topic_label: str | None
    published_at: str
    published_date: str
    content_text: str
    content_html: str | None
    raw_html: str | None
    tickers: list[str]
    fomo_score: float
    fomo_explain_json: str
    content_sha256: str
    simhash64: int
    simhash_bucket: int


@dataclass(frozen=True, slots=True)
class InsertResult:
    inserted: bool
    reason: str | None = None
    article_id: str | None = None


def insert_article(con: psycopg.Connection, article: ArticleRecord) -> InsertResult:
    row = con.execute(
        "SELECT id FROM articles WHERE url = %s LIMIT 1", (article.url,)
    ).fetchone()
    if row:
        return InsertResult(False, "duplicate_url", str(row["id"]))

    dedup = find_duplicate(
        con,
        published_date=article.published_date,
        content_sha256=article.content_sha256,
        simhash64=article.simhash64,
        simhash_bucket=article.simhash_bucket,
    )
    if dedup.is_duplicate:
        return InsertResult(False, dedup.reason, dedup.canonical_id)

    row = con.execute(
        """
        INSERT INTO articles (
            title,
            url,
            source,
            category,
            seed_section,
            topic_label,
            published_at,
            published_date,
            content_text,
            content_html,
            raw_html,
            tickers_json,
            fomo_score,
            fomo_explain_json,
            content_sha256,
            simhash64,
            simhash_bucket
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            article.title,
            article.url,
            article.source,
            article.category,
            article.seed_section,
            article.topic_label,
            article.published_at,
            article.published_date,
            article.content_text,
            article.content_html,
            article.raw_html,
            json.dumps(article.tickers, ensure_ascii=False),
            article.fomo_score,
            article.fomo_explain_json,
            article.content_sha256,
            article.simhash64,
            article.simhash_bucket,
        ),
    ).fetchone()
    con.commit()
    return InsertResult(True, article_id=str(row["id"]))
