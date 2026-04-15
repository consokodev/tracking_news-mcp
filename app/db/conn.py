import os

import psycopg
from psycopg.rows import dict_row

from app.config import DATABASE_URL


def connect(dsn: str | None = None) -> psycopg.Connection:
    resolved_dsn = dsn or os.getenv("DATABASE_URL", DATABASE_URL)
    con = psycopg.connect(resolved_dsn, row_factory=dict_row, autocommit=False)
    return con
