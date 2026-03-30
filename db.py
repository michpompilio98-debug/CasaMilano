"""
Database layer — supports PostgreSQL (Supabase) and SQLite (local fallback).

Priority for connection URL:
  1. Environment variable DATABASE_URL
  2. .streamlit/secrets.toml  [database] url
  3. Local SQLite file (casamilan.db)
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Detect backend ────────────────────────────────────────────────────────────

def _get_db_url() -> str | None:
    # 1. Env var
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    # 2. Streamlit secrets (only available when running inside Streamlit)
    try:
        import streamlit as st
        url = st.secrets.get("database", {}).get("url")
        if url:
            return url
    except Exception:
        pass
    return None


_DB_URL = _get_db_url()
_USE_PG = _DB_URL is not None

if _USE_PG:
    import psycopg2
    import psycopg2.extras
    print(f"[db] Using PostgreSQL: {_DB_URL[:40]}...")
else:
    _SQLITE_PATH = Path(__file__).parent / "casamilan.db"
    print(f"[db] Using SQLite: {_SQLITE_PATH}")


# ── Connection helpers ────────────────────────────────────────────────────────

def _pg_conn():
    conn = psycopg2.connect(_DB_URL)
    conn.autocommit = False
    return conn


def _sqlite_conn():
    conn = sqlite3.connect(_SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Schema ────────────────────────────────────────────────────────────────────

_PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id              TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    title           TEXT,
    price           REAL,
    sqm             REAL,
    price_per_sqm   REAL,
    rooms           INTEGER,
    zone            TEXT,
    address         TEXT,
    year_built      INTEGER,
    floor           TEXT,
    energy_class    TEXT,
    url             TEXT,
    description     TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    is_new          INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id          SERIAL PRIMARY KEY,
    source      TEXT NOT NULL,
    scraped_at  TEXT NOT NULL,
    count       INTEGER,
    status      TEXT
);
"""

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id              TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    title           TEXT,
    price           REAL,
    sqm             REAL,
    price_per_sqm   REAL,
    rooms           INTEGER,
    zone            TEXT,
    address         TEXT,
    year_built      INTEGER,
    floor           TEXT,
    energy_class    TEXT,
    url             TEXT,
    description     TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    is_new          INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    scraped_at  TEXT NOT NULL,
    count       INTEGER,
    status      TEXT
);
"""


def init_db():
    if _USE_PG:
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(_PG_SCHEMA)
            conn.commit()
        finally:
            conn.close()
    else:
        with _sqlite_conn() as conn:
            conn.executescript(_SQLITE_SCHEMA)


# ── Write operations ──────────────────────────────────────────────────────────

def upsert_listing(listing: dict):
    now = datetime.utcnow().isoformat()

    if _USE_PG:
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM listings WHERE id = %s", (listing["id"],))
                exists = cur.fetchone()
                if exists:
                    cur.execute(
                        """UPDATE listings SET price=%s, sqm=%s, price_per_sqm=%s,
                           last_seen=%s, is_new=0 WHERE id=%s""",
                        (listing.get("price"), listing.get("sqm"),
                         listing.get("price_per_sqm"), now, listing["id"]),
                    )
                else:
                    cur.execute(
                        """INSERT INTO listings
                           (id,source,title,price,sqm,price_per_sqm,rooms,zone,
                            address,year_built,floor,energy_class,url,description,
                            first_seen,last_seen,is_new)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1)""",
                        (listing["id"], listing["source"], listing.get("title"),
                         listing.get("price"), listing.get("sqm"), listing.get("price_per_sqm"),
                         listing.get("rooms"), listing.get("zone"), listing.get("address"),
                         listing.get("year_built"), listing.get("floor"), listing.get("energy_class"),
                         listing.get("url"), listing.get("description"), now, now),
                    )
            conn.commit()
        finally:
            conn.close()
    else:
        with _sqlite_conn() as conn:
            existing = conn.execute(
                "SELECT id FROM listings WHERE id = ?", (listing["id"],)
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE listings SET price=:price, sqm=:sqm,
                       price_per_sqm=:price_per_sqm, last_seen=:last_seen, is_new=0
                       WHERE id=:id""",
                    {**listing, "last_seen": now},
                )
            else:
                conn.execute(
                    """INSERT INTO listings
                       (id,source,title,price,sqm,price_per_sqm,rooms,zone,
                        address,year_built,floor,energy_class,url,description,
                        first_seen,last_seen,is_new)
                       VALUES
                       (:id,:source,:title,:price,:sqm,:price_per_sqm,:rooms,:zone,
                        :address,:year_built,:floor,:energy_class,:url,:description,
                        :first_seen,:last_seen,1)""",
                    {**listing, "first_seen": now, "last_seen": now},
                )


def log_scrape(source: str, count: int, status: str):
    now = datetime.utcnow().isoformat()
    if _USE_PG:
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scrape_log (source,scraped_at,count,status) VALUES (%s,%s,%s,%s)",
                    (source, now, count, status),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        with _sqlite_conn() as conn:
            conn.execute(
                "INSERT INTO scrape_log (source,scraped_at,count,status) VALUES (?,?,?,?)",
                (source, now, count, status),
            )


# ── Read operations ───────────────────────────────────────────────────────────

def get_listings(
    zones=None,
    max_price_per_sqm=None,
    min_rooms=None,
    max_rooms=None,
    min_year=None,
    only_new=False,
    source=None,
):
    ph = "%s" if _USE_PG else "?"  # placeholder style

    conditions = []
    params = []

    if zones:
        placeholders = ",".join([ph] * len(zones))
        conditions.append(f"zone IN ({placeholders})")
        params.extend(zones)
    if max_price_per_sqm:
        conditions.append(f"price_per_sqm <= {ph}")
        params.append(max_price_per_sqm)
    if min_rooms:
        conditions.append(f"rooms >= {ph}")
        params.append(min_rooms)
    if max_rooms:
        conditions.append(f"rooms <= {ph}")
        params.append(max_rooms)
    if min_year:
        conditions.append(f"(year_built >= {ph} OR year_built IS NULL)")
        params.append(min_year)
    if only_new:
        conditions.append("is_new = 1")
    if source:
        conditions.append(f"source = {ph}")
        params.append(source)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"SELECT * FROM listings {where} ORDER BY first_seen DESC"

    if _USE_PG:
        conn = _pg_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    else:
        with _sqlite_conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
