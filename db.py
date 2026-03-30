"""
Database layer — supports Supabase (REST API) and SQLite (local fallback).

Priority:
  1. Streamlit secrets: [supabase] url + key
  2. Env vars: SUPABASE_URL + SUPABASE_KEY
  3. Local SQLite (casamilan.db)
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Detect backend ────────────────────────────────────────────────────────────

def _get_supabase_creds() -> tuple[str, str] | tuple[None, None]:
    # 1. Streamlit secrets
    try:
        import streamlit as st
        sb = st.secrets.get("supabase", {})
        if sb.get("url") and sb.get("key"):
            return sb["url"], sb["key"]
    except Exception:
        pass
    # 2. Env vars
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if url and key:
        return url, key
    return None, None


_SB_URL, _SB_KEY = _get_supabase_creds()
_USE_SUPABASE = _SB_URL is not None

if _USE_SUPABASE:
    from supabase import create_client
    _sb = create_client(_SB_URL, _SB_KEY)
    print(f"[db] Using Supabase: {_SB_URL}")
else:
    _SQLITE_PATH = Path(__file__).parent / "casamilan.db"
    print(f"[db] Using SQLite: {_SQLITE_PATH}")


# ── Schema (SQLite only — Supabase tables created via dashboard or migration) ──

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

_SB_SCHEMA_SQL = """
create table if not exists listings (
    id              text primary key,
    source          text not null,
    title           text,
    price           real,
    sqm             real,
    price_per_sqm   real,
    rooms           integer,
    zone            text,
    address         text,
    year_built      integer,
    floor           text,
    energy_class    text,
    url             text,
    description     text,
    first_seen      text not null,
    last_seen       text not null,
    is_new          integer default 1
);

create table if not exists scrape_log (
    id          bigserial primary key,
    source      text not null,
    scraped_at  text not null,
    count       integer,
    status      text
);
"""


def init_db():
    if _USE_SUPABASE:
        # Create tables via Supabase SQL editor (run once)
        try:
            _sb.rpc("exec_sql", {"query": _SB_SCHEMA_SQL}).execute()
        except Exception:
            # Tables may already exist or RPC not available — try direct insert to verify
            pass
    else:
        with sqlite3.connect(_SQLITE_PATH) as conn:
            conn.executescript(_SQLITE_SCHEMA)


# ── Write operations ──────────────────────────────────────────────────────────

def upsert_listing(listing: dict):
    now = datetime.utcnow().isoformat()

    if _USE_SUPABASE:
        row = {
            "id": listing["id"],
            "source": listing["source"],
            "title": listing.get("title"),
            "price": listing.get("price"),
            "sqm": listing.get("sqm"),
            "price_per_sqm": listing.get("price_per_sqm"),
            "rooms": listing.get("rooms"),
            "zone": listing.get("zone"),
            "address": listing.get("address"),
            "year_built": listing.get("year_built"),
            "floor": listing.get("floor"),
            "energy_class": listing.get("energy_class"),
            "url": listing.get("url"),
            "description": listing.get("description"),
            "first_seen": now,
            "last_seen": now,
            "is_new": 1,
        }
        # upsert: insert or update on conflict
        _sb.table("listings").upsert(row, on_conflict="id").execute()
    else:
        with sqlite3.connect(_SQLITE_PATH) as conn:
            conn.row_factory = sqlite3.Row
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
    if _USE_SUPABASE:
        _sb.table("scrape_log").insert({
            "source": source, "scraped_at": now, "count": count, "status": status
        }).execute()
    else:
        with sqlite3.connect(_SQLITE_PATH) as conn:
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
    if _USE_SUPABASE:
        q = _sb.table("listings").select("*")
        if zones:
            q = q.in_("zone", zones)
        if max_price_per_sqm:
            q = q.lte("price_per_sqm", max_price_per_sqm)
        if min_rooms:
            q = q.gte("rooms", min_rooms)
        if max_rooms:
            q = q.lte("rooms", max_rooms)
        if min_year:
            q = q.gte("year_built", min_year)
        if only_new:
            q = q.eq("is_new", 1)
        if source:
            q = q.eq("source", source)
        q = q.order("first_seen", desc=True).limit(1000)
        result = q.execute()
        return result.data or []
    else:
        conditions, params = [], []
        if zones:
            conditions.append(f"zone IN ({','.join('?'*len(zones))})")
            params.extend(zones)
        if max_price_per_sqm:
            conditions.append("price_per_sqm <= ?")
            params.append(max_price_per_sqm)
        if min_rooms:
            conditions.append("rooms >= ?")
            params.append(min_rooms)
        if max_rooms:
            conditions.append("rooms <= ?")
            params.append(max_rooms)
        if min_year:
            conditions.append("(year_built >= ? OR year_built IS NULL)")
            params.append(min_year)
        if only_new:
            conditions.append("is_new = 1")
        if source:
            conditions.append("source = ?")
            params.append(source)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"SELECT * FROM listings {where} ORDER BY first_seen DESC"

        with sqlite3.connect(_SQLITE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(query, params).fetchall()]
