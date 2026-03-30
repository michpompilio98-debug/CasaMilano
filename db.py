import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "casamilan.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id          TEXT PRIMARY KEY,
                source      TEXT NOT NULL,
                title       TEXT,
                price       REAL,
                sqm         REAL,
                price_per_sqm REAL,
                rooms       INTEGER,
                zone        TEXT,
                address     TEXT,
                year_built  INTEGER,
                floor       TEXT,
                energy_class TEXT,
                url         TEXT,
                description TEXT,
                first_seen  TEXT NOT NULL,
                last_seen   TEXT NOT NULL,
                is_new      INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS scrape_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                scraped_at  TEXT NOT NULL,
                count       INTEGER,
                status      TEXT
            );
        """)


def upsert_listing(listing: dict):
    now = datetime.utcnow().isoformat()
    listing_id = listing["id"]

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, first_seen FROM listings WHERE id = ?", (listing_id,)
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE listings SET
                    price = :price, sqm = :sqm, price_per_sqm = :price_per_sqm,
                    last_seen = :last_seen, is_new = 0
                WHERE id = :id
            """, {**listing, "last_seen": now})
        else:
            conn.execute("""
                INSERT INTO listings
                    (id, source, title, price, sqm, price_per_sqm, rooms, zone,
                     address, year_built, floor, energy_class, url, description,
                     first_seen, last_seen, is_new)
                VALUES
                    (:id, :source, :title, :price, :sqm, :price_per_sqm, :rooms, :zone,
                     :address, :year_built, :floor, :energy_class, :url, :description,
                     :first_seen, :last_seen, 1)
            """, {**listing, "first_seen": now, "last_seen": now})


def log_scrape(source: str, count: int, status: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO scrape_log (source, scraped_at, count, status) VALUES (?, ?, ?, ?)",
            (source, datetime.utcnow().isoformat(), count, status),
        )


def get_listings(
    zones=None,
    max_price_per_sqm=None,
    min_rooms=None,
    max_rooms=None,
    min_year=None,
    only_new=False,
    source=None,
):
    query = "SELECT * FROM listings WHERE 1=1"
    params = []

    if zones:
        placeholders = ",".join("?" * len(zones))
        query += f" AND zone IN ({placeholders})"
        params.extend(zones)
    if max_price_per_sqm:
        query += " AND price_per_sqm <= ?"
        params.append(max_price_per_sqm)
    if min_rooms:
        query += " AND rooms >= ?"
        params.append(min_rooms)
    if max_rooms:
        query += " AND rooms <= ?"
        params.append(max_rooms)
    if min_year:
        query += " AND (year_built >= ? OR year_built IS NULL)"
        params.append(min_year)
    if only_new:
        query += " AND is_new = 1"
    if source:
        query += " AND source = ?"
        params.append(source)

    query += " ORDER BY first_seen DESC"

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
