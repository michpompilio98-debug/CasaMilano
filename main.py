"""
CasaMilano — Real Estate Bot for Milan

Usage:
    python main.py scrape          # Run all scrapers once
    python main.py scrape --source immobiliare
    python main.py schedule        # Start scheduled scraping (every 4h)
    python main.py dashboard       # Launch Streamlit dashboard
"""

import sys
import subprocess
from db import init_db, upsert_listing, log_scrape
from config import SCRAPE_INTERVAL_HOURS


def run_scrapers(source_filter: str | None = None):
    from scrapers.immobiliare import ImmobiliareScraper
    from scrapers.idealista import IdealistaScraper
    from scrapers.subito import SubitoScraper
    from scrapers.casa import CasaScraper
    from scrapers.gabetti import GabettiScraper

    scrapers = {
        "immobiliare": ImmobiliareScraper(),
        "idealista": IdealistaScraper(),
        "subito": SubitoScraper(),
        "casa": CasaScraper(),
        "gabetti": GabettiScraper(),
    }

    if source_filter:
        scrapers = {k: v for k, v in scrapers.items() if k == source_filter}
        if not scrapers:
            print(f"Unknown source: {source_filter}. Valid: {list(scrapers.keys())}")
            return

    for name, scraper in scrapers.items():
        print(f"\n=== Scraping {name} ===")
        try:
            listings = scraper.run()
            for listing in listings:
                upsert_listing(listing)
            log_scrape(name, len(listings), "ok")
            print(f"[{name}] Saved {len(listings)} listings")
        except Exception as e:
            log_scrape(name, 0, f"error: {e}")
            print(f"[{name}] ERROR: {e}")


def start_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_scrapers,
        "interval",
        hours=SCRAPE_INTERVAL_HOURS,
        id="scrape_job",
    )

    print(f"Scheduler started — scraping every {SCRAPE_INTERVAL_HOURS}h")
    print("Running first scrape now...")
    run_scrapers()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


def launch_dashboard():
    subprocess.run(["streamlit", "run", "dashboard.py"])


if __name__ == "__main__":
    init_db()
    args = sys.argv[1:]

    if not args or args[0] == "scrape":
        source = None
        if len(args) > 2 and args[1] == "--source":
            source = args[2]
        run_scrapers(source)

    elif args[0] == "schedule":
        start_scheduler()

    elif args[0] == "dashboard":
        launch_dashboard()

    else:
        print(__doc__)
