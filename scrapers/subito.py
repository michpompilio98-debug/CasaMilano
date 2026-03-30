import time
import random
import requests
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from config import SEARCH_CRITERIA

BASE_URL = "https://www.subito.it"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9",
}

ZONE_QUERIES = [
    "porta romana milano",
    "bocconi milano",
    "ripamonti milano",
    "cermenate milano",
    "vigentino milano",
]


class SubitoScraper(BaseScraper):
    source = "subito"

    def fetch_listings(self) -> list[dict]:
        all_listings = []

        for zone_query in ZONE_QUERIES:
            listings = self._search(zone_query)
            all_listings.extend(listings)
            time.sleep(random.uniform(2.0, 4.0))

        seen = set()
        unique = []
        for l in all_listings:
            if l["id"] not in seen:
                seen.add(l["id"])
                unique.append(l)

        print(f"[subito] Found {len(unique)} listings total")
        return unique

    def _search(self, zone_query: str, max_pages: int = 3) -> list[dict]:
        listings = []
        min_price = 50000
        max_price = SEARCH_CRITERIA["max_price_per_sqm"] * SEARCH_CRITERIA["max_sqm"]

        base_url = (
            f"{BASE_URL}/annunci/immobili/vendita/appartamenti-ville/lombardia/milano/"
            f"?q={zone_query.replace(' ', '+')}"
            f"&ps={int(min_price)}&pe={int(max_price)}"
        )

        for page in range(1, max_pages + 1):
            url = base_url if page == 1 else f"{base_url}&o={page}"
            print(f"[subito] Scraping '{zone_query}' page {page}")

            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
            except Exception as e:
                print(f"[subito] Request error: {e}")
                break

            page_listings = self._parse_page(resp.text)
            if not page_listings:
                break

            listings.extend(page_listings)
            time.sleep(random.uniform(2.0, 3.5))

        return listings

    def _parse_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        listings = []

        cards = soup.select("article[class*='item']")
        for card in cards:
            parsed = self._parse_card(card)
            if parsed:
                listings.append(parsed)

        return listings

    def _parse_card(self, card) -> dict | None:
        try:
            link_el = card.select_one("a[href]")
            if not link_el:
                return None

            href = link_el.get("href", "")
            listing_id = href.strip("/").split("-")[-1].split(".")[0]
            url = href if href.startswith("http") else BASE_URL + href

            title_el = card.select_one("h2, [class*='title']")
            title = title_el.get_text(strip=True) if title_el else ""

            price_el = card.select_one("[class*='price']")
            price = self._parse_number(price_el.get_text()) if price_el else None

            desc_el = card.select_one("[class*='description'], [class*='body']")
            description = desc_el.get_text(strip=True) if desc_el else ""

            # Extract sqm and rooms from description
            import re
            sqm = None
            sqm_match = re.search(r"(\d+)\s*m²", description, re.IGNORECASE)
            if sqm_match:
                sqm = float(sqm_match.group(1))

            rooms = None
            rooms_match = re.search(r"(\d)\s*local", description, re.IGNORECASE)
            if rooms_match:
                rooms = int(rooms_match.group(1))

            location_el = card.select_one("[class*='location'], [class*='city']")
            address = location_el.get_text(strip=True) if location_el else ""

            return {
                "id": listing_id,
                "title": title,
                "price": price,
                "sqm": sqm,
                "rooms": rooms,
                "address": f"{address} {title}",
                "floor": "",
                "energy_class": "",
                "url": url,
                "year_built": None,
                "description": description,
            }
        except Exception:
            return None

    @staticmethod
    def _parse_number(text: str) -> float | None:
        import re
        cleaned = re.sub(r"[^\d,.]", "", text).replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except (ValueError, AttributeError):
            return None
