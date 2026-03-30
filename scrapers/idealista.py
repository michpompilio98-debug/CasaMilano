import time
import random
import requests
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from config import SEARCH_CRITERIA

BASE_URL = "https://www.idealista.it"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.idealista.it/",
}

# Idealista zone slugs for Milan southern neighborhoods
ZONE_SLUGS = [
    "porta-romana",
    "bocconi",
    "ripamonti",
    "vigentino",
    "corvetto",
]


class IdealistaScraper(BaseScraper):
    source = "idealista"

    def fetch_listings(self) -> list[dict]:
        all_listings = []
        rooms = SEARCH_CRITERIA["room_types"]

        for zone_slug in ZONE_SLUGS:
            for room_count in rooms:
                listings = self._search(zone_slug, room_count)
                all_listings.extend(listings)
                time.sleep(random.uniform(3.0, 6.0))

        # Deduplicate by ID
        seen = set()
        unique = []
        for l in all_listings:
            if l["id"] not in seen:
                seen.add(l["id"])
                unique.append(l)

        print(f"[idealista] Found {len(unique)} listings total")
        return unique

    def _search(self, zone: str, rooms: int, max_pages: int = 3) -> list[dict]:
        listings = []
        min_sqm = SEARCH_CRITERIA["min_sqm"]
        max_sqm = SEARCH_CRITERIA["max_sqm"]
        max_price = SEARCH_CRITERIA["max_price_per_sqm"] * max_sqm

        room_suffix = f"{rooms}-locali"
        url = (
            f"{BASE_URL}/vendita-case/milano/{zone}/{room_suffix}/"
            f"con-prezzo_max_{int(max_price)},dimensione-minima_{min_sqm}/"
        )

        for page in range(1, max_pages + 1):
            page_url = url if page == 1 else f"{url}pagina-{page}.htm"
            print(f"[idealista] Scraping {zone}/{rooms}L page {page}")

            try:
                resp = requests.get(page_url, headers=HEADERS, timeout=15)
                if resp.status_code == 403:
                    print(f"[idealista] 403 Forbidden on {page_url} — site may require Playwright")
                    break
                resp.raise_for_status()
            except Exception as e:
                print(f"[idealista] Request error: {e}")
                break

            page_listings = self._parse_page(resp.text, rooms)
            if not page_listings:
                break

            listings.extend(page_listings)
            time.sleep(random.uniform(3.0, 5.0))

        return listings

    def _parse_page(self, html: str, rooms: int) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        listings = []

        cards = soup.select("article.item")
        for card in cards:
            parsed = self._parse_card(card, rooms)
            if parsed:
                listings.append(parsed)

        return listings

    def _parse_card(self, card, rooms: int) -> dict | None:
        try:
            link_el = card.select_one("a.item-link")
            if not link_el:
                return None

            href = link_el.get("href", "")
            listing_id = href.strip("/").split("/")[-1]
            url = BASE_URL + href if href.startswith("/") else href
            title = link_el.get_text(strip=True)

            price_el = card.select_one(".item-price")
            price = self._parse_number(price_el.get_text()) if price_el else None

            detail_els = card.select(".item-detail")
            sqm = None
            for el in detail_els:
                text = el.get_text()
                if "m²" in text:
                    sqm = self._parse_number(text)
                    break

            address_el = card.select_one(".item-address")
            address = address_el.get_text(strip=True) if address_el else ""

            return {
                "id": listing_id,
                "title": title,
                "price": price,
                "sqm": sqm,
                "rooms": rooms,
                "address": address,
                "floor": "",
                "energy_class": "",
                "url": url,
                "year_built": None,
                "description": "",
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
