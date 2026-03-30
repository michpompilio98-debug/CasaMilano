import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from config import SEARCH_CRITERIA

BASE_URL = "https://www.gabetti.it"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.google.it/",
}


class GabettiScraper(BaseScraper):
    source = "gabetti"

    def fetch_listings(self) -> list[dict]:
        min_rooms = min(SEARCH_CRITERIA["room_types"])
        max_rooms = max(SEARCH_CRITERIA["room_types"])
        min_sqm = SEARCH_CRITERIA["min_sqm"]
        max_price = SEARCH_CRITERIA["max_price_per_sqm"] * SEARCH_CRITERIA["max_sqm"]

        all_listings = []

        for page in range(1, 6):
            url = (
                f"{BASE_URL}/vendita/abitativo/milano"
                f"?locali_min={min_rooms}&locali_max={max_rooms}"
                f"&superficie_min={min_sqm}&prezzo_max={int(max_price)}"
                f"&page={page}"
            )
            print(f"[gabetti] Page {page}")

            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
            except Exception as e:
                print(f"[gabetti] Error: {e}")
                break

            listings = self._parse_page(resp.text)
            if not listings:
                print(f"[gabetti] No listings on page {page}, stopping")
                break

            all_listings.extend(listings)
            print(f"[gabetti] Page {page}: {len(listings)} listings")
            time.sleep(random.uniform(1.5, 3.0))

        # Deduplicate
        seen = set()
        unique = [l for l in all_listings if not (l["id"] in seen or seen.add(l["id"]))]
        print(f"[gabetti] Total: {len(unique)} unique listings")
        return unique

    def _parse_page(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ")

        # Parse listing cards from body text using regex patterns
        listings = []

        # Find all listing blocks via structured card parsing
        cards = soup.select(
            "[class*='PropertyCard'], [class*='property-card'], "
            "[class*='listing'], [class*='annuncio'], article"
        )

        if cards:
            for card in cards:
                parsed = self._parse_card(card)
                if parsed:
                    listings.append(parsed)
        else:
            # Fallback: extract from inline Next.js chunks
            listings = self._parse_nextjs(soup)

        return listings

    def _parse_card(self, card) -> dict | None:
        try:
            text = card.get_text(" ")

            # Price
            price_match = re.search(r"([\d\.]+)\s*€", text)
            if not price_match:
                return None
            price = float(price_match.group(1).replace(".", ""))

            # Sqm
            sqm_match = re.search(r"(\d+)\s*m²", text)
            sqm = float(sqm_match.group(1)) if sqm_match else None

            # Rooms
            rooms_match = re.search(r"(\d)\s*local", text, re.IGNORECASE)
            rooms = int(rooms_match.group(1)) if rooms_match else None

            # Address/title
            link = card.select_one("a[href*='/annuncio/'], a[href*='/immobile/'], a[href]")
            href = link["href"] if link else ""
            url = BASE_URL + href if href.startswith("/") else href

            # Extract ID from URL
            listing_id = re.search(r"/(\d+)/?$", href)
            if not listing_id:
                listing_id = re.search(r"[A-Z0-9]{6,}", href)
            lid = listing_id.group(1) if listing_id else href[-20:]

            title_el = card.select_one("h2, h3, [class*='title'], [class*='Title']")
            title = title_el.get_text(strip=True) if title_el else text[:80].strip()

            addr_el = card.select_one("[class*='address'], [class*='location'], [class*='Address']")
            address = addr_el.get_text(strip=True) if addr_el else ""

            return {
                "id": lid,
                "title": title,
                "price": price,
                "sqm": sqm,
                "rooms": rooms,
                "address": f"{title} {address}",
                "floor": "",
                "energy_class": "",
                "url": url,
                "year_built": None,
                "description": "",
            }
        except Exception:
            return None

    def _parse_nextjs(self, soup) -> list[dict]:
        """Extract listings from inline Next.js RSC payload."""
        listings = []
        for script in soup.find_all("script"):
            content = script.string or ""
            if "self.__next_f" not in content:
                continue
            # Find price patterns in the RSC payload
            prices = re.findall(r'"price[^"]*":\s*(\d+)', content)
            addresses = re.findall(r'"address[^"]*":\s*"([^"]+)"', content)
            sqms = re.findall(r'"surface[^"]*":\s*(\d+)', content)
            ids = re.findall(r'"id[^"]*":\s*"?(\d{5,})"?', content)

            for i, pid in enumerate(ids[:20]):
                try:
                    price = float(prices[i]) if i < len(prices) else None
                    sqm = float(sqms[i]) if i < len(sqms) else None
                    address = addresses[i] if i < len(addresses) else ""
                    listings.append({
                        "id": pid,
                        "title": address,
                        "price": price,
                        "sqm": sqm,
                        "rooms": None,
                        "address": address,
                        "floor": "",
                        "energy_class": "",
                        "url": f"{BASE_URL}/annuncio/{pid}/",
                        "year_built": None,
                        "description": "",
                    })
                except Exception:
                    continue
            if listings:
                break

        return listings
