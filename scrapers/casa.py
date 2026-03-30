import re
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from config import SEARCH_CRITERIA

BASE_URL = "https://www.casa.it"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
}


class CasaScraper(BaseScraper):
    source = "casa"

    def __init__(self):
        self._session = None

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            # Warm up: visit homepage to get cookies
            self._session.get(BASE_URL + "/", headers=HEADERS, timeout=15)
        return self._session

    def fetch_listings(self) -> list[dict]:
        min_rooms = min(SEARCH_CRITERIA["room_types"])
        max_rooms = max(SEARCH_CRITERIA["room_types"])
        min_sqm = SEARCH_CRITERIA["min_sqm"]
        max_price = SEARCH_CRITERIA["max_price_per_sqm"] * SEARCH_CRITERIA["max_sqm"]

        all_listings = []

        base_params = (
            f"?locali_min={min_rooms}&locali_max={max_rooms}"
            f"&superficie_min={min_sqm}&prezzo_max={int(max_price)}"
        )

        # Fetch multiple pages from Milan overall (zone filtering done in normalize())
        for page in range(1, 26):
            url = f"{BASE_URL}/vendita/residenziale/milano/{base_params}&page={page}"
            print(f"[casa] Page {page}: {url}")

            try:
                session = self._get_session()
                headers = {**HEADERS, "Referer": BASE_URL + "/"}
                resp = session.get(url, headers=headers, timeout=15)
                resp.raise_for_status()
            except Exception as e:
                print(f"[casa] Error on page {page}: {e}")
                break

            page_listings = self._extract_listings(resp.text)
            if not page_listings:
                print(f"[casa] No listings on page {page}, stopping")
                break

            all_listings.extend(page_listings)
            print(f"[casa] Page {page}: {len(page_listings)} listings")
            time.sleep(random.uniform(1.5, 3.0))

        # Deduplicate
        seen = set()
        unique = []
        for l in all_listings:
            if l["id"] not in seen:
                seen.add(l["id"])
                unique.append(l)

        print(f"[casa] Total: {len(unique)} unique listings")
        return unique

    def _extract_listings(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")

        for script in soup.find_all("script"):
            if script.string and "window.__INITIAL_STATE__" in script.string:
                match = re.search(
                    r"window\.__INITIAL_STATE__\s*=\s*JSON\.parse\(\"(.*?)\"\);",
                    script.string,
                    re.DOTALL,
                )
                if not match:
                    continue
                try:
                    raw_json = match.group(1).encode().decode("unicode_escape")
                    data = json.loads(raw_json)
                    items = data.get("search", {}).get("list", [])
                    parsed = [self._parse_item(i) for i in items]
                    return [p for p in parsed if p]
                except Exception as e:
                    print(f"[casa] JSON parse error: {e}")

        return []

    def _parse_item(self, item: dict) -> dict | None:
        try:
            listing_id = str(item.get("id", ""))
            if not listing_id:
                return None

            features = item.get("features", {}) or {}
            price_info = features.get("price", {}) or {}
            price_raw = price_info.get("value", "")
            price = None
            if price_raw:
                try:
                    price = float(str(price_raw).replace(".", "").replace(",", "."))
                except ValueError:
                    pass

            sqm = features.get("mq")
            rooms = features.get("rooms")
            floor = features.get("level", "")
            energy_class = features.get("energyClass", "")

            title_data = item.get("title", {})
            title = title_data.get("main", "") if isinstance(title_data, dict) else str(title_data)

            geo = item.get("geoInfos", {}) or {}
            street = geo.get("street", "")
            district = geo.get("district_name", "")
            address = f"{street}, {district}, Milano".strip(", ")

            uri = item.get("uri", "")
            url = f"{BASE_URL}{uri}" if uri.startswith("/") else uri

            description = item.get("description", "") or ""
            year_match = re.search(r"\b(20[0-9]{2})\b", description)
            year_built = int(year_match.group(1)) if year_match else None

            return {
                "id": listing_id,
                "title": title,
                "price": price,
                "sqm": float(sqm) if sqm else None,
                "rooms": int(rooms) if rooms else None,
                # Use district_name + street as address for zone detection
                "address": f"{title} {address}",
                "floor": floor,
                "energy_class": energy_class,
                "url": url,
                "year_built": year_built,
                "description": description[:500],
            }
        except Exception:
            return None
