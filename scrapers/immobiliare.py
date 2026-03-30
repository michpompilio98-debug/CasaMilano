import json
import time
import random
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from scrapers.base import BaseScraper
from config import SEARCH_CRITERIA

BASE_URL = "https://www.immobiliare.it"

ZONE_IDS = {
    "Porta Romana": "10182",
    "Bocconi": "10172",
    "Ripamonti": "10204",
    "Vigentino/Cermenate": "10219",
    "Corvetto/Fondazione Prada": "10179",
}


class ImmobiliareScraper(BaseScraper):
    source = "immobiliare"

    def fetch_listings(self) -> list[dict]:
        rooms = SEARCH_CRITERIA["room_types"]
        zone_ids = list(ZONE_IDS.values())

        qs_parts = [
            "categoria=2",
            f"prezzoMaximo={SEARCH_CRITERIA['max_price_per_sqm'] * SEARCH_CRITERIA['max_sqm']}",
            f"superficieMinima={SEARCH_CRITERIA['min_sqm']}",
            f"superficieMassima={SEARCH_CRITERIA['max_sqm']}",
        ]
        for r in rooms:
            qs_parts.append(f"locali[]={r}")
        for zid in zone_ids:
            qs_parts.append(f"idQuartiere[]={zid}")

        base_qs = "&".join(qs_parts)
        all_listings = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="it-IT",
                viewport={"width": 1280, "height": 800},
            )
            # Intercept JSON API responses if available
            captured = []

            def handle_response(response):
                if "api.immobiliare.it" in response.url or (
                    "immobiliare.it" in response.url and "search" in response.url
                ):
                    try:
                        data = response.json()
                        if isinstance(data, dict) and "results" in data:
                            captured.extend(data["results"])
                    except Exception:
                        pass

            page = context.new_page()
            page.on("response", handle_response)

            for pg in range(1, 6):
                url = f"{BASE_URL}/vendita-case/milano/?{base_qs}&pag={pg}"
                print(f"[immobiliare] Page {pg}: {url}")

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    page.wait_for_timeout(random.randint(2000, 4000))
                except PWTimeout:
                    print(f"[immobiliare] Timeout on page {pg}")
                    break

                # Try __NEXT_DATA__ first
                listings = self._extract_next_data(page)
                if listings:
                    all_listings.extend(listings)
                    print(f"[immobiliare] Page {pg}: {len(listings)} listings (next_data)")
                elif captured:
                    parsed = [self._parse_api_item(i) for i in captured]
                    parsed = [p for p in parsed if p]
                    all_listings.extend(parsed)
                    print(f"[immobiliare] Page {pg}: {len(parsed)} listings (api intercept)")
                    captured.clear()
                else:
                    # HTML fallback
                    listings = self._extract_html(page)
                    if not listings:
                        print(f"[immobiliare] Page {pg}: no listings, stopping")
                        break
                    all_listings.extend(listings)
                    print(f"[immobiliare] Page {pg}: {len(listings)} listings (html)")

            browser.close()

        # Deduplicate
        seen = set()
        unique = []
        for l in all_listings:
            if l["id"] not in seen:
                seen.add(l["id"])
                unique.append(l)

        print(f"[immobiliare] Total: {len(unique)} unique listings")
        return unique

    def _extract_next_data(self, page) -> list[dict]:
        try:
            next_data_json = page.evaluate(
                "() => { const el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null; }"
            )
            if not next_data_json:
                return []

            data = json.loads(next_data_json)
            # Navigate through Next.js data structure
            queries = (
                data.get("props", {})
                .get("pageProps", {})
                .get("dehydratedState", {})
                .get("queries", [])
            )
            listings = []
            for query in queries:
                results = (
                    query.get("state", {})
                    .get("data", {})
                    .get("results", [])
                )
                for item in results:
                    parsed = self._parse_api_item(item)
                    if parsed:
                        listings.append(parsed)
            return listings
        except Exception as e:
            print(f"[immobiliare] next_data error: {e}")
            return []

    def _extract_html(self, page) -> list[dict]:
        try:
            from bs4 import BeautifulSoup
            html = page.content()
            soup = BeautifulSoup(html, "lxml")
            listings = []
            cards = soup.select("li[data-listing-id]")
            for card in cards:
                parsed = self._parse_html_card(card)
                if parsed:
                    listings.append(parsed)
            return listings
        except Exception as e:
            print(f"[immobiliare] html parse error: {e}")
            return []

    def _parse_api_item(self, item: dict) -> dict | None:
        try:
            re_data = item.get("realEstate", item)
            listing_id = str(re_data.get("id", ""))
            if not listing_id:
                return None

            props = re_data.get("properties", [{}])
            prop = props[0] if props else {}
            price_info = re_data.get("price", {})

            price = price_info.get("value")
            sqm = prop.get("surface")
            rooms = prop.get("rooms")
            address = prop.get("location", {}).get("address", "")
            floor = str(prop.get("floor", {}).get("abbreviation", ""))
            energy = prop.get("energy", {}).get("class", "")
            title = re_data.get("title", "")
            url = f"{BASE_URL}/annunci/{listing_id}/"

            return {
                "id": listing_id,
                "title": title,
                "price": float(price) if price else None,
                "sqm": float(sqm) if sqm else None,
                "rooms": int(rooms) if rooms else None,
                "address": address,
                "floor": floor,
                "energy_class": energy,
                "url": url,
                "year_built": None,
                "description": "",
            }
        except Exception:
            return None

    def _parse_html_card(self, card) -> dict | None:
        try:
            import re as re_module
            listing_id = card.get("data-listing-id", "")
            if not listing_id:
                return None

            title_el = card.select_one("a[class*='title'], h2 a, [class*='Title']")
            title = title_el.get_text(strip=True) if title_el else ""

            price_el = card.select_one("[class*='price'], [class*='Price']")
            price_text = price_el.get_text(strip=True) if price_el else ""
            price = self._parse_number(price_text)

            text = card.get_text(" ")
            sqm_match = re_module.search(r"(\d+)\s*m²", text)
            sqm = float(sqm_match.group(1)) if sqm_match else None

            rooms_match = re_module.search(r"(\d)\s*local", text, re_module.IGNORECASE)
            rooms = int(rooms_match.group(1)) if rooms_match else None

            addr_el = card.select_one("[class*='location'], [class*='Location'], [class*='address']")
            address = addr_el.get_text(strip=True) if addr_el else ""

            link_el = card.select_one("a[href*='/annunci/']")
            url = BASE_URL + link_el["href"] if link_el else ""

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
