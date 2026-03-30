from abc import ABC, abstractmethod
from config import ZONE_KEYWORDS


class BaseScraper(ABC):
    source: str = ""

    @abstractmethod
    def fetch_listings(self) -> list[dict]:
        """Return list of raw listing dicts from the source."""
        ...

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw listing dict to the DB schema."""
        price = raw.get("price")
        sqm = raw.get("sqm")
        price_per_sqm = None
        if price and sqm and sqm > 0:
            price_per_sqm = round(price / sqm, 0)

        return {
            "id": f"{self.source}_{raw['id']}",
            "source": self.source,
            "title": raw.get("title", ""),
            "price": price,
            "sqm": sqm,
            "price_per_sqm": price_per_sqm,
            "rooms": raw.get("rooms"),
            "zone": self._detect_zone(raw.get("address", "") + " " + raw.get("title", "")),
            "address": raw.get("address", ""),
            "year_built": raw.get("year_built"),
            "floor": raw.get("floor", ""),
            "energy_class": raw.get("energy_class", ""),
            "url": raw.get("url", ""),
            "description": raw.get("description", ""),
        }

    def _detect_zone(self, text: str) -> str:
        text_lower = text.lower()
        for zone, keywords in ZONE_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                return zone
        return "Altro"

    def run(self) -> list[dict]:
        raw_listings = self.fetch_listings()
        return [self.normalize(r) for r in raw_listings]
