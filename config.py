SEARCH_CRITERIA = {
    "zones": [
        "Porta Romana",
        "Bocconi",
        "Fondazione Prada",
        "Ripamonti",
        "Cermenate",
    ],
    "max_price_per_sqm": 7000,
    "min_year_built": 2015,
    "room_types": [2, 3],  # 2=bilocale, 3=trilocale
    "min_sqm": 40,
    "max_sqm": 120,
}

# Zone keywords for matching listings (broader set for fuzzy matching)
ZONE_KEYWORDS = {
    "Porta Romana": ["porta romana", "porta_romana", "corso di porta romana", "viale beatrice d'este", "viale bligny"],
    "Bocconi": ["bocconi", "viale isonzo", "corso lodi", "via bocconi", "viale ortles", "via col di lana", "piazzale medaglie d'oro"],
    "Fondazione Prada": ["fondazione prada", "largo isarco", "via isarco", "viale ortles", "via pestalozzi", "piazza lodi"],
    "Ripamonti": ["ripamonti", "via ripamonti", "vigentino", "fatima", "bernardino verro", "ramusio"],
    "Cermenate": ["cermenate", "via cermenate", "brenta", "via brenta", "corvetto", "piazzale corvetto"],
}

# Scraping schedule (every 4 hours)
SCRAPE_INTERVAL_HOURS = 4
