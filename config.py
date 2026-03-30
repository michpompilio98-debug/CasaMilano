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
    "Porta Romana": [
        "porta romana", "corso di porta romana", "viale bligny",
        "viale beatrice d'este", "via marcona", "via cadore", "viale piceno",
        "via muratori", "via tiraboschi", "piazzale medaglie d'oro",
    ],
    "Bocconi": [
        "bocconi", "viale isonzo", "corso lodi", "via bocconi",
        "via col di lana", "via montemartini", "viale caldara",
        "via tabacchi", "piazza xxiv maggio", "via vigevano",
    ],
    "Fondazione Prada": [
        "fondazione prada", "largo isarco", "via isarco", "via pestalozzi",
        "piazza lodi", "viale ortles", "via lorenzini", "via brembo",
    ],
    "Ripamonti": [
        "ripamonti", "vigentino", "fatima", "bernardino verro", "ramusio",
        "via chiaradia", "piazza chiaradia", "via selvanesco", "via quaranta",
        "via noto", "via montegani", "via pompeo leoni",
    ],
    "Cermenate": [
        "cermenate", "corvetto", "piazzale corvetto", "via brenta",
        "via oglio", "via adda", "via arno", "via mincio",
        "via monti sabini", "via casoretto",
    ],
}

# Scraping schedule (every 4 hours)
SCRAPE_INTERVAL_HOURS = 4
