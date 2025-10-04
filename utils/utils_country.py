"""
utils_country.py
Attach a country code to a domain using:
1) Known outlet map (best)
2) Country TLD fallback (good)
3) Optional: NER on text (last resort; off by default here)
"""

import tldextract
from producers.outlet_country_map import OUTLET_COUNTRY_MAP

# Minimal TLD → ISO-3 for common cases; add as needed
TLD_COUNTRY_MAP = {
    "uk": "GBR",
    "de": "DEU",
    "fr": "FRA",
    "us": "USA",
    "il": "ISR",
    "qa": "QAT",
    "es": "ESP",
    "it": "ITA",
    "jp": "JPN",
    "cn": "CHN",
    "ru": "RUS",
    "in": "IND",
}

def get_country_from_domain(domain: str, text: str = "") -> str:
    # 1) Exact outlet/domain match
    if domain in OUTLET_COUNTRY_MAP:
        return OUTLET_COUNTRY_MAP[domain]

    # 2) TLD fallback (e.g., 'co.uk' → 'uk')
    ext = tldextract.extract(domain)
    if ext.suffix:
        tld = ext.suffix.split(".")[-1].lower()
        if tld in TLD_COUNTRY_MAP:
            return TLD_COUNTRY_MAP[tld]

    # 3) Optional NER: off by default to keep deps light
    #    (Enable if you need: spaCy + en_core_web_sm)
    # try:
    #     import spacy
    #     nlp = spacy.load("en_core_web_sm")
    #     doc = nlp(text or "")
    #     for ent in doc.ents:
    #         if ent.label_ == "GPE":
    #             g = ent.text.lower()
    #             if "israel" in g: return "ISR"
    #             if "palestin" in g: return "PSE"
    #             if "germany" in g: return "DEU"
    #             if "france" in g: return "FRA"
    #             if "britain" in g or "england" in g or "uk" in g: return "GBR"
    #             if "america" in g or "us" in g or "usa" in g: return "USA"
    # except Exception:
    #     pass

    return "UNK"
