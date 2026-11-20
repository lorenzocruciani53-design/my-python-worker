"""Listino di riacquisto Ready2Game.

Il valutatore https://www.ready2game.it/pages/vendita-usato contiene il listino
completo dentro la pagina (blocco <script id="R2GBuybackSource">):
  - console: importo fisso, detrazione accessori, valore controller
  - giochi: formula che parte dal prezzo P del gioco nel negozio Ready2Game

Il negozio è Shopify: /products.json restituisce tutto il catalogo (titolo,
tag piattaforma, prezzo). Il sito ha una verifica anti-bot, per questo si
usa un browser vero (Playwright) solo per scaricare il listino, 1 volta al giorno.
Le formule qui sotto replicano 1:1 quelle del sito (calcConsolePrice,
calcGameBasePrice, calcGamePrice).
"""
from __future__ import annotations

import json
import math
import re
import time
from pathlib import Path

PAGE_URL = "https://www.ready2game.it/pages/vendita-usato"
SHOP_URL = "https://www.ready2game.it/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")

RETRO_BOX_BONUS_PLATFORMS = {"Game Boy", "NES", "Nintendo 64", "SNES"}
CREDIT_BONUS = 0.10          # credito negozio: +10%
FREE_SHIPPING_FROM = 50.0    # spedizione gratuita verso R2G da 50 € di valutazione base


# --------------------------------------------------------------------------
# Parsing dell'oggetto JavaScript del listino -> dict Python
# --------------------------------------------------------------------------
def js_object_to_python(src: str) -> dict:
    """Converte l'object literal JS del listino in un dict (senza eseguire JS)."""
    s = src.strip()
    s = re.sub(r"^=\s*", "", s).rstrip().rstrip(";")
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S)
    out, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c in "'\"":
            q, j, buf = c, i + 1, []
            while j < n and s[j] != q:
                if s[j] == "\\" and j + 1 < n:
                    buf.append(s[j + 1]); j += 2; continue
                buf.append(s[j]); j += 1
            out.append(json.dumps("".join(buf))); i = j + 1
        elif c.isalpha() or c == "_":
            j = i
            while j < n and (s[j].isalnum() or s[j] == "_"):
                j += 1
            word = s[i:j]
            k = j
            while k < n and s[k].isspace():
                k += 1
            if k < n and s[k] == ":":
                out.append(json.dumps(word))
            elif word == "Infinity":
                out.append("1e18")
            else:
                out.append(word)  # null / true / false
            i = j
        else:
            out.append(c); i += 1
    txt = re.sub(r",(\s*[}\]])", r"\1", "".join(out))  # virgole finali
    return json.loads(txt)


def extract_price_config(html: str) -> dict:
    m = re.search(r'id="R2GBuybackSource"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        raise ValueError("Blocco R2GBuybackSource non trovato: il sito è cambiato")
    raw = m.group(1).strip()
    try:
        raw = json.loads(raw)  # nel sito è una stringa JSON che contiene il JS
    except json.JSONDecodeError:
        pass
    return js_object_to_python(raw)


# --------------------------------------------------------------------------
# Formule (identiche al sito)
# --------------------------------------------------------------------------
def _r(v: float) -> float:
    """Come roundMoney del sito: Math.round((v + EPSILON) * 100) / 100."""
    return math.floor((v + 2.220446049250313e-16) * 100 + 0.5) / 100


def game_base_price(p: float, rules: dict) -> float:
    p = max(0.0, float(p or 0))
    if p < rules["below499"]["maxExclusive"]:
        return rules["below499"]["fixedPrice"]
    if p <= rules["upTo599"]["maxInclusive"]:
        return rules["upTo599"]["fixedPrice"]
    br = next((b for b in rules["brackets"] if b["min"] <= p <= b["max"]), rules["brackets"][-1])
    return _r(max(0.0, (p * rules["resaleNetMultiplier"] - rules["fixedCost"]) * br["rate"]))


def game_price(p: float, rules: dict, platform: str, has_box: bool = True, disc_ok: bool = True) -> float:
    if not disc_ok:
        return 0.01
    base = game_base_price(p, rules)
    if platform in RETRO_BOX_BONUS_PLATFORMS:
        return _r(base * 1.5) if has_box else base
    if not has_box:
        return _r(base * (0.6 if platform == "Switch" else 0.5))
    return base


def console_price(pricing: dict, controllers_cfg: dict, *, banned=False, working=True,
                  damaged=False, accessories_complete=True, n_controllers=0) -> float | None:
    if pricing.get("price") is None:
        return None
    if banned or not working:
        value = 1.0
    else:
        value = float(pricing["price"])
        if float(pricing.get("accessoryDeduction") or 0) > 0 and not accessories_complete:
            value -= float(pricing["accessoryDeduction"])
        value = max(0.0, value)
        if damaged:
            value *= 0.5
    ck = pricing.get("controllerKey")
    if ck and ck in controllers_cfg:
        value += max(0, n_controllers) * float(controllers_cfg[ck]["price"])
    return _r(value)


# --------------------------------------------------------------------------
# Download del listino + catalogo con Playwright
# --------------------------------------------------------------------------
_PLATFORM_TAGS = {
    "3DS e 2DS", "DS", "Game Boy", "GameCube", "NES", "Nintendo 64", "Playstation 1",
    "Playstation 2", "Playstation 3", "Playstation 4", "Playstation 5", "Playstation Vita",
    "PSP", "SNES", "Switch", "Wii e Wii U", "Xbox 360", "Xbox Classic", "Xbox One",
    "Xbox Series X e S",
}


def clean_game_title(title: str) -> str:
    t = re.sub(r"^\s*Gioco( Nuovo Sigillato)?\s+", "", title, flags=re.I)
    t = re.sub(r"\s+per\s+(Nintendo|Sony|Microsoft|PS\d|Playstation|Xbox|Wii|Game ?Boy|GameCube|DS|3DS|2DS|PSP|SNES|NES|Switch).*$",
               "", t, flags=re.I)
    return t.strip()


def download_listino(out_path: Path) -> dict:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(user_agent=UA, locale="it-IT")
        page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_selector("#R2GBuybackSource", state="attached", timeout=60000)
        cfg = extract_price_config(page.content())
        products = page.evaluate("""async () => {
            const all = [];
            for (let p = 1; p < 200; p++) {
                const r = await fetch(`/products.json?limit=250&page=${p}`);
                if (!r.ok) break;
                const j = await r.json();
                if (!j.products.length) break;
                for (const x of j.products) all.push({
                    id: x.id, title: x.title, handle: x.handle, tags: x.tags,
                    variants: x.variants.map(v => ({price: v.price, available: v.available}))
                });
                await new Promise(res => setTimeout(res, 400));
            }
            return all;
        }""")
        browser.close()

    games = []
    for p in products:
        tags = set(p["tags"])
        if "Giochi" not in tags or len(p["variants"]) != 1:
            continue  # il sito valuta solo giochi a variante singola
        plat = next((t for t in p["tags"] if t in _PLATFORM_TAGS), None)
        if plat == "DS":
            plat = "DS, Lite e DSi"
        if not plat:
            continue
        price = float(p["variants"][0]["price"])
        games.append({
            "id": p["id"], "title": p["title"], "name": clean_game_title(p["title"]),
            "platform": plat, "shop_price": price,
            "url": f"{SHOP_URL}products/{p['handle']}",
            "buyback_with_box": game_price(price, cfg["games"], plat, True),
            "buyback_no_box": game_price(price, cfg["games"], plat, False),
        })

    data = {"updated": int(time.time()), "config": cfg, "games": games}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    return data


def load_listino(path: Path, max_age_h: float = 24) -> dict:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if time.time() - data.get("updated", 0) < max_age_h * 3600:
            return data
    try:
        return download_listino(path)
    except Exception as e:  # sito giù: si usa l'ultimo listino salvato
        if path.exists():
            print(f"[R2G] aggiornamento fallito ({e}); uso il listino salvato")
            return json.loads(path.read_text(encoding="utf-8"))
        raise


if __name__ == "__main__":
    import sys
    d = download_listino(Path(sys.argv[1] if len(sys.argv) > 1 else "data/listino.json"))
    print(f"Console: {len(d['config']['consoles'])}  Giochi: {len(d['games'])}")
