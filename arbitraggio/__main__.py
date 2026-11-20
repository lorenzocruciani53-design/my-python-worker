"""Cacciatore di affari per il riacquisto Ready2Game.

Uso:
  python -m arbitraggio                 # gira per sempre, un giro ogni N minuti
  python -m arbitraggio --una-volta     # un solo giro (per cron / GitHub Actions)
  python -m arbitraggio --prova ebay "ps5 slim"   # prova un sito e mostra cosa riconosce
  python -m arbitraggio --listino       # forza l'aggiornamento del listino R2G
"""
from __future__ import annotations

import argparse
import json
import random
import re
import time
import traceback
from pathlib import Path

from . import analisi, dashboard, notifiche
from . import ready2game as r2g
from .db import DB
from .fonti.ebay import Ebay
from .fonti.sperimentali import Subito, Vinted, Wallapop
from .matcher import GameIndex, riconosci

ROOT = Path(__file__).parent
DATA = ROOT / "data"
SOURCES = {"ebay": Ebay, "vinted": Vinted, "subito": Subito, "wallapop": Wallapop}

# ricerche "larghe" per piattaforma: i titoli trovati vengono confrontati con tutto il catalogo
PLATFORM_QUERIES = {
    "Playstation 5": ["gioco ps5", "giochi ps5"], "Playstation 4": ["gioco ps4", "giochi ps4"],
    "Playstation 3": ["gioco ps3", "giochi ps3"], "Playstation 2": ["gioco ps2", "giochi ps2"],
    "Playstation 1": ["gioco ps1 psx"], "Switch": ["gioco switch", "giochi nintendo switch"],
    "Wii e Wii U": ["gioco wii", "gioco wii u"], "3DS e 2DS": ["gioco 3ds"], "DS, Lite e DSi": ["gioco nintendo ds"],
    "Game Boy": ["gioco game boy", "gioco gba"], "GameCube": ["gioco gamecube"], "PSP": ["gioco psp"],
    "Playstation Vita": ["gioco ps vita"], "Xbox One": ["gioco xbox one"], "Xbox 360": ["gioco xbox 360"],
}
SHORT = {"Playstation 5": "ps5", "Playstation 4": "ps4", "Playstation 3": "ps3", "Playstation 2": "ps2",
         "Playstation 1": "ps1", "Switch": "switch", "Wii e Wii U": "wii", "3DS e 2DS": "3ds",
         "DS, Lite e DSi": "nintendo ds", "Game Boy": "game boy", "GameCube": "gamecube", "PSP": "psp",
         "Playstation Vita": "ps vita", "Xbox One": "xbox one", "Xbox 360": "xbox 360"}


def console_query(label: str) -> str:
    q = re.sub(r"\(.*?\)|\+.*$|con Lettore Dischi|Edition|Carbon Black|White|Black|Disk Drive", "", label)
    return re.sub(r"\s+", " ", q.replace("All-Digital", "digital").replace("Disco", "")).strip()


def build_queries(listino: dict, cfg: dict, giro: int) -> list[tuple[str, float | None, str | None]]:
    """(query, prezzo massimo, piattaforma suggerita)."""
    qs = []
    for key, c in listino["config"]["consoles"].items():
        if c.get("price"):
            qs.append((console_query(c["label"]), float(c["price"]) * 1.1, None))
    for plat, lst in PLATFORM_QUERIES.items():
        mx = max((g["buyback_with_box"] for g in listino["games"] if g["platform"] == plat), default=50)
        for q in lst:
            qs.append((q, mx, plat))
    # giochi singoli di valore, a rotazione
    gcfg = cfg["giochi"]
    ricchi = sorted((g for g in listino["games"] if g["buyback_with_box"] >= gcfg["valore_minimo_ricerca_singola"]),
                    key=lambda g: -g["buyback_with_box"])
    n = gcfg["giochi_singoli_per_giro"]
    if ricchi:
        start = (giro * n) % len(ricchi)
        for g in (ricchi + ricchi)[start:start + n]:
            qs.append((f"{g['name']} {SHORT.get(g['platform'], '')}", g["buyback_with_box"], g["platform"]))
    return qs


def giro(cfg, listino, idx, db, sources, n_giro, verbose=False):
    soglie, trovati = cfg["soglie"], 0
    for q, mx, plat in build_queries(listino, cfg, n_giro):
        for name, src in sources.items():
            try:
                items = src.search(q, mx)
            except Exception as ex:
                print(f"[{name}] '{q}': {ex}")
                continue
            for l in items:
                if db.gia_visto(l.uid):
                    continue
                db.segna_visto(l.uid)
                m = riconosci(l.title, l.description, idx, listino, plat)
                if not m:
                    continue
                db.registra_prezzo(m.label, l.site, l.price)
                costi = {"default": src.default_costs, **cfg["costi"]}
                d = analisi.analizza(l, m, listino, costi, db.mediana(m.label))
                if not d:
                    continue
                if verbose:
                    print(f"  {l.site:8} {l.price:7.2f}€ → R2G {d.valutazione:7.2f}€  guad {d.guadagno:+7.2f}  {m.label} | {l.title[:60]}")
                if analisi.classifica(d, soglie) is None:
                    continue
                # annuncio promettente: scarico le recensioni del venditore e ricalcolo
                try:
                    src.enrich_seller(l)
                except Exception:
                    pass
                d = analisi.analizza(l, m, listino, costi, db.mediana(m.label))
                d.livello = analisi.classifica(d, soglie) or ""
                if not d.livello:
                    continue
                invia = d.livello != "👀 DA VERIFICARE" or soglie.get("notifica_da_verificare")
                db.salva_affare(d, notificato=bool(invia and notifiche.invia(d)))
                trovati += 1
            db.commit()
            time.sleep(cfg["pausa_tra_ricerche_sec"] * random.uniform(0.7, 1.4))
    return trovati


def main():
    ap = argparse.ArgumentParser(description="Cacciatore affari Ready2Game")
    ap.add_argument("--una-volta", action="store_true")
    ap.add_argument("--listino", action="store_true")
    ap.add_argument("--prova", nargs=2, metavar=("SITO", "RICERCA"))
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    a = ap.parse_args()

    cfg = json.loads(Path(a.config).read_text(encoding="utf-8"))
    lpath = DATA / "listino.json"
    listino = r2g.download_listino(lpath) if a.listino else r2g.load_listino(lpath, cfg["listino_ore_validita"])
    idx = GameIndex(listino["games"])
    print(f"Listino R2G: {len(listino['config']['consoles'])} console, {len(listino['games'])} giochi")
    if a.listino:
        return

    if a.prova:
        site, q = a.prova
        src = SOURCES[site](cfg["siti"][site])
        for l in src.search(q):
            m = riconosci(l.title, l.description, idx, listino)
            d = m and analisi.analizza(l, m, listino, {"default": src.default_costs, **cfg["costi"]})
            print(f"{l.price:7.2f}€ | {l.title[:70]:70} | " + (f"{m.label} → R2G {d.valutazione:.2f}€ guad {d.guadagno:+.2f}" if d else "—"))
        return

    db = DB(DATA / "affari.sqlite")
    sources = {n: SOURCES[n](c) for n, c in cfg["siti"].items() if c.get("attivo")}
    print("Siti attivi:", ", ".join(sources) or "nessuno")
    if not sources:
        print('Nessun sito attivo: metti "attivo": true per almeno un sito in config.json')
        return
    n = 0
    while True:
        t0 = time.time()
        try:
            if time.time() - listino.get("updated", 0) > cfg["listino_ore_validita"] * 3600:
                listino = r2g.load_listino(lpath, cfg["listino_ore_validita"]); idx = GameIndex(listino["games"])
            k = giro(cfg, listino, idx, db, sources, n)
            dashboard.genera(db, listino, DATA / "dashboard.html")
            print(f"[{time.strftime('%H:%M')}] giro {n} completato in {time.time()-t0:.0f}s, nuovi affari: {k}")
        except Exception:
            traceback.print_exc()
        n += 1
        if a.una_volta:
            break
        time.sleep(max(30, cfg["intervallo_minuti"] * 60 - (time.time() - t0)))


if __name__ == "__main__":
    main()
