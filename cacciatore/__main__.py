"""Cacciatore di affari per il riacquisto Ready2Game.

Uso:
  python -m cacciatore                 # gira per sempre, un giro ogni N minuti
  python -m cacciatore --una-volta     # un solo giro (per cron / GitHub Actions)
  python -m cacciatore --prova ebay "ps5 slim"   # prova un sito e mostra cosa riconosce
  python -m cacciatore --listino       # forza l'aggiornamento del listino R2G
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
import traceback
from pathlib import Path

from . import ai, analisi, dashboard, listino_excel, notifiche
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


def carica_listino(lpath: Path, ore: float, forza: bool = False) -> dict:
    listino = r2g.download_listino(lpath) if forza else r2g.load_listino(lpath, ore)
    xlsx = DATA / "listino_console.xlsx"
    if xlsx.exists():
        listino["console_excel"] = listino_excel.leggi_excel(xlsx)
    return listino


def build_queries(listino: dict, cfg: dict, giro: int) -> list[tuple[str, float | None, str | None]]:
    """(query, prezzo massimo, piattaforma suggerita)."""
    qs = []
    excel = listino.get("console_excel")
    if excel:
        for key, c in excel.items():
            prezzi = [p for p in c["gradi"].values() if p]
            if prezzi:
                qs.append((console_query(listino["config"]["consoles"].get(key, {}).get("label", c["label"])),
                           max(prezzi) * 1.1, None))
    else:
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
    soglie, trovati, inviati, ai_usate = cfg["soglie"], 0, 0, 0
    max_notifiche = soglie.get("max_notifiche_per_giro", 8)
    conta = {n: {"annunci": 0, "riconosciuti": 0, "errori": 0} for n in sources}
    for q, mx, plat in build_queries(listino, cfg, n_giro):
        for name, src in sources.items():
            try:
                items = src.search(q, mx)
            except Exception as ex:
                print(f"[{name}] '{q}': {ex}")
                conta[name]["errori"] += 1
                continue
            conta[name]["annunci"] += len(items)
            for l in items:
                if db.gia_visto(l.uid):
                    continue
                db.segna_visto(l.uid)
                m = riconosci(l.title, l.description, idx, listino, plat)
                if not m:
                    continue
                conta[name]["riconosciuti"] += 1
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
                # --- controllo intelligente (AI gratuita di GitHub) sugli annunci da notificare ---
                acfg = cfg.get("ai", {})
                if acfg.get("attiva") and d.livello != "👀 DA VERIFICARE" and ai_usate < acfg.get("max_per_giro", 15):
                    ai_usate += 1
                    excel_item = (listino.get("console_excel") or {}).get(m.key) if m.kind == "console" else None
                    v = ai.verifica(d, excel_item, acfg.get("modello", "openai/gpt-4.1-mini"),
                                    acfg.get("modello_gemini", "gemini-2.5-flash"),
                                    acfg.get("modello_locale", "qwen2.5vl:3b"), acfg.get("fornitore", "locale"))
                    if v is None:
                        if acfg.get("obbligatoria", True):
                            d.livello = "👀 DA VERIFICARE"
                            d.note.insert(0, "AI non disponibile: non verificato")
                    else:
                        print(f"  [AI] {v.get('verdetto')} · truffa {v.get('rischio_truffa')} · {v.get('motivo')} | {l.title[:60]}")
                        d.note.insert(0, f"🤖 AI: {v.get('motivo', '')} (rischio truffa {v.get('rischio_truffa', '?')}/100)")
                        if v.get("accessori_mancanti"):
                            d.note.insert(1, "🤖 mancano: " + ", ".join(map(str, v["accessori_mancanti"]))[:120])
                        if v["verdetto"] == "scarta" or v.get("e_il_prodotto_giusto") is False:
                            continue
                        if v["verdetto"] != "compra" or int(v.get("rischio_truffa") or 0) > 40:
                            d.livello = "👀 DA VERIFICARE"
                invia = (d.livello != "👀 DA VERIFICARE" or soglie.get("notifica_da_verificare")) \
                    and inviati < max_notifiche
                ok = bool(invia and notifiche.invia(d))
                inviati += ok
                db.salva_affare(d, notificato=ok)
                print(f"  AFFARE {d.livello} {l.site} {l.price:.2f}€ -> {d.valutazione:.2f}€ (+{d.guadagno:.2f}) "
                      f"{m.label} | {l.title[:70]}{'' if ok else ' [non notificato]'}")
                trovati += 1
            db.commit()
        # una pausa per ricerca (non per sito): i siti sono diversi, il giro resta sotto i 10 minuti
        time.sleep(cfg["pausa_tra_ricerche_sec"] * random.uniform(0.7, 1.4))
    for n, c in conta.items():
        print(f"[{n}] annunci letti: {c['annunci']}, riconosciuti: {c['riconosciuti']}, ricerche fallite: {c['errori']}")
    return trovati


def main():
    ap = argparse.ArgumentParser(description="Cacciatore affari Ready2Game")
    ap.add_argument("--una-volta", action="store_true")
    ap.add_argument("--prova-ai", action="store_true", help="manda un annuncio di prova all'AI e mostra la risposta")
    ap.add_argument("--durata-minuti", type=float, default=0,
                    help="gira in continuo per N minuti e poi si ferma (0 = per sempre)")
    ap.add_argument("--listino", action="store_true")
    ap.add_argument("--prova", nargs=2, metavar=("SITO", "RICERCA"))
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    a = ap.parse_args()

    cfg = json.loads(Path(a.config).read_text(encoding="utf-8"))
    notifiche.SOLO_A = cfg.get("notifiche_solo_a", [])
    lpath = DATA / "listino.json"
    listino = carica_listino(lpath, cfg["listino_ore_validita"], forza=a.listino)
    idx = GameIndex(listino["games"])
    print(f"Listino R2G: {len(listino.get('console_excel') or listino['config']['consoles'])} console"
          f"{' (Excel, per grado)' if listino.get('console_excel') else ''}, {len(listino['games'])} giochi")
    if a.listino:
        return

    if a.prova_ai:
        from .fonti.base import Listing as _L
        from .matcher import riconosci as _ric
        prove = [("Nintendo Switch OLED bianca completa di tutto, perfetta", "Vendo per inutilizzo, dock, joycon, cavi originali. Spedisco.", 150, 40,
                  "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2b/Nintendo_Switch_OLED_Model.jpg/640px-Nintendo_Switch_OLED_Model.jpg"),
                 ("PS5 Slim 1TB come nuova", "contattami su whatsapp, pagamento solo bonifico", 250, 0, ""),
                 ("Nintendo Switch con joycon", "joycon con drift, schermo graffiato, la vendo così", 90, 12, "")]
        for i, (t, desc, p, rec, img) in enumerate(prove):
            l = _L(site="prova", id=str(i), title=t, price=p, url="https://esempio", description=desc,
                   seller_reviews=rec, seller_rating=0.99 if rec else None, image=img)
            m = _ric(t, desc, idx, listino)
            d = m and analisi.analizza(l, m, listino, {"default": lambda x: (5.0, 1.0), **cfg["costi"]})
            if not d:
                print("PROVA AI:", t, "-> già scartato dalle regole (non serve l'AI)")
                continue
            _a = cfg.get("ai", {})
            t0 = time.time()
            print("PROVA AI:", t, "->", ai.verifica(d, (listino.get("console_excel") or {}).get(m.key),
                  _a.get("modello", ""), _a.get("modello_gemini", "gemini-2.5-flash"),
                  _a.get("modello_locale", "qwen2.5vl:3b"), _a.get("fornitore", "locale")), f"({time.time()-t0:.0f}s)")
        # annunci VERI di Wallapop, con le loro foto
        try:
            veri = Wallapop(cfg["siti"]["wallapop"]).search("nintendo switch oled")[:40]
        except Exception as ex:
            veri = []
            print("PROVA AI: ricerca Wallapop fallita:", ex)
        provati = 0
        for l in veri:
            m = _ric(l.title, l.description, idx, listino)
            if not m or m.kind != "console":
                continue
            d = analisi.analizza(l, m, listino, {"default": lambda x: (5.0, 1.0), **cfg["costi"]})
            if not d:
                continue
            t0 = time.time()
            v = ai.verifica(d, (listino.get("console_excel") or {}).get(m.key), "", "",
                            _a.get("modello_locale", "qwen2.5vl:3b"), _a.get("fornitore", "locale"))
            print(f"PROVA AI REALE: {l.price:.0f}€ | {l.title[:60]} | foto: {bool(l.image)} -> {v} ({time.time()-t0:.0f}s)")
            provati += 1
            if provati >= 3:
                break
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
    print("Notifiche a:", ", ".join(cfg.get("notifiche_solo_a") or ["tutti i bot"]))
    _acfg = cfg.get("ai", {})
    if _acfg.get("attiva") and _acfg.get("fornitore", "locale") == "locale":
        print("Controllo AI: locale (" + _acfg.get("modello_locale", "qwen2.5vl:3b") + ") "
              + ("pronta" if ai.locale_disponibile() else "NON raggiungibile -> avvisi con le sole regole"))
    elif _acfg.get("attiva") and os.environ.get("GEMINI_API_KEY"):
        print("Controllo AI: attivo (Google Gemini " + _acfg.get("modello_gemini", "gemini-2.5-flash") + ")")
    elif _acfg.get("attiva") and ai.disponibile():
        print("Controllo AI: attivo (GitHub Models " + _acfg.get("modello", "") + ")")
    else:
        print("Controllo AI: NON attivo -> avvisi con le sole regole")
    if not sources:
        print('Nessun sito attivo: metti "attivo": true per almeno un sito in config.json')
        return
    n, inizio = 0, time.time()
    while True:
        t0 = time.time()
        try:
            if time.time() - listino.get("updated", 0) > cfg["listino_ore_validita"] * 3600:
                listino = carica_listino(lpath, cfg["listino_ore_validita"]); idx = GameIndex(listino["games"])
            k = giro(cfg, listino, idx, db, sources, int(time.time() // 400))  # rotazione giochi legata all'orario
            dashboard.genera(db, listino, DATA / "dashboard.html")
            print(f"[{time.strftime('%H:%M')}] giro {n} completato in {time.time()-t0:.0f}s, nuovi affari: {k}")
        except Exception:
            traceback.print_exc()
        n += 1
        if a.una_volta:
            break
        attesa = max(cfg.get("pausa_minima_tra_giri_sec", 30), cfg["intervallo_minuti"] * 60 - (time.time() - t0))
        if a.durata_minuti and time.time() + attesa + 8 * 60 > inizio + a.durata_minuti * 60:
            print("Tempo a disposizione finito: chiudo (il prossimo turno riparte da qui).")
            break
        time.sleep(attesa)


if __name__ == "__main__":
    main()
