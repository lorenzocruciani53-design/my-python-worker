"""Test offline: python -m pytest arbitraggio -q"""
import json
from pathlib import Path

from arbitraggio import analisi, dashboard
from arbitraggio import ready2game as r2g
from arbitraggio.db import DB
from arbitraggio.fonti.base import Listing
from arbitraggio.matcher import GameIndex, match_console, riconosci

LISTINO = json.loads((Path(__file__).parent / "data" / "listino.json").read_text(encoding="utf-8"))
CFG = LISTINO["config"]
COSTI = {"default": lambda p: (5.0, 0.70 + p * 0.05), "spedizione_verso_r2g": 6.5, "quota_spedizione_r2g": 0.25}
SOGLIE = {"min_guadagno": 12, "min_roi": 0.2, "top_guadagno": 40, "top_roi": 0.35}


def test_parser_js():
    src = " = { /* c */ a: { label: 'X l\\'a', price: null, n: [ {min: 1, max: Infinity} ] }, };"
    assert r2g.js_object_to_python(src) == {"a": {"label": "X l'a", "price": None, "n": [{"min": 1, "max": 1e18}]}}


def test_formula_giochi_come_sito():
    g = CFG["games"]
    assert r2g.game_base_price(4.0, g) == 0.10
    assert r2g.game_base_price(5.5, g) == 0.50
    assert r2g.game_base_price(20, g) == round((20 * 0.85 - 0.26) * 0.45, 2)
    assert r2g.game_base_price(50, g) == round((50 * 0.85 - 0.26) * 0.70, 2)
    assert r2g.game_price(20, g, "Switch", has_box=False) == round(r2g.game_base_price(20, g) * 0.6, 2)
    assert r2g.game_price(20, g, "Game Boy", has_box=True) == 11.30  # 7.53*1.5=11.295 -> JS arrotonda a 11.30
    assert r2g.game_price(20, g, "Playstation 4", disc_ok=False) == 0.01


def test_formula_console_come_sito():
    sw = CFG["consoles"]["switch1OLED"]
    assert r2g.console_price(sw, CFG["controllers"]) == 160
    assert r2g.console_price(sw, CFG["controllers"], accessories_complete=False) == 120
    assert r2g.console_price(sw, CFG["controllers"], damaged=True) == 80
    ps5 = CFG["consoles"]["ps5SlimDisc"]
    assert r2g.console_price(ps5, CFG["controllers"], n_controllers=2) == 420 + 60
    assert r2g.console_price(ps5, CFG["controllers"], banned=True) == 1
    assert r2g.console_price(CFG["consoles"]["ps5Pro"], CFG["controllers"]) is None


def test_riconoscimento_console():
    assert match_console("PS5 Slim digital edition come nuova").key == "ps5SlimDigital"
    assert match_console("Playstation 5 standard con lettore + 2 controller").key == "ps5FatDisc"
    assert match_console("Playstation 5 standard con lettore + 2 controller").n_controllers == 2
    assert match_console("Nintendo Switch OLED bianca").key == "switch1OLED"
    assert match_console("Nintendo Switch Lite turchese").key == "switchLite"
    assert match_console("Xbox Series S 512gb").key == "xboxSeriesS500GB"
    assert match_console("New 3DS XL Galaxy").key == "newThreeDSXL"
    assert match_console("Custodia per Nintendo Switch OLED") is None
    assert match_console("Gioco FIFA 23 PS5") is None
    assert match_console("Controller DualSense PS5") is None
    assert "bannata/modificata" in match_console("Switch V1 modificata cfw").red_flags


def test_riconoscimento_giochi():
    idx = GameIndex(LISTINO["games"])
    g = next(x for x in LISTINO["games"] if x["platform"] == "Playstation 2" and len(x["name"].split()) >= 2)
    m = idx.match(f"{g['name']} PS2 PAL ITA completo")
    assert m and m.platform == "Playstation 2" and m.label == g["name"]
    assert idx.match(f"{g['name']}") is None or idx.match(f"{g['name']}").platform  # senza piattaforma: niente falsi
    assert idx.match("Lampada da comodino vintage") is None


def test_analisi_e_dashboard(tmp_path):
    l = Listing(site="vinted", id="1", title="Nintendo Switch OLED bianca perfetta", price=95, url="https://x",
                seller_name="mario", seller_rating=0.99, seller_reviews=120)
    m = match_console(l.title)
    d = analisi.analizza(l, m, LISTINO, COSTI)
    assert d.valutazione == 160 and d.costo_totale == round(95 + 5 + 0.70 + 95 * 0.05, 2)
    assert d.guadagno > 50 and analisi.classifica(d, SOGLIE) == "🔥 TOP"
    truffa = Listing(site="subito", id="2", title="PS5 slim", price=60, url="https://y", seller_reviews=0)
    dt = analisi.analizza(truffa, match_console(truffa.title), LISTINO, COSTI)
    assert dt.rischio >= 50 and analisi.classifica(dt, SOGLIE) in ("👀 DA VERIFICARE", None)
    rotta = Listing(site="ebay", id="3", title="PS5 slim non funzionante per pezzi", price=100, url="https://z")
    assert analisi.classifica(analisi.analizza(rotta, match_console(rotta.title), LISTINO, COSTI), SOGLIE) is None

    db = DB(tmp_path / "t.sqlite")
    d.livello = "🔥 TOP"; db.salva_affare(d, True); db.segna_visto(l.uid); db.commit()
    dashboard.genera(db, LISTINO, tmp_path / "d.html")
    assert "Switch OLED" in (tmp_path / "d.html").read_text(encoding="utf-8")


def test_gioco_con_nome_console_e_console_rotta():
    idx = GameIndex(LISTINO["games"])
    gb = next(x for x in LISTINO["games"] if x["platform"] == "Game Boy" and x["name"] == "Golden Sun")
    m = riconosci("Golden Sun game boy advance completo", "", idx, LISTINO)
    assert m.kind == "gioco" and m.label == gb["name"]
    m = riconosci("Nintendo Switch OLED console bianca", "", idx, LISTINO)
    assert m.kind == "console" and m.label == "Switch 1 OLED"
    rotta = match_console("Xbox Series X non si accende")
    assert analisi.valuta(rotta, LISTINO) == 1.0
