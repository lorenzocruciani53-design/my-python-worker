"""Test offline: python -m pytest cacciatore -q"""
import json
from pathlib import Path

from cacciatore import analisi, dashboard
from cacciatore import ready2game as r2g
from cacciatore.db import DB
from cacciatore.fonti.base import Listing
from cacciatore.matcher import GameIndex, match_console, riconosci

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
    assert analisi.analizza(truffa, match_console(truffa.title), LISTINO, COSTI) is None  # < 40% del valore: scartato
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


# ---------------- listino Excel (1 dicembre) ----------------
from cacciatore import listino_excel
from cacciatore.matcher import norm

XL = listino_excel.leggi_excel(Path(__file__).parent / "data" / "listino_console.xlsx")
LISTINO_XL = {**LISTINO, "console_excel": XL}


def test_excel_letto_tutto():
    assert len(XL) == 52
    assert XL["switch1Standard"]["gradi"] == {"eccellente": 120, "molto_buono": 115, "buono": 102, "accettabile": 85}
    assert XL["switch1Standard"]["detrazioni"]["Dock originale"] == 25
    assert XL["wiiU"]["obbligatori"] == ["GamePad funzionante"]
    assert XL["ps5SlimDisc"]["controller"]["price"] == 30


def test_esempio_del_listino_switch_senza_dock():
    # "Switch Standard Molto Buono a 115 € ... senza dock originale. La valutazione scende a 90 €."
    v, g, _ = listino_excel.valuta_console(XL["switch1Standard"], norm("Nintendo Switch ottime condizioni senza dock"))
    assert (v, g) == (90, "molto_buono")


def test_obbligatori_e_gradi():
    assert listino_excel.valuta_console(XL["switch1OLED"], norm("Switch OLED solo console"))[0] is None
    assert listino_excel.valuta_console(XL["wiiU"], norm("Wii U senza gamepad"))[0] is None
    assert listino_excel.valuta_console(XL["ps2Slim"], norm("PS2 slim molto usata graffi profondi"))[0] is None  # Accettabile: "—"
    assert listino_excel.valuta_console(XL["ps5SlimDisc"], norm("PS5 slim"), n_controller=1)[0] == 417 + 30


def test_analisi_con_excel():
    l = Listing(site="vinted", id="9", title="Xbox Series X 1TB lettore ottime condizioni", price=300, url="https://x",
                seller_rating=1.0, seller_reviews=80)
    d = analisi.analizza(l, match_console(l.title), LISTINO_XL, COSTI)
    assert d.valutazione == 500 and d.match.label == "Xbox Series X Disco 1 TB"
    assert any("grado stimato" in n for n in d.note)
    l2 = Listing(site="vinted", id="10", title="Nintendo Switch joycon drift", price=50, url="https://x")
    assert analisi.analizza(l2, match_console(l2.title), LISTINO_XL, COSTI) is None


ACCESSORI = [
    "Memory card varia per Nintendo Switch o altro", "Micro SD 128GB per Nintendo Switch",
    "Scheda di memoria Nintendo Switch 256gb", "Pellicola vetro temperato Switch OLED",
    "Cavo HDMI per PS5", "Caricatore Nintendo Switch originale", "Dock Nintendo Switch originale",
    "Base di ricarica DualSense PS5", "Borsa trasporto Nintendo Switch", "Grip joycon Nintendo Switch",
    "Ventola raffreddamento PS4 slim", "Scocca ricambio Game Boy Advance SP", "Telecomando Wii originale",
    "Sensor bar Wii", "Adattatore Switch HDMI", "Hard disk 1TB per PS4", "Kit accessori Switch Lite",
    "Supporto da muro PS5", "Joy-con compatibili Switch", "Funko pop PlayStation", "Tazza PS5 controller",
    "Skin adesiva PS5 slim", "Custodia rigida Switch Lite", "Cuffie Xbox Series X",
    "PS5 Pro ricambi scheda madre", "Gift card PlayStation 50 euro", "Accessori per Nintendo Switch OLED",
]
CONSOLE_VERE = [
    ("Nintendo Switch OLED bianca completa", "switch1OLED"), ("PS5 Slim con lettore 1TB", "ps5SlimDisc"),
    ("Console Nintendo Switch Lite turchese", "switchLite"), ("Xbox Series S 512GB come nuova", "xboxSeriesS500GB"),
    ("PlayStation 4 Pro 1TB + 2 controller", "ps4Pro1TB"), ("Nintendo 3DS XL rosso", "threeDSXL"),
    ("Game Boy Color giallo funzionante", "gameBoyColor"), ("Wii U 32GB nera con gamepad", "wiiU"),
]


def test_accessori_non_sono_console():
    sbagliati = [t for t in ACCESSORI if match_console(t)]
    assert not sbagliati, f"scambiati per console: {sbagliati}"


def test_console_vere_ancora_riconosciute():
    for t, k in CONSOLE_VERE:
        m = match_console(t)
        assert m and m.key == k, (t, m and m.key)


def test_notifiche_solo_a(monkeypatch):
    from cacciatore import notifiche
    monkeypatch.setenv("TELEGRAM_BOTS", '[{"nome":"Lorenzo","token":"a","chat_id":1},{"nome":"Simone","token":"b","chat_id":2}]')
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.setattr(notifiche, "SOLO_A", ["Lorenzo"])
    assert notifiche.destinatari() == [("a", "1")]
    monkeypatch.setattr(notifiche, "SOLO_A", [])
    assert len(notifiche.destinatari()) == 2


# Titoli VERI dal primo giro su GitHub (25/11): giochi e oggetti scambiati per console
DAL_GIRO_FALSI = [
    "WII U THE LEGEND OF ZELDA", "wii u balance", "Juego Extreme G Racing (XG3) - GameCube",
    "Harry Potter e il Calice di Fuoco (GameCube)", "Sonic Mega Collection (Nintendo GameCube)",
    "The Legend Of Zelda Ocarina of Time Nintendo 3ds", "Zero Escape Virtue’s Last Reward 3DS PAL UK",
    "Espositore XL Game Boy + cartucce", "COMPLETO - Tetris - Gameboy", "Scatola protettiva Game Boy Pocket",
    "Bomberman Max (Game Boy Color)", "Accessorio game boy advance sp", "Lotto Skylanders Imaginators PS4 + Portale + 6 pg",
    "Videogiochi ps vita", "Rayman Legends & Origins (PS Vita)", "God of war ita psp", "psp film",
    "Guardiani della Notte: Cronache di Hinokami PS4", "Lote de 12 Videojuegos para PS4", "Balatro switch 1",
    "Pokémon Rosso (Nintendo Game Boy)", "Mini Console Retró Nintendo NES",
]
DAL_GIRO_VERI = ["Nintendo 3DS Rossa", "New Nintendo 2DS XL Negra", "Nintendo 3DS XL Blu", "PS5 Slim da 1 TB",
                 "Ps5 Slim 1T", "Xbox Series X", "Nintendo Classic Mini: NES (Entertainment System)"]


def test_titoli_reali_del_primo_giro():
    assert [t for t in DAL_GIRO_FALSI if match_console(t)] == []
    assert [t for t in DAL_GIRO_VERI if not match_console(t)] == []
