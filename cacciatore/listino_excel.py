"""Listino console dal file Excel di Ready2Game (es. 'Listino 1 Dicembre').

Per ogni console legge:
  - prezzo per grado: eccellente / molto_buono / buono / accettabile ("—" = non ritirano)
  - dotazione obbligatoria (senza, NON ritirano la console)
  - detrazioni per ogni accessorio mancante
  - valore di ogni controller originale in più

Per aggiornare il listino basta sostituire data/listino_console.xlsx con il nuovo file.
"""
from __future__ import annotations

import re
from pathlib import Path

GRADI = ["eccellente", "molto_buono", "buono", "accettabile"]

# nome modello nell'Excel -> chiave usata dal riconoscimento annunci (matcher.py)
MODELLI = {
    "Switch Standard 32 GB": "switch1Standard", "Switch Lite 32 GB": "switchLite",
    "Switch OLED 64 GB": "switch1OLED", "Switch 2 Standard 256 GB": "switch2",
    "Wii": "wii", "Wii U": "wiiU", "GameCube": "gameCube",
    "2DS": "twoDS", "3DS": "threeDS", "3DS XL": "threeDSXL", "New 2DS XL": "newTwoDSXL",
    "New 3DS": "newThreeDS", "New 3DS XL": "newThreeDSXL",
    "DS": "ds", "DS Lite": "dsLite", "DSi": "dsi", "DSi XL": "dsiXL",
    "Game Boy Classic": "gameBoyClassic", "Game Boy Pocket": "gameBoyPocket",
    "Game Boy Color": "gameBoyColor", "Game Boy Advance": "gameBoyAdvance",
    "Game Boy Advance SP": "gameBoyAdvanceSP", "Game Boy Micro": "gameBoyMicro",
    "NES Mini": "nesMini", "SNES Mini": "snesMini",
    "PS4 Slim 500 GB": "ps4Slim500GB", "PS4 Slim 1 TB": "ps4Slim1TB",
    "PS4 Fat 500 GB": "ps4Fat500GB", "PS4 Fat 1 TB": "ps4Fat1TB", "PS4 Pro 1 TB": "ps4Pro1TB",
    "PS5 Standard Digital 825 GB": "ps5FatDigital", "PS5 Standard Disco 825 GB": "ps5FatDisc",
    "PS5 Slim Digital 825 GB / 1 TB": "ps5SlimDigital", "PS5 Slim Disco 825 GB / 1 TB": "ps5SlimDisc",
    "PS5 Pro 2 TB": "ps5Pro", "PS1 Classic Mini": "ps1ClassicMini",
    "PS2 Fat": "ps2Fat", "PS2 Slim": "ps2Slim", "PS3 Slim": "ps3Slim", "PS3 Super Slim": "ps3SuperSlim",
    "PlayStation Portal": "playstationPortal", "PS Vita (tutte le versioni)": "psVita",
    "PSP (tutte le versioni)": "psp",
    "Xbox Series S 512 GB": "xboxSeriesS500GB", "Xbox Series S 1 TB": "xboxSeriesS1TB",
    "Xbox Series X Disco 1 TB": "xboxSeriesXDisc", "Xbox Series X Digital 1 TB": "xboxSeriesXDigital",
    "Xbox One S 1 TB Disco": "xboxOneS1TBDisc", "Xbox One S 500 GB All-Digital": "xboxOneS500GBDigital",
    "Xbox One S 1 TB All-Digital": "xboxOneS1TBDigital", "Xbox One S 500 GB Disco": "xboxOneS500GBDisc",
    "Xbox One X 1 TB": "xboxOneX1TB",
}


def _num(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    m = re.search(r"\d+(?:[.,]\d+)?", str(v))
    return float(m.group().replace(",", ".")) if m else None


def _detrazioni(txt) -> dict[str, float]:
    out = {}
    for riga in str(txt or "").splitlines():
        m = re.match(r"\s*(.+?)\s*[−-]\s*(\d+(?:[.,]\d+)?)\s*€", riga)
        if m:
            out[m.group(1).strip()] = float(m.group(2).replace(",", "."))
    return out


def _obbligatori(txt) -> list[str]:
    return [r.split(":", 1)[1].strip() for r in str(txt or "").splitlines()
            if r.lower().startswith("obbligatorio")]


def _controller(txt) -> dict | None:
    t = str(txt or "")
    m = re.match(r"\s*(.+?)\s*·\s*\+\s*(\d+(?:[.,]\d+)?)\s*€", t)
    return {"label": m.group(1), "price": float(m.group(2).replace(",", "."))} if m else None


def leggi_excel(path: Path) -> dict[str, dict]:
    import openpyxl

    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb["Prezzi console"] if "Prezzi console" in wb.sheetnames else wb.worksheets[0]
    console, header = {}, None
    for row in ws.iter_rows(values_only=True):
        if header is None:
            if row and row[0] == "Marca" and row[1] == "Modello":
                header = row
            continue
        if not row or not row[1]:
            continue
        nome = str(row[1]).strip()
        key = MODELLI.get(nome)
        if not key:
            print(f"[listino] modello non riconosciuto nell'Excel, ignorato: {nome!r}")
            continue
        console[key] = {
            "label": nome, "marca": row[0],
            "gradi": {g: _num(row[2 + i]) for i, g in enumerate(GRADI)},
            "dotazione": str(row[6] or ""),
            "obbligatori": _obbligatori(row[6]),
            "detrazioni": _detrazioni(row[7]),
            "controller": _controller(row[8]),
        }
    return console


# --------------------------------------------------------------------------
# Accessori: come si riconoscono nel testo dell'annuncio
# --------------------------------------------------------------------------
ACCESSORI_RX = {
    "dock": r"dock", "alimentatore": r"alimentator|caricator|cavo (di )?alimentazione|trasformator",
    "hdmi": r"hdmi", "grip": r"grip|impugnatura", "caricatore": r"caricator|alimentator",
    "cavo video": r"cavo (video|av|scart|component)|cavi video|\bscart\b|cavo av",
    "barra sensore": r"sensore|sensor ?bar|barra",
    "joy-con": r"joy ?-?con", "gamepad": r"gamepad|game pad|tablet|controller wii u",
}
_SENZA = r"(senza|manca|mancante|mancano|privo di|priva di|no)\s+(il |l |la |lo |i |gli |le )?"


def accessorio_chiave(nome: str) -> str | None:
    n = nome.lower()
    for k in ACCESSORI_RX:
        if k.split()[0] in n:
            return k
    if "cavo hdmi" in n:
        return "hdmi"
    if "joy-con" in n:
        return "joy-con"
    return None


def manca(testo_norm: str, chiave: str) -> bool:
    rx = ACCESSORI_RX.get(chiave)
    if not rx:
        return False
    return bool(re.search(_SENZA + r"(\w+ ){0,3}?(" + rx + ")", testo_norm))


def solo_console(testo_norm: str) -> bool:
    return bool(re.search(r"\bsolo (la )?console\b|\bsolo corpo\b|\bconsole sola\b|\bsolo unita\b", testo_norm))


def grado_da_testo(testo_norm: str, condizione_sito: str = "") -> str:
    """Stima prudente del grado. Mai 'eccellente': non vedi l'oggetto prima di spedirlo."""
    t = f"{testo_norm} {condizione_sito.lower()}"
    if re.search(r"accettabil|discret|usurat|molto usat|segni evidenti|graffi profond|ingiallit|rovinat|scheggi", t):
        return "accettabile"
    if re.search(r"come nuov|pari al nuovo|perfett|ottim|nuovo con|mai usat|sigillat|immacolat|molto buon|eccellent|new with|as new", t):
        return "molto_buono"
    return "buono"


def valuta_console(item: dict, testo_norm: str, condizione_sito: str = "",
                   n_controller: int = 0) -> tuple[float | None, str, list[str]]:
    """(valutazione, grado, note). valutazione None = Ready2Game non la ritira / da scartare."""
    note = []
    grado = grado_da_testo(testo_norm, condizione_sito)
    prezzo = item["gradi"].get(grado)
    if prezzo is None:
        # grado senza offerta: prova il grado sotto (più prudente)
        for g in ["buono", "accettabile"]:
            if item["gradi"].get(g) is not None and GRADI.index(g) >= GRADI.index(grado):
                grado, prezzo = g, item["gradi"][g]
                break
    if prezzo is None:
        # es. Switch 2 o Portal: quotati solo "Molto Buono"
        if grado == "buono" and item["gradi"].get("molto_buono") is not None:
            grado, prezzo = "molto_buono", item["gradi"]["molto_buono"]
            note.append("⚠️ quotata solo in grado Molto Buono: verifica le condizioni")
        else:
            return None, grado, [f"Ready2Game non ritira questo modello in grado {grado}"]
    for ob in item["obbligatori"]:
        k = accessorio_chiave(ob)
        if (k and manca(testo_norm, k)) or (k == "joy-con" and re.search(r"joy ?con.{0,20}(rott|drift|non funz|compatibil|non original)", testo_norm)) \
                or (k == "joy-con" and solo_console(testo_norm)):
            return None, grado, [f"🚫 manca obbligatorio: {ob}"]
    sola = solo_console(testo_norm)
    for nome, euro in item["detrazioni"].items():
        k = accessorio_chiave(nome)
        if sola or (k and manca(testo_norm, k)):
            prezzo -= euro
            note.append(f"−{euro:.0f} € manca {nome}")
    if item.get("controller") and n_controller > 0:
        prezzo += n_controller * item["controller"]["price"]
        note.append(f"+{n_controller * item['controller']['price']:.0f} € {n_controller}× {item['controller']['label']}")
    if prezzo <= 0:
        return None, grado, note + ["valutazione a 0 € dopo le detrazioni"]
    return round(prezzo, 2), grado, note
