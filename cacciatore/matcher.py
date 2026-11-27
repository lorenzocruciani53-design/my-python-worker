"""Riconosce cosa vende un annuncio (console o gioco del listino) e le sue condizioni."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field


def norm(text: str) -> str:
    t = unicodedata.normalize("NFD", str(text or ""))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn").lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


# --------------------------------------------------------------------------
# Console: regole (chiave listino, parole obbligatorie, parole escluse). Vince la prima.
# --------------------------------------------------------------------------
_PS5 = r"(ps5|playstation 5|play station 5)"
_PS4 = r"(ps4|playstation 4|play station 4)"
_DIG = r"(digital|digitale|all digital|senza lettore)"
CONSOLE_RULES: list[tuple[str, list[str], list[str]]] = [
    ("ps5Pro", [_PS5, r"\bpro\b"], []),
    ("ps5SlimDigital", [_PS5, r"slim", _DIG], []),
    ("ps5SlimDisc", [_PS5, r"slim"], []),
    ("ps5FatDigital", [_PS5, _DIG], []),
    ("ps5FatDisc", [_PS5], [r"slim"]),
    ("playstationPortal", [r"portal"], []),
    ("ps4Pro1TB", [_PS4, r"\bpro\b"], []),
    ("ps4Slim1TB", [_PS4, r"slim", r"(1 ?tb|1000 ?gb)"], []),
    ("ps4Slim500GB", [_PS4, r"slim"], []),
    ("ps4Fat1TB", [_PS4, r"(1 ?tb|1000 ?gb)"], []),
    ("ps4Fat500GB", [_PS4], []),
    ("ps3SuperSlim", [r"(ps3|playstation 3)", r"super ?slim"], []),
    ("ps3Slim", [r"(ps3|playstation 3)"], []),
    ("ps2Slim", [r"(ps2|playstation 2)", r"slim"], []),
    ("ps2Fat", [r"(ps2|playstation 2)"], []),
    ("ps1ClassicMini", [r"(ps1|playstation)", r"(classic|mini)"], []),
    ("xboxSeriesXDigital", [r"series x", _DIG], []),
    ("xboxSeriesXDisc", [r"series x"], []),
    ("xboxSeriesS1TB", [r"series s", r"(1 ?tb|carbon|black|nera)"], []),
    ("xboxSeriesS500GB", [r"series s"], []),
    ("xboxOneX1TB", [r"xbox one x"], []),
    ("xboxOneS1TBDigital", [r"xbox one s", _DIG, r"1 ?tb"], []),
    ("xboxOneS500GBDigital", [r"xbox one s", _DIG], []),
    ("xboxOneS1TBDisc", [r"xbox one s", r"1 ?tb"], []),
    ("xboxOneS500GBDisc", [r"xbox one s"], []),
    ("switch2", [r"switch 2|switch2"], []),
    ("switch1OLED", [r"switch", r"oled"], []),
    ("switchLite", [r"switch", r"lite"], []),
    ("switch1Standard", [r"(nintendo switch|switch)"], [r"lite", r"oled"]),
    ("wiiU", [r"wii ?u\b"], []),
    ("wii", [r"\bwii\b"], []),
    ("gameCube", [r"game ?cube"], []),
    ("newThreeDSXL", [r"new", r"3ds", r"xl|ll"], []),
    ("newTwoDSXL", [r"new", r"2ds", r"xl|ll"], []),
    ("newThreeDS", [r"new", r"3ds"], []),
    ("threeDSXL", [r"3ds", r"xl|ll"], []),
    ("threeDS", [r"3ds"], []),
    ("twoDS", [r"2ds"], []),
    ("dsiXL", [r"\bdsi\b", r"xl|ll"], []),
    ("dsi", [r"\bdsi\b"], []),
    ("dsLite", [r"\bds\b", r"lite"], []),
    ("ds", [r"nintendo ds\b"], []),
    ("psVita", [r"(ps ?vita|playstation vita)"], []),
    ("psp", [r"\bpsp\b"], []),
    ("gameBoyMicro", [r"game ?boy", r"micro"], []),
    ("gameBoyAdvanceSP", [r"(game ?boy advance|gba)", r"\bsp\b"], []),
    ("gameBoyAdvance", [r"(game ?boy advance|\bgba\b)"], []),
    ("gameBoyColor", [r"game ?boy color"], []),
    ("gameBoyPocket", [r"game ?boy pocket"], []),
    ("gameBoyClassic", [r"game ?boy"], []),
    ("snesMini", [r"snes", r"mini|classic"], []),
    ("nesMini", [r"\bnes\b", r"mini|classic"], []),
]

# Se compaiono queste parole l'annuncio NON è una console intera
NOT_A_CONSOLE = [r"\bgioc(o|hi)\b", r"\bgame\b(?! ?boy| ?cube)", r"\bcustodia\b", r"\bcover\b",
                 r"\bskin\b", r"\bsolo (scatola|controller|joy ?con|pad|dock|cavo|alimentatore)",
                 r"\bscatola vuota\b", r"\bvolante\b", r"\bcuffie\b", r"\bheadset\b", r"\bcaricatore\b(?!.*console)",
                 r"\bstand\b", r"\bsupporto\b", r"\bdock\b(?!.*(console|switch completa))",
                 r"\bcartuccia\b", r"\bamiibo\b", r"\bguida\b", r"\bpelle\b", r"\bbatteria\b",
                 # memorie, protezioni, ricambi, gadget
                 r"\bmemory\b", r"\bmemoria\b", r"\bmicro ?sd\b", r"\bsd ?card\b", r"\bscheda\b", r"\bsdxc\b", r"\bsdhc\b",
                 r"\bhard ?disk\b", r"\bssd\b", r"\bpellicol", r"\bvetro temperato\b", r"\bprotezion", r"\badesiv",
                 r"\bricambi?\b", r"\bpezzi di ricambio\b", r"\bscocca\b", r"\bshell\b", r"\bventol", r"\blettore (di )?ricambio\b",
                 r"\bborsa\b", r"\bzaino\b", r"\bastuccio\b", r"\bbauletto\b", r"\btracolla\b",
                 r"\badattatore\b", r"\bconvertitore\b", r"\bprolunga\b", r"\bstazione di ricarica\b", r"\bbasetta\b",
                 r"\btelecomando\b", r"\bnunchu", r"\bsensor ?bar\b", r"\btastiera\b", r"\bmouse\b",
                 r"\bwebcam\b", r"\btelecamera\b", r"\bcamera\b", r"\bmicrofono\b", r"\bpsvr\b", r"\bvisore\b",
                 r"\bmanuale\b", r"\bposter\b", r"\bfigure\b", r"\bfunko\b", r"\bpeluche\b", r"\bgadget\b",
                 r"\bmaglia", r"\bfelpa\b", r"\btazza\b", r"\blampada\b", r"\bportachiavi\b", r"\bcarta regalo\b",
                 r"\bgift ?card\b", r"\bricarica\b", r"\babbonamento\b", r"\bcodice\b", r"\bvoucher\b",
                 r"\bminiatura\b", r"\bmodellino\b", r"\breplica\b", r"\bportatile per\b", r"\bkit\b",
                 # titolo che INIZIA con un accessorio ("Controller PS5…", "Cavo HDMI per…")
                 r"^(controller|joy ?con|pad|joypad|dualsense|dualshock|gamepad|cavo|cavi|caricatore|alimentatore"
                 r"|dock|base|supporto|grip|volante|custodia|cover|case|borsa|memory|scheda|batteria|accessori)\b",
                 # "… per Nintendo Switch / per PS5": accessorio o gioco PER la console, non la console
                 r"\bper (la |il )?(nintendo |sony |microsoft )?(switch|ps ?[1-5]|playstation|xbox|wii|3ds|2ds|nds|ds|psp|vita|game ?boy|gamecube)\b(?!.*\bconsole\b)",
                 r"\bcompatibil", r"\bnon original"]

PLATFORM_HINTS = {
    "Playstation 5": [_PS5], "Playstation 4": [_PS4], "Playstation 3": [r"(ps3|playstation 3)"],
    "Playstation 2": [r"(ps2|playstation 2)"], "Playstation 1": [r"(ps1|psx|playstation 1|psone)"],
    "Switch": [r"switch"], "Wii e Wii U": [r"\bwii\b", r"wii ?u"], "3DS e 2DS": [r"[23]ds"],
    "DS, Lite e DSi": [r"\bnds\b", r"nintendo ds\b", r"\bds\b"], "Game Boy": [r"game ?boy", r"\bgba?\b", r"\bgbc\b"],
    "GameCube": [r"game ?cube", r"\bngc\b"], "PSP": [r"\bpsp\b"], "Playstation Vita": [r"vita"],
    "Xbox One": [r"xbox one"], "Xbox 360": [r"xbox 360"], "Xbox Series X e S": [r"series [xs]"],
}

# --------------------------------------------------------------------------
# Condizioni dedotte dal testo
# --------------------------------------------------------------------------
RED_FLAGS = {  # annuncio da scartare o quasi
    "non funzionante": r"non funzion|guast|rott[ao]\b|difettos|per pezzi|ricambi|\bko\b|da riparare|non si accende|non legge|blue ?light|ylod|rrod|drift grave",
    "bannata/modificata": r"bannat|\bban\b|modificat|modded|jailbreak|\bcfw\b|chip(pata)?\b|sbloccat|hackat",
    "solo scatola/vuoto": r"solo (la )?scatola|scatola vuota|custodia vuota|solo custodia|box vuot|solo (la )?cover|senza (il )?(gioco|disco|cartuccia)\b",
    "non originale": r"\breplica\b|\bcopia\b|\bfalso\b|\bfake\b|repro|non originale|\br4\b|backup",
    "digitale/codice": r"codice|\bkey\b|digital code|download|account",
    "non è una vendita": r"\bcerco\b|\bscambio\b|\bpermut|\baffitto\b|\bnoleggio\b|\bcompro\b",
}
YELLOW_FLAGS = {
    "senza accessori": r"senza (cavi|alimentatore|caricatore|dock|accessori|controller|pad|joy ?con|sensore|gamepad)|solo console",
    "senza custodia": r"senza (custodia|scatola|confezione|box)|solo (il )?(disco|cartuccia)|loose|sfus[oi]",
    "danni estetici": r"graff|ammacc|crep|segni|usura|scheggi|ingiallit",
    "lotto": r"\blott[oi]\b|\bstock\b|\bbundle\b|\bpacchetto\b",
}


@dataclass
class Match:
    kind: str                       # "console" | "gioco"
    key: str                        # chiave console o id gioco
    label: str
    platform: str = ""
    confidence: float = 0.0
    red_flags: list[str] = field(default_factory=list)
    yellow_flags: list[str] = field(default_factory=list)
    has_box: bool = True
    accessories_complete: bool = True
    damaged: bool = False
    n_controllers: int = 0
    shop_price: float = 0.0
    grado: str = ""                                   # grado stimato (listino Excel)
    note_valutazione: list[str] = field(default_factory=list)


def flags(text: str) -> tuple[list[str], list[str]]:
    t = norm(text)
    red = [k for k, rx in RED_FLAGS.items() if re.search(rx, t)]
    yel = [k for k, rx in YELLOW_FLAGS.items() if re.search(rx, t)]
    return red, yel


def _count_controllers(t: str) -> int:
    m = re.search(r"(\d|due|tre|quattro)\s*(controller|pad|joystick|dualsense|dualshock|joypad)", t)
    if m:
        return {"due": 2, "tre": 3, "quattro": 4}.get(m.group(1), None) or int(m.group(1))
    if re.search(r"\bcon (il )?(controller|pad|joypad|dualsense|dualshock)\b|\+ ?(controller|pad)", t):
        return 1
    return 0


# Parole ammesse nel titolo di una console. Se il titolo contiene altre parole
# (es. "Zelda", "Sonic", "Tetris", "balance") quasi sempre è un gioco o altro.
CONSOLE_VOCAB = set("""
nintendo switch oled lite ps5 ps4 ps3 ps2 ps1 psx psone playstation play station sony xbox microsoft series one
wii gamecube game cube boy gameboy gb gbc gba color colour advance sp micro pocket classic mini nes snes famicom
entertainment system super ds dsi nds xl ll 3ds 2ds new vita psp portal pro slim fat digital digitale edition edizione
standard disco dischi disc lettore drive all tb gb t g modello versione v1 v2 cfi console handheld portatile
usata usato usate nuova nuovo nuove come perfetta perfetto perfette ottime ottimo ottima ottimi buone buono buona
condizioni stato funzionante funzionanti funziona tutto tutti completa completo completi originale originali garanzia
scontrino imballo scatola box confezione cavi cavo accessori alimentatore caricatore caricabatterie dock hdmi grip
joycon joy con controller controllers pad joypad dualsense dualshock gamepad sensore barra telecomando telecomandi
bianca bianco nera nero rossa rosso blu azzurra azzurro turchese grigia grigio corallo gialla giallo verde viola
rosa neon argento oro trasparente black white red blue grey gray negra negro blanca blanco roja azul
con e ed da in di del della dei la il lo i gli le senza vendo vendesi vende offerta occasione prezzo trattabile
spedizione spedita inclusa incluso inclusi ritiro mano a regalo pari al poco pochissimo mai usata
pal ita italiana europea eu memoria interna base
galaxy limited special speciale
non si accende rotta rotto rotti difettosa difettoso guasta guasto drift bannata bannato modificata modificato
cfw jailbreak pezzi per legge v1 v2 hackerata sbloccata
""".split())


def solo_parole_da_console(t: str) -> bool:
    for w in t.split():
        if w in CONSOLE_VOCAB or w.isdigit() or len(w) <= 1 or re.fullmatch(r"\d+(gb|tb|t|g|mb)", w):
            continue
        return False
    return True


def match_console(title: str, description: str = "") -> Match | None:
    t = norm(title)
    if any(re.search(rx, t) for rx in NOT_A_CONSOLE):
        return None
    if not solo_parole_da_console(t):
        return None
    for key, must, excl in CONSOLE_RULES:
        if all(re.search(rx, t) for rx in must) and not any(re.search(rx, t) for rx in excl):
            full = norm(f"{title} {description}")
            red, yel = flags(full)
            return Match(kind="console", key=key, label=key, confidence=0.9,
                         red_flags=red, yellow_flags=yel,
                         accessories_complete="senza accessori" not in yel,
                         damaged=bool(re.search(r"crep|rott|ammacc|scheggi", full)),
                         n_controllers=_count_controllers(full))
    return None


# --------------------------------------------------------------------------
# Giochi: indice per token sul catalogo R2G
# --------------------------------------------------------------------------
_STOP = set("gioco giochi per il lo la i gli le di da del della dei con e ed a in edition edizione "
            "nintendo sony playstation ps1 ps2 ps3 ps4 ps5 xbox one switch wii u ds 3ds 2ds nds psp vita "
            "game boy advance gba gamecube pal ita italiano completo originale usato nuovo ottime "
            "condizioni sigillato the of".split())


def _tokens(text: str) -> list[str]:
    return [w for w in norm(text).split() if w not in _STOP and len(w) > 1]


# Oggetti legati a un gioco ma che NON sono il gioco
NON_UN_GIOCO = [r"\bmanual[ei]\b", r"\bsolo (la )?(custodia|scatola|cover|copertina|box|libretto)\b",
                r"\bcustodia vuota\b", r"\bscatola vuota\b", r"\bbox vuot", r"\bguida (strategica|ufficiale)\b",
                r"\bartbook\b", r"\bposter\b", r"\bsteelbook\b", r"\bcolonna sonora\b", r"\bsoundtrack\b",
                r"\bost\b", r"\bamiibo\b", r"\bfigure\b", r"\bfunko\b", r"\bpeluche\b", r"\bmaglietta\b",
                r"\blibretto\b", r"\bcopertina\b", r"\binsert\b", r"\bcodice\b", r"\bdlc\b", r"\bkey\b",
                r"\bdigitale\b", r"\bdownload\b", r"\brepro\b", r"\breplica\b", r"\bfalso\b", r"\bcopia\b"]


class GameIndex:
    def __init__(self, games: list[dict]):
        self.games = games
        self.by_token: dict[str, set[int]] = {}
        self.tok = []
        for i, g in enumerate(games):
            toks = set(_tokens(g["name"]))
            self.tok.append(toks)
            for w in toks:
                self.by_token.setdefault(w, set()).add(i)

    def platform_of(self, text: str) -> str | None:
        t = norm(text)
        for plat, rxs in PLATFORM_HINTS.items():
            if any(re.search(rx, t) for rx in rxs):
                return plat
        return None

    def match(self, title: str, description: str = "", platform_hint: str | None = None) -> Match | None:
        if any(re.search(rx, norm(title)) for rx in NON_UN_GIOCO):
            return None
        plat = platform_hint or self.platform_of(title) or self.platform_of(description)
        q = set(_tokens(title))
        if not q:
            return None
        cand: set[int] = set()
        for w in q:
            cand |= self.by_token.get(w, set())
        best, best_s = None, 0.0
        for i in cand:
            g = self.games[i]
            if plat and g["platform"] != plat:
                continue
            gt = self.tok[i]
            if not gt:
                continue
            inter = len(gt & q)
            recall = inter / len(gt)            # parole del gioco presenti nell'annuncio
            precision = inter / len(q)
            s = 0.75 * recall + 0.25 * precision
            # numeri (es. "fifa 23" vs "fifa 22") devono coincidere
            nums_g = {w for w in gt if w.isdigit()}
            if nums_g and not nums_g <= q:
                s -= 0.5
            if s > best_s:
                best, best_s = g, s
        if not best or best_s < 0.72 or not plat:
            return None
        full = f"{title} {description}"
        red, yel = flags(full)
        return Match(kind="gioco", key=str(best["id"]), label=best["name"], platform=best["platform"],
                     confidence=round(best_s, 2), red_flags=red, yellow_flags=yel,
                     has_box="senza custodia" not in yel, shop_price=best["shop_price"])


def riconosci(title: str, description: str, idx: "GameIndex", listino: dict,
              platform_hint: str | None = None) -> Match | None:
    """Decide se l'annuncio è una console o un gioco del catalogo."""
    c = match_console(title, description)
    g = idx.match(title, description, platform_hint or None)
    if g and (not c or (g.confidence >= 0.8 and not re.search(r"\bconsole\b", norm(title)))):
        return g
    if c:
        c.label = listino["config"]["consoles"].get(c.key, {}).get("label", c.key)
    return c
