"""Calcolo del guadagno, affidabilità del venditore, rischio e punteggio dell'affare."""
from __future__ import annotations

from dataclasses import dataclass, field

from . import listino_excel
from . import ready2game as r2g
from .fonti.base import Listing
from .matcher import Match, norm


@dataclass
class Deal:
    listing: Listing
    match: Match
    valutazione: float            # pagamento PayPal (base)
    valutazione_credito: float    # credito negozio +10%
    costo_totale: float
    guadagno: float
    guadagno_credito: float
    roi: float
    affidabilita: int             # 0-100 venditore
    rischio: int                  # 0-100 (alto = pericoloso)
    punteggio: int                # 0-100 qualità complessiva
    livello: str                  # "🔥 TOP" | "✅ BUONO" | "👀 DA VERIFICARE"
    note: list[str] = field(default_factory=list)
    mediana_mercato: float | None = None


def valuta(match: Match, listino: dict, listing: Listing | None = None) -> float | None:
    cfg = listino["config"]
    excel = listino.get("console_excel") or {}
    if match.kind == "console" and match.key in excel:
        # listino Excel (per grado, con accessori obbligatori e detrazioni)
        if "bannata/modificata" in match.red_flags or "non funzionante" in match.red_flags:
            return None
        testo = norm(f"{listing.title} {listing.description}") if listing else ""
        cond = str((listing.extra or {}).get("condizione", "")) if listing else ""
        val, grado, note = listino_excel.valuta_console(excel[match.key], testo, cond, match.n_controllers)
        match.grado, match.note_valutazione = grado, note
        if excel[match.key].get("label"):
            match.label = excel[match.key]["label"]
        return val
    if match.kind == "console":
        pricing = cfg["consoles"].get(match.key)
        if not pricing:
            return None
        return r2g.console_price(pricing, cfg["controllers"], damaged=match.damaged,
                                 banned="bannata/modificata" in match.red_flags,
                                 working="non funzionante" not in match.red_flags,
                                 accessories_complete=match.accessories_complete,
                                 n_controllers=match.n_controllers)
    return r2g.game_price(match.shop_price, cfg["games"], match.platform, has_box=match.has_box,
                          disc_ok="non funzionante" not in match.red_flags)


def affidabilita_venditore(l: Listing) -> tuple[int, list[str]]:
    note = []
    if l.seller_rating is None and l.seller_reviews is None:
        return 50, ["recensioni venditore non disponibili"]
    score = 50.0
    if l.seller_rating is not None:
        r = float(l.seller_rating)
        score = 100 * r if r >= 0.9 else 100 * r - 25
        if r < 0.9:
            note.append(f"⚠️ solo {r*100:.0f}% recensioni positive")
    n = l.seller_reviews or 0
    if n == 0:
        score -= 25; note.append("⚠️ venditore senza recensioni")
    elif n < 5:
        score -= 12; note.append(f"venditore con poche recensioni ({n})")
    elif n >= 50:
        score += 5
    return max(0, min(100, round(score))), note


def analizza(l: Listing, m: Match, listino: dict, costi: dict, mediana: float | None = None) -> Deal | None:
    val = valuta(m, listino, l)
    if val is None or val <= 0:
        return None
    # prezzo troppo basso rispetto al valore: quasi sempre accessorio, pezzo rotto o truffa -> scarto
    if val > 15 and l.price < costi.get("prezzo_minimo_perc", 0.40) * val:
        return None
    ship, fee = l.shipping, l.buyer_fee
    dship, dfee = costi["default"](l.price)
    ship = dship if ship is None else ship
    fee = dfee if fee is None else fee
    # spedizione verso Ready2Game: gratuita sopra 50 € (si possono raggruppare più articoli nella batch)
    sped_r2g = 0.0 if val >= r2g.FREE_SHIPPING_FROM else costi["spedizione_verso_r2g"] * costi["quota_spedizione_r2g"]
    costo = round(l.price + ship + fee + sped_r2g, 2)
    credito = round(val * (1 + r2g.CREDIT_BONUS), 2)
    guadagno = round(val - costo, 2)
    roi = guadagno / costo if costo > 0 else 0

    aff, note = affidabilita_venditore(l)
    if m.grado:
        note = [f"grado stimato: {m.grado.replace('_', ' ')}"] + m.note_valutazione + note
    rischio = 0
    if m.red_flags:
        rischio += 60; note += [f"🚫 {f}" for f in m.red_flags]
    for f in m.yellow_flags:
        rischio += 10; note.append(f"⚠️ {f}")
    if l.price < 0.55 * val and val > 30:
        rischio += 20; note.append("⚠️ prezzo molto basso: verifica bene che sia la console intera")
    if m.confidence < 0.85:
        rischio += 10; note.append(f"riconoscimento incerto ({m.confidence:.0%})")
    rischio += max(0, 60 - aff) // 2
    if mediana and l.price < mediana * 0.7:
        note.append(f"💡 {(1 - l.price / mediana):.0%} sotto il prezzo medio visto ({mediana:.0f} €)")
    rischio = min(100, rischio)

    # punteggio: guadagno assoluto + ROI, penalizzato da rischio
    p_g = min(50, guadagno * 1.0)
    p_r = min(30, roi * 60)
    p_a = aff * 0.2
    punteggio = max(0, min(100, round(p_g + p_r + p_a - rischio * 0.6)))

    return Deal(listing=l, match=m, valutazione=val, valutazione_credito=credito, costo_totale=costo,
                guadagno=guadagno, guadagno_credito=round(credito - costo, 2), roi=round(roi, 3),
                affidabilita=aff, rischio=rischio, punteggio=punteggio, livello="", note=note,
                mediana_mercato=mediana)


def classifica(d: Deal, soglie: dict) -> str | None:
    """Restituisce il livello di notifica, o None se non vale la pena."""
    if d.match.red_flags or d.guadagno <= 0:
        return None
    if d.guadagno >= soglie["top_guadagno"] and d.roi >= soglie["top_roi"] and d.rischio <= 30:
        return "🔥 TOP"
    if d.guadagno >= soglie["min_guadagno"] and d.roi >= soglie["min_roi"] and d.rischio <= 50:
        return "✅ BUONO"
    if d.guadagno >= soglie["min_guadagno"] and d.roi >= soglie["min_roi"]:
        return "👀 DA VERIFICARE"
    return None
