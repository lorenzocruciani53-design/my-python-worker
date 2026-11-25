"""Notifiche Telegram a uno o più bot.

Configurazione (variabile d'ambiente, NON nel codice: il repository è pubblico):
  TELEGRAM_BOTS = [{"nome": "Lorenzo", "token": "123:ABC", "chat_id": 111},
                   {"nome": "Simone",  "token": "456:DEF", "chat_id": 222}]
In alternativa, per un solo bot: TELEGRAM_TOKEN + TELEGRAM_CHAT_ID (anche più id separati da virgola).
"""
from __future__ import annotations

import html
import json
import os

import requests


# Nomi dei bot attivi (da config.json "notifiche_solo_a"). Vuoto = tutti.
SOLO_A: list[str] = []


def destinatari() -> list[tuple[str, str]]:
    """Lista di (token, chat_id)."""
    out = []
    solo = {n.strip().lower() for n in SOLO_A if n.strip()}
    raw = os.environ.get("TELEGRAM_BOTS", "").strip()
    if raw:
        try:
            for b in json.loads(raw):
                if solo and str(b.get("nome", "")).strip().lower() not in solo:
                    continue
                if b.get("token") and b.get("chat_id"):
                    out.append((str(b["token"]).strip(), str(b["chat_id"]).strip()))
        except (json.JSONDecodeError, AttributeError, TypeError):
            print("[Telegram] TELEGRAM_BOTS non è un JSON valido")
    tok = os.environ.get("TELEGRAM_TOKEN", "").strip()
    for cid in os.environ.get("TELEGRAM_CHAT_ID", "").split(","):
        if tok and cid.strip():
            out.append((tok, cid.strip()))
    return list(dict.fromkeys(out))


DOMANDE_CONSOLE = ("Funziona tutto (lettore, wifi, audio/video)? · È bannata o modificata? · "
                   "Accessori e controller sono originali? · Mi mandi una foto da accesa?")
DOMANDE_GIOCO = "Il disco/cartuccia è senza graffi profondi? · Custodia e copertina sono originali?"


def formatta(d) -> str:
    l, m = d.listing, d.match
    e = html.escape
    vend = l.seller_name or "venditore"
    if l.seller_rating is not None:
        vend += f" · {l.seller_rating*100:.0f}% positivi"
    if l.seller_reviews is not None:
        vend += f" · {l.seller_reviews} recensioni"
    righe = [
        f"<b>{e(d.livello)} +{d.guadagno:.2f} € ({d.roi*100:.0f}%)</b>  ·  {l.site.upper()}",
        f"<b>{e(l.title[:120])}</b>",
        f"🎯 Riconosciuto: {e(m.label)}{' · ' + e(m.platform) if m.platform else ''}",
        f"💶 Costo totale: <b>{d.costo_totale:.2f} €</b> (prezzo {l.price:.2f})",
        f"🏷 Ti danno: <b>{d.valutazione:.2f} €</b> PayPal · {d.valutazione_credito:.2f} € credito (+{d.guadagno_credito:.2f})",
        f"⭐ {e(vend)}  ·  affidabilità {d.affidabilita}/100  ·  rischio {d.rischio}/100",
        f"📊 Punteggio affare: <b>{d.punteggio}/100</b>",
    ]
    if d.note:
        righe.append("📝 " + e(" · ".join(d.note[:7])))
    righe.append("❓ Chiedi al venditore: " + e(DOMANDE_CONSOLE if m.kind == "console" else DOMANDE_GIOCO))
    righe.append(f'<a href="{e(l.url)}">👉 Apri annuncio</a>')
    return "\n".join(righe)


def _send(token: str, chat_id: str, testo: str, foto: str = "") -> bool:
    try:
        if foto and len(testo) <= 1024:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", timeout=20,
                              data={"chat_id": chat_id, "photo": foto, "caption": testo, "parse_mode": "HTML"})
            if r.ok:
                return True
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", timeout=20,
                          data={"chat_id": chat_id, "text": testo, "parse_mode": "HTML"})
        if not r.ok:
            print(f"[Telegram] errore {r.status_code}: {r.text[:200]}")
        return r.ok
    except requests.RequestException as ex:
        print(f"[Telegram] errore: {ex}")
        return False


def invia(d) -> bool:
    testo, dest = formatta(d), destinatari()
    if not dest:
        print("\n[NOTIFICA]\n" + testo + "\n")
        return False
    return all([_send(t, c, testo, d.listing.image) for t, c in dest])


def messaggio(testo: str):
    dest = destinatari()
    if not dest:
        print(testo); return
    for t, c in dest:
        _send(t, c, html.escape(testo))
