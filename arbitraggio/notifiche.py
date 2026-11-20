"""Notifiche Telegram. Variabili d'ambiente: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID (anche più id separati da virgola)."""
from __future__ import annotations

import html
import os

import requests


def _chat_ids() -> list[str]:
    return [c.strip() for c in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if c.strip()]


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
        f"🏷 Valutazione R2G: <b>{d.valutazione:.2f} €</b> PayPal · {d.valutazione_credito:.2f} € credito (+{d.guadagno_credito:.2f})",
        f"⭐ {e(vend)}  ·  affidabilità {d.affidabilita}/100  ·  rischio {d.rischio}/100",
        f"📊 Punteggio affare: <b>{d.punteggio}/100</b>",
    ]
    if d.note:
        righe.append("📝 " + e(" · ".join(d.note[:6])))
    righe.append(f'<a href="{e(l.url)}">👉 Apri annuncio</a>')
    return "\n".join(righe)


def invia(d) -> bool:
    token, ids = os.environ.get("TELEGRAM_TOKEN"), _chat_ids()
    testo = formatta(d)
    if not token or not ids:
        print("\n[NOTIFICA]\n" + testo + "\n")
        return False
    ok = True
    for cid in ids:
        try:
            if d.listing.image:
                r = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", timeout=20,
                                  data={"chat_id": cid, "photo": d.listing.image, "caption": testo[:1024],
                                        "parse_mode": "HTML"})
                if r.ok:
                    continue
            r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", timeout=20,
                              data={"chat_id": cid, "text": testo, "parse_mode": "HTML",
                                    "disable_web_page_preview": "false"})
            ok &= r.ok
        except requests.RequestException as ex:
            print(f"[Telegram] errore: {ex}"); ok = False
    return ok


def messaggio(testo: str):
    token, ids = os.environ.get("TELEGRAM_TOKEN"), _chat_ids()
    if not token or not ids:
        print(testo); return
    for cid in ids:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", timeout=20,
                          data={"chat_id": cid, "text": testo})
        except requests.RequestException:
            pass
