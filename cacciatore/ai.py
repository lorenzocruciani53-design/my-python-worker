"""Controllo intelligente degli annunci con un modello AI gratuito (GitHub Models).

Funziona dentro GitHub Actions con il GITHUB_TOKEN del repository (permesso "models: read"):
nessuna chiave da creare, nessun costo. Il piano gratuito ha un limite di richieste
al giorno, per questo l'AI legge SOLO gli annunci che hanno già superato i filtri.

L'AI riceve: titolo, descrizione, foto, prezzo, recensioni del venditore, modello
riconosciuto, valutazione Ready2Game e le regole del listino. Risponde con un verdetto:
  "compra"   -> ti arriva l'avviso
  "verifica" -> solo in dashboard (dubbi)
  "scarta"   -> ignorato (accessorio, rotto, truffa, modello sbagliato…)
"""
from __future__ import annotations

import json
import os
import re

import requests

URL = "https://models.github.ai/inference/chat/completions"

ISTRUZIONI = """Sei un esperto di compravendita di console e videogiochi usati in Italia.
Un rivenditore compra oggetti usati online e li SPEDISCE DIRETTAMENTE a Ready2Game senza vederli:
se l'oggetto non è come previsto, perde soldi. Devi essere PRUDENTE: nel dubbio "verifica" o "scarta".

Analizza l'annuncio e rispondi SOLO con un JSON:
{"verdetto": "compra" | "verifica" | "scarta",
 "e_il_prodotto_giusto": true/false,      // è davvero il modello indicato (non un gioco, accessorio, scatola, clone, lotto diverso)?
 "grado": "eccellente" | "molto_buono" | "buono" | "accettabile" | "sconosciuto",
 "accessori_mancanti": ["..."],
 "rischio_truffa": 0-100,
 "motivo": "spiegazione breve in italiano, max 25 parole"}

Regole:
- "scarta" se: non è il prodotto indicato, è rotto/da riparare/per pezzi, bannato o modificato,
  mancano accessori OBBLIGATORI, è un lotto confuso, è uno scambio o una ricerca, è un codice digitale.
- Segnali di truffa: prezzo molto sotto il mercato + venditore senza recensioni, frasi tipo "solo bonifico",
  "contattami su WhatsApp", "regalo", foto prese da internet, testo copiato o in altra lingua senza senso.
- "compra" solo se è chiaramente il prodotto giusto, completo, in buono stato e il venditore sembra affidabile.
"""


GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{modello}:generateContent"


def disponibile() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GITHUB_TOKEN"))


def _gemini(testo: str, modello: str) -> dict | None:
    """Google Gemini (piano gratuito con un account Google). Chiave in GEMINI_API_KEY."""
    key = os.environ["GEMINI_API_KEY"]
    body = {"system_instruction": {"parts": [{"text": ISTRUZIONI}]},
            "contents": [{"role": "user", "parts": [{"text": testo}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"}}
    try:
        r = requests.post(GEMINI_URL.format(modello=modello), params={"key": key}, json=body, timeout=60)
        if r.status_code == 429:
            print("[AI] Gemini: limite gratuito raggiunto")
            return None
        if r.status_code >= 400:
            print(f"[AI] Gemini errore HTTP {r.status_code}: {r.text[:300]}")
            return None
        parti = r.json()["candidates"][0]["content"]["parts"]
        out = estrai_json("".join(p.get("text", "") for p in parti))
        if out is None:
            print(f"[AI] Gemini: risposta non in JSON: {r.text[:300]!r}")
            return None
        out["verdetto"] = str(out.get("verdetto", "verifica")).lower()
        return out
    except Exception as ex:
        print(f"[AI] Gemini non disponibile: {type(ex).__name__}: {ex}")
        return None


def verifica(d, listino_item: dict | None, modello: str = "openai/gpt-4.1-mini",
             modello_gemini: str = "gemini-2.5-flash") -> dict | None:
    """Restituisce il verdetto dell'AI, oppure None se l'AI non è raggiungibile.
    Usa Google Gemini se c'è GEMINI_API_KEY, altrimenti GitHub Models."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token and not os.environ.get("GEMINI_API_KEY"):
        return None
    l, m = d.listing, d.match
    regole = ""
    if listino_item:
        regole = (f"Listino Ready2Game per questo modello: prezzi per grado {listino_item.get('gradi')}; "
                  f"dotazione: {listino_item.get('dotazione', '').replace(chr(10), ', ')}; "
                  f"detrazioni: {listino_item.get('detrazioni')}.")
    testo = (
        f"Sito: {l.site}\nTitolo: {l.title}\nDescrizione: {(l.description or '(nessuna)')[:1500]}\n"
        f"Prezzo: {l.price:.2f} € (costo totale con spedizione e commissioni: {d.costo_totale:.2f} €)\n"
        f"Condizione indicata dal sito: {(l.extra or {}).get('condizione', '')}\n"
        f"Venditore: {l.seller_name or '?'} · recensioni: {l.seller_reviews if l.seller_reviews is not None else 'sconosciute'}"
        f" · positive: {'' if l.seller_rating is None else f'{l.seller_rating*100:.0f}%'}\n"
        f"Luogo: {l.location or '?'}\n"
        f"Il programma lo ha riconosciuto come: {m.label} ({m.kind}{', ' + m.platform if m.platform else ''})\n"
        f"Ready2Game lo pagherebbe: {d.valutazione:.2f} € (guadagno stimato {d.guadagno:.2f} €)\n{regole}"
    )
    if os.environ.get("GEMINI_API_KEY"):
        return _gemini(testo + (f"\nFoto: {l.image}" if l.image else ""), modello_gemini)
    content: list | str = testo
    if l.image:
        content = [{"type": "text", "text": testo}, {"type": "image_url", "image_url": {"url": l.image}}]
    body = {"model": modello, "temperature": 0, "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": ISTRUZIONI}, {"role": "user", "content": content}]}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json",
               "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    try:
        r = requests.post(URL, json=body, headers=headers, timeout=60)
        if r.status_code >= 400 and l.image:   # alcuni modelli/immagini falliscono: riprova solo testo
            body["messages"][1]["content"] = testo
            r = requests.post(URL, json=body, headers=headers, timeout=60)
        if r.status_code == 429:
            print("[AI] limite giornaliero gratuito raggiunto")
            return None
        if r.status_code >= 400:
            print(f"[AI] errore HTTP {r.status_code}: {r.text[:300]}")
            return None
        try:
            contenuto = r.json()["choices"][0]["message"]["content"]
        except Exception:
            print(f"[AI] risposta non valida (HTTP {r.status_code}, {r.headers.get('content-type')}): {r.text[:300]!r}")
            return None
        out = estrai_json(contenuto)
        if out is None:
            print(f"[AI] testo non in formato JSON: {str(contenuto)[:300]!r}")
            return None
        out["verdetto"] = str(out.get("verdetto", "verifica")).lower()
        return out
    except Exception as ex:
        print(f"[AI] non disponibile: {type(ex).__name__}: {ex}")
        return None


def estrai_json(testo) -> dict | None:
    """Accetta JSON puro, JSON dentro ```json ... ``` o con testo intorno."""
    if isinstance(testo, list):  # alcuni modelli rispondono a blocchi
        testo = "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in testo)
    t = str(testo or "").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t)
    try:
        v = json.loads(t)
        return v if isinstance(v, dict) else None
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                return None
    return None
