"""Archivio SQLite: annunci già visti, affari trovati, storico prezzi per le statistiche."""
from __future__ import annotations

import json
import sqlite3
import statistics
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS visti (uid TEXT PRIMARY KEY, ts INTEGER);
CREATE TABLE IF NOT EXISTS prezzi (prodotto TEXT, sito TEXT, prezzo REAL, ts INTEGER);
CREATE INDEX IF NOT EXISTS ix_prezzi ON prezzi(prodotto, ts);
CREATE TABLE IF NOT EXISTS affari (
  uid TEXT PRIMARY KEY, ts INTEGER, sito TEXT, titolo TEXT, url TEXT, immagine TEXT,
  prodotto TEXT, tipo TEXT, piattaforma TEXT, prezzo REAL, costo REAL, valutazione REAL,
  valutazione_credito REAL, guadagno REAL, roi REAL, affidabilita INTEGER, rischio INTEGER,
  punteggio INTEGER, livello TEXT, venditore TEXT, note TEXT, notificato INTEGER DEFAULT 0
);
"""


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.c = sqlite3.connect(path)
        self.c.row_factory = sqlite3.Row
        self.c.executescript(SCHEMA)

    def gia_visto(self, uid: str) -> bool:
        return self.c.execute("SELECT 1 FROM visti WHERE uid=?", (uid,)).fetchone() is not None

    def segna_visto(self, uid: str):
        self.c.execute("INSERT OR IGNORE INTO visti VALUES (?,?)", (uid, int(time.time())))

    def registra_prezzo(self, prodotto: str, sito: str, prezzo: float):
        self.c.execute("INSERT INTO prezzi VALUES (?,?,?,?)", (prodotto, sito, prezzo, int(time.time())))

    def mediana(self, prodotto: str, giorni: int = 30) -> float | None:
        rows = self.c.execute("SELECT prezzo FROM prezzi WHERE prodotto=? AND ts>?",
                              (prodotto, int(time.time()) - giorni * 86400)).fetchall()
        return statistics.median([r[0] for r in rows]) if len(rows) >= 5 else None

    def salva_affare(self, d, notificato: bool):
        l, m = d.listing, d.match
        self.c.execute("""INSERT OR REPLACE INTO affari VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (l.uid, int(time.time()), l.site, l.title, l.url, l.image, m.label, m.kind, m.platform,
             l.price, d.costo_totale, d.valutazione, d.valutazione_credito, d.guadagno, d.roi,
             d.affidabilita, d.rischio, d.punteggio, d.livello,
             f"{l.seller_name} ({'' if l.seller_rating is None else f'{l.seller_rating*100:.0f}%, '}{l.seller_reviews or 0} rec.)",
             json.dumps(d.note, ensure_ascii=False), int(notificato)))

    def affari(self, giorni: int = 14) -> list[dict]:
        rows = self.c.execute("SELECT * FROM affari WHERE ts>? ORDER BY ts DESC",
                              (int(time.time()) - giorni * 86400,)).fetchall()
        return [dict(r) for r in rows]

    def statistiche(self) -> dict:
        q = lambda sql, *a: self.c.execute(sql, a).fetchone()[0]
        day = int(time.time()) - 86400
        return {
            "annunci_analizzati": q("SELECT COUNT(*) FROM visti"),
            "annunci_24h": q("SELECT COUNT(*) FROM visti WHERE ts>?", day),
            "affari_totali": q("SELECT COUNT(*) FROM affari"),
            "affari_24h": q("SELECT COUNT(*) FROM affari WHERE ts>?", day),
            "guadagno_potenziale_24h": round(q("SELECT COALESCE(SUM(guadagno),0) FROM affari WHERE ts>? AND livello!='👀 DA VERIFICARE'", day), 2),
            "per_sito": [dict(r) for r in self.c.execute(
                "SELECT sito, COUNT(*) n, ROUND(AVG(guadagno),2) medio, ROUND(MAX(guadagno),2) max FROM affari GROUP BY sito ORDER BY n DESC")],
            "prodotti_migliori": [dict(r) for r in self.c.execute(
                "SELECT prodotto, COUNT(*) n, ROUND(AVG(guadagno),2) medio FROM affari GROUP BY prodotto ORDER BY SUM(guadagno) DESC LIMIT 15")],
        }

    def commit(self):
        self.c.commit()
