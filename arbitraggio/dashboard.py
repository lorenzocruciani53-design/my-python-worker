"""Dashboard: una pagina HTML con tutti gli affari, le statistiche e il listino.

Viene rigenerata a ogni giro in arbitraggio/data/dashboard.html.
Per vederla dal telefono sulla stessa rete:  python -m arbitraggio.dashboard --serve
"""
from __future__ import annotations

import html
import json
import time
from pathlib import Path

TEMPLATE = """<!doctype html><html lang="it"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="120">
<title>Cacciatore Affari</title><style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#16181d;--mut:#6b7280;--line:#e5e7eb;--ok:#15803d;--top:#c2410c;--warn:#a16207}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#181b22;--fg:#e8eaee;--mut:#9aa1ad;--line:#2a2f3a;--ok:#4ade80;--top:#fb923c;--warn:#facc15}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
main{max-width:1200px;margin:auto;padding:16px}h1{font-size:22px;margin:4px 0}.mut{color:var(--mut)}
.kpi{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}
.k{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px}.k b{display:block;font-size:22px}
.bar{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}input,select{padding:8px;border-radius:8px;border:1px solid var(--line);background:var(--card);color:var(--fg)}
.wrap{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;min-width:900px}th,td{padding:8px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{cursor:pointer;font-size:12px;color:var(--mut);position:sticky;top:0;background:var(--card)}td.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
img{width:54px;height:54px;object-fit:cover;border-radius:6px}.g{color:var(--ok);font-weight:700}.lv{white-space:nowrap}
a{color:inherit}.note{font-size:12px;color:var(--mut);max-width:320px}h2{font-size:16px;margin:22px 0 8px}
.two{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px}
</style></head><body><main>
<h1>🎮 Cacciatore Affari Ready2Game</h1><div class="mut">Aggiornato: __AGG__ · listino R2G del __LIST__</div>
<div class="kpi">__KPI__</div>
<div class="bar"><input id="q" placeholder="Cerca titolo/prodotto…"><select id="s"><option value="">Tutti i siti</option>__SITI__</select>
<select id="l"><option value="">Tutti i livelli</option><option>🔥 TOP</option><option>✅ BUONO</option><option>👀 DA VERIFICARE</option></select></div>
<div class="wrap"><table id="t"><thead><tr><th></th><th>Livello</th><th>Annuncio</th><th>Sito</th><th data-n>Prezzo</th>
<th data-n>Costo tot.</th><th data-n>Valut. R2G</th><th data-n>Guadagno</th><th data-n>ROI</th><th data-n>Venditore</th><th data-n>Rischio</th><th data-n>Punti</th><th>Quando</th></tr></thead><tbody>__ROWS__</tbody></table></div>
<div class="two"><div><h2>Per sito</h2><div class="wrap"><table style="min-width:0">__SITO__</table></div></div>
<div><h2>Prodotti più redditizi</h2><div class="wrap"><table style="min-width:0">__PROD__</table></div></div></div>
</main><script>
const t=document.getElementById('t'),rows=[...t.tBodies[0].rows];
function f(){const q=document.getElementById('q').value.toLowerCase(),s=document.getElementById('s').value,l=document.getElementById('l').value;
rows.forEach(r=>{r.style.display=(!q||r.innerText.toLowerCase().includes(q))&&(!s||r.dataset.s===s)&&(!l||r.dataset.l===l)?'':'none'})}
['q','s','l'].forEach(i=>document.getElementById(i).oninput=f);
t.tHead.querySelectorAll('th').forEach((th,i)=>th.onclick=()=>{const n=th.hasAttribute('data-n'),d=th.dataset.d=th.dataset.d==='1'?'0':'1';
rows.sort((a,b)=>{let x=a.cells[i].dataset.v??a.cells[i].innerText,y=b.cells[i].dataset.v??b.cells[i].innerText;if(n){x=+x;y=+y}return (x>y?1:x<y?-1:0)*(d==='1'?-1:1)}).forEach(r=>t.tBodies[0].appendChild(r))});
</script></body></html>"""


def genera(db, listino: dict, out: Path):
    e = html.escape
    st = db.statistiche()
    aff = db.affari(14)
    kpi = [("Annunci analizzati", st["annunci_analizzati"]), ("Annunci ultime 24h", st["annunci_24h"]),
           ("Affari ultime 24h", st["affari_24h"]), ("Guadagno potenziale 24h", f"{st['guadagno_potenziale_24h']:.0f} €"),
           ("Prodotti nel listino", len(listino["config"]["consoles"]) + len(listino["games"]))]
    rows = []
    for a in aff:
        note = " · ".join(json.loads(a["note"] or "[]"))
        img = f'<img src="{e(a["immagine"])}" loading="lazy" alt="">' if a["immagine"] else ""
        rows.append(
            f'<tr data-s="{e(a["sito"])}" data-l="{e(a["livello"])}"><td>{img}</td><td class="lv">{e(a["livello"])}</td>'
            f'<td><a href="{e(a["url"])}" target="_blank" rel="noopener">{e(a["titolo"][:90])}</a><div class="note">🎯 {e(a["prodotto"])} {e(a["piattaforma"] or "")}<br>{e(note)}</div></td>'
            f'<td>{e(a["sito"])}</td><td class="n" data-v="{a["prezzo"]}">{a["prezzo"]:.2f} €</td>'
            f'<td class="n" data-v="{a["costo"]}">{a["costo"]:.2f} €</td><td class="n" data-v="{a["valutazione"]}">{a["valutazione"]:.2f} €</td>'
            f'<td class="n g" data-v="{a["guadagno"]}">+{a["guadagno"]:.2f} €</td><td class="n" data-v="{a["roi"]}">{a["roi"]*100:.0f}%</td>'
            f'<td class="n" data-v="{a["affidabilita"]}" title="{e(a["venditore"])}">{a["affidabilita"]}/100</td>'
            f'<td class="n" data-v="{a["rischio"]}">{a["rischio"]}</td><td class="n" data-v="{a["punteggio"]}">{a["punteggio"]}</td>'
            f'<td data-v="{a["ts"]}">{time.strftime("%d/%m %H:%M", time.localtime(a["ts"]))}</td></tr>')
    sito = "<tr><th>Sito</th><th>Affari</th><th>Medio</th><th>Max</th></tr>" + "".join(
        f'<tr><td>{e(r["sito"])}</td><td class="n">{r["n"]}</td><td class="n">{r["medio"]} €</td><td class="n">{r["max"]} €</td></tr>'
        for r in st["per_sito"])
    prod = "<tr><th>Prodotto</th><th>Affari</th><th>Guadagno medio</th></tr>" + "".join(
        f'<tr><td>{e(r["prodotto"])}</td><td class="n">{r["n"]}</td><td class="n">{r["medio"]} €</td></tr>'
        for r in st["prodotti_migliori"])
    siti = "".join(f"<option>{e(r['sito'])}</option>" for r in st["per_sito"])
    page = (TEMPLATE.replace("__AGG__", time.strftime("%d/%m/%Y %H:%M"))
            .replace("__LIST__", time.strftime("%d/%m %H:%M", time.localtime(listino.get("updated", 0))))
            .replace("__KPI__", "".join(f'<div class="k"><span class="mut">{e(k)}</span><b>{v}</b></div>' for k, v in kpi))
            .replace("__SITI__", siti).replace("__ROWS__", "".join(rows) or '<tr><td colspan="13" class="mut">Ancora nessun affare trovato.</td></tr>')
            .replace("__SITO__", sito).replace("__PROD__", prod))
    out.write_text(page, encoding="utf-8")


if __name__ == "__main__":
    import argparse
    import functools
    import http.server

    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--porta", type=int, default=8080)
    a = ap.parse_args()
    d = Path(__file__).parent / "data"
    if a.serve:
        h = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(d))
        print(f"Dashboard su http://0.0.0.0:{a.porta}/dashboard.html")
        http.server.ThreadingHTTPServer(("0.0.0.0", a.porta), h).serve_forever()
