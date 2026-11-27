# 🎮 Cacciatore Console Ready2Game

> **Listino console:** `data/listino_console.xlsx`, il file Excel di Ready2Game (dal 1° dicembre).
> Prezzo per grado (Eccellente / Molto Buono / Buono / Accettabile), accessori **obbligatori**
> (senza, la console viene scartata) e detrazioni per ogni accessorio mancante. Il grado viene stimato
> in modo **prudente** dal testo dell'annuncio: mai "Eccellente", perché il pacco va direttamente a
> Ready2Game senza che tu lo veda. Quando esce un nuovo listino basta sostituire il file.
> I giochi usano il catalogo e la formula del sito.


Cerca console e giochi in vendita sui marketplace, li confronta con la **valutazione
ufficiale Ready2Game** e ti manda una notifica Telegram quando il guadagno è interessante.

## 🤖 Controllo AI gratuito e sempre acceso

- **AI**: prima di avvisarti, un modello AI gratuito di GitHub (GitHub Models, nessuna chiave da creare)
  legge titolo, descrizione, foto e recensioni del venditore e decide: *compra* (ti arriva l'avviso),
  *verifica* (solo in dashboard) o *scarta*. Se l'AI non risponde, nessun avviso (`"obbligatoria": true`).
- **24/7**: ogni turno su GitHub Actions cerca in continuo per ~50 minuti e alla fine fa partire da solo
  il turno successivo. Per spegnere: `"sempre_acceso": false` in `config.json`.

## Cosa fa

1. **Listino R2G automatico**: legge dal valutatore (`/pages/vendita-usato`) il listino completo:
   52 console, controller e formula dei giochi. Legge anche tutto il catalogo giochi
   (circa 1.750 titoli) e usa le **stesse formule del sito**: confezione, accessori,
   danni, controller extra, console bannata o non funzionante = 1 €.
   Si aggiorna da solo ogni 24 ore.
2. **Ricerca sui siti**: per ogni giro fa 88 ricerche (tutte le console, i giochi per piattaforma
   e, a rotazione, i giochi di valore uno per uno).
3. **Riconoscimento**: capisce il modello esatto (es. PS5 Slim Digital ≠ PS5 Fat con lettore) e
   il gioco del catalogo, e distingue un gioco da una console.
4. **Analisi**:
   - **guadagno netto** = valutazione − (prezzo + spedizione + commissioni + quota spedizione verso R2G);
   - **ROI**, calcolato anche con il credito negozio +10%;
   - **recensioni del venditore** (% positive e numero) → affidabilità 0-100;
   - **parole sospette**: rotta/per pezzi, bannata/modificata, solo scatola, replica,
     codice digitale, "cerco/scambio" → annuncio scartato;
   - **avvisi**: senza accessori, senza custodia, graffi, lotto;
   - **prezzo troppo basso** rispetto al valore → possibile truffa;
   - **storico prezzi**: confronta l'annuncio con la mediana dei prezzi visti negli ultimi 30 giorni.
5. **Livelli**: 🔥 TOP (≥ 40 € e ≥ 35%, rischio basso), ✅ BUONO (≥ 12 € e ≥ 20%),
   👀 DA VERIFICARE (buon guadagno ma rischioso: va solo in dashboard).
6. **Dashboard** `data/dashboard.html`: tutti gli affari, filtri, ordinamento, statistiche per sito
   e prodotti più redditizi.

## Siti

| Sito | Stato |
|---|---|
| eBay.it | ⏸ spento: serve l'API ufficiale con chiavi gratuite |
| Vinted | 🧪 acceso, sperimentale: mai collaudato dal vivo |
| Subito | 🧪 acceso, sperimentale |
| Wallapop | 🧪 acceso, sperimentale |
| Facebook Marketplace, Amazon | ❌ non inclusi: Facebook richiede il login; Amazon blocca i programmi e non vende usato sotto il prezzo di riacquisto |

Per attivare un sito metti `"attivo": true` in `config.json`. I siti senza API ufficiale possono
cambiare formato o bloccare le richieste, e l'uso automatico può violare i loro termini: usali
con pause lunghe e a tuo rischio.

## Avvio su GitHub Actions (gratis, nessun PC acceso)

Il workflow `.github/workflows/cacciatore-affari.yml` fa un giro ogni 15 minuti e si ricorda
gli annunci già visti tra un giro e l'altro. Basta aggiungere su GitHub
(**Settings → Secrets and variables → Actions → New repository secret**) questi segreti:

| Nome | Valore |
|---|---|
| `TELEGRAM_BOTS` | elenco dei bot che ricevono gli avvisi, es. `[{"nome":"Lorenzo","token":"...","chat_id":123},{"nome":"Simone","token":"...","chat_id":456}]` |
| `EBAY_CLIENT_ID` / `EBAY_CLIENT_SECRET` | (facoltativo) chiavi da developer.ebay.com |

La dashboard di ogni giro si scarica dalla pagina **Actions** (allegato "dashboard").
Limite: i server di GitHub vengono spesso bloccati da Vinted, Subito e Wallapop.
Per quei siti serve un PC o una VPS.

## Installazione (PC o VPS)

```bash
pip install -r cacciatore/requirements.txt
python -m playwright install chromium

export TELEGRAM_BOTS='[{"token":"...","chat_id":123}]'   # uno o più bot
export EBAY_CLIENT_ID="..."                       # developer.ebay.com → Production keys
export EBAY_CLIENT_SECRET="..."

python -m cacciatore --prova vinted "ps5 slim"    # prova veloce
python -m cacciatore                            # avvio continuo (un giro ogni 5 minuti)
python -m cacciatore.dashboard --serve          # dashboard su http://IP-DEL-PC:8080/dashboard.html
```

Senza `TELEGRAM_TOKEN` le notifiche vengono stampate a schermo.

## Soglie (`config.json`)

Si parte con valori prudenti: almeno 12 € di guadagno e il 20% di ROI. Dopo qualche giorno guarda in
dashboard quali affari arrivano: se sono troppi alza `min_guadagno`, se sono pochi abbassalo.

## Test

```bash
python -m pytest cacciatore -q
```
