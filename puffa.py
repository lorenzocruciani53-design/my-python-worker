import json
import os
import threading
import time
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# =========================
# CONFIG
# =========================

TOKEN_1 = "8462903531:AAGfqam9G74Im-jGJjOChJBccwv2Z6_SX4M"
TOKEN_2 = "7807080838:AAHUgsVYKCzFPoYrTZ3-cS9mcVMA6P3iXfs"
TOKEN_3 = "8539051757:AAFwdNLpI9vPMp3az-s0Q2cAU1qDnM8J5qg"

BOT_KEYS = ["bot1", "bot2", "bot3"]
BOT_TOKENS = {
    "bot1": TOKEN_1,
    "bot2": TOKEN_2,
    "bot3": TOKEN_3,
}

PERSONE = ["Lorenzo", "Gianluca", "Matteo"]

DB_FILE = "database.json"
DB_LOCK = threading.Lock()

SEND_BOTS = {k: Bot(token=t) for k, t in BOT_TOKENS.items()}

# =========================
# DB (ATOMICO)
# =========================
def _default_db():
    return {
        "puff": {},
        "guadagni": {p: 0.0 for p in PERSONE},
        "chat_ids": {k: [] for k in BOT_KEYS},
        "storico": []   # nuovo campo
    }

def _read_db_nolock():
    if not os.path.exists(DB_FILE):
        return _default_db()
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)
    except:
        return _default_db()

    db.setdefault("puff", {})
    db.setdefault("guadagni", {p: 0.0 for p in PERSONE})
    db.setdefault("chat_ids", {k: [] for k in BOT_KEYS})
    for k in BOT_KEYS:
        db["chat_ids"].setdefault(k, [])
    for p in PERSONE:
        db["guadagni"].setdefault(p, 0.0)
    return db

def _write_db_nolock(db):
    tmp = DB_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4, ensure_ascii=False)
    os.replace(tmp, DB_FILE)

def db_update(fn):
    """Esegue read-modify-write sotto lo stesso lock (zero race)."""
    with DB_LOCK:
        db = _read_db_nolock()
        result = fn(db)
        _write_db_nolock(db)
        return result

def register_chat(bot_key: str, chat_id: int):
    def _fn(db):
        s = set(db["chat_ids"].get(bot_key, []))
        s.add(chat_id)
        db["chat_ids"][bot_key] = sorted(list(s))
    db_update(_fn)

# =========================
# NORMALIZZAZIONE + PARSING ROBUSTO
# =========================
def clean_text(s: str) -> str:
    # converte virgole in spazi e normalizza
    return s.replace(",", " ").strip()

def norm_gusto(s: str) -> str:
    return clean_text(s).lower()

def norm_persona(s: str):
    x = clean_text(s).lower()
    for p in PERSONE:
        if p.lower() == x:
            return p
    return None

def tokens_after_command(message_text: str, command: str):
    """
    Prende tutto quello che sta dopo /command (anche multilinea) e lo tokenizza.
    Supporta anche: mango 2, fragola 9 sulla stessa riga.
    """
    text = message_text.strip()
    lines = text.split("\n")
    first = lines[0].strip()

    # rimuove "/command" dalla prima riga
    if first.lower().startswith("/" + command.lower()):
        first_rest = first[len(command) + 1:].strip()  # +1 per lo slash
    else:
        first_rest = first

    rest = []
    if first_rest:
        rest.append(first_rest)
    if len(lines) > 1:
        rest.extend(lines[1:])

    joined = clean_text(" ".join(rest))
    if not joined:
        return []
    return joined.split()

def parse_aggiungi(message_text: str):
    """
    Restituisce lista di (gusto, q) leggendo coppie ripetute:
    mango 2 fragola 9 cocomero 7
    """
    t = tokens_after_command(message_text, "aggiungi")
    out = []
    i = 0
    while i + 1 < len(t):
        gusto = norm_gusto(t[i])
        q = int(t[i + 1])
        out.append((gusto, q))
        i += 2
    return out

def parse_vendi(message_text: str):
    """
    Restituisce lista di (gusto, q, persona, prezzo) leggendo quadruple ripetute:
    mango 3 lorenzo 7 fragola 2 matteo 8
    """
    t = tokens_after_command(message_text, "vendi")
    out = []
    i = 0
    while i + 3 < len(t):
        gusto = norm_gusto(t[i])
        q = int(t[i + 1])
        persona = norm_persona(t[i + 2])
        prezzo = float(t[i + 3])
        out.append((gusto, q, persona, prezzo))
        i += 4
    return out

# =========================
# BROADCAST: SOLO agli altri 2
# =========================

async def broadcast_to_others(source_bot_key: str, text: str):
    db = db_update(lambda d: d)  # snapshot safe

    for bot_key in BOT_KEYS:
        if bot_key == source_bot_key:
            continue

        chat_ids = db["chat_ids"].get(bot_key, [])
        bot = SEND_BOTS[bot_key]

        for chat_id in chat_ids:
            try:
                await bot.send_message(chat_id=chat_id, text=text)
            except:
                pass

# =========================
# HANDLERS
# =========================

def make_handlers(bot_key: str):

    async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        await update.message.reply_text(
            "👋 GESTIONE PUFF\n\n"

            "📋 COMANDI:\n"
            "/help → mostra questo messaggio\n"
            "/aggiungi → aggiunge puff allo stock\n"
            "/vendi → registra una vendita\n"
            "/puff → mostra tutte le puff disponibili\n"
            "/conto → mostra guadagni totali\n"
            "/disponibilita gusto → controlla quante puff ci sono\n"
            "/annulla → annulla l'ultima operazione\n"
            "/cancella → cancella tutta la cronologia\n\n"

            "📦 ESEMPI AGGIUNGI:\n"
            "/aggiungi mango 2\n\n"

            "/aggiungi\n"
            "mango 2\n"
            "fragola 9\n"
            "cocomero 7\n\n"

            "/aggiungi\n"
            "mango 2, fragola 9, cocomero 7\n\n"

            "💰 ESEMPI VENDI:\n"
            "/vendi mango 3 Lorenzo 30\n\n"

            "/vendi\n"
            "mango 3 lorenzo 30\n"
            "fragola 2 matteo 30\n\n"

            "/vendi\n"
            "mango 3 lorenzo 30, fragola 2 matteo 30\n\n"

            "↩️ ANNULLARE OPERAZIONE:\n"
            "/annulla\n"
            "annulla l'ultima aggiunta o vendita\n\n"

            "🧹 CANCELLARE CRONOLOGIA:\n"
            "/cancella\n"
            "cancella tutte le operazioni salvate"
        )

    async def aggiungi_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        try:
            righe = parse_aggiungi(update.message.text)
            if not righe:
                raise ValueError("no righe")

            def _fn(db):
                totale = 0
                dettagli = []
                for gusto, q in righe:

                    if q <= 0:
                        continue

                    db["storico"].append({
                        "tipo": "aggiungi",
                        "gusto": gusto,
                        "quantita": q
                    })
                    db["puff"][gusto] = int(db["puff"].get(gusto, 0)) + int(q)
                    totale += int(q)
                    dettagli.append(f"{gusto} +{q}")
                return totale, dettagli

            totale, dettagli = db_update(_fn)

            if totale <= 0:
                await update.message.reply_text("❌ niente aggiunto (quantità <= 0)")
                return

            await update.message.reply_text(
                "✅ aggiunte puff:\n" + "\n".join(dettagli) + f"\n\nTotale aggiunto: {totale}"
            )

        except:
            await update.message.reply_text(
                "❌ uso:\n"
                "/aggiungi mango 2\n\n"
                "oppure\n"
                "/aggiungi\nmango 2\nfragola 9\n\n"
                "oppure\n"
                "/aggiungi\nmango 2, fragola 9, cocomero 7"
            )

    async def vendi_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        try:
            righe = parse_vendi(update.message.text)
            if not righe:
                raise ValueError("no righe")

            def _fn(db):
                report = []
                guadagno_operazione = 0.0

                for gusto, q, persona, prezzo in righe:
                    if persona is None:
                        continue
                    if q <= 0 or prezzo < 0:
                        continue

                    disponibili = int(db["puff"].get(gusto, 0))
                    if disponibili < q:
                        continue

                    db["puff"][gusto] = disponibili - q
                    db["storico"].append({
                    "tipo": "vendi",
                    "gusto": gusto,
                    "quantita": q,
                    "persona": persona,
                    "prezzo": prezzo
                })

                    guadagno = float(q) * float(prezzo)
                    db["guadagni"][persona] = float(db["guadagni"].get(persona, 0.0)) + guadagno
                    guadagno_operazione += guadagno

                    report.append(f"🔥 {persona} ha venduto {q} puff {gusto} ({prezzo:.2f}€)")

                totale = sum(float(v) for v in db["guadagni"].values())
                return report, guadagno_operazione, totale

            report, guadagno_operazione, totale = db_update(_fn)

            if not report:
                await update.message.reply_text("❌ niente registrato (nome non valido / stock insufficiente / formato)")
                return

            msg = (
                "💨 NUOVA VENDITA\n\n"
                + "\n".join(report)
                + f"\n\n💰 Guadagno operazione: {guadagno_operazione:.2f}€"
                + f"\n📊 Totale: {totale:.2f}€"
            )

            await broadcast_to_others(bot_key, msg)
            await update.message.reply_text("✅ vendita registrata (inviata agli altri)")

        except:
            await update.message.reply_text(
                "❌ uso:\n"
                "/vendi mango 3 Lorenzo 7\n\n"
                "oppure\n"
                "/vendi\nmango 3 Lorenzo 7\nfragola 2 Matteo 8\n\n"
                "oppure\n"
                "/vendi\nmango 3 lorenzo 7, fragola 2 matteo 8"
            )

    async def puff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        db = db_update(lambda d: d)
        puff = db["puff"]

        totale = sum(int(v) for v in puff.values())
        text = f"📦 PUFF TOTALI: {totale}\n\n"

        if not puff:
            text += "nessuna puff disponibile"
        else:
            for gusto, q in puff.items():
                text += f"{gusto}: {q}\n"

        await update.message.reply_text(text)

    async def conto_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        db = db_update(lambda d: d)
        text = "💰 CONTO\n\n"
        totale = 0.0

        for p in PERSONE:
            v = float(db["guadagni"].get(p, 0.0))
            text += f"{p}: {v:.2f}€\n"
            totale += v

        text += f"\nTotale: {totale:.2f}€"
        await update.message.reply_text(text)

    async def disponibilita_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        register_chat(bot_key, update.effective_chat.id)

        try:
            if not context.args:
                raise ValueError("no args")
            gusto = norm_gusto(context.args[0])

            db = db_update(lambda d: d)
            q = int(db["puff"].get(gusto, 0))

            if q <= 0:
                await update.message.reply_text(f"❌ {gusto}: non disponibile")
            else:
                await update.message.reply_text(f"✅ {gusto}: {q} disponibili")
        except:
            await update.message.reply_text("uso: /disponibilita gusto")

    async def annulla_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

        def _fn(db):

            if not db.get("storico"):
                return "❌ nessuna operazione da annullare"

            op = db["storico"].pop()

            if op["tipo"] == "aggiungi":

                gusto = op["gusto"]
                q = op["quantita"]

                db["puff"][gusto] = max(0, db["puff"].get(gusto, 0) - q)

                return f"↩️ annullato aggiungi {gusto} {q}"

            if op["tipo"] == "vendi":

                gusto = op["gusto"]
                q = op["quantita"]
                persona = op["persona"]
                prezzo = op["prezzo"]

                db["puff"][gusto] += q
                db["guadagni"][persona] -= q * prezzo

                return f"↩️ annullata vendita {gusto} {q}"

        msg = db_update(_fn)

        await update.message.reply_text(msg)

    async def cancella_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

        def _fn(db):

            db["storico"] = []

            return "🧹 cronologia cancellata"

        msg = db_update(_fn)

        await update.message.reply_text(msg)

    return {
        "help": help_cmd,
        "aggiungi": aggiungi_cmd,
        "vendi": vendi_cmd,
        "puff": puff_cmd,
        "conto": conto_cmd,
        "soldi": conto_cmd,
        "disponibilita": disponibilita_cmd,
        "annulla": annulla_cmd,
        "cancella": cancella_cmd
    }

# =========================
# THREAD PER BOT
# =========================

def run_bot(bot_key: str):
    token = BOT_TOKENS[bot_key]
    handlers = make_handlers(bot_key)

    app = ApplicationBuilder().token(token).build()
    for cmd, fn in handlers.items():
        app.add_handler(CommandHandler(cmd, fn))

    print(f"🚀 {bot_key} avviato")
    app.run_polling()

def main():
    db_update(lambda d: d)  # crea db se manca

    threads = []
    for bot_key in BOT_KEYS:
        t = threading.Thread(target=run_bot, args=(bot_key,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)

    print("✅ Tutti e 3 i bot sono online")

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()