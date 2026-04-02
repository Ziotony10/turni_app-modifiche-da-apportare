from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional
import sqlite3, os, time
from datetime import date, datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets

app = FastAPI(title="Gestione Turni")
DB_PATH    = "turni.db"
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_PG     = bool(DATABASE_URL)

if USE_PG:
    import psycopg2, psycopg2.extras

# ─── Sicurezza ────────────────────────────────────────────────────────────────
SECRET_KEY   = os.environ.get("JWT_SECRET", secrets.token_hex(32))
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 60 * 24

pwd_context   = CryptContext(schemes=["sha256_crypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ─── Config turni ─────────────────────────────────────────────────────────────
TURNO_ORARI = {
    "M":  (7*60,  15*60), "M1": (8*60,  16*60),
    "M2": (9*60,  17*60), "M3": (11*60, 19*60),
    "P":  (15*60, 23*60), "N":  (23*60, 7*60),
}
TURNI_CONFIG = {
    "M":   {"lavorativo": True,  "label": "Mattino 7-15",     "colore": "#3B82F6"},
    "M1":  {"lavorativo": True,  "label": "Mattino 1  8-16",  "colore": "#2563EB"},
    "M2":  {"lavorativo": True,  "label": "Mattino 2  9-17",  "colore": "#1D4ED8"},
    "M3":  {"lavorativo": True,  "label": "Mattino 3 11-19",  "colore": "#1E40AF"},
    "P":   {"lavorativo": True,  "label": "Pomeriggio 15-23", "colore": "#F59E0B"},
    "N":   {"lavorativo": True,  "label": "Notte 23-7",       "colore": "#6366F1"},
    "RC":  {"lavorativo": False, "label": "Riposo Comp.",     "colore": "#10B981"},
    "R":   {"lavorativo": False, "label": "Riposo Dom.",      "colore": "#EF4444"},
    "ROT": {"lavorativo": False, "label": "Rid. Orario",      "colore": "#8B5CF6"},
    "RF":  {"lavorativo": False, "label": "Riposo Festivo",   "colore": "#EC4899"},
    "MAL": {"lavorativo": False, "label": "Malattia",         "colore": "#F97316"},
    "F":   {"lavorativo": False, "label": "Ferie",            "colore": "#14B8A6"},
    "F-P": {"lavorativo": False, "label": "Ferie su P",       "colore": "#84CC16"},
    "F-N": {"lavorativo": False, "label": "Ferie su N",       "colore": "#06B6D4"},
}
FESTIVITA = {
    "2025-01-01","2025-01-06","2025-04-20","2025-04-21","2025-04-25",
    "2025-05-01","2025-06-02","2025-08-15","2025-11-01","2025-12-07",
    "2025-12-08","2025-12-25","2025-12-26",
    "2026-01-01","2026-01-06","2026-04-05","2026-04-06","2026-04-25",
    "2026-05-01","2026-06-02","2026-08-15","2026-11-01","2026-12-07",
    "2026-12-08","2026-12-25","2026-12-26",
    "2027-01-01","2027-01-06","2027-03-28","2027-03-29","2027-04-25",
    "2027-05-01","2027-06-02","2027-08-15","2027-11-01","2027-12-07",
    "2027-12-08","2027-12-25","2027-12-26",
}
NOTTE_ASSENZA = {"F": 0.0, "F-P": 3.0, "F-N": 7.0}

IMPOSTAZIONI_DEFAULTS = {
    "retribuzione_totale": "2573.39", "tariffa_nott_50": "7.53974",
    "tariffa_dom": "8.39811", "tariffa_nott_ord": "5.27782",
    "tariffa_strao_fer_d": "22.61922", "tariffa_strao_fer_n": "24.12716",
    "tariffa_strao_fest_d": "24.12716", "tariffa_strao_fest_n": "24.36517",
    "tariffa_rep_feriale": "15.26", "tariffa_rep_semifestiva": "32.99",
    "tariffa_rep_festiva": "53.13", "indennita_turno": "279.66",
    "trattenuta_sindacato": "18.86", "trattenuta_regionale": "50.00",
    "trattenuta_comunale": "0.00",
    "trattenuta_pegaso": "33.90", "aliquota_inps": "9.19", "detrazioni_annue": "1955.00",
    "tariffa_fest_riposo": "98.97654",
}

# ─── DB helpers ───────────────────────────────────────────────────────────────
def get_db():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(sql):
    """Adatta la query da SQLite a PostgreSQL."""
    if not USE_PG:
        return sql
    sql = sql.replace("?", "%s")
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("DEFAULT CURRENT_TIMESTAMP", "DEFAULT NOW()")
    return sql

def ex(conn, sql, params=()):
    if USE_PG:
        cur = conn.cursor()
        cur.execute(q(sql), params)
        return cur
    return conn.execute(q(sql), params)

def fetchall(conn, sql, params=()):
    cur = ex(conn, sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def fetchone(conn, sql, params=()):
    cur = ex(conn, sql, params)
    row = cur.fetchone()
    return dict(row) if row else None

# ─── Calcolo ore ──────────────────────────────────────────────────────────────
def split_dn(start, end):
    if end <= start: end += 1440
    notturni = [(0,360),(1200,1440),(1440,1800)]
    nott = 0
    for ns, ne in notturni:
        s, e = max(start,ns), min(end,ne)
        if e > s: nott += e - s
    tot = end - start
    return round((tot-nott)/60,2), round(nott/60,2)

def to_min(s):
    try:
        h, m = s.strip().split(":")
        return int(h)*60 + int(m)
    except: return None

def calcola_ore(turno, ora_inizio, ora_fine, data_str):
    r = {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,
         "strao_notturno":0.0,"strao_fest_diurno":0.0,"strao_fest_notturno":0.0}
    ei = to_min(ora_inizio) if ora_inizio else None
    ef = to_min(ora_fine)   if ora_fine   else None
    std = TURNO_ORARI.get(turno)

    try:
        festivo = data_str in FESTIVITA
    except:
        festivo = False

    if turno == "R":
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_fest_diurno"]=d; r["strao_fest_notturno"]=n
        return r
    if turno == "RC":
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_diurno"]=d; r["strao_notturno"]=n
        return r
    if not std:
        if ei is not None and ef is not None:
            d,n = split_dn(ei,ef); r["strao_diurno"]=d; r["strao_notturno"]=n
        return r

    si, sf = std

    if festivo:
        ini = ei if ei is not None else si
        fin = ef if ef is not None else sf
        d,n = split_dn(ini, fin)
        r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r

    if ei is None and ef is None:
        d,n = split_dn(si,sf); r["ore_diurne"]=d; r["ore_notturne"]=n
        return r

    ini = ei if ei is not None else si
    fin = ef if ef is not None else sf
    sfn = sf if sf > si else sf+1440
    fn  = fin if fin > ini else fin+1440

    oi, of = max(ini,si), min(fn,sfn)
    if of > oi:
        d,n = split_dn(oi,of); r["ore_diurne"]+=d; r["ore_notturne"]+=n
    if ini < si:
        d,n = split_dn(ini,si); r["strao_diurno"]+=d; r["strao_notturno"]+=n
    if fn > sfn:
        d,n = split_dn(sfn,fn); r["strao_diurno"]+=d; r["strao_notturno"]+=n
    return r

def calcola_tipo_rep(turno, data_str):
    if turno == "RC": return "semifestiva"
    if turno == "R": return "festiva"
    if data_str in FESTIVITA: return "festiva"
    if TURNI_CONFIG.get(turno,{}).get("lavorativo"): return "feriale"
    return ""

# ─── Init DB ──────────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
    try:
        ex(conn, """CREATE TABLE IF NOT EXISTS utenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        ex(conn, """CREATE TABLE IF NOT EXISTS turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            turno TEXT, ora_inizio TEXT, ora_fine TEXT,
            ore_diurne REAL DEFAULT 0, ore_notturne REAL DEFAULT 0,
            strao_diurno REAL DEFAULT 0, strao_notturno REAL DEFAULT 0,
            strao_fest_diurno REAL DEFAULT 0, strao_fest_notturno REAL DEFAULT 0,
            reperibilita TEXT, note TEXT,
            UNIQUE(user_id, data))""")
        ex(conn, """CREATE TABLE IF NOT EXISTS impostazioni (
            user_id INTEGER NOT NULL,
            chiave TEXT NOT NULL,
            valore TEXT,
            PRIMARY KEY (user_id, chiave))""")
        ex(conn, """CREATE TABLE IF NOT EXISTS tabelle_turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,
            num_settimane INTEGER NOT NULL,
            turni_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        # ── TURNI TEAM ─────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS team_operatori (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            posizione INTEGER NOT NULL,
            attivo INTEGER DEFAULT 1)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            operatore_id INTEGER NOT NULL,
            turno_base TEXT,
            turno_var TEXT,
            flags TEXT DEFAULT '',
            modificato_da TEXT,
            modificato_il TEXT,
            UNIQUE(data, operatore_id))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_modifica TEXT NOT NULL,
            utente TEXT NOT NULL,
            data_turno TEXT NOT NULL,
            operatore_nome TEXT,
            campo TEXT,
            vecchio_valore TEXT,
            nuovo_valore TEXT,
            flags TEXT)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_colonne_destra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            rep1 TEXT, rep2 TEXT, rep3 TEXT,
            fest_m1 TEXT, fest_m2 TEXT, fest_p1 TEXT, fest_p2 TEXT,
            UNIQUE(data))""")

        # ── LOG ACCESSI ────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS log_accessi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            esito TEXT,
            ip TEXT,
            user_agent TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP)""")

        if not USE_PG:
            # Migrazione utenti: aggiungi colonne se mancano
            u_cols = [r[1] for r in conn.execute("PRAGMA table_info(utenti)").fetchall()]
            if "is_admin" not in u_cols:
                conn.execute("ALTER TABLE utenti ADD COLUMN is_admin INTEGER DEFAULT 0")
            if "is_editor" not in u_cols:
                conn.execute("ALTER TABLE utenti ADD COLUMN is_editor INTEGER DEFAULT 0")
            # Imposta antonino.adragna come admin e editor
            conn.execute("UPDATE utenti SET is_admin=1, is_editor=1 WHERE username='antonino.adragna'")

            cols = [r[1] for r in conn.execute("PRAGMA table_info(turni)").fetchall()]
            if "user_id" not in cols:
                conn.execute("ALTER TABLE turni ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")
            for col, typ in [("ora_inizio","TEXT"),("ora_fine","TEXT"),("ore_diurne","REAL"),
                             ("ore_notturne","REAL"),("strao_fest_diurno","REAL"),("strao_fest_notturno","REAL")]:
                if col not in cols:
                    conn.execute(f"ALTER TABLE turni ADD COLUMN {col} {typ} DEFAULT 0")
            imp_cols = [r[1] for r in conn.execute("PRAGMA table_info(impostazioni)").fetchall()]
            if "user_id" not in imp_cols:
                conn.execute("ALTER TABLE impostazioni RENAME TO impostazioni_old")
                conn.execute("""CREATE TABLE impostazioni (
                    user_id INTEGER NOT NULL, chiave TEXT NOT NULL, valore TEXT,
                    PRIMARY KEY (user_id, chiave))""")
                conn.execute("INSERT INTO impostazioni SELECT 1, chiave, valore FROM impostazioni_old")
                conn.execute("DROP TABLE impostazioni_old")
        else:
            # PostgreSQL: aggiungi colonne se mancano
            for col_def in [
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0",
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_editor INTEGER DEFAULT 0",
            ]:
                try: ex(conn, col_def)
                except: conn.rollback()
            # Imposta antonino.adragna come admin e editor
            ex(conn, "UPDATE utenti SET is_admin=1, is_editor=1 WHERE username='antonino.adragna'")

        conn.commit()
    finally:
        conn.close()

init_db()

# ─── Auth helpers ─────────────────────────────────────────────────────────────
def hash_password(pwd):    return pwd_context.hash(pwd)
def verify_password(p, h): return pwd_context.verify(p, h)

def create_token(user_id, username, is_admin=False):
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode({"sub": str(user_id), "username": username, "is_admin": is_admin, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        uid = int(payload.get("sub"))
        if not uid: raise HTTPException(401, "Token non valido")
        return {"id": uid, "username": payload.get("username"), "is_admin": payload.get("is_admin", False)}
    except JWTError:
        raise HTTPException(401, "Token non valido o scaduto")

def require_admin(user=Depends(get_current_user)):
    if not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli amministratori")
    return user

def require_editor(user=Depends(get_current_user)):
    conn = get_db()
    u = fetchone(conn, "SELECT is_admin, is_editor FROM utenti WHERE id=?", (user["id"],))
    conn.close()
    if not u or (not u.get("is_editor") and not u.get("is_admin") and user["username"] != "antonino.adragna"):
        raise HTTPException(403, "Accesso riservato agli editor")
    return user

def get_user_settings(user_id, conn):
    rows = fetchall(conn, "SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,))
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try: result[r["chiave"]] = float(r["valore"])
        except: pass
    return result

# ─── Helper log accesso ───────────────────────────────────────────────────────
def _log_accesso(username: str, esito: str, request: Request = None):
    try:
        ip = request.client.host if request and request.client else "—"
        ua = (request.headers.get("user-agent", "—")[:120]) if request else "—"
        conn2 = get_db()
        ex(conn2, "INSERT INTO log_accessi (username, esito, ip, user_agent) VALUES (?,?,?,?)",
           (username, esito, ip, ua))
        conn2.commit()
        conn2.close()
    except:
        pass

# ─── Modelli ──────────────────────────────────────────────────────────────────
class RegisterInput(BaseModel):
    username: str; password: str; nome: Optional[str] = None

class TurnoInput(BaseModel):
    turno:        Optional[str]  = None
    ora_inizio:   Optional[str]  = None
    ora_fine:     Optional[str]  = None
    reperibilita: Optional[bool] = False
    note:         Optional[str]  = None

class ImpostazioniInput(BaseModel):
    valori: dict

# ─── Auth endpoints ───────────────────────────────────────────────────────────
@app.post("/api/auth/register")
def register(payload: RegisterInput):
    if len(payload.username) < 3: raise HTTPException(400, "Username troppo corto (min 3)")
    if len(payload.password) < 6: raise HTTPException(400, "Password troppo corta (min 6)")
    conn = get_db()
    try:
        ex(conn, "INSERT INTO utenti (username, nome, password_hash) VALUES (?,?,?)",
           (payload.username.strip().lower(), payload.nome or payload.username, hash_password(payload.password)))
        conn.commit()
        user = fetchone(conn, "SELECT id, is_admin FROM utenti WHERE username=?", (payload.username.strip().lower(),))
        if payload.username.strip().lower() == "antonino.adragna":
            ex(conn, "UPDATE utenti SET is_admin=1 WHERE username=?", (payload.username.strip().lower(),))
            conn.commit()
            user["is_admin"] = 1
        token = create_token(user["id"], payload.username.strip().lower(), bool(user.get("is_admin")))
        return {"access_token": token, "token_type": "bearer", "username": payload.username, "is_admin": bool(user.get("is_admin"))}
    except (psycopg2.errors.UniqueViolation if USE_PG else sqlite3.IntegrityError):
        conn.rollback()
        raise HTTPException(400, "Username già esistente")
    finally:
        conn.close()

@app.post("/api/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends(), request: Request = None):
    conn = get_db()
    user = fetchone(conn, "SELECT * FROM utenti WHERE username=?", (form.username.strip().lower(),))
    conn.close()
    if not user or not verify_password(form.password, user["password_hash"]):
        _log_accesso(form.username.strip().lower(), "fallito", request)
        raise HTTPException(401, "Credenziali non corrette")
    _log_accesso(user["username"], "ok", request)
    token = create_token(user["id"], user["username"], bool(user.get("is_admin")))
    return {"access_token": token, "token_type": "bearer", "username": user["username"], "nome": user["nome"], "is_admin": bool(user.get("is_admin"))}

@app.get("/api/auth/me")
def me(current_user=Depends(get_current_user)):
    conn = get_db()
    user = fetchone(conn, "SELECT id, username, nome, is_admin FROM utenti WHERE id=?", (current_user["id"],))
    conn.close()
    if not user:
        raise HTTPException(401, "Utente non trovato")
    is_admin = bool(user.get("is_admin")) or user["username"] == "antonino.adragna"
    if user["username"] == "antonino.adragna" and not user.get("is_admin"):
        try:
            conn2 = get_db()
            ex(conn2, "UPDATE utenti SET is_admin=1 WHERE username='antonino.adragna'")
            conn2.commit()
            conn2.close()
        except: pass
    return {"id": user["id"], "username": user["username"], "nome": user["nome"], "is_admin": is_admin}

# ─── Admin: gestione utenti ───────────────────────────────────────────────────
@app.get("/api/admin/utenti")
def get_utenti(admin=Depends(require_admin)):
    conn = get_db()
    rows = fetchall(conn, "SELECT id, username, nome, is_admin, is_editor, created_at FROM utenti ORDER BY created_at")
    conn.close()
    return rows

@app.post("/api/admin/utenti/{user_id}/admin")
def toggle_admin(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT is_admin, username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    if user["username"] == "antonino.adragna":
        raise HTTPException(400, "Non puoi modificare l'admin principale")
    new_val = 0 if user["is_admin"] else 1
    ex(conn, "UPDATE utenti SET is_admin=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_admin": bool(new_val)}

@app.delete("/api/admin/utenti/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    if user["username"] == "antonino.adragna":
        raise HTTPException(400, "Non puoi eliminare l'admin principale")
    ex(conn, "DELETE FROM turni WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM impostazioni WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM utenti WHERE id=?", (user_id,))
    conn.commit(); conn.close()
    return {"ok": True}

class ResetPasswordInput(BaseModel):
    nuova_password: str

class ChangePasswordInput(BaseModel):
    password_attuale: str
    nuova_password: str

@app.post("/api/auth/change-password")
def change_password(payload: ChangePasswordInput, user=Depends(get_current_user)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = get_db()
    u = fetchone(conn, "SELECT password_hash FROM utenti WHERE id=?", (user["id"],))
    if not u or not verify_password(payload.password_attuale, u["password_hash"]):
        conn.close()
        raise HTTPException(400, "Password attuale non corretta")
    ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?",
       (hash_password(payload.nuova_password), user["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/api/admin/utenti/{user_id}/reset-password")
def reset_password(user_id: int, payload: ResetPasswordInput, admin=Depends(require_admin)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = get_db()
    user = fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?",
       (hash_password(payload.nuova_password), user_id))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Admin: tabelle turni ─────────────────────────────────────────────────────
@app.get("/api/admin/tabelle")
def get_tabelle(user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT id, nome, tipo, num_settimane, created_at FROM tabelle_turni ORDER BY tipo, nome")
    conn.close()
    return rows

@app.get("/api/admin/tabelle/{tab_id}")
def get_tabella(tab_id: int, user=Depends(get_current_user)):
    conn = get_db()
    row = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (tab_id,))
    conn.close()
    if not row: raise HTTPException(404, "Tabella non trovata")
    import json
    row["turni_json"] = json.loads(row["turni_json"])
    return row

class TabellaTurniInput(BaseModel):
    nome: str
    tipo: str
    num_settimane: int
    turni: list

@app.post("/api/admin/tabelle")
def create_tabella(payload: TabellaTurniInput, admin=Depends(require_admin)):
    import json
    conn = get_db()
    ex(conn, "INSERT INTO tabelle_turni (nome, tipo, num_settimane, turni_json) VALUES (?,?,?,?)",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.put("/api/admin/tabelle/{tab_id}")
def update_tabella(tab_id: int, payload: TabellaTurniInput, admin=Depends(require_admin)):
    import json
    conn = get_db()
    ex(conn, "UPDATE tabelle_turni SET nome=?, tipo=?, num_settimane=?, turni_json=? WHERE id=?",
       (payload.nome, payload.tipo, payload.num_settimane, json.dumps(payload.turni), tab_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/admin/tabelle/{tab_id}")
def delete_tabella(tab_id: int, admin=Depends(require_admin)):
    conn = get_db()
    ex(conn, "DELETE FROM tabelle_turni WHERE id=?", (tab_id,))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Admin: health, stats, log accessi ───────────────────────────────────────

@app.get("/api/health")
async def health_check():
    t0 = time.time()
    try:
        conn = get_db()
        fetchone(conn, "SELECT 1 as ok")
        conn.close()
        db_ms = round((time.time() - t0) * 1000, 1)
        db_ok = True
    except:
        db_ms = -1
        db_ok = False
    return {
        "status": "ok",
        "db_ok": db_ok,
        "db_latency_ms": db_ms,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/admin/stats")
def get_stats(admin=Depends(require_admin)):
    conn = get_db()
    stats = {}
    stats["utenti"]           = (fetchone(conn, "SELECT COUNT(*) as n FROM utenti") or {}).get("n", 0)
    try:
        stats["turni"]        = (fetchone(conn, "SELECT COUNT(*) as n FROM turni") or {}).get("n", 0)
    except: stats["turni"] = 0
    try:
        stats["tabelle"]      = (fetchone(conn, "SELECT COUNT(*) as n FROM tabelle_turni") or {}).get("n", 0)
    except: stats["tabelle"] = 0
    try:
        stats["team_operatori"] = (fetchone(conn, "SELECT COUNT(*) as n FROM team_operatori WHERE attivo=1") or {}).get("n", 0)
    except: stats["team_operatori"] = 0
    try:
        stats["log_accessi_oggi"]   = (fetchone(conn,
            "SELECT COUNT(*) as n FROM log_accessi WHERE timestamp >= date('now')") or {}).get("n", 0)
    except: stats["log_accessi_oggi"] = 0
    try:
        stats["login_falliti_oggi"] = (fetchone(conn,
            "SELECT COUNT(*) as n FROM log_accessi WHERE esito='fallito' AND timestamp >= date('now')") or {}).get("n", 0)
    except: stats["login_falliti_oggi"] = 0
    conn.close()
    return stats

@app.get("/api/admin/log-accessi")
def get_log_accessi(limit: int = 200, admin=Depends(require_admin)):
    conn = get_db()
    try:
        logs = fetchall(conn, "SELECT * FROM log_accessi ORDER BY id DESC LIMIT ?", (limit,))
    except:
        logs = []
    conn.close()
    return logs

# ─── Applica tabella turni ────────────────────────────────────────────────────
class ApplicaTabella(BaseModel):
    tab_id: int
    data_inizio: str
    data_fine: Optional[str] = None
    settimana_inizio: int
    giorno_inizio: int
    anno_fine: int

@app.post("/api/tabella/applica")
def applica_tabella(payload: ApplicaTabella, user=Depends(get_current_user)):
    import json
    from datetime import timedelta
    conn = get_db()
    tab = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (payload.tab_id,))
    if not tab: conn.close(); raise HTTPException(404, "Tabella non trovata")

    settimane = json.loads(tab["turni_json"])
    num_sett = len(settimane)

    data_inizio = date.fromisoformat(payload.data_inizio)
    if payload.data_fine:
        data_fine = date.fromisoformat(payload.data_fine)
    else:
        data_fine = date(payload.anno_fine, 12, 31)

    sett_idx = (payload.settimana_inizio - 1) % num_sett
    giorno_idx = payload.giorno_inizio

    data_cur = data_inizio
    cur_sett = sett_idx
    cur_giorno = giorno_idx

    inseriti = 0
    while data_cur <= data_fine:
        turno_raw = settimane[cur_sett][cur_giorno] if cur_sett < len(settimane) else ""
        turno = turno_raw.strip().split()[0] if turno_raw.strip() else ""

        TURNI_VALIDI = set(TURNI_CONFIG.keys())
        if turno not in TURNI_VALIDI:
            turno = None

        if turno:
            data_str = data_cur.isoformat()
            ore = calcola_ore(turno, None, None, data_str)
            tipo_rep = ""
            std = TURNO_ORARI.get(turno)
            if std:
                def mins_to_hhmm(m):
                    m = m % 1440
                    return f"{m//60:02d}:{m%60:02d}"
                std_ini_str = mins_to_hhmm(std[0])
                std_fin_str = mins_to_hhmm(std[1])
            else:
                std_ini_str = None
                std_fin_str = None

            if USE_PG:
                ex(conn, """INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=EXCLUDED.turno, ora_inizio=EXCLUDED.ora_inizio, ora_fine=EXCLUDED.ora_fine,
                      ore_diurne=EXCLUDED.ore_diurne, ore_notturne=EXCLUDED.ore_notturne,
                      strao_diurno=EXCLUDED.strao_diurno, strao_notturno=EXCLUDED.strao_notturno,
                      strao_fest_diurno=EXCLUDED.strao_fest_diurno,
                      strao_fest_notturno=EXCLUDED.strao_fest_notturno,
                      reperibilita=EXCLUDED.reperibilita""",
                   (user["id"],data_str,turno,std_ini_str,std_fin_str,
                    ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
                    ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,None))
            else:
                conn.execute("""INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=excluded.turno, ora_inizio=excluded.ora_inizio, ora_fine=excluded.ora_fine,
                      ore_diurne=excluded.ore_diurne, ore_notturne=excluded.ore_notturne,
                      strao_diurno=excluded.strao_diurno, strao_notturno=excluded.strao_notturno,
                      strao_fest_diurno=excluded.strao_fest_diurno,
                      strao_fest_notturno=excluded.strao_fest_notturno,
                      reperibilita=excluded.reperibilita""",
                   (user["id"],data_str,turno,std_ini_str,std_fin_str,
                    ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
                    ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,None))
            inseriti += 1

        cur_giorno += 1
        if cur_giorno >= 7:
            cur_giorno = 0
            cur_sett = (cur_sett + 1) % num_sett
        data_cur += timedelta(days=1)

    conn.commit(); conn.close()
    return {"ok": True, "inseriti": inseriti}

# ─── API ──────────────────────────────────────────────────────────────────────
@app.get("/api/config")
def get_config():
    out = {}
    for k, v in TURNI_CONFIG.items():
        orari = TURNO_ORARI.get(k)
        out[k] = {**v, "std_ini": orari[0] if orari else None, "std_fin": orari[1] if orari else None}
    return out

@app.get("/api/festivita")
def get_festivita():
    return list(FESTIVITA)

@app.get("/api/turni/{anno}/{mese}")
def get_turni_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                    (user["id"], f"{anno:04d}-{mese:02d}-%"))
    conn.close()
    result = {}
    for r in rows:
        d = r["data"]
        result[d if isinstance(d, str) else d.isoformat()] = r
    return result

@app.post("/api/turni/{data}")
def set_turno(data: str, payload: TurnoInput, user=Depends(get_current_user)):
    ore     = calcola_ore(payload.turno or "", payload.ora_inizio, payload.ora_fine, data)
    tipo_rep = calcola_tipo_rep(payload.turno or "", data) if payload.reperibilita else ""
    conn = get_db()
    if USE_PG:
        ex(conn, """INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=EXCLUDED.turno, ora_inizio=EXCLUDED.ora_inizio, ora_fine=EXCLUDED.ora_fine,
              ore_diurne=EXCLUDED.ore_diurne, ore_notturne=EXCLUDED.ore_notturne,
              strao_diurno=EXCLUDED.strao_diurno, strao_notturno=EXCLUDED.strao_notturno,
              strao_fest_diurno=EXCLUDED.strao_fest_diurno, strao_fest_notturno=EXCLUDED.strao_fest_notturno,
              reperibilita=EXCLUDED.reperibilita, note=EXCLUDED.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    else:
        conn.execute("""INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=excluded.turno, ora_inizio=excluded.ora_inizio, ora_fine=excluded.ora_fine,
              ore_diurne=excluded.ore_diurne, ore_notturne=excluded.ore_notturne,
              strao_diurno=excluded.strao_diurno, strao_notturno=excluded.strao_notturno,
              strao_fest_diurno=excluded.strao_fest_diurno, strao_fest_notturno=excluded.strao_fest_notturno,
              reperibilita=excluded.reperibilita, note=excluded.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    conn.commit(); conn.close()
    return {"ok": True, **ore, "tipo_reperibilita": tipo_rep}

@app.delete("/api/turni/{data}")
def delete_turno(data: str, user=Depends(get_current_user)):
    conn = get_db()
    ex(conn, "DELETE FROM turni WHERE user_id=? AND data=?", (user["id"], data))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/turni-mese/{anno}/{mese}")
def delete_mese(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    if USE_PG:
        ex(conn, "DELETE FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
           (user["id"], anno, mese))
    else:
        ex(conn, "DELETE FROM turni WHERE user_id=? AND data LIKE ?",
           (user["id"], f"{anno:04d}-{mese:02d}-%"))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/riepilogo/{anno}")
def get_riepilogo(anno: int, user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                    (user["id"], f"{anno:04d}-%"))
    conn.close()
    mesi = {}
    for r in rows:
        d = r["data"] if isinstance(r["data"], str) else r["data"].isoformat()
        mese = int(d[5:7])
        if mese not in mesi:
            mesi[mese] = {"turni": [], "ore_diurne": 0, "ore_notturne": 0,
                          "strao_diurno": 0, "strao_notturno": 0,
                          "strao_fest_diurno": 0, "strao_fest_notturno": 0}
        m = mesi[mese]
        m["turni"].append(r)
        for k in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[k] += float(r.get(k) or 0)
    return mesi

@app.get("/api/impostazioni")
def get_impostazioni(user=Depends(get_current_user)):
    conn = get_db()
    s = get_user_settings(user["id"], conn)
    conn.close()
    return s

@app.post("/api/impostazioni")
def save_impostazioni(payload: ImpostazioniInput, user=Depends(get_current_user)):
    conn = get_db()
    for k, v in payload.valori.items():
        if USE_PG:
            ex(conn, """INSERT INTO impostazioni (user_id, chiave, valore) VALUES (%s,%s,%s)
               ON CONFLICT(user_id, chiave) DO UPDATE SET valore=EXCLUDED.valore""",
               (user["id"], k, str(v)))
        else:
            conn.execute("""INSERT INTO impostazioni (user_id, chiave, valore) VALUES (?,?,?)
               ON CONFLICT(user_id, chiave) DO UPDATE SET valore=excluded.valore""",
               (user["id"], k, str(v)))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Team ─────────────────────────────────────────────────────────────────────
@app.get("/api/team/operatori")
def get_team_operatori(user=Depends(get_current_user)):
    conn = get_db()
    ops = fetchall(conn, "SELECT * FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    conn.close()
    return ops

@app.post("/api/team/operatori")
def add_team_operatore(payload: dict, user=Depends(require_editor)):
    conn = get_db()
    ex(conn, "INSERT INTO team_operatori (nome, posizione) VALUES (?,?)",
       (payload.get("nome",""), payload.get("posizione", 0)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.put("/api/team/operatori/{op_id}")
def update_team_operatore(op_id: int, payload: dict, user=Depends(require_editor)):
    conn = get_db()
    ex(conn, "UPDATE team_operatori SET nome=?, posizione=? WHERE id=?",
       (payload.get("nome",""), payload.get("posizione",0), op_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/team/operatori/{op_id}")
def delete_team_operatore(op_id: int, user=Depends(require_editor)):
    conn = get_db()
    ex(conn, "UPDATE team_operatori SET attivo=0 WHERE id=?", (op_id,))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/team/turni/{anno}/{mese}")
def get_team_turni(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, """SELECT tt.*, to2.nome as operatore_nome, to2.posizione
        FROM team_turni tt
        JOIN team_operatori to2 ON tt.operatore_id=to2.id
        WHERE tt.data LIKE ? AND to2.attivo=1
        ORDER BY to2.posizione, tt.data""",
        (f"{anno:04d}-{mese:02d}-%",))
    conn.close()
    return rows

@app.post("/api/team/turni/{data}/{op_id}")
def set_team_turno(data: str, op_id: int, payload: dict, user=Depends(require_editor)):
    conn = get_db()
    now = datetime.now().isoformat()[:19]
    # Log della modifica
    old = fetchone(conn, "SELECT turno_base, flags FROM team_turni WHERE data=? AND operatore_id=?", (data, op_id))
    ex(conn, """INSERT INTO team_turni (data, operatore_id, turno_base, turno_var, flags, modificato_da, modificato_il)
       VALUES (?,?,?,?,?,?,?)
       ON CONFLICT(data, operatore_id) DO UPDATE SET
         turno_base=excluded.turno_base, turno_var=excluded.turno_var,
         flags=excluded.flags, modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
       (data, op_id, payload.get("turno_base",""), payload.get("turno_var",""),
        payload.get("flags",""), user["username"], now))
    # Log
    op = fetchone(conn, "SELECT nome FROM team_operatori WHERE id=?", (op_id,))
    ex(conn, """INSERT INTO team_log (data_modifica, utente, data_turno, operatore_nome, campo, vecchio_valore, nuovo_valore, flags)
       VALUES (?,?,?,?,?,?,?,?)""",
       (now, user["username"], data, op["nome"] if op else str(op_id),
        "turno_base", old["turno_base"] if old else "", payload.get("turno_base",""),
        payload.get("flags","")))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/team/turni/{data}/{op_id}")
def delete_team_turno(data: str, op_id: int, user=Depends(require_editor)):
    conn = get_db()
    ex(conn, "DELETE FROM team_turni WHERE data=? AND operatore_id=?", (data, op_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/team/colonne-destra/{anno}/{mese}")
def get_colonne_destra(anno: int, mese: int, user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT * FROM team_colonne_destra WHERE data LIKE ?",
                    (f"{anno:04d}-{mese:02d}-%",))
    conn.close()
    return {r["data"]: r for r in rows}

@app.post("/api/team/colonne-destra/{data}")
def set_colonne_destra(data: str, payload: dict, user=Depends(require_editor)):
    conn = get_db()
    vals = (data,
            payload.get("rep1",""), payload.get("rep2",""), payload.get("rep3",""),
            payload.get("fest_m1",""), payload.get("fest_m2",""),
            payload.get("fest_p1",""), payload.get("fest_p2",""))
    ex(conn, """INSERT INTO team_colonne_destra (data,rep1,rep2,rep3,fest_m1,fest_m2,fest_p1,fest_p2)
       VALUES (?,?,?,?,?,?,?,?)
       ON CONFLICT(data) DO UPDATE SET
         rep1=excluded.rep1, rep2=excluded.rep2, rep3=excluded.rep3,
         fest_m1=excluded.fest_m1, fest_m2=excluded.fest_m2,
         fest_p1=excluded.fest_p1, fest_p2=excluded.fest_p2""", vals)
    conn.commit(); conn.close()
    return {"ok": True}

class TeamBulkInput(BaseModel):
    data_inizio: str
    settimana: list

@app.post("/api/team/carica-template")
def carica_template_team(payload: TeamBulkInput, user=Depends(require_editor)):
    d_start = date.fromisoformat(payload.data_inizio)
    d_end   = date(d_start.year, 12, 31)
    num_ops = len(payload.settimana[0]) if payload.settimana else 13

    conn = get_db()
    inseriti = 0
    now = date.today().isoformat()

    ops = fetchall(conn, "SELECT id, posizione FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    n = len(ops)
    if n == 0:
        conn.close()
        return {"ok": False, "errore": "Nessun operatore configurato"}

    for op in ops:
        op_id = op["id"]
        pos   = op["posizione"]
        op_offset_weeks = (1 - pos + n) % n
        d_op_start = d_start + timedelta(weeks=op_offset_weeks)
        if d_op_start > d_end:
            continue

        d_cur = d_op_start
        while d_cur <= d_end:
            dow = d_cur.weekday()
            col_idx = pos - 1
            row = payload.settimana[dow] if dow < len(payload.settimana) else []
            if col_idx < len(row):
                cell = row[col_idx]
                turno = cell.get("turno_base", "")
                flags_val = cell.get("flags", "")
                if turno:
                    ex(conn, """INSERT INTO team_turni (data,operatore_id,turno_base,turno_var,flags,modificato_da,modificato_il)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(data,operatore_id) DO UPDATE SET
                         turno_base=excluded.turno_base, flags=excluded.flags,
                         modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
                       (d_cur.isoformat(), op_id, turno, "", flags_val, user["username"], now))
                    inseriti += 1
            d_cur += timedelta(days=1)

    conn.commit(); conn.close()
    return {"ok": True, "inseriti": inseriti}

@app.get("/api/team/me")
def team_me(user=Depends(get_current_user)):
    conn = get_db()
    u = fetchone(conn, "SELECT is_admin, is_editor FROM utenti WHERE id=?", (user["id"],))
    conn.close()
    is_editor = bool(u.get("is_editor")) or user["username"] == "antonino.adragna"
    return {"is_editor": is_editor, "is_admin": bool(u.get("is_admin"))}

@app.get("/api/team/log")
def get_team_log(limit: int = 100, user=Depends(get_current_user)):
    conn = get_db()
    logs = fetchall(conn,
        "SELECT * FROM team_log ORDER BY id DESC LIMIT ?", (limit,))
    conn.close()
    return logs

@app.post("/api/admin/utenti/{user_id}/editor")
def toggle_editor(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    u = fetchone(conn, "SELECT is_editor FROM utenti WHERE id=?", (user_id,))
    if not u: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if u.get("is_editor") else 1
    ex(conn, "UPDATE utenti SET is_editor=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_editor": bool(new_val)}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
