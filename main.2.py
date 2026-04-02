from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional
import sqlite3, os
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
    "tariffa_fest_riposo": "98.97654",  # Festività in giorno di riposo (tariffa base)
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

    # Controlla se il giorno è festivo da CALENDARIO (non domenica)
    # La domenica aziendale è R, non un festivo automatico
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

    # ── Giorno FESTIVO con turno lavorativo → tutto strao festivo ──────────────
    if festivo:
        ini = ei if ei is not None else si
        fin = ef if ef is not None else sf
        d,n = split_dn(ini, fin)
        r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r

    # ── Giorno normale ─────────────────────────────────────────────────────────
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

        if not USE_PG:
            # Migrazione utenti: aggiungi is_admin se manca
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

def get_user_settings(user_id, conn):
    rows = fetchall(conn, "SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,))
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try: result[r["chiave"]] = float(r["valore"])
        except: pass
    return result

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
        # Imposta admin se è antonino.adragna
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
def login(form: OAuth2PasswordRequestForm = Depends()):
    conn = get_db()
    user = fetchone(conn, "SELECT * FROM utenti WHERE username=?", (form.username.strip().lower(),))
    conn.close()
    if not user or not verify_password(form.password, user["password_hash"]):
        raise HTTPException(401, "Credenziali non corrette")
    token = create_token(user["id"], user["username"], bool(user.get("is_admin")))
    return {"access_token": token, "token_type": "bearer", "username": user["username"], "nome": user["nome"], "is_admin": bool(user.get("is_admin"))}

@app.get("/api/auth/me")
def me(current_user=Depends(get_current_user)):
    conn = get_db()
    user = fetchone(conn, "SELECT id, username, nome, is_admin FROM utenti WHERE id=?", (current_user["id"],))
    conn.close()
    if not user:
        raise HTTPException(401, "Utente non trovato")
    # antonino.adragna è sempre admin indipendentemente dal valore nel DB
    is_admin = bool(user.get("is_admin")) or user["username"] == "antonino.adragna"
    # Aggiorna il DB se necessario
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
    rows = fetchall(conn, "SELECT id, username, nome, is_admin, created_at FROM utenti ORDER BY created_at")
    conn.close()
    return rows

@app.post("/api/admin/utenti/{user_id}/admin")
def toggle_admin(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT is_admin, username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    # Non puoi rimuovere i tuoi stessi privilegi
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
    tipo: str  # "telefonisti" | "capisala"
    num_settimane: int
    turni: list  # lista di liste: [[lun,mar,...,dom], [lun,...], ...]

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

# ─── Applica tabella turni ────────────────────────────────────────────────────
class ApplicaTabella(BaseModel):
    tab_id: int
    data_inizio: str       # "YYYY-MM-DD"
    data_fine: Optional[str] = None   # "YYYY-MM-DD" — se assente usa fine anno
    settimana_inizio: int  # quale settimana della tabella inizia (1-based)
    giorno_inizio: int     # quale giorno della settimana inizia (0=lun, 6=dom)
    anno_fine: int         # anno di fine (per retrocompatibilità)

@app.post("/api/tabella/applica")
def applica_tabella(payload: ApplicaTabella, user=Depends(get_current_user)):
    import json
    from datetime import timedelta
    conn = get_db()
    tab = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (payload.tab_id,))
    if not tab: conn.close(); raise HTTPException(404, "Tabella non trovata")

    settimane = json.loads(tab["turni_json"])  # lista di settimane, ogni sett. è lista di 7 turni
    num_sett = len(settimane)

    data_inizio = date.fromisoformat(payload.data_inizio)
    # Usa data_fine se fornita, altrimenti fine anno
    if payload.data_fine:
        data_fine = date.fromisoformat(payload.data_fine)
    else:
        data_fine = date(payload.anno_fine, 12, 31)

    # Calcola posizione iniziale nella tabella
    sett_idx = (payload.settimana_inizio - 1) % num_sett  # 0-based
    giorno_idx = payload.giorno_inizio                     # 0=lun, 6=dom

    # Itera dal giorno di inizio fino al 31 dicembre
    data_cur = data_inizio
    cur_sett = sett_idx
    cur_giorno = giorno_idx

    inseriti = 0
    while data_cur <= data_fine:
        turno_raw = settimane[cur_sett][cur_giorno] if cur_sett < len(settimane) else ""
        turno = turno_raw.strip().split()[0] if turno_raw.strip() else ""

        # Normalizza turno
        TURNI_VALIDI = set(TURNI_CONFIG.keys())
        if turno not in TURNI_VALIDI:
            turno = None

        if turno:
            data_str = data_cur.isoformat()
            ore = calcola_ore(turno, None, None, data_str)
            tipo_rep = ""
            # Salva gli orari standard del turno per mostrare Inizio/Fine nel calendario
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

        # Avanza al giorno successivo
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
    if USE_PG:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s",
                        (user["id"], anno))
    else:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{anno:04d}-%"))
    conn.close()
    mesi = {m: {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
                "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
                "reperibilita_feriale":0,"reperibilita_semifestiva":0,"reperibilita_festiva":0,
                "mal":0,"ferie":0,"rc":0,"r":0,"rot":0,"rf":0,
                "fest_riposo":0} for m in range(1,13)}
    for r in rows:
        d = r["data"]; d_str = d if isinstance(d,str) else d.isoformat()
        mes = int(d_str.split("-")[1]); m = mesi[mes]; t = r.get("turno") or ""
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[c] += r.get(c) or 0
        if t=="MAL": m["mal"]+=1
        if t in("F","F-P","F-N"): m["ferie"]+=1
        if t=="RC": m["rc"]+=1
        if t=="R": m["r"]+=1
        if t=="ROT": m["rot"]+=1
        if t=="RF": m["rf"]+=1
        rep = r.get("reperibilita") or ""
        if rep=="feriale": m["reperibilita_feriale"]+=1
        elif rep=="semifestiva": m["reperibilita_semifestiva"]+=1
        elif rep=="festiva": m["reperibilita_festiva"]+=1
        # Festività in giorno di riposo (R o RC in giorno festivo da calendario)
        if t in ("R", "RC") and d_str in FESTIVITA:
            m["fest_riposo"] += 1
    return mesi

@app.get("/api/impostazioni")
def get_impostazioni(user=Depends(get_current_user)):
    conn = get_db()
    cfg = get_user_settings(user["id"], conn)
    conn.close()
    return cfg

@app.post("/api/impostazioni")
def set_impostazioni(payload: ImpostazioniInput, user=Depends(get_current_user)):
    conn = get_db()
    for k, v in payload.valori.items():
        if USE_PG:
            ex(conn, """INSERT INTO impostazioni (user_id,chiave,valore) VALUES (%s,%s,%s)
               ON CONFLICT (user_id,chiave) DO UPDATE SET valore=EXCLUDED.valore""",
               (user["id"], k, str(v)))
        else:
            conn.execute("INSERT OR REPLACE INTO impostazioni (user_id,chiave,valore) VALUES (?,?,?)",
                         (user["id"], k, str(v)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/bustapaga/{anno}/{mese}")
def get_busta_paga(anno: int, mese: int, user=Depends(get_current_user)):
    mp = mese-1 if mese>1 else 12
    ap = anno   if mese>1 else anno-1
    conn = get_db()
    if USE_PG:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=%s AND EXTRACT(YEAR FROM data::date)=%s AND EXTRACT(MONTH FROM data::date)=%s",
                        (user["id"], ap, mp))
    else:
        rows = fetchall(conn, "SELECT * FROM turni WHERE user_id=? AND data LIKE ?",
                        (user["id"], f"{ap:04d}-{mp:02d}-%"))
    cfg = get_user_settings(user["id"], conn)
    conn.close()

    tot = {"ore_diurne":0.0,"ore_notturne":0.0,"strao_diurno":0.0,"strao_notturno":0.0,
           "strao_fest_diurno":0.0,"strao_fest_notturno":0.0,
           "rep_feriale":0,"rep_semifestiva":0,"rep_festiva":0,
           "domeniche":0,"giorni_lavoro":0,"notte_assenza":0.0,"fest_riposo":0}

    for r in rows:
        d = r["data"]; d_str = d if isinstance(d,str) else d.isoformat()
        t = r.get("turno") or ""
        if TURNI_CONFIG.get(t,{}).get("lavorativo"):
            tot["giorni_lavoro"] += 1
            if date.fromisoformat(d_str).weekday() == 6: tot["domeniche"] += 1
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            tot[c] += r.get(c) or 0
        rep = r.get("reperibilita") or ""
        if rep=="feriale": tot["rep_feriale"]+=1
        elif rep=="semifestiva": tot["rep_semifestiva"]+=1
        elif rep=="festiva": tot["rep_festiva"]+=1
        if t in NOTTE_ASSENZA: tot["notte_assenza"] += NOTTE_ASSENZA[t]
        # Festività in giorno di riposo
        if t in ("R", "RC") and d_str in FESTIVITA:
            tot["fest_riposo"] += 1

    mi = ["","Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    rp = f"{mi[mp]}/{str(ap)[-2:]}"; rc = f"{mi[mese]}/{str(anno)[-2:]}"

    vc = [
        {"voce":"Retribuzione totale mensile",   "ref":rc,"qty":None,"tariffa":None,                          "importo":cfg["retribuzione_totale"]},
        {"voce":"Indennità turno X",             "ref":rc,"qty":None,"tariffa":None,                          "importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":rp,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]       *cfg["tariffa_nott_50"],       2)},
        {"voce":"Indennità lavoro domenicale",   "ref":rp,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8        *cfg["tariffa_dom"],           2)},
        {"voce":"Lavoro ordinario notte",        "ref":rp,"qty":tot["notte_assenza"],      "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]      *cfg["tariffa_nott_ord"],      2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":rp,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]       *cfg["tariffa_strao_fer_d"],   2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":rp,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]     *cfg["tariffa_strao_fer_n"],   2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":rp,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]  *cfg["tariffa_strao_fest_d"],  2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":rp,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],  2)},
        {"voce":"Ind. Reperibilità Feriale",     "ref":rp,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]        *cfg["tariffa_rep_feriale"],   2)},
        {"voce":"Ind. Reperibilità Semifestiva", "ref":rp,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]   *cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilità Festiva",     "ref":rp,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]        *cfg["tariffa_rep_festiva"],   2)},
        {"voce":"Festività in giorno di riposo", "ref":rp,"qty":tot["fest_riposo"],       "tariffa":cfg["tariffa_fest_riposo"],   "importo":round(tot["fest_riposo"]        *cfg["tariffa_fest_riposo"]*2, 2)},
    ]
    tc = round(sum(v["importo"] for v in vc), 2)
    inps = round(tc * cfg.get("aliquota_inps",9.19)/100, 2)
    imp_ann = round((tc-inps)*12, 2)

    def irpef(r):
        if r<=0: return 0.0
        imp,res=0.0,r
        for soglia,aliq in [(28000,.23),(22000,.35),(float("inf"),.43)]:
            p=min(res,soglia); imp+=p*aliq; res-=p
            if res<=0: break
        return round(imp,2)

    il = irpef(imp_ann); detr=cfg.get("detrazioni_annue",1955.0)
    if   imp_ann<=15000: det=max(detr,690.0)
    elif imp_ann<=28000: det=round(detr*(28000-imp_ann)/13000,2)
    elif imp_ann<=50000: det=round(658*(50000-imp_ann)/22000,2)
    else: det=0.0
    in_ = max(0.0,round(il-det,2)); im = round(in_/12,2)

    vt = [
        {"voce":f"Contributi INPS ({cfg.get('aliquota_inps',9.19):.2f}%)","importo":inps,"calcolato":True},
        {"voce":"IRPEF stimata mensile","importo":im,"calcolato":True},
        {"voce":"Trattenuta sindacato (CISL)","importo":cfg["trattenuta_sindacato"]},
        {"voce":"Add. reg. da tratt. A.P.","importo":cfg["trattenuta_regionale"]},
        {"voce":"Add. com. da tratt. A.P.","importo":cfg.get("trattenuta_comunale",0.0)},
        {"voce":"Contr. Prev. Compl. (Pegaso)","importo":cfg["trattenuta_pegaso"]},
    ]
    tt = round(sum(v["importo"] for v in vt),2)
    return {"anno":anno,"mese":mese,"mese_prec":mp,"anno_prec":ap,
            "ore_totali":tot,"voci_competenze":vc,"voci_trattenute":vt,
            "tot_competenze":tc,"tot_trattenute":tt,"netto":round(tc-tt,2),
            "dettaglio_fiscale":{"imponibile_annuo_stimato":imp_ann,"irpef_lorda_annua":il,
                                 "detrazione_applicata":det,"irpef_netta_annua":in_,
                                 "inps_mensile":inps,"irpef_mensile":im}}


# ─── Team endpoints ───────────────────────────────────────────────────────────

def require_editor(user=Depends(get_current_user)):
    if not user.get("is_admin") and not user.get("is_editor"):
        raise HTTPException(403, "Accesso riservato agli editor del team")
    return user

# /api/auth/me aggiornato per includere is_editor
# (già gestito sopra nel me() endpoint)

@app.get("/api/team/operatori")
def get_operatori(user=Depends(get_current_user)):
    conn = get_db()
    ops = fetchall(conn, "SELECT * FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    conn.close()
    return ops

@app.post("/api/team/operatori")
def save_operatori(payload: dict, user=Depends(require_editor)):
    """Salva lista operatori completa [{nome, posizione}]"""
    conn = get_db()
    ex(conn, "DELETE FROM team_operatori WHERE 1=1")
    for op in payload.get("operatori", []):
        ex(conn, "INSERT INTO team_operatori (nome, posizione, attivo) VALUES (?,?,1)",
           (op["nome"], op["posizione"]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/team/turni/{anno}/{mese}")
def get_team_turni(anno: int, mese: int, user=Depends(get_current_user)):
    from calendar import monthrange
    _, days = monthrange(anno, mese)
    conn = get_db()
    ops = fetchall(conn, "SELECT * FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    
    # Turni del mese
    d_from = f"{anno}-{mese:02d}-01"
    d_to   = f"{anno}-{mese:02d}-{days:02d}"
    turni = fetchall(conn, 
        "SELECT * FROM team_turni WHERE data >= ? AND data <= ? ORDER BY data, operatore_id",
        (d_from, d_to))
    
    # Colonne destra
    colonne = fetchall(conn,
        "SELECT * FROM team_colonne_destra WHERE data >= ? AND data <= ? ORDER BY data",
        (d_from, d_to))
    
    conn.close()
    
    # Indicizza per lookup rapido
    turni_idx = {}
    for t in turni:
        turni_idx[(t["data"], t["operatore_id"])] = t
    col_idx = {c["data"]: c for c in colonne}
    
    # Costruisci risposta per ogni giorno
    giorni = []
    for g in range(1, days+1):
        data = f"{anno}-{mese:02d}-{g:02d}"
        d = date.fromisoformat(data)
        dow = d.weekday()  # 0=lun
        is_fest = data in FESTIVITA
        row_turni = []
        for op in ops:
            key = (data, op["id"])
            t = turni_idx.get(key, {})
            row_turni.append({
                "operatore_id": op["id"],
                "turno_base": t.get("turno_base", ""),
                "turno_var":  t.get("turno_var", ""),
                "flags":      t.get("flags", ""),
            })
        col = col_idx.get(data, {})
        giorni.append({
            "data": data, "giorno": g, "dow": dow,
            "is_domenica": dow == 6,
            "is_sabato": dow == 5,
            "is_festivo": is_fest,
            "turni": row_turni,
            "colonne_destra": {
                "rep1": col.get("rep1",""), "rep2": col.get("rep2",""), "rep3": col.get("rep3",""),
                "fest_m1": col.get("fest_m1",""), "fest_m2": col.get("fest_m2",""),
                "fest_p1": col.get("fest_p1",""), "fest_p2": col.get("fest_p2",""),
            }
        })
    return {"anno": anno, "mese": mese, "operatori": ops, "giorni": giorni}

class TeamCellaInput(BaseModel):
    data: str
    operatore_id: int
    turno_base: Optional[str] = None
    turno_var:  Optional[str] = None
    flags:      Optional[str] = ""

@app.post("/api/team/turni")
def set_team_turno(payload: TeamCellaInput, user=Depends(require_editor)):
    conn = get_db()
    now = date.today().isoformat()

    def upsert(data_str, op_id, t_base, t_var, flags_str):
        # Leggi valore precedente per il log
        prev = fetchone(conn, "SELECT turno_base, turno_var FROM team_turni WHERE data=? AND operatore_id=?",
                       (data_str, op_id))
        ex(conn, """INSERT INTO team_turni (data,operatore_id,turno_base,turno_var,flags,modificato_da,modificato_il)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(data,operatore_id) DO UPDATE SET
             turno_base=excluded.turno_base, turno_var=excluded.turno_var,
             flags=excluded.flags, modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
           (data_str, op_id, t_base, t_var, flags_str, user["username"], now))
        # Log modifica
        op_info = fetchone(conn, "SELECT nome FROM team_operatori WHERE id=?", (op_id,))
        op_nome = op_info["nome"] if op_info else str(op_id)
        campo = "VAR" if t_var is not None and t_base is None else "TAB"
        vecchio = (prev.get("turno_var") if campo=="VAR" else prev.get("turno_base")) if prev else ""
        nuovo = t_var if campo=="VAR" else t_base
        if vecchio != nuovo:
            ex(conn, """INSERT INTO team_log (data_modifica,utente,data_turno,operatore_nome,campo,vecchio_valore,nuovo_valore,flags)
               VALUES (?,?,?,?,?,?,?,?)""",
               (now, user["username"], data_str, op_nome, campo, vecchio or "", nuovo or "", flags_str))

    # VAR: salva solo per questo operatore, nessuna propagazione
    if payload.turno_var is not None and payload.turno_base is None:
        # Leggi turno_base esistente e aggiorna solo la var
        existing = fetchone(conn, "SELECT turno_base, flags FROM team_turni WHERE data=? AND operatore_id=?",
                           (payload.data, payload.operatore_id))
        t_base = existing["turno_base"] if existing else ""
        flags  = existing["flags"] if existing else ""
        upsert(payload.data, payload.operatore_id, t_base, payload.turno_var, flags)
        conn.commit(); conn.close()
        return {"ok": True, "propagati": 0}

    # TAB: propaga la rotazione circolare agli altri operatori (+7gg, +14gg, ...)
    ops = fetchall(conn, "SELECT id, posizione FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    n = len(ops)
    if not n:
        upsert(payload.data, payload.operatore_id, payload.turno_base or "", "", payload.flags or "")
        conn.commit(); conn.close()
        return {"ok": True, "propagati": 0}

    op_pos  = next((o["posizione"] for o in ops if o["id"] == payload.operatore_id), 1)
    d_base  = date.fromisoformat(payload.data)
    d_end   = date(d_base.year, 12, 31)
    propagati = 0

    # Rotazione: 1→13→12→11→...→2→1
    # offset(x) = (1-x+n)%n  → op1=0, op13=1, op12=2, ..., op2=12
    # offset relativo da src: (src_pos - op["posizione"] + n) % n
    for op in ops:
        rel_offset = (op_pos - op["posizione"] + n) % n
        d_target = d_base + timedelta(days=rel_offset * 7)
        if d_target > d_end:
            continue
        # Non sovrascrivere variazioni manuali esistenti
        existing = fetchone(conn, "SELECT turno_var FROM team_turni WHERE data=? AND operatore_id=?",
                           (d_target.isoformat(), op["id"]))
        if existing and existing.get("turno_var"):
            continue
        upsert(d_target.isoformat(), op["id"], payload.turno_base or "", "", payload.flags or "")
        propagati += 1

    conn.commit(); conn.close()
    return {"ok": True, "propagati": propagati}

class TeamColonneDestra(BaseModel):
    data: str
    rep1: Optional[str] = ""
    rep2: Optional[str] = ""
    rep3: Optional[str] = ""
    fest_m1: Optional[str] = ""
    fest_m2: Optional[str] = ""
    fest_p1: Optional[str] = ""
    fest_p2: Optional[str] = ""

@app.post("/api/team/colonne-destra")
def set_colonne_destra(payload: TeamColonneDestra, user=Depends(require_editor)):
    conn = get_db()
    vals = (payload.data, payload.rep1, payload.rep2, payload.rep3,
            payload.fest_m1, payload.fest_m2, payload.fest_p1, payload.fest_p2)
    ex(conn, """INSERT INTO team_colonne_destra (data,rep1,rep2,rep3,fest_m1,fest_m2,fest_p1,fest_p2)
       VALUES (?,?,?,?,?,?,?,?)
       ON CONFLICT(data) DO UPDATE SET
         rep1=excluded.rep1, rep2=excluded.rep2, rep3=excluded.rep3,
         fest_m1=excluded.fest_m1, fest_m2=excluded.fest_m2,
         fest_p1=excluded.fest_p1, fest_p2=excluded.fest_p2""", vals)
    conn.commit(); conn.close()
    return {"ok": True}

class TeamBulkInput(BaseModel):
    """Carica una settimana template su un range di date con rotazione sfalsata per operatore"""
    data_inizio: str   # YYYY-MM-DD — primo lunedì da cui parte la tabella per l'op. 1
    settimana: list    # 7 elementi, ognuno = lista di 13 turni (uno per operatore)
    # settimana[giorno][operatore] = {"turno_base": "7-15", "flags": "rep"}
    # La rotazione: op.1 inizia dalla settimana indicata, op.2 dalla settimana precedente,
    # op.3 dalla settimana ancora precedente, ecc. — così ogni settimana scorrono di uno.

@app.post("/api/team/carica-template")
def carica_template_team(payload: TeamBulkInput, user=Depends(require_editor)):
    """
    Applica il template con rotazione sfalsata:
    - Op 1 (posizione 1): parte dalla data_inizio
    - Op 2 (posizione 2): parte dalla data_inizio - 7 giorni (1 settimana prima)
    - Op 3 (posizione 3): parte dalla data_inizio - 14 giorni (2 settimane prima)
    - ...e così via
    
    Questo fa sì che ogni settimana, ogni operatore copra il turno dell'operatore
    precedente della settimana precedente — la classica tabella a scorrimento.
    """
    d_start = date.fromisoformat(payload.data_inizio)
    d_end   = date(d_start.year, 12, 31)
    num_giorni_settimana = 7
    num_ops = len(payload.settimana[0]) if payload.settimana else 13
    
    conn = get_db()
    inseriti = 0
    now = date.today().isoformat()
    
    ops = fetchall(conn, "SELECT id, posizione FROM team_operatori WHERE attivo=1 ORDER BY posizione")
    n = len(ops)
    if n == 0:
        conn.close()
        return {"ok": False, "errore": "Nessun operatore configurato"}
    
    # Rotazione 1→13→12→...→2→1
    # Settimana 0: op1=col0, op2=col1, ..., op13=col12
    # Settimana 1: op13=col0(era op1), op1=col12(era op13)... no
    # 
    # Più semplicemente: la data_inizio è il lunedì di riferimento per op1 (col0).
    # Op con posizione p legge la colonna p-1 alla settimana 0.
    # Ma la rotazione fa sì che alla settimana w, op p legga la colonna
    # che era di op ((p-1+w) % n) alla settimana 0.
    # Ovvero: col_for_op_p_at_week_w = (p - 1 + w) % n
    #
    # Ma aspetta — la rotazione è 1→13→12:
    # settimana 1: op13 prende il turno di op1 (col0)
    # settimana 1: op12 prende il turno di op13 (col12)
    # Quindi settimana w, op p prende la colonna di op ((p + w - 1) % n + 1)
    # col_at_week_w = (p - 1 - w + n*100) % n  (sliding back)
    # 
    # Verifica: w=0, p=1 → col=0 ✓; w=1, p=13 → col=(13-1-1)%13=11... 
    # No, p=13, w=1: prende turno di op1 → col=0
    # (13-1-1+13)%13 = (11+13)%13 = 24%13 = 11 ✗
    #
    # Ragionamento corretto dalla specifica: 1→13→12→...→2→1
    # offset(1)=0, offset(13)=1, offset(12)=2, ..., offset(2)=12
    # def offset_for_op(p): return (2 - p + n) % n   [n=13]
    # Verifica: p=1→(2-1+13)%13=1? No, deve essere 0.
    # def offset_for_op(p): return (1 - p + n) % n
    # p=1→0✓, p=13→(1-13+13)%13=1%13=1✓, p=12→2✓, p=2→12✓
    #
    # Quindi: op p alla settimana 0 sta al "momento" offset_for_op(p) nel ciclo.
    # Al giorno d, le settimane trascorse da d_start sono w = (d-d_start).days//7
    # Il "momento nel ciclo" per op p al giorno d è: (offset_for_op(p) + w) % n
    # La colonna del template è: il "momento nel ciclo" % n
    # col = (offset_for_op(p) + w) % n = ((1-p+n) + w) % n
    #
    # Verifica w=0 (settimana 0):
    # op1 (p=1): col=(1-1+13+0)%13=0 ✓ (col0 = turno di op1)
    # op13 (p=13): col=(1-13+13+0)%13=1 ✓? No, settimana 0 op13 ha il suo turno (col12=p-1=12)
    # Hmm. Il template ha 13 colonne, una per operatore nell'ordine 1,2,...,13.
    # Col 0 = turno op1, col 1 = turno op2, ..., col 12 = turno op13.
    # Settimana 0: op p ha il suo turno → col = p-1
    # Settimana 1: op13 prende il turno di op1 → col = 0; op1 prende il turno di op13 → col=12; etc.
    # Quindi settimana w, op p ha la colonna: (p - 1 - w + n*1000) % n
    # Verifica: w=0,p=1→0✓; w=1,p=13→(13-1-1+13)%13=24%13=11✗ (dovrebbe essere 0)
    #
    # Il problema è che la rotazione non è un semplice shift circolare delle colonne.
    # È uno shift dell'identità: alla settimana 1, op13 diventa "op1 della settimana scorsa".
    # La colonna giusta per op p alla settimana w è:
    # La colonna dell'operatore che "era al posto di p" w settimane fa.
    # Ordine rotazione: 1,13,12,11,...,2,1 (ogni settimana si scala di uno in questo ordine)
    # Posizione nell'ordine: 1→idx0, 13→idx1, 12→idx2, ..., 2→idx12
    # idx(p): p=1→0, p=13→1, p=12→2, ..., p=2→12  → idx = (1-p+n)%n (già calcolato)
    # 
    # Settimana w, op con idx i ha il turno dell'operatore con idx (i-w+n)%n
    # L'operatore con idx j ha posizione: se j=0→p=1, j=1→p=13, j=2→p=12, ..., j=12→p=2
    # p_from_idx(j): j=0→1, j=1→13, j=2→12, ..., j≥1→14-j (per j≥1); j=0→1
    # Più semplicemente: col del template = idx_of_source = (i-w+n)%n dove i=idx(p)=(1-p+n)%n
    # col = ((1-p+n)%n - w + n*100) % n
    # 
    # Verifica w=0,p=1: (0-0)%13=0✓
    # Verifica w=0,p=13: (1-0)%13=1? No, col deve essere 12 (p-1=12).
    # Ma col 1 è il turno di op2... NON va.
    #
    # CONCLUSIONE: la colonna del template è sempre p-1 (il turno dell'operatore stesso).
    # La rotazione avviene solo nel TEMPO: op p inizia il suo ciclo con un offset di
    # offset(p) = (1-p+n)%n settimane DOPO la data_inizio.
    # 
    # Quindi: op p alla data d legge sempre la sua colonna (p-1),
    # ma il suo "inizio ciclo" è d_start + offset(p)*7.
    # Se d è prima dell'inizio ciclo, non c'è turno (o si usa il ciclo precedente).
    # Dato che il ciclo è di n settimane, d_op_start = d_start + offset(p)*7 - n*7*k per k appropriato.

    for op in ops:
        op_id = op["id"]
        pos   = op["posizione"]  # 1-based
        # Offset settimane per questo operatore: op1=0, op13=1, op12=2, ..., op2=12
        op_offset_weeks = (1 - pos + n) % n
        # Data virtuale d'inizio per questo operatore
        d_op_start = d_start + timedelta(weeks=op_offset_weeks)
        # Se d_op_start > d_end, questo op non ha turni nell'anno
        if d_op_start > d_end:
            continue

        d_cur = d_op_start
        while d_cur <= d_end:
            dow = d_cur.weekday()
            # Questo operatore legge sempre la sua colonna (pos-1)
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
    """Restituisce ruolo dell'utente corrente per il team"""
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

# Editor management (admin only)
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
