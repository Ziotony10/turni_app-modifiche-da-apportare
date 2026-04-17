from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, RootModel
from typing import Optional, List
import sqlite3, os, time
from datetime import date, datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets
import json

app = FastAPI(title="Gestione Turni")
DB_PATH      = "turni.db"
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_PG       = bool(DATABASE_URL)

if USE_PG:
    import psycopg2, psycopg2.extras

# ─── Sicurezza ─────────────────────────────────────────────────────────────────
SECRET_KEY   = os.environ.get("JWT_SECRET", secrets.token_hex(32))
ALGORITHM    = "HS256"
TOKEN_EXPIRE = 60 * 24
INITIAL_ADMIN_USERNAME = (os.environ.get("INITIAL_ADMIN_USERNAME") or "").strip().lower()
INITIAL_ADMIN_PASSWORD = os.environ.get("INITIAL_ADMIN_PASSWORD") or ""
INITIAL_ADMIN_NAME     = os.environ.get("INITIAL_ADMIN_NAME") or "Amministratore"

pwd_context   = CryptContext(schemes=["sha256_crypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ─── Config turni ──────────────────────────────────────────────────────────────
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
    "trattenuta_comunale": "0.00", "trattenuta_pegaso": "33.90",
    "aliquota_inps": "9.19", "detrazioni_annue": "1955.00",
    "tariffa_fest_riposo": "98.97654",
}

# ─── DB helpers ────────────────────────────────────────────────────────────────
def get_db():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def q(sql):
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

def get_user_record(conn, user_id: int):
    return fetchone(conn, "SELECT id, username, nome, is_admin, is_editor, is_team_editor FROM utenti WHERE id=?", (user_id,))

def get_limit_placeholder():
    return "%s" if USE_PG else "?"

def get_team_operator_for_user(conn, user_id: int):
    return fetchone(conn, """SELECT o.id, o.nome, o.posizione, o.linked_user_id
                             FROM team_operatori o
                             WHERE o.attivo=1 AND o.linked_user_id=?""", (user_id,))

def _log_team_ferie(conn, actor_username: str, user_id: int, username: str,
                    operatore_id: int, operatore_nome: str, data_turno: str,
                    action: str, status_from: Optional[str], status_to: Optional[str]):
    ex(conn, """INSERT INTO team_ferie_log
       (actor_username, user_id, username, operatore_id, operatore_nome, data_turno, action, status_from, status_to)
       VALUES (?,?,?,?,?,?,?,?,?)""",
       (actor_username, user_id, username, operatore_id, operatore_nome, data_turno, action, status_from, status_to))

# ─── Calcolo ore ───────────────────────────────────────────────────────────────
def split_dn(start, end):
    if end <= start: end += 1440
    notturni = [(0, 360), (1200, 1440), (1440, 1800)]
    nott = 0
    for ns, ne in notturni:
        s, e = max(start, ns), min(end, ne)
        if e > s: nott += e - s
    tot = end - start
    return round((tot - nott) / 60, 2), round(nott / 60, 2)

def to_min(s):
    try:
        h, m = s.strip().split(":")
        return int(h) * 60 + int(m)
    except:
        return None

def calcola_ore(turno, ora_inizio, ora_fine, data_str):
    r = {"ore_diurne": 0.0, "ore_notturne": 0.0, "strao_diurno": 0.0,
         "strao_notturno": 0.0, "strao_fest_diurno": 0.0, "strao_fest_notturno": 0.0}
    ei = to_min(ora_inizio) if ora_inizio else None
    ef = to_min(ora_fine) if ora_fine else None
    std = TURNO_ORARI.get(turno)
    try:
        festivo = data_str in FESTIVITA
    except:
        festivo = False

    if turno == "R":
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r
    if turno == "RC":
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_diurno"] = d; r["strao_notturno"] = n
        return r
    if not std:
        if ei is not None and ef is not None:
            d, n = split_dn(ei, ef); r["strao_diurno"] = d; r["strao_notturno"] = n
        return r

    si, sf = std
    if festivo:
        ini = ei if ei is not None else si
        fin = ef if ef is not None else sf
        d, n = split_dn(ini, fin)
        r["strao_fest_diurno"] = d; r["strao_fest_notturno"] = n
        return r

    if ei is None and ef is None:
        d, n = split_dn(si, sf); r["ore_diurne"] = d; r["ore_notturne"] = n
        return r

    ini = ei if ei is not None else si
    fin = ef if ef is not None else sf
    sfn = sf if sf > si else sf + 1440
    fn = fin if fin > ini else fin + 1440
    oi, of = max(ini, si), min(fn, sfn)
    if of > oi:
        d, n = split_dn(oi, of); r["ore_diurne"] += d; r["ore_notturne"] += n
    if ini < si:
        d, n = split_dn(ini, si); r["strao_diurno"] += d; r["strao_notturno"] += n
    if fn > sfn:
        d, n = split_dn(sfn, fn); r["strao_diurno"] += d; r["strao_notturno"] += n
    return r

def calcola_tipo_rep(turno, data_str):
    if turno == "RC": return "semifestiva"
    if turno == "R": return "festiva"
    if data_str in FESTIVITA: return "festiva"
    if TURNI_CONFIG.get(turno, {}).get("lavorativo"): return "feriale"
    return ""

# ─── Init DB ───────────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
    try:
        # ── Core tables ────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS utenti (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            nome TEXT,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_editor INTEGER DEFAULT 0,
            is_team_editor INTEGER DEFAULT 0,
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

        # ── Login / accessi logs ────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS log_accessi (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            esito TEXT,
            ip TEXT,
            user_agent TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS login_page_visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip_address TEXT,
            user_agent TEXT,
            referrer TEXT,
            is_bot INTEGER DEFAULT 0,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP)""")

        # ── Team tables ─────────────────────────────────────────────────────────
        ex(conn, """CREATE TABLE IF NOT EXISTS team_operatori (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            posizione INTEGER NOT NULL,
            linked_user_id INTEGER,
            attivo INTEGER DEFAULT 1)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_turni (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            operatore_id INTEGER NOT NULL,
            turno_base TEXT,
            turno_var TEXT,
            flags TEXT DEFAULT '',
            flags_base TEXT DEFAULT '',
            flags_var TEXT DEFAULT '',
            modificato_da TEXT,
            modificato_il TEXT,
            UNIQUE(data, operatore_id))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_colonne_destra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT UNIQUE NOT NULL,
            rep1 TEXT, rep2 TEXT, rep3 TEXT,
            fest_m1 TEXT, fest_m2 TEXT, fest_p1 TEXT, fest_p2 TEXT)""")

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

        ex(conn, """CREATE TABLE IF NOT EXISTS team_ferie_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            operatore_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            stato TEXT NOT NULL DEFAULT 'pending',
            requested_by TEXT,
            reviewed_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(operatore_id, data))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_ferie_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            actor_username TEXT,
            user_id INTEGER,
            username TEXT,
            operatore_id INTEGER,
            operatore_nome TEXT,
            data_turno TEXT NOT NULL,
            action TEXT NOT NULL,
            status_from TEXT,
            status_to TEXT)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_weekly (
            giorno_settimana INTEGER NOT NULL,
            posizione INTEGER NOT NULL,
            turno_base TEXT,
            turno_var TEXT,
            flags TEXT DEFAULT '',
            PRIMARY KEY (giorno_settimana, posizione))""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_reperibili_weekly (
            giorno_settimana INTEGER PRIMARY KEY,
            rep1_pos INTEGER,
            rep2_pos INTEGER,
            rep3_pos INTEGER,
            fest_m1_pos INTEGER,
            fest_m2_pos INTEGER,
            fest_p1_pos INTEGER,
            fest_p2_pos INTEGER)""")

        ex(conn, """CREATE TABLE IF NOT EXISTS team_template_config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            start_date TEXT,
            end_date TEXT)""")

        # ── Migrations / defaults (SQLite) ─────────────────────────────────────
        if not USE_PG:
            u_cols = [r[1] for r in conn.execute("PRAGMA table_info(utenti)").fetchall()]
            for col in ["is_admin", "is_editor", "is_team_editor"]:
                if col not in u_cols:
                    conn.execute(f"ALTER TABLE utenti ADD COLUMN {col} INTEGER DEFAULT 0")

            cols = [r[1] for r in conn.execute("PRAGMA table_info(turni)").fetchall()]
            if "user_id" not in cols:
                conn.execute("ALTER TABLE turni ADD COLUMN user_id INTEGER NOT NULL DEFAULT 1")
            for col, typ in [("ora_inizio","TEXT"),("ora_fine","TEXT"),("ore_diurne","REAL"),
                              ("ore_notturne","REAL"),("strao_fest_diurno","REAL"),("strao_fest_notturno","REAL")]:
                if col not in cols:
                    conn.execute(f"ALTER TABLE turni ADD COLUMN {col} {typ} DEFAULT 0")

            team_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()]
            for col in ["flags_base", "flags_var"]:
                if col not in team_cols:
                    conn.execute(f"ALTER TABLE team_turni ADD COLUMN {col} TEXT DEFAULT ''")

            op_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_operatori)").fetchall()]
            if "linked_user_id" not in op_cols:
                conn.execute("ALTER TABLE team_operatori ADD COLUMN linked_user_id INTEGER")

            tpl_rep_cols = [r[1] for r in conn.execute("PRAGMA table_info(team_template_reperibili_weekly)").fetchall()]
            for col in ["fest_m1_pos", "fest_m2_pos", "fest_p1_pos", "fest_p2_pos"]:
                if col not in tpl_rep_cols:
                    conn.execute(f"ALTER TABLE team_template_reperibili_weekly ADD COLUMN {col} INTEGER")

            if "flags_base" in [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()] and "flags_var" in [r[1] for r in conn.execute("PRAGMA table_info(team_turni)").fetchall()]:
                conn.execute("""
                    UPDATE team_turni
                    SET
                      flags_base = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') = '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_base,'')
                      END,
                      flags_var = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') <> '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_var,'')
                      END
                    WHERE COALESCE(flags,'') <> ''
                """)

            imp_cols = [r[1] for r in conn.execute("PRAGMA table_info(impostazioni)").fetchall()]
            if "user_id" not in imp_cols:
                conn.execute("ALTER TABLE impostazioni RENAME TO impostazioni_old")
                conn.execute("""CREATE TABLE impostazioni (
                    user_id INTEGER NOT NULL, chiave TEXT NOT NULL, valore TEXT,
                    PRIMARY KEY (user_id, chiave))""")
                conn.execute("INSERT INTO impostazioni SELECT 1, chiave, valore FROM impostazioni_old")
                conn.execute("DROP TABLE impostazioni_old")

            # Default operatori se vuota
            if fetchone(conn, "SELECT COUNT(*) as cnt FROM team_operatori")["cnt"] == 0:
                for i in range(1, 14):
                    conn.execute("INSERT INTO team_operatori (nome, posizione, attivo) VALUES (?,?,1)",
                                 (f"Operatore {i}", i))

            # Admin iniziale opzionale tramite env
            if fetchone(conn, "SELECT COUNT(*) as cnt FROM utenti")["cnt"] == 0 and INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD:
                hashed = pwd_context.hash(INITIAL_ADMIN_PASSWORD)
                conn.execute(
                    "INSERT INTO utenti (username, nome, password_hash, is_admin, is_editor, is_team_editor) VALUES (?,?,?,1,1,1)",
                    (INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_NAME, hashed))

        else:
            # PostgreSQL: aggiungi colonne se mancano
            for col_def in [
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0",
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_editor INTEGER DEFAULT 0",
                "ALTER TABLE utenti ADD COLUMN IF NOT EXISTS is_team_editor INTEGER DEFAULT 0",
                "ALTER TABLE team_turni ADD COLUMN IF NOT EXISTS flags_base TEXT DEFAULT ''",
                "ALTER TABLE team_turni ADD COLUMN IF NOT EXISTS flags_var TEXT DEFAULT ''",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_m1_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_m2_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_p1_pos INTEGER",
                "ALTER TABLE team_template_reperibili_weekly ADD COLUMN IF NOT EXISTS fest_p2_pos INTEGER",
            ]:
                try:
                    ex(conn, col_def)
                except:
                    conn.rollback()
            try:
                ex(conn, """
                    UPDATE team_turni
                    SET
                      flags_base = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') = '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_base,'')
                      END,
                      flags_var = CASE
                        WHEN COALESCE(flags_base,'') = '' AND COALESCE(flags_var,'') = '' AND COALESCE(turno_var,'') <> '' THEN COALESCE(flags,'')
                        ELSE COALESCE(flags_var,'')
                      END
                    WHERE COALESCE(flags,'') <> ''
                """)
            except:
                conn.rollback()
            if (fetchone(conn, "SELECT COUNT(*) as cnt FROM utenti") or {}).get("cnt", 0) == 0 and INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD:
                ex(conn,
                   "INSERT INTO utenti (username, nome, password_hash, is_admin, is_editor, is_team_editor) VALUES (?,?,?,?,?,?)",
                   (INITIAL_ADMIN_USERNAME, INITIAL_ADMIN_NAME, pwd_context.hash(INITIAL_ADMIN_PASSWORD), 1, 1, 1))

        conn.commit()
    finally:
        conn.close()


init_db()

# ─── Auth helpers ──────────────────────────────────────────────────────────────
def hash_password(pwd):     return pwd_context.hash(pwd)
def verify_password(p, h):  return pwd_context.verify(p, h)

def create_token(user_id, username, is_admin=False):
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE)
    return jwt.encode(
        {"sub": str(user_id), "username": username, "is_admin": is_admin, "exp": exp},
        SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        uid = int(payload.get("sub"))
        if not uid: raise HTTPException(401, "Token non valido")
        conn = get_db()
        try:
            user = get_user_record(conn, uid)
        finally:
            conn.close()
        if not user:
            raise HTTPException(401, "Utente non trovato")
        return {
            "id": user["id"],
            "username": user["username"],
            "nome": user.get("nome"),
            "is_admin": bool(user.get("is_admin")),
            "is_editor": bool(user.get("is_editor")),
            "is_team_editor": bool(user.get("is_team_editor")),
            "token_is_admin": bool(payload.get("is_admin", False)),
        }
    except JWTError:
        raise HTTPException(401, "Token non valido o scaduto")

def require_admin(user=Depends(get_current_user)):
    if not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli amministratori")
    return user

def require_editor(user=Depends(get_current_user)):
    if not user.get("is_editor") and not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli editor")
    return user

def require_team_editor(user=Depends(get_current_user)):
    if not user.get("is_team_editor") and not user.get("is_admin"):
        raise HTTPException(403, "Accesso riservato agli editor team")
    return user

def get_user_settings(user_id, conn):
    rows = fetchall(conn, "SELECT chiave, valore FROM impostazioni WHERE user_id=?", (user_id,))
    result = {k: float(v) for k, v in IMPOSTAZIONI_DEFAULTS.items()}
    for r in rows:
        try:
            result[r["chiave"]] = float(r["valore"])
        except:
            pass
    return result

def _log_accesso(username: str, esito: str, request: Request = None):
    try:
        ip = request.client.host if request and request.client else "—"
        ua = (request.headers.get("user-agent", "—")[:200]) if request else "—"
        conn2 = get_db()
        ex(conn2, "INSERT INTO log_accessi (username, esito, ip, user_agent) VALUES (?,?,?,?)",
           (username, esito, ip, ua))
        conn2.commit()
        conn2.close()
    except:
        pass

# ─── Modelli ───────────────────────────────────────────────────────────────────
class RegisterInput(BaseModel):
    username: str; password: str; nome: Optional[str] = None

class TurnoInput(BaseModel):
    turno: Optional[str] = None
    ora_inizio: Optional[str] = None
    ora_fine: Optional[str] = None
    reperibilita: Optional[bool] = False
    note: Optional[str] = None

class ImpostazioniInput(BaseModel):
    valori: dict

class ResetPasswordInput(BaseModel):
    nuova_password: str

class ChangePasswordInput(BaseModel):
    password_attuale: str
    nuova_password: str

class TabellaTurniInput(BaseModel):
    nome: str; tipo: str; num_settimane: int; turni: list

class ApplicaTabella(BaseModel):
    tab_id: int; data_inizio: str; data_fine: Optional[str] = None
    settimana_inizio: int; giorno_inizio: int; anno_fine: int

class TeamCellaInput(BaseModel):
    data: str
    operatore_id: int
    turno_base: Optional[str] = None   # None = non aggiornare
    turno_var: Optional[str] = None    # None = non aggiornare
    flags: Optional[str] = ""
    flags_base: Optional[str] = None
    flags_var: Optional[str] = None
    col: Optional[str] = "base"        # 'base' o 'var'

class TeamBulkInput(BaseModel):
    data_inizio: str; settimana: list

class TeamOperatoreItem(BaseModel):
    nome: str
    posizione: int

class TeamOperatoriInput(BaseModel):
    operatori: List[TeamOperatoreItem]

class TeamOperatoreUpdateInput(BaseModel):
    nome: str
    posizione: int

class TeamOperatoreLinkInput(BaseModel):
    user_id: Optional[int] = None

class TeamColonneDestraInput(BaseModel):
    data: str
    rep1: Optional[str] = ""
    rep2: Optional[str] = ""
    rep3: Optional[str] = ""
    fest_m1: Optional[str] = ""
    fest_m2: Optional[str] = ""
    fest_p1: Optional[str] = ""
    fest_p2: Optional[str] = ""

class TeamTemplatePosizioneInput(BaseModel):
    posizione: int
    turno_base: Optional[str] = ""
    turno_var: Optional[str] = ""
    flags: Optional[str] = ""

class TeamTemplateReperibiliInput(BaseModel):
    rep1_pos: Optional[int] = None
    rep2_pos: Optional[int] = None
    rep3_pos: Optional[int] = None
    fest_m1_pos: Optional[int] = None
    fest_m2_pos: Optional[int] = None
    fest_p1_pos: Optional[int] = None
    fest_p2_pos: Optional[int] = None

class TeamTemplateWeekInput(BaseModel):
    posizioni: dict[str, List[TeamTemplatePosizioneInput]]
    reperibili: dict[str, TeamTemplateReperibiliInput] = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class TeamFerieBatchInput(BaseModel):
    add_dates: List[str] = []
    remove_dates: List[str] = []

class TeamFerieReviewInput(BaseModel):
    operatore_id: int
    dates: List[str]
    status: str


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def _date_in_range(d: date, start_obj: Optional[date], end_obj: Optional[date]) -> bool:
    if start_obj and end_obj:
        return start_obj <= d <= end_obj
    if start_obj:
        return d >= start_obj
    if end_obj:
        return d <= end_obj
    return True


def _clear_team_schedule_in_range(conn, start_date_str: Optional[str], end_date_str: Optional[str]):
    start_obj = _parse_iso_date(start_date_str)
    end_obj = _parse_iso_date(end_date_str)
    if start_obj and end_obj and end_obj < start_obj:
        start_obj, end_obj = end_obj, start_obj

    if start_obj and end_obj:
        bounds = (start_obj.isoformat(), end_obj.isoformat())
        ex(conn, "DELETE FROM team_turni WHERE data >= ? AND data <= ?", bounds)
        ex(conn, "DELETE FROM team_colonne_destra WHERE data >= ? AND data <= ?", bounds)
        return
    if start_obj:
        ex(conn, "DELETE FROM team_turni WHERE data >= ?", (start_obj.isoformat(),))
        ex(conn, "DELETE FROM team_colonne_destra WHERE data >= ?", (start_obj.isoformat(),))
        return
    if end_obj:
        ex(conn, "DELETE FROM team_turni WHERE data <= ?", (end_obj.isoformat(),))
        ex(conn, "DELETE FROM team_colonne_destra WHERE data <= ?", (end_obj.isoformat(),))
        return

    ex(conn, "DELETE FROM team_turni")
    ex(conn, "DELETE FROM team_colonne_destra")


def _compute_team_template_slot(template_map: dict, d: date, posizione: int, operator_count: int,
                                start_week_monday: Optional[date]) -> dict:
    if not start_week_monday or operator_count <= 0:
        return {"turno_base": "", "turno_var": "", "flags": ""}
    dow = d.weekday()
    cur_mon = d - timedelta(days=d.weekday())
    sett_idx = (cur_mon - start_week_monday).days // 7
    sett_ciclo = (sett_idx % operator_count) + 1
    pos_orig = ((posizione + sett_ciclo - 2) % operator_count) + 1
    tpl_row = template_map.get(dow, {}).get(pos_orig, {})
    return {
        "turno_base": tpl_row.get("turno_base", "") or "",
        "turno_var": tpl_row.get("turno_var", "") or "",
        "flags": tpl_row.get("flags", "") or "",
    }


def _compute_team_rep_defaults(rep_template: dict, d: date, operator_count: int,
                               start_week_monday: Optional[date]) -> dict:
    defaults = {
        "rep1": "", "rep2": "", "rep3": "",
        "fest_m1": "", "fest_m2": "", "fest_p1": "", "fest_p2": "",
    }
    if not start_week_monday or operator_count <= 0:
        return defaults
    dow = d.weekday()
    if dow not in rep_template:
        return defaults
    cur_mon = d - timedelta(days=d.weekday())
    sett_idx = (cur_mon - start_week_monday).days // 7
    field_map = {
        "rep1": "rep1",
        "rep2": "rep2",
        "rep3": "rep3",
        "fest_m1": "fest_m1",
        "fest_m2": "fest_m2",
        "fest_p1": "fest_p1",
        "fest_p2": "fest_p2",
    }
    for key, base_pos in rep_template.get(dow, {}).items():
        if base_pos:
            mapped = field_map.get(key)
            if mapped:
                defaults[mapped] = str(((int(base_pos) - sett_idx - 1) % operator_count) + 1)
    return defaults


def _preserve_team_schedule_outside_range(conn, ops: List[dict], template_map: dict, rep_template: dict,
                                          old_start: Optional[str], old_end: Optional[str],
                                          new_start: Optional[str], new_end: Optional[str]):
    old_start_obj = _parse_iso_date(old_start)
    old_end_obj = _parse_iso_date(old_end)
    new_start_obj = _parse_iso_date(new_start)
    new_end_obj = _parse_iso_date(new_end)
    if old_start_obj and old_end_obj and old_end_obj < old_start_obj:
        old_start_obj, old_end_obj = old_end_obj, old_start_obj
    if new_start_obj and new_end_obj and new_end_obj < new_start_obj:
        new_start_obj, new_end_obj = new_end_obj, new_start_obj
    if not old_start_obj:
        return

    operator_count = len(ops)
    if operator_count <= 0:
        return

    start_week_monday = old_start_obj - timedelta(days=old_start_obj.weekday())
    horizon_start = date(2024, 1, 1)
    horizon_end = date(2030, 12, 31)
    existing_turni = {
        (r["data"], r["operatore_id"])
        for r in fetchall(conn, "SELECT data, operatore_id FROM team_turni WHERE data >= ? AND data <= ?",
                          (horizon_start.isoformat(), horizon_end.isoformat()))
    }
    existing_cols = {
        r["data"]
        for r in fetchall(conn, "SELECT data FROM team_colonne_destra WHERE data >= ? AND data <= ?",
                          (horizon_start.isoformat(), horizon_end.isoformat()))
    }
    now = datetime.now().isoformat()[:19]
    d = horizon_start
    while d <= horizon_end:
        old_in_range = _date_in_range(d, old_start_obj, old_end_obj)
        new_in_range = _date_in_range(d, new_start_obj, new_end_obj)
        if old_in_range and not new_in_range:
            data_str = d.isoformat()
            for op in ops:
                key = (data_str, op["id"])
                if key in existing_turni:
                    continue
                tpl = _compute_team_template_slot(template_map, d, op["posizione"], operator_count, start_week_monday)
                if not (tpl["turno_base"] or tpl["turno_var"] or tpl["flags"]):
                    continue
                ex(conn, """INSERT INTO team_turni
                       (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                   (data_str, op["id"], tpl["turno_base"], tpl["turno_var"], tpl["flags"], tpl["flags"], "", "template-preserve", now))
                existing_turni.add(key)

            if data_str not in existing_cols:
                rep_defaults = _compute_team_rep_defaults(rep_template, d, operator_count, start_week_monday)
                if rep_defaults["rep1"] or rep_defaults["rep2"] or rep_defaults["rep3"]:
                    ex(conn, """INSERT INTO team_colonne_destra
                           (data, rep1, rep2, rep3, fest_m1, fest_m2, fest_p1, fest_p2)
                           VALUES (?,?,?,?,?,?,?,?)""",
                       (data_str, rep_defaults["rep1"], rep_defaults["rep2"], rep_defaults["rep3"],
                        rep_defaults["fest_m1"], rep_defaults["fest_m2"], rep_defaults["fest_p1"], rep_defaults["fest_p2"]))
                    existing_cols.add(data_str)
                elif rep_defaults["fest_m1"] or rep_defaults["fest_m2"] or rep_defaults["fest_p1"] or rep_defaults["fest_p2"]:
                    ex(conn, """INSERT INTO team_colonne_destra
                           (data, rep1, rep2, rep3, fest_m1, fest_m2, fest_p1, fest_p2)
                           VALUES (?,?,?,?,?,?,?,?)""",
                       (data_str, "", "", "", rep_defaults["fest_m1"], rep_defaults["fest_m2"], rep_defaults["fest_p1"], rep_defaults["fest_p2"]))
                    existing_cols.add(data_str)
        d += timedelta(days=1)

# ─── Auth endpoints ────────────────────────────────────────────────────────────
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
        token = create_token(user["id"], payload.username.strip().lower(), bool(user.get("is_admin")))
        return {"access_token": token, "token_type": "bearer", "username": payload.username,
                "is_admin": bool(user.get("is_admin"))}
    except Exception as e:
        conn.rollback()
        if "UNIQUE" in str(e) or "unique" in str(e):
            raise HTTPException(400, "Username già esistente")
        raise HTTPException(500, str(e))
    finally:
        conn.close()

@app.post("/api/auth/login")
def login(form: OAuth2PasswordRequestForm = Depends(), request: Request = None):
    conn = get_db()
    user = fetchone(conn, "SELECT * FROM utenti WHERE username=?", (form.username.strip().lower(),))
    success = bool(user and verify_password(form.password, user["password_hash"]))
    esito = "ok" if success else "fallito"
    _log_accesso(form.username.strip().lower(), esito, request)
    if not success:
        conn.close()
        raise HTTPException(401, "Credenziali non corrette")
    token = create_token(user["id"], user["username"], bool(user.get("is_admin")))
    conn.close()
    return {"access_token": token, "token_type": "bearer",
            "username": user["username"], "nome": user["nome"],
            "is_admin": bool(user.get("is_admin"))}

@app.get("/api/auth/me")
def me(current_user=Depends(get_current_user)):
    return {
        "id": current_user["id"],
        "username": current_user["username"],
        "nome": current_user.get("nome"),
        "is_admin": bool(current_user.get("is_admin")),
        "is_editor": bool(current_user.get("is_editor")),
        "is_team_editor": bool(current_user.get("is_team_editor")),
    }

@app.post("/api/auth/change-password")
def change_password(payload: ChangePasswordInput, user=Depends(get_current_user)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = get_db()
    u = fetchone(conn, "SELECT password_hash FROM utenti WHERE id=?", (user["id"],))
    if not u or not verify_password(payload.password_attuale, u["password_hash"]):
        conn.close(); raise HTTPException(400, "Password attuale non corretta")
    ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?", (hash_password(payload.nuova_password), user["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Page visit tracking ───────────────────────────────────────────────────────
@app.post("/api/log-page-visit")
async def log_page_visit(request: Request):
    fwd = request.headers.get("X-Forwarded-For")
    ip = fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "—")
    ua = request.headers.get("User-Agent", "")
    ref = request.headers.get("Referer", "")
    is_bot = any(kw in ua.lower() for kw in ["bot", "crawler", "spider", "ping", "monitor", "uptime"])
    try:
        conn = get_db()
        ex(conn, "INSERT INTO login_page_visits (ip_address, user_agent, referrer, is_bot) VALUES (?,?,?,?)",
           (ip, ua, ref, is_bot))
        conn.commit(); conn.close()
    except Exception as e:
        print(f"Errore log visita: {e}")
    return {"status": "ok"}

# ─── Admin: utenti ─────────────────────────────────────────────────────────────
@app.get("/api/admin/utenti")
def get_utenti(admin=Depends(require_admin)):
    conn = get_db()
    rows = fetchall(conn, """
        SELECT u.id, u.username, u.nome, u.is_admin, u.is_editor, u.is_team_editor, u.created_at,
               o.id AS linked_operatore_id, o.nome AS linked_operatore_nome, o.posizione AS linked_operatore_posizione
        FROM utenti u
        LEFT JOIN team_operatori o ON o.linked_user_id = u.id AND o.attivo=1
        ORDER BY u.created_at
    """)
    conn.close()
    return rows

@app.post("/api/admin/utenti/{user_id}/admin")
def toggle_admin(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT is_admin, username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if user["is_admin"] else 1
    ex(conn, "UPDATE utenti SET is_admin=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_admin": bool(new_val)}

@app.post("/api/admin/utenti/{user_id}/editor")
def toggle_editor(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    u = fetchone(conn, "SELECT is_editor, username FROM utenti WHERE id=?", (user_id,))
    if not u: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if u.get("is_editor") else 1
    ex(conn, "UPDATE utenti SET is_editor=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_editor": bool(new_val)}

@app.post("/api/admin/utenti/{user_id}/team-editor")
def toggle_team_editor(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    u = fetchone(conn, "SELECT is_team_editor, username FROM utenti WHERE id=?", (user_id,))
    if not u: raise HTTPException(404, "Utente non trovato")
    new_val = 0 if u.get("is_team_editor") else 1
    ex(conn, "UPDATE utenti SET is_team_editor=? WHERE id=?", (new_val, user_id))
    conn.commit(); conn.close()
    return {"ok": True, "is_team_editor": bool(new_val)}

@app.delete("/api/admin/utenti/{user_id}")
def delete_user(user_id: int, admin=Depends(require_admin)):
    conn = get_db()
    user = fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    ex(conn, "DELETE FROM turni WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM impostazioni WHERE user_id=?", (user_id,))
    ex(conn, "UPDATE team_operatori SET linked_user_id=NULL WHERE linked_user_id=?", (user_id,))
    ex(conn, "DELETE FROM team_ferie_requests WHERE user_id=?", (user_id,))
    ex(conn, "DELETE FROM utenti WHERE id=?", (user_id,))
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/api/admin/utenti/{user_id}/reset-password")
def reset_password(user_id: int, payload: ResetPasswordInput, admin=Depends(require_admin)):
    if len(payload.nuova_password) < 6:
        raise HTTPException(400, "Password troppo corta (min 6 caratteri)")
    conn = get_db()
    user = fetchone(conn, "SELECT username FROM utenti WHERE id=?", (user_id,))
    if not user: raise HTTPException(404, "Utente non trovato")
    ex(conn, "UPDATE utenti SET password_hash=? WHERE id=?", (hash_password(payload.nuova_password), user_id))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Admin: health / stats / logs ──────────────────────────────────────────────
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
    return {"status": "ok", "db_ok": db_ok, "db_latency_ms": db_ms,
            "db_type": "PostgreSQL" if USE_PG else "SQLite",
            "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/admin/status")
def get_status(admin=Depends(require_admin)):
    t0 = time.time()
    try:
        conn = get_db()
        fetchone(conn, "SELECT 1 as x")
        conn.close()
        db_ms = round((time.time() - t0) * 1000, 1)
        db_ok = True
    except:
        db_ms = -1; db_ok = False
    return {"site": "ok", "db": "ok" if db_ok else "error", "db_ms": db_ms,
            "db_type": "PostgreSQL (Supabase)" if USE_PG else "SQLite"}

@app.get("/api/admin/stats")
def get_stats(admin=Depends(require_admin)):
    conn = get_db()
    stats = {}
    for k, q_str in [
        ("utenti",            "SELECT COUNT(*) as n FROM utenti"),
        ("turni",             "SELECT COUNT(*) as n FROM turni"),
        ("tabelle",           "SELECT COUNT(*) as n FROM tabelle_turni"),
        ("team_operatori",    "SELECT COUNT(*) as n FROM team_operatori WHERE attivo=1"),
    ]:
        try:
            stats[k] = (fetchone(conn, q_str) or {}).get("n", 0)
        except:
            stats[k] = 0
    try:
        if USE_PG:
            stats["log_accessi_oggi"] = (fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE timestamp::date = CURRENT_DATE") or {}).get("n", 0)
            stats["login_falliti_oggi"] = (fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE esito='fallito' AND timestamp::date = CURRENT_DATE") or {}).get("n", 0)
        else:
            stats["log_accessi_oggi"] = (fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE date(timestamp) = date('now')") or {}).get("n", 0)
            stats["login_falliti_oggi"] = (fetchone(conn,
                "SELECT COUNT(*) as n FROM log_accessi WHERE esito='fallito' AND date(timestamp) = date('now')") or {}).get("n", 0)
    except:
        stats["log_accessi_oggi"] = 0; stats["login_falliti_oggi"] = 0
    conn.close()
    return stats

@app.get("/api/admin/log-accessi")
def get_log_accessi(limit: int = 200, admin=Depends(require_admin)):
    conn = get_db()
    try:
        logs = fetchall(conn, f"SELECT * FROM log_accessi ORDER BY id DESC LIMIT {get_limit_placeholder()}", (limit,))
    except:
        logs = []
    conn.close()
    for r in logs:
        if r.get("timestamp") and not isinstance(r["timestamp"], str):
            r["timestamp"] = r["timestamp"].isoformat()
    return logs

@app.get("/api/admin/page-visits")
async def get_page_visits(admin=Depends(require_admin)):
    try:
        conn = get_db()
        rows = fetchall(conn, "SELECT * FROM login_page_visits ORDER BY timestamp DESC LIMIT 200")
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(500, str(e))

# ─── Admin: tabelle turni ──────────────────────────────────────────────────────
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

# ─── Applica tabella turni ─────────────────────────────────────────────────────
@app.post("/api/tabella/applica")
def applica_tabella(payload: ApplicaTabella, user=Depends(get_current_user)):
    import json
    conn = get_db()
    tab = fetchone(conn, "SELECT * FROM tabelle_turni WHERE id=?", (payload.tab_id,))
    if not tab: conn.close(); raise HTTPException(404, "Tabella non trovata")
    settimane = json.loads(tab["turni_json"])
    num_sett = len(settimane)
    data_inizio = date.fromisoformat(payload.data_inizio)
    data_fine = date.fromisoformat(payload.data_fine) if payload.data_fine else date(payload.anno_fine, 12, 31)
    sett_idx = (payload.settimana_inizio - 1) % num_sett
    giorno_idx = payload.giorno_inizio
    data_cur = data_inizio
    cur_sett = sett_idx
    cur_giorno = giorno_idx
    inseriti = 0

    def mins_to_hhmm(m):
        m = m % 1440
        return f"{m//60:02d}:{m%60:02d}"

    while data_cur <= data_fine:
        turno_raw = settimane[cur_sett][cur_giorno] if cur_sett < len(settimane) else ""
        turno = turno_raw.strip().split()[0] if turno_raw.strip() else ""
        if turno not in set(TURNI_CONFIG.keys()):
            turno = None
        if turno:
            data_str = data_cur.isoformat()
            ore = calcola_ore(turno, None, None, data_str)
            std = TURNO_ORARI.get(turno)
            si_str = mins_to_hhmm(std[0]) if std else None
            sf_str = mins_to_hhmm(std[1]) if std else None
            if USE_PG:
                ex(conn, """INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=EXCLUDED.turno,ora_inizio=EXCLUDED.ora_inizio,ora_fine=EXCLUDED.ora_fine,
                      ore_diurne=EXCLUDED.ore_diurne,ore_notturne=EXCLUDED.ore_notturne,
                      strao_diurno=EXCLUDED.strao_diurno,strao_notturno=EXCLUDED.strao_notturno,
                      strao_fest_diurno=EXCLUDED.strao_fest_diurno,strao_fest_notturno=EXCLUDED.strao_fest_notturno,
                      reperibilita=EXCLUDED.reperibilita""",
                   (user["id"],data_str,turno,si_str,sf_str,ore["ore_diurne"],ore["ore_notturne"],
                    ore["strao_diurno"],ore["strao_notturno"],ore["strao_fest_diurno"],ore["strao_fest_notturno"],None,None))
            else:
                conn.execute("""INSERT INTO turni
                      (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
                       strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(user_id,data) DO UPDATE SET
                      turno=excluded.turno,ora_inizio=excluded.ora_inizio,ora_fine=excluded.ora_fine,
                      ore_diurne=excluded.ore_diurne,ore_notturne=excluded.ore_notturne,
                      strao_diurno=excluded.strao_diurno,strao_notturno=excluded.strao_notturno,
                      strao_fest_diurno=excluded.strao_fest_diurno,strao_fest_notturno=excluded.strao_fest_notturno,
                      reperibilita=excluded.reperibilita""",
                   (user["id"],data_str,turno,si_str,sf_str,ore["ore_diurne"],ore["ore_notturne"],
                    ore["strao_diurno"],ore["strao_notturno"],ore["strao_fest_diurno"],ore["strao_fest_notturno"],None,None))
            inseriti += 1
        cur_giorno += 1
        if cur_giorno >= 7:
            cur_giorno = 0
            cur_sett = (cur_sett + 1) % num_sett
        data_cur += timedelta(days=1)
    conn.commit(); conn.close()
    return {"ok": True, "inseriti": inseriti}

# ─── Turni personali ───────────────────────────────────────────────────────────
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
    ore = calcola_ore(payload.turno or "", payload.ora_inizio, payload.ora_fine, data)
    tipo_rep = calcola_tipo_rep(payload.turno or "", data) if payload.reperibilita else ""
    conn = get_db()
    if USE_PG:
        ex(conn, """INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=EXCLUDED.turno,ora_inizio=EXCLUDED.ora_inizio,ora_fine=EXCLUDED.ora_fine,
              ore_diurne=EXCLUDED.ore_diurne,ore_notturne=EXCLUDED.ore_notturne,
              strao_diurno=EXCLUDED.strao_diurno,strao_notturno=EXCLUDED.strao_notturno,
              strao_fest_diurno=EXCLUDED.strao_fest_diurno,strao_fest_notturno=EXCLUDED.strao_fest_notturno,
              reperibilita=EXCLUDED.reperibilita,note=EXCLUDED.note""",
           (user["id"],data,payload.turno,payload.ora_inizio,payload.ora_fine,
            ore["ore_diurne"],ore["ore_notturne"],ore["strao_diurno"],ore["strao_notturno"],
            ore["strao_fest_diurno"],ore["strao_fest_notturno"],tipo_rep or None,payload.note))
    else:
        conn.execute("""INSERT INTO turni
              (user_id,data,turno,ora_inizio,ora_fine,ore_diurne,ore_notturne,
               strao_diurno,strao_notturno,strao_fest_diurno,strao_fest_notturno,reperibilita,note)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id,data) DO UPDATE SET
              turno=excluded.turno,ora_inizio=excluded.ora_inizio,ora_fine=excluded.ora_fine,
              ore_diurne=excluded.ore_diurne,ore_notturne=excluded.ore_notturne,
              strao_diurno=excluded.strao_diurno,strao_notturno=excluded.strao_notturno,
              strao_fest_diurno=excluded.strao_fest_diurno,strao_fest_notturno=excluded.strao_fest_notturno,
              reperibilita=excluded.reperibilita,note=excluded.note""",
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
    mesi = {m: {
        "ore_diurne": 0.0, "ore_notturne": 0.0, "strao_diurno": 0.0, "strao_notturno": 0.0,
        "strao_fest_diurno": 0.0, "strao_fest_notturno": 0.0,
        "reperibilita_feriale": 0, "reperibilita_semifestiva": 0, "reperibilita_festiva": 0,
        "mal": 0, "ferie": 0, "rc": 0, "r": 0, "rot": 0, "rf": 0, "fest_riposo": 0
    } for m in range(1, 13)}
    for r in rows:
        d = r["data"]; d_str = d if isinstance(d, str) else d.isoformat()
        mes = int(d_str.split("-")[1]); m = mesi[mes]; t = r.get("turno") or ""
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            m[c] += r.get(c) or 0
        if t == "MAL": m["mal"] += 1
        if t in ("F","F-P","F-N"): m["ferie"] += 1
        if t == "RC": m["rc"] += 1
        if t == "R": m["r"] += 1
        if t == "ROT": m["rot"] += 1
        if t == "RF": m["rf"] += 1
        rep = r.get("reperibilita") or ""
        if rep == "feriale": m["reperibilita_feriale"] += 1
        elif rep == "semifestiva": m["reperibilita_semifestiva"] += 1
        elif rep == "festiva": m["reperibilita_festiva"] += 1
        if t in ("R", "RC") and d_str in FESTIVITA:
            m["fest_riposo"] += 1
    return mesi

@app.get("/api/impostazioni")
def get_impostazioni(user=Depends(get_current_user)):
    conn = get_db()
    s = get_user_settings(user["id"], conn)
    conn.close()
    return s

@app.post("/api/impostazioni")
def set_impostazioni(payload: ImpostazioniInput, user=Depends(get_current_user)):
    conn = get_db()
    for k, v in payload.valori.items():
        if USE_PG:
            ex(conn, """INSERT INTO impostazioni (user_id,chiave,valore) VALUES (%s,%s,%s)
               ON CONFLICT(user_id,chiave) DO UPDATE SET valore=EXCLUDED.valore""",
               (user["id"], k, str(v)))
        else:
            conn.execute("INSERT OR REPLACE INTO impostazioni (user_id,chiave,valore) VALUES (?,?,?)",
                         (user["id"], k, str(v)))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/bustapaga/{anno}/{mese}")
def get_busta_paga(anno: int, mese: int, user=Depends(get_current_user)):
    mp = mese - 1 if mese > 1 else 12
    ap = anno if mese > 1 else anno - 1
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
        d = r["data"]; d_str = d if isinstance(d, str) else d.isoformat()
        t = r.get("turno") or ""
        if TURNI_CONFIG.get(t, {}).get("lavorativo"):
            tot["giorni_lavoro"] += 1
            if date.fromisoformat(d_str).weekday() == 6: tot["domeniche"] += 1
        for c in ["ore_diurne","ore_notturne","strao_diurno","strao_notturno","strao_fest_diurno","strao_fest_notturno"]:
            tot[c] += r.get(c) or 0
        rep = r.get("reperibilita") or ""
        if rep == "feriale": tot["rep_feriale"] += 1
        elif rep == "semifestiva": tot["rep_semifestiva"] += 1
        elif rep == "festiva": tot["rep_festiva"] += 1
        if t in NOTTE_ASSENZA: tot["notte_assenza"] += NOTTE_ASSENZA[t]
        if t in ("R", "RC") and d_str in FESTIVITA: tot["fest_riposo"] += 1

    mi = ["","Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    rp = f"{mi[mp]}/{str(ap)[-2:]}"; rc = f"{mi[mese]}/{str(anno)[-2:]}"

    vc = [
        {"voce":"Retribuzione totale mensile",   "ref":rc,"qty":None,"tariffa":None,"importo":cfg["retribuzione_totale"]},
        {"voce":"Indennità turno X",             "ref":rc,"qty":None,"tariffa":None,"importo":cfg["indennita_turno"]},
        {"voce":"Ore notturne in turno 50%",     "ref":rp,"qty":tot["ore_notturne"],       "tariffa":cfg["tariffa_nott_50"],        "importo":round(tot["ore_notturne"]*cfg["tariffa_nott_50"],2)},
        {"voce":"Indennità lavoro domenicale",   "ref":rp,"qty":tot["domeniche"]*8,        "tariffa":cfg["tariffa_dom"],            "importo":round(tot["domeniche"]*8*cfg["tariffa_dom"],2)},
        {"voce":"Lavoro ordinario notte",        "ref":rp,"qty":tot["notte_assenza"],      "tariffa":cfg["tariffa_nott_ord"],       "importo":round(tot["notte_assenza"]*cfg["tariffa_nott_ord"],2)},
        {"voce":"Str. Feriale Diurno 150%",      "ref":rp,"qty":tot["strao_diurno"],       "tariffa":cfg["tariffa_strao_fer_d"],   "importo":round(tot["strao_diurno"]*cfg["tariffa_strao_fer_d"],2)},
        {"voce":"Str. Feriale Notturno 160%",    "ref":rp,"qty":tot["strao_notturno"],     "tariffa":cfg["tariffa_strao_fer_n"],   "importo":round(tot["strao_notturno"]*cfg["tariffa_strao_fer_n"],2)},
        {"voce":"Str. Festivo Diurno 160%",      "ref":rp,"qty":tot["strao_fest_diurno"],  "tariffa":cfg["tariffa_strao_fest_d"],  "importo":round(tot["strao_fest_diurno"]*cfg["tariffa_strao_fest_d"],2)},
        {"voce":"Str. Festivo Notturno 175%",    "ref":rp,"qty":tot["strao_fest_notturno"],"tariffa":cfg["tariffa_strao_fest_n"],  "importo":round(tot["strao_fest_notturno"]*cfg["tariffa_strao_fest_n"],2)},
        {"voce":"Ind. Reperibilità Feriale",     "ref":rp,"qty":tot["rep_feriale"],        "tariffa":cfg["tariffa_rep_feriale"],   "importo":round(tot["rep_feriale"]*cfg["tariffa_rep_feriale"],2)},
        {"voce":"Ind. Reperibilità Semifestiva", "ref":rp,"qty":tot["rep_semifestiva"],    "tariffa":cfg["tariffa_rep_semifestiva"],"importo":round(tot["rep_semifestiva"]*cfg["tariffa_rep_semifestiva"],2)},
        {"voce":"Ind. Reperibilità Festiva",     "ref":rp,"qty":tot["rep_festiva"],        "tariffa":cfg["tariffa_rep_festiva"],   "importo":round(tot["rep_festiva"]*cfg["tariffa_rep_festiva"],2)},
        {"voce":"Festività in giorno di riposo", "ref":rp,"qty":tot["fest_riposo"],        "tariffa":cfg["tariffa_fest_riposo"],   "importo":round(tot["fest_riposo"]*cfg["tariffa_fest_riposo"]*2,2)},
    ]
    tc = round(sum(v["importo"] for v in vc), 2)
    inps = round(tc * cfg.get("aliquota_inps", 9.19) / 100, 2)
    imp_ann = round((tc - inps) * 12, 2)

    def irpef(r):
        if r <= 0: return 0.0
        imp, res = 0.0, r
        for soglia, aliq in [(28000, .23), (22000, .35), (float("inf"), .43)]:
            p = min(res, soglia); imp += p * aliq; res -= p
            if res <= 0: break
        return round(imp, 2)

    il = irpef(imp_ann); detr = cfg.get("detrazioni_annue", 1955.0)
    if   imp_ann <= 15000: det = max(detr, 690.0)
    elif imp_ann <= 28000: det = round(detr * (28000 - imp_ann) / 13000, 2)
    elif imp_ann <= 50000: det = round(658 * (50000 - imp_ann) / 22000, 2)
    else: det = 0.0
    in_ = max(0.0, round(il - det, 2)); im = round(in_ / 12, 2)

    vt = [
        {"voce": f"Contributi INPS ({cfg.get('aliquota_inps',9.19):.2f}%)", "importo": inps, "calcolato": True},
        {"voce": "IRPEF stimata mensile", "importo": im, "calcolato": True},
        {"voce": "Trattenuta sindacato (CISL)", "importo": cfg["trattenuta_sindacato"]},
        {"voce": "Add. reg. da tratt. A.P.", "importo": cfg["trattenuta_regionale"]},
        {"voce": "Add. com. da tratt. A.P.", "importo": cfg.get("trattenuta_comunale", 0.0)},
        {"voce": "Contr. Prev. Compl. (Pegaso)", "importo": cfg["trattenuta_pegaso"]},
    ]
    tt = round(sum(v["importo"] for v in vt), 2)
    return {"anno": anno, "mese": mese, "mese_prec": mp, "anno_prec": ap,
            "ore_totali": tot, "voci_competenze": vc, "voci_trattenute": vt,
            "tot_competenze": tc, "tot_trattenute": tt, "netto": round(tc - tt, 2),
            "dettaglio_fiscale": {"imponibile_annuo_stimato": imp_ann, "irpef_lorda_annua": il,
                                  "detrazione_applicata": det, "irpef_netta_annua": in_,
                                  "inps_mensile": inps, "irpef_mensile": im}}

# ─── Team: operatori ───────────────────────────────────────────────────────────
@app.get("/api/team/me")
def team_me(user=Depends(get_current_user)):
    conn = get_db()
    op = get_team_operator_for_user(conn, user["id"])
    pending_count_row = fetchone(conn, "SELECT COUNT(*) AS cnt FROM team_ferie_requests WHERE stato='pending'")
    pending_rows = fetchall(conn, """
        SELECT operatore_id, operatore_nome, username, COUNT(*) AS giorni
        FROM (
            SELECT r.operatore_id, o.nome AS operatore_nome, u.username, r.data
            FROM team_ferie_requests r
            LEFT JOIN team_operatori o ON o.id = r.operatore_id
            LEFT JOIN utenti u ON u.id = r.user_id
            WHERE r.stato='pending'
        ) x
        GROUP BY operatore_id, operatore_nome, username
        ORDER BY operatore_nome
    """)
    conn.close()
    return {
        "is_editor": bool(user.get("is_team_editor")) or bool(user.get("is_admin")),
        "is_admin": bool(user.get("is_admin")),
        "linked_operatore_id": op["id"] if op else None,
        "linked_operatore_nome": op["nome"] if op else None,
        "linked_operatore_posizione": op["posizione"] if op else None,
        "can_request_ferie": bool(op),
        "ferie_pending_count": int((pending_count_row or {}).get("cnt", 0) or 0),
        "ferie_pending_summary": pending_rows,
    }

@app.get("/api/team/operatori")
def get_team_operatori(user=Depends(get_current_user)):
    conn = get_db()
    ops = fetchall(conn, """
        SELECT o.*, u.username AS linked_username, u.nome AS linked_nome
        FROM team_operatori o
        LEFT JOIN utenti u ON u.id = o.linked_user_id
        WHERE o.attivo=1
        ORDER BY o.posizione
    """)
    conn.close()
    return ops

@app.post("/api/team/operatori")
def save_operatori(payload: TeamOperatoriInput, user=Depends(require_team_editor)):
    conn = get_db()
    existing = fetchall(conn, "SELECT id, posizione, linked_user_id FROM team_operatori")
    by_position = {row["posizione"]: row for row in existing}
    active_positions = set()
    for op in payload.operatori:
        nome = op.nome.strip()
        if not nome:
            continue
        active_positions.add(op.posizione)
        row = by_position.get(op.posizione)
        if row:
            ex(conn, "UPDATE team_operatori SET nome=?, attivo=1 WHERE id=?", (nome, row["id"]))
        else:
            ex(conn, "INSERT INTO team_operatori (nome, posizione, attivo) VALUES (?,?,1)",
               (nome, op.posizione))
    for posizione, row in by_position.items():
        if posizione not in active_positions:
            ex(conn, "UPDATE team_operatori SET attivo=0 WHERE id=?", (row["id"],))
    max_active_position = max(active_positions) if active_positions else 0
    ex(conn, "DELETE FROM team_template_weekly WHERE posizione > ?", (max_active_position,))
    for campo in ("rep1_pos", "rep2_pos", "rep3_pos", "fest_m1_pos", "fest_m2_pos", "fest_p1_pos", "fest_p2_pos"):
        ex(conn,
           f"UPDATE team_template_reperibili_weekly SET {campo}=NULL WHERE COALESCE({campo}, 0) > ?",
           (max_active_position,))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/api/admin/team/operatori-links")
def get_team_operatori_links(admin=Depends(require_admin)):
    conn = get_db()
    rows = fetchall(conn, """
        SELECT o.id, o.nome, o.posizione, o.linked_user_id, u.username AS linked_username, u.nome AS linked_nome
        FROM team_operatori o
        LEFT JOIN utenti u ON u.id = o.linked_user_id
        WHERE o.attivo=1
        ORDER BY o.posizione
    """)
    conn.close()
    return rows

@app.post("/api/admin/team/operatori/{op_id}/link")
def set_team_operatore_link(op_id: int, payload: TeamOperatoreLinkInput, admin=Depends(require_admin)):
    conn = get_db()
    op = fetchone(conn, "SELECT id, nome, linked_user_id FROM team_operatori WHERE id=? AND attivo=1", (op_id,))
    if not op:
        conn.close()
        raise HTTPException(404, "Operatore non trovato")
    user_id = payload.user_id
    if user_id is not None:
        user = fetchone(conn, "SELECT id FROM utenti WHERE id=?", (user_id,))
        if not user:
            conn.close()
            raise HTTPException(404, "Utente non trovato")
        ex(conn, "UPDATE team_operatori SET linked_user_id=NULL WHERE linked_user_id=? AND id<>?", (user_id, op_id))
    ex(conn, "UPDATE team_operatori SET linked_user_id=? WHERE id=?", (user_id, op_id))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.put("/api/team/operatori/{op_id}")
def update_team_operatore(op_id: int, payload: TeamOperatoreUpdateInput, user=Depends(require_team_editor)):
    conn = get_db()
    ex(conn, "UPDATE team_operatori SET nome=?, posizione=? WHERE id=?",
       (payload.nome.strip(), payload.posizione, op_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/api/team/operatori/{op_id}")
def delete_team_operatore(op_id: int, user=Depends(require_team_editor)):
    conn = get_db()
    ex(conn, "UPDATE team_operatori SET attivo=0 WHERE id=?", (op_id,))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Team: turni ───────────────────────────────────────────────────────────────
@app.get("/api/team/turni/{anno}/{mese}")
def get_team_turni(anno: int, mese: int,
                   start_date: str = None, end_date: str = None,
                   user=Depends(get_current_user)):
    try:
        from calendar import monthrange
        _, days = monthrange(anno, mese)
        conn = get_db()

        template_rows = fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
        template = {}
        for r in template_rows:
            g = r["giorno_settimana"]
            if g not in template: template[g] = {}
            template[g][r["posizione"]] = {
                "turno_base": r["turno_base"] or "", "turno_var": r["turno_var"] or "", "flags": r["flags"] or ""}

        rep_template_rows = fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
        rep_template = {
            r["giorno_settimana"]: {
                "rep1": r.get("rep1_pos"),
                "rep2": r.get("rep2_pos"),
                "rep3": r.get("rep3_pos"),
                "fest_m1": r.get("fest_m1_pos"),
                "fest_m2": r.get("fest_m2_pos"),
                "fest_p1": r.get("fest_p1_pos"),
                "fest_p2": r.get("fest_p2_pos"),
            }
            for r in rep_template_rows
        }

        template_cfg = fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
        if template_cfg:
            if start_date is None:
                start_date = template_cfg.get("start_date") or None
            if end_date is None:
                end_date = template_cfg.get("end_date") or None

        ops = fetchall(conn, """
            SELECT o.*, u.username AS linked_username, u.nome AS linked_nome
            FROM team_operatori o
            LEFT JOIN utenti u ON u.id = o.linked_user_id
            WHERE o.attivo=1
            ORDER BY o.posizione
        """)
        operator_count = max(len(ops), 1)
        d_from = f"{anno:04d}-{mese:02d}-01"
        d_to   = f"{anno:04d}-{mese:02d}-{days:02d}"
        turni_esistenti = fetchall(conn,
            "SELECT * FROM team_turni WHERE data >= ? AND data <= ? ORDER BY data, operatore_id", (d_from, d_to))
        turni_idx = {(t["data"], t["operatore_id"]): t for t in turni_esistenti}
        colonne = fetchall(conn,
            "SELECT * FROM team_colonne_destra WHERE data >= ? AND data <= ? ORDER BY data", (d_from, d_to))
        col_idx = {c["data"]: c for c in colonne}
        ferie_params = [d_from, d_to]
        ferie_sql = """
            SELECT r.*, u.username, o.nome AS operatore_nome
            FROM team_ferie_requests r
            LEFT JOIN utenti u ON u.id = r.user_id
            LEFT JOIN team_operatori o ON o.id = r.operatore_id
            WHERE r.data >= ? AND r.data <= ? AND r.stato IN ('pending','approved')
        """
        linked_op = get_team_operator_for_user(conn, user["id"]) if not (user.get("is_team_editor") or user.get("is_admin")) else None
        if linked_op:
            ferie_sql += " AND r.operatore_id = ?"
            ferie_params.append(linked_op["id"])
        ferie_rows = fetchall(conn, ferie_sql, tuple(ferie_params))
        ferie_idx = {(r["data"], r["operatore_id"]): r for r in ferie_rows}
        conn.close()

        start_date_obj = None; end_date_obj = None; start_week_monday = None
        if start_date:
            try:
                start_date_obj = date.fromisoformat(start_date)
                start_week_monday = start_date_obj - timedelta(days=start_date_obj.weekday())
            except: pass
        if end_date:
            try: end_date_obj = date.fromisoformat(end_date)
            except: pass
        if start_date_obj and end_date_obj and end_date_obj < start_date_obj:
            start_date_obj, end_date_obj = end_date_obj, start_date_obj

        giorni = []
        for g in range(1, days + 1):
            data = f"{anno:04d}-{mese:02d}-{g:02d}"
            d = date.fromisoformat(data)
            dow = d.weekday()
            is_fest = data in FESTIVITA

            if start_date_obj and end_date_obj:
                in_range = start_date_obj <= d <= end_date_obj
            elif start_date_obj:
                in_range = d >= start_date_obj
            elif end_date_obj:
                in_range = d <= end_date_obj
            else:
                in_range = True

            if in_range and start_week_monday:
                cur_mon = d - timedelta(days=d.weekday())
                sett_idx = (cur_mon - start_week_monday).days // 7
                sett_ciclo = (sett_idx % operator_count) + 1
            else:
                sett_ciclo = None

            row_turni = []
            for op in ops:
                pos = op["posizione"]
                tpl = {"turno_base": "", "turno_var": "", "flags_base": "", "flags_var": ""}
                if in_range and sett_ciclo:
                    pos_orig = ((pos + sett_ciclo - 2) % operator_count) + 1
                    tpl_row = template.get(dow, {}).get(pos_orig, {})
                    tpl = {
                        "turno_base": tpl_row.get("turno_base", ""),
                        "turno_var": tpl_row.get("turno_var", ""),
                        "flags_base": tpl_row.get("flags", ""),
                        "flags_var": "",
                    }

                key = (data, op["id"])
                if key in turni_idx:
                    row = turni_idx[key]
                    shared_flags = row.get("flags", "") or ""
                    tpl = {
                        "turno_base": row.get("turno_base", "") or "",
                        "turno_var": row.get("turno_var", "") or "",
                        "flags_base": row.get("flags_base", "") or (shared_flags if not (row.get("turno_var") or "") else ""),
                        "flags_var": row.get("flags_var", "") or (shared_flags if (row.get("turno_var") or "") else ""),
                    }

                row_turni.append({
                    "operatore_id": op["id"],
                    "turno_base": tpl.get("turno_base", ""),
                    "turno_var":  tpl.get("turno_var", ""),
                    "flags_base": tpl.get("flags_base", ""),
                    "flags_var":  tpl.get("flags_var", ""),
                    "ferie_request": ferie_idx.get((data, op["id"])),
                })

            col = col_idx.get(data, {})
            rep_defaults = {"rep1": "", "rep2": "", "rep3": "", "fest_m1": "", "fest_m2": "", "fest_p1": "", "fest_p2": ""}
            if in_range and start_week_monday and dow in rep_template and operator_count > 0:
                cur_mon = d - timedelta(days=d.weekday())
                sett_idx = (cur_mon - start_week_monday).days // 7
                rep_defaults = _compute_team_rep_defaults(rep_template, d, operator_count, start_week_monday)
            giorni.append({
                "data": data, "giorno": g, "dow": dow,
                "is_domenica": dow == 6, "is_sabato": dow == 5, "is_festivo": is_fest,
                "turni": row_turni,
                "colonne_destra": {
                    "rep1": col.get("rep1","") or rep_defaults["rep1"],
                    "rep2": col.get("rep2","") or rep_defaults["rep2"],
                    "rep3": col.get("rep3","") or rep_defaults["rep3"],
                    "fest_m1": col.get("fest_m1","") or rep_defaults["fest_m1"],
                    "fest_m2": col.get("fest_m2","") or rep_defaults["fest_m2"],
                    "fest_p1": col.get("fest_p1","") or rep_defaults["fest_p1"],
                    "fest_p2": col.get("fest_p2","") or rep_defaults["fest_p2"],
                }
            })

        return {"anno": anno, "mese": mese,
                "start_date": start_date or "", "end_date": end_date or "",
                "operatori": ops, "giorni": giorni}
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(500, f"Errore interno: {str(e)}")


@app.post("/api/team/turni")
def set_team_turni(payload: TeamCellaInput, user=Depends(require_team_editor)):
    """
    Salva un turno team. Se col='var', aggiorna solo turno_var preservando turno_base esistente.
    Se col='base', aggiorna solo turno_base preservando turno_var esistente.
    """
    data    = payload.data
    op_id   = payload.operatore_id
    col     = payload.col or "base"
    flags_base = payload.flags_base
    flags_var  = payload.flags_var
    now     = datetime.now().isoformat()[:19]

    conn = get_db()
    existing = fetchone(conn, "SELECT turno_base, turno_var, flags, flags_base, flags_var FROM team_turni WHERE data=? AND operatore_id=?",
                        (data, op_id))
    existing_flags_base = (existing.get("flags_base") if existing else None) or ""
    existing_flags_var  = (existing.get("flags_var") if existing else None) or ""
    if existing and not existing_flags_base and not existing_flags_var and existing.get("flags"):
        if existing.get("turno_var"):
            existing_flags_var = existing.get("flags") or ""
        else:
            existing_flags_base = existing.get("flags") or ""

    if flags_base is None and flags_var is None:
        incoming_flags = payload.flags or ""
        if col == "var":
            flags_base = existing_flags_base
            flags_var = incoming_flags
        else:
            flags_base = incoming_flags
            flags_var = existing_flags_var
    else:
        flags_base = existing_flags_base if flags_base is None else flags_base
        flags_var = existing_flags_var if flags_var is None else flags_var

    # Preserva la colonna non modificata
    if col == "var":
        turno_base = existing["turno_base"] if existing else (payload.turno_base or "")
        turno_var  = payload.turno_var if payload.turno_var is not None else ""
    else:
        turno_base = payload.turno_base if payload.turno_base is not None else ""
        turno_var  = existing["turno_var"] if existing else (payload.turno_var or "")

    shared_flags = flags_var if turno_var else flags_base

    ex(conn, """INSERT INTO team_turni (data, operatore_id, turno_base, turno_var, flags, flags_base, flags_var, modificato_da, modificato_il)
       VALUES (?,?,?,?,?,?,?,?,?)
       ON CONFLICT(data, operatore_id) DO UPDATE SET
         turno_base=excluded.turno_base, turno_var=excluded.turno_var,
         flags=excluded.flags, flags_base=excluded.flags_base, flags_var=excluded.flags_var,
         modificato_da=excluded.modificato_da, modificato_il=excluded.modificato_il""",
       (data, op_id, turno_base, turno_var, shared_flags, flags_base, flags_var, user["username"], now))

    op = fetchone(conn, "SELECT nome FROM team_operatori WHERE id=?", (op_id,))
    campo_log = "turno_var" if col == "var" else "turno_base"
    vecchio = (existing.get(campo_log) or "") if existing else ""
    nuovo   = turno_var if col == "var" else turno_base
    log_flags = flags_var if col == "var" else flags_base
    ex(conn, """INSERT INTO team_log (data_modifica, utente, data_turno, operatore_nome, campo, vecchio_valore, nuovo_valore, flags)
       VALUES (?,?,?,?,?,?,?,?)""",
       (now, user["username"], data, op["nome"] if op else str(op_id), campo_log, vecchio, nuovo, log_flags))

    conn.commit(); conn.close()
    return {"ok": True, "propagati": 0}

@app.delete("/api/team/turni/{data}/{op_id}")
def delete_team_turno(data: str, op_id: int, user=Depends(require_team_editor)):
    conn = get_db()
    ex(conn, "DELETE FROM team_turni WHERE data=? AND operatore_id=?", (data, op_id))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Team: colonne destra ──────────────────────────────────────────────────────
@app.post("/api/team/colonne-destra")
def set_colonne_destra(payload: TeamColonneDestraInput, user=Depends(require_team_editor)):
    data = payload.data
    conn = get_db()
    vals = (data,
            payload.rep1 or "", payload.rep2 or "", payload.rep3 or "",
            payload.fest_m1 or "", payload.fest_m2 or "",
            payload.fest_p1 or "", payload.fest_p2 or "")
    ex(conn, """INSERT INTO team_colonne_destra (data,rep1,rep2,rep3,fest_m1,fest_m2,fest_p1,fest_p2)
       VALUES (?,?,?,?,?,?,?,?)
       ON CONFLICT(data) DO UPDATE SET
         rep1=excluded.rep1, rep2=excluded.rep2, rep3=excluded.rep3,
         fest_m1=excluded.fest_m1, fest_m2=excluded.fest_m2,
         fest_p1=excluded.fest_p1, fest_p2=excluded.fest_p2""", vals)
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/api/team/ferie/request-batch")
def save_team_ferie_request(payload: TeamFerieBatchInput, user=Depends(get_current_user)):
    conn = get_db()
    op = get_team_operator_for_user(conn, user["id"])
    if not op:
        conn.close()
        raise HTTPException(403, "Account non associato a un operatore team")

    add_dates = sorted({d for d in payload.add_dates if _parse_iso_date(d)})
    remove_dates = sorted({d for d in payload.remove_dates if _parse_iso_date(d)})
    for data_turno in add_dates:
        row = fetchone(conn, "SELECT id, stato, user_id FROM team_ferie_requests WHERE operatore_id=? AND data=?",
                       (op["id"], data_turno))
        if row and row["user_id"] != user["id"] and not (user.get("is_team_editor") or user.get("is_admin")):
            conn.close()
            raise HTTPException(403, "Richiesta ferie già presente per questo giorno")
        if row:
            old_status = row.get("stato")
            ex(conn, """UPDATE team_ferie_requests
                        SET user_id=?, stato='pending', requested_by=?, reviewed_by=NULL, updated_at=?
                        WHERE id=?""",
               (user["id"], user["username"], datetime.now().isoformat()[:19], row["id"]))
            _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                            "requested", old_status, "pending")
        else:
            ex(conn, """INSERT INTO team_ferie_requests
                        (user_id, operatore_id, data, stato, requested_by, updated_at)
                        VALUES (?,?,?,?,?,?)""",
               (user["id"], op["id"], data_turno, "pending", user["username"], datetime.now().isoformat()[:19]))
            _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                            "requested", None, "pending")

    for data_turno in remove_dates:
        row = fetchone(conn, "SELECT id, stato, user_id FROM team_ferie_requests WHERE operatore_id=? AND data=?",
                       (op["id"], data_turno))
        if not row:
            continue
        if row["user_id"] != user["id"] and not (user.get("is_team_editor") or user.get("is_admin")):
            conn.close()
            raise HTTPException(403, "Non puoi rimuovere questa richiesta")
        ex(conn, "DELETE FROM team_ferie_requests WHERE id=?", (row["id"],))
        _log_team_ferie(conn, user["username"], user["id"], user["username"], op["id"], op["nome"], data_turno,
                        "removed", row.get("stato"), None)

    conn.commit()
    conn.close()
    return {"ok": True, "linked_operatore_id": op["id"]}

@app.post("/api/team/ferie/review")
def review_team_ferie(payload: TeamFerieReviewInput, user=Depends(require_team_editor)):
    new_status = (payload.status or "").strip().lower()
    if new_status not in {"approved", "rejected"}:
        raise HTTPException(400, "Stato non valido")
    dates = sorted({d for d in payload.dates if _parse_iso_date(d)})
    if not dates:
        raise HTTPException(400, "Nessuna data valida")

    conn = get_db()
    op = fetchone(conn, "SELECT id, nome FROM team_operatori WHERE id=?", (payload.operatore_id,))
    if not op:
        conn.close()
        raise HTTPException(404, "Operatore non trovato")
    rows = fetchall(conn, f"""
        SELECT r.*, u.username
        FROM team_ferie_requests r
        LEFT JOIN utenti u ON u.id = r.user_id
        WHERE r.operatore_id=? AND r.data IN ({','.join([get_limit_placeholder() for _ in dates])})
    """, tuple([payload.operatore_id] + dates))
    row_by_date = {r["data"]: r for r in rows}
    now = datetime.now().isoformat()[:19]
    for data_turno in dates:
        row = row_by_date.get(data_turno)
        if not row:
            continue
        ex(conn, "UPDATE team_ferie_requests SET stato=?, reviewed_by=?, updated_at=? WHERE id=?",
           (new_status, user["username"], now, row["id"]))
        _log_team_ferie(conn, user["username"], row["user_id"], row.get("username"), op["id"], op["nome"], data_turno,
                        "reviewed", row.get("stato"), new_status)
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/team/ferie/dashboard")
def get_team_ferie_dashboard(user=Depends(get_current_user)):
    conn = get_db()
    is_editor = bool(user.get("is_team_editor")) or bool(user.get("is_admin"))
    linked_op = get_team_operator_for_user(conn, user["id"])
    pending_rows = fetchall(conn, """
        SELECT r.operatore_id, o.nome AS operatore_nome, u.username, u.nome,
               COUNT(*) AS giorni,
               MIN(r.data) AS first_day,
               MAX(r.data) AS last_day
        FROM team_ferie_requests r
        LEFT JOIN team_operatori o ON o.id = r.operatore_id
        LEFT JOIN utenti u ON u.id = r.user_id
        WHERE r.stato='pending'
        GROUP BY r.operatore_id, o.nome, u.username, u.nome
        ORDER BY first_day, operatore_nome
    """) if is_editor else []
    if is_editor:
        for row in pending_rows:
            dates = fetchall(conn, "SELECT data FROM team_ferie_requests WHERE stato='pending' AND operatore_id=? ORDER BY data",
                             (row["operatore_id"],))
            row["dates"] = [d["data"] for d in dates]
    if is_editor:
        recent_log = fetchall(conn, f"""
            SELECT * FROM team_ferie_log
            ORDER BY id DESC
            LIMIT {get_limit_placeholder()}
        """, (120,))
    elif linked_op:
        recent_log = fetchall(conn, f"""
            SELECT * FROM team_ferie_log
            WHERE operatore_id=?
            ORDER BY id DESC
            LIMIT {get_limit_placeholder()}
        """, (linked_op["id"], 60))
    else:
        recent_log = []
    conn.close()
    return {
        "pending": pending_rows,
        "recent_log": recent_log,
        "linked_operatore_id": linked_op["id"] if linked_op else None,
    }

@app.get("/api/admin/team/ferie/pending")
def get_admin_team_ferie_pending(admin=Depends(require_admin)):
    conn = get_db()
    groups = fetchall(conn, """
        SELECT r.operatore_id, o.nome AS operatore_nome, u.username, u.nome,
               COUNT(*) AS giorni,
               MIN(r.data) AS first_day,
               MAX(r.data) AS last_day
        FROM team_ferie_requests r
        LEFT JOIN team_operatori o ON o.id = r.operatore_id
        LEFT JOIN utenti u ON u.id = r.user_id
        WHERE r.stato='pending'
        GROUP BY r.operatore_id, o.nome, u.username, u.nome
        ORDER BY first_day, operatore_nome
    """)
    for group in groups:
        rows = fetchall(conn, "SELECT data FROM team_ferie_requests WHERE stato='pending' AND operatore_id=? ORDER BY data",
                        (group["operatore_id"],))
        group["dates"] = [r["data"] for r in rows]
    conn.close()
    return groups

@app.get("/api/admin/team/ferie/log")
def get_admin_team_ferie_log(limit: int = 150, admin=Depends(require_admin)):
    conn = get_db()
    rows = fetchall(conn, f"SELECT * FROM team_ferie_log ORDER BY id DESC LIMIT {get_limit_placeholder()}", (limit,))
    conn.close()
    return rows

# ─── Team: template settimanale ────────────────────────────────────────────────
@app.get("/api/team/template-week")
def get_team_template_week(user=Depends(get_current_user)):
    conn = get_db()
    rows = fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
    rep_rows = fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
    cfg = fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
    conn.close()
    template = {}
    for r in rows:
        g = r["giorno_settimana"]
        if g not in template: template[g] = []
        template[g].append({"posizione": r["posizione"], "turno_base": r["turno_base"] or "",
                             "turno_var": r["turno_var"] or "", "flags": r["flags"] or ""})
    reperibili = {
        str(r["giorno_settimana"]): {
            "rep1_pos": r.get("rep1_pos"),
            "rep2_pos": r.get("rep2_pos"),
            "rep3_pos": r.get("rep3_pos"),
            "fest_m1_pos": r.get("fest_m1_pos"),
            "fest_m2_pos": r.get("fest_m2_pos"),
            "fest_p1_pos": r.get("fest_p1_pos"),
            "fest_p2_pos": r.get("fest_p2_pos"),
        }
        for r in rep_rows
    }
    return {
        "posizioni": template,
        "reperibili": reperibili,
        "start_date": (cfg or {}).get("start_date", "") or "",
        "end_date": (cfg or {}).get("end_date", "") or "",
    }

@app.post("/api/team/template-week")
def save_team_template_week(payload: TeamTemplateWeekInput, user=Depends(require_team_editor)):
    conn = get_db()
    old_rows = fetchall(conn, "SELECT * FROM team_template_weekly ORDER BY giorno_settimana, posizione")
    old_rep_rows = fetchall(conn, "SELECT * FROM team_template_reperibili_weekly ORDER BY giorno_settimana")
    old_cfg = fetchone(conn, "SELECT start_date, end_date FROM team_template_config WHERE id=1")
    ops = fetchall(conn, "SELECT * FROM team_operatori WHERE attivo=1 ORDER BY posizione")

    old_template = {}
    for r in old_rows:
        g = r["giorno_settimana"]
        if g not in old_template:
            old_template[g] = {}
        old_template[g][r["posizione"]] = {
            "turno_base": r["turno_base"] or "",
            "turno_var": r["turno_var"] or "",
            "flags": r["flags"] or "",
        }
    old_reperibili = {
        r["giorno_settimana"]: {
            "rep1": r.get("rep1_pos"),
            "rep2": r.get("rep2_pos"),
            "rep3": r.get("rep3_pos"),
            "fest_m1": r.get("fest_m1_pos"),
            "fest_m2": r.get("fest_m2_pos"),
            "fest_p1": r.get("fest_p1_pos"),
            "fest_p2": r.get("fest_p2_pos"),
        }
        for r in old_rep_rows
    }
    _preserve_team_schedule_outside_range(
        conn, ops, old_template, old_reperibili,
        (old_cfg or {}).get("start_date"), (old_cfg or {}).get("end_date"),
        payload.start_date, payload.end_date
    )
    _clear_team_schedule_in_range(conn, payload.start_date, payload.end_date)

    ex(conn, "DELETE FROM team_template_weekly")
    ex(conn, "DELETE FROM team_template_reperibili_weekly")
    for giorno_str, posizioni in payload.posizioni.items():
        giorno = int(giorno_str)
        for pos in posizioni:
            ex(conn, "INSERT INTO team_template_weekly (giorno_settimana, posizione, turno_base, turno_var, flags) VALUES (?,?,?,?,?)",
               (giorno, pos.posizione, pos.turno_base or "", pos.turno_var or "", pos.flags or ""))
    for giorno_str, rep in payload.reperibili.items():
        giorno = int(giorno_str)
        ex(conn, """INSERT INTO team_template_reperibili_weekly
           (giorno_settimana, rep1_pos, rep2_pos, rep3_pos, fest_m1_pos, fest_m2_pos, fest_p1_pos, fest_p2_pos)
           VALUES (?,?,?,?,?,?,?,?)""",
           (giorno, rep.rep1_pos, rep.rep2_pos, rep.rep3_pos,
            rep.fest_m1_pos, rep.fest_m2_pos, rep.fest_p1_pos, rep.fest_p2_pos))
    ex(conn,
       """INSERT INTO team_template_config (id, start_date, end_date)
          VALUES (1, ?, ?)
          ON CONFLICT(id) DO UPDATE SET start_date=excluded.start_date, end_date=excluded.end_date""",
       (payload.start_date or None, payload.end_date or None))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Team: log ─────────────────────────────────────────────────────────────────
@app.get("/api/team/log")
def get_team_log(limit: int = 100, user=Depends(get_current_user)):
    conn = get_db()
    logs = fetchall(conn, f"SELECT * FROM team_log ORDER BY id DESC LIMIT {get_limit_placeholder()}", (limit,))
    conn.close()
    return logs

# ─── Static ────────────────────────────────────────────────────────────────────
app.mount("/", StaticFiles(directory="static", html=True), name="static")
