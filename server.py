#!/usr/bin/env python3
"""
StatsPack Analytics Lab — analytical companion for the Visitor Management System (VMS)
Single-file backend, Python standard library only. Run:  python3 server.py  ->  http://localhost:8460

How the VMS connects (no manual CSV shuffling):
  * Every client gets an API key (managed by the StatsPack super user).
  * The VMS pushes each check-in / check-out as it happens:
        POST /ingest/visits      (JSON object, JSON array, or raw CSV body)
        X-API-Key: <client key>
  * The same endpoint accepts the VMS's existing real-time CSV exports verbatim
    (Content-Type: text/csv), so "just connect" means pointing the VMS export hook at this URL.
  * Re-sending a visit with the same visit_id updates it (that's how checkout lands).
  * GET /ingest/ping tests connectivity + key.
"""
import json, os, re, csv, io, sqlite3, hashlib, secrets, threading, time, urllib.request, urllib.parse
from datetime import datetime, timedelta, date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT", 8460))          # most hosts inject PORT; default 8460
# Set DB_PATH to a persistent location (e.g. /var/data/analyticslab.db) in production
# so the database survives deploys/restarts. Locally it defaults to the project folder.
DB = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyticslab.db"))
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
LOCK = threading.Lock()

# ---------------------------------------------------------------- helpers
def now(): return datetime.now().replace(microsecond=0)
def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S")
def hpw(pw, salt): return hashlib.sha256((salt + pw).encode()).hexdigest()

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
IS_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

class _PGRow(dict):
    """dict row that also supports positional access, like sqlite3.Row."""
    __slots__ = ()
    def __getitem__(self, k):
        if isinstance(k, int):
            return list(self.values())[k]
        return dict.__getitem__(self, k)

class _PGCursorWrap:
    """Makes a psycopg cursor behave like sqlite3's execute() result."""
    def __init__(self, cur): self._cur = cur
    def fetchone(self):
        r = self._cur.fetchone()
        return _PGRow(r) if r is not None else None
    def fetchall(self): return [_PGRow(r) for r in self._cur.fetchall()]
    def __iter__(self):
        for r in self._cur: yield _PGRow(r)

class PGConn:
    """Thin adapter so the rest of the app can speak SQLite-flavoured SQL to Postgres."""
    def __init__(self, dsn):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise SystemExit(
                "DATABASE_URL is set, but the Postgres driver is missing.\n"
                'Install it with:  pip install "psycopg[binary]"\n'
                "Or unset DATABASE_URL to use the built-in SQLite storage.")
        self._psycopg = psycopg
        self._conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=False)
    @staticmethod
    def _translate(sql, has_params=True):
        # psycopg parses % as a placeholder marker whenever parameters are passed, so any
        # literal % in the SQL (e.g. LIKE 'Demo%') must be doubled first. When no params
        # are passed psycopg does no parsing, so % must be left exactly as written.
        if has_params:
            sql = sql.replace("%", "%%")
        # ? placeholders -> %s ; sqlite autoincrement -> postgres identity
        out, in_s, quote = [], False, ""
        for ch in sql:
            if in_s:
                out.append(ch)
                if ch == quote: in_s = False
                continue
            if ch in ("'", '"'): in_s, quote = True, ch; out.append(ch); continue
            out.append("%s" if ch == "?" else ch)
        sql = "".join(out)
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
        return sql
    def execute(self, sql, params=()):
        cur = self._conn.cursor()
        params = tuple(params)
        if params:
            cur.execute(self._translate(sql, True), params)
        else:
            cur.execute(self._translate(sql, False))
        return _PGCursorWrap(cur)
    def executescript(self, script):
        cur = self._conn.cursor()
        for stmt in [x.strip() for x in script.split(";") if x.strip()]:
            cur.execute(self._translate(stmt, False))
        self._conn.commit()
    def commit(self): self._conn.commit()
    def close(self): self._conn.close()

def connect():
    if IS_PG:
        return PGConn(DATABASE_URL)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def table_columns(conn, table):
    """Column names for a table, on either engine."""
    if IS_PG:
        return [r["column_name"] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=?", (table,))]
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]

SCHEMA = """
CREATE TABLE IF NOT EXISTS clients(
  id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, api_key TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL DEFAULT 1, max_visit_mins INTEGER NOT NULL DEFAULT 60,
  hourly_rate REAL NOT NULL DEFAULT 0, currency TEXT NOT NULL DEFAULT 'BWP',
  created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY, client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
  name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, salt TEXT NOT NULL, pw TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('super','admin')), active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(
  token TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS visits(
  id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  visit_id TEXT NOT NULL, visitor_name TEXT NOT NULL DEFAULT '', contact TEXT NOT NULL DEFAULT '',
  id_number TEXT NOT NULL DEFAULT '', region TEXT NOT NULL DEFAULT '', town TEXT NOT NULL DEFAULT '',
  host_department TEXT NOT NULL DEFAULT '', purpose TEXT NOT NULL DEFAULT '',
  person_type TEXT NOT NULL DEFAULT 'Visitor',
  hourly_rate REAL,
  check_in TEXT NOT NULL, check_out TEXT,
  UNIQUE(client_id, visit_id));
CREATE INDEX IF NOT EXISTS ix_visits_client_in ON visits(client_id, check_in);
CREATE TABLE IF NOT EXISTS geocache(
  place TEXT PRIMARY KEY, lat REAL, lon REAL, found INTEGER NOT NULL DEFAULT 0,
  looked_up_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ingest_log(
  id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  ts TEXT NOT NULL, source TEXT NOT NULL, rows_in INTEGER NOT NULL,
  inserted INTEGER NOT NULL, updated INTEGER NOT NULL, errors INTEGER NOT NULL, note TEXT NOT NULL DEFAULT '');
"""

# ---------------------------------------------------------------- seed
def seed(conn):
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]:
        return
    t = iso(now())
    def add_user(cid, name, email, pw, role):
        salt = secrets.token_hex(8)
        conn.execute("INSERT INTO users(client_id,name,email,salt,pw,role,active,created_at) VALUES(?,?,?,?,?,?,1,?)",
                     (cid, name, email, salt, hpw(pw, salt), role, t))
    add_user(None, "StatsPack HQ", "admin@statspack.co.ls", "super123", "super")
    demo_key = os.environ.get("DEMO_API_KEY") or ("spk_live_" + secrets.token_hex(16))
    conn.execute("INSERT INTO clients(name,api_key,active,max_visit_mins,hourly_rate,currency,created_at) VALUES(?,?,1,60,?,?,?)",
                 ("Demo Client (Botswana Insurance)", demo_key, 85.0, "BWP", t))
    cid = conn.execute("SELECT id FROM clients WHERE name LIKE 'Demo%'").fetchone()["id"]
    add_user(cid, "Demo Admin", "admin@demo.client", "admin123", "admin")

    # ---- permanent test client for the VMS integration (empty data on purpose)
    # TEST_API_KEY env var pins this key so it survives restarts/redeploys even without a disk
    test_key = os.environ.get("TEST_API_KEY") or ("spk_test_" + secrets.token_hex(16))
    conn.execute("INSERT INTO clients(name,api_key,active,max_visit_mins,hourly_rate,currency,created_at) VALUES(?,?,1,60,?,?,?)",
                 ("SmartRegister Test Environment", test_key, 85.0, "BWP", t))
    tcid = conn.execute("SELECT id FROM clients WHERE name='SmartRegister Test Environment'").fetchone()["id"]
    add_user(tcid, "SmartRegister Test Admin", "vms@test.client", "vms123", "admin")

    # ---- demo visits shaped like the VMS analytics PDF (last ~6 months up to today)
    import random
    rng = random.Random(42)
    regions = [("Maseru","Maseru",41),("Southern","Gaborone",25),("Kgalagadi","Moshupa",15),
               ("Tsabong","Tsabong",10),("Kgatleng","Mochudi",9)]
    purposes = [("Tender Collection",34,18),("Enquiry",20,25),("Statement Collection",16,34),
                ("Payment",16,14),("Proposal Pitch",8,16),("register a case",3,49),
                ("Service/Product demo",2,19),("executive lunch",1,16)]
    depts = [("Information technology",29,13),("Customer Service",27,12),("Administration",26,13),
             ("Quality Assurance",8,6),("Legal",8,10),("Finance and Accounts",4,11),
             ("Human Resource",3,9),("Procurement",2,8),("Sales",2,9)]
    names = ["Thabang Moremoholo","Teboho Morai","Mokeke","Moleboheng Ntai","Ntholeng Lechesa",
             "Alister","Thapelo Tlale","Karabo Nkuebe","Lineo Mahao","Palesa Sello","Tumelo Rants'o",
             "Kea Modise","Bonolo Seetso","Neo Phiri","Lerato Mokoena","Katlego Pule","Refilwe Dube",
             "Onalenna Kgosi","Tshepo Molefe","Naledi Kau","Boitumelo Rre","Sechaba Lets'olo",
             "Mpho Ramaili","Limpho Thamae","Rethabile Nteso","Khotso Mda","Puleng Rasekoai",
             "Tefo Makara","Amohelang Sese","Itumeleng Tau"]
    def pick(weighted):
        total = sum(w for *_, w in [(x[0], x[-2] if len(x) > 2 else x[1]) for x in weighted]) if False else 0
        r = rng.uniform(0, sum(x[1] for x in weighted)); acc = 0
        for x in weighted:
            acc += x[1]
            if r <= acc: return x
        return weighted[-1]
    today = date.today()
    vseq = 1000
    monthly_target = [20, 7, 8, 70, 32, 24]  # shape from the PDF, oldest -> current month
    for mi, count in enumerate(monthly_target):
        mdate = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        # month start for (5-mi) months ago
        y, m = today.year, today.month
        back = 5 - mi
        m -= back
        while m <= 0: m += 12; y -= 1
        for _ in range(count):
            d0 = date(y, m, 1)
            if (y, m) == (today.year, today.month):
                dday = rng.randint(1, max(1, today.day))
            else:
                dday = rng.randint(1, 28)
            reg = pick([(r, w) for r, ttown, w in [(x[0], x[1], x[2]) for x in regions]])
            region = reg[0]; town = dict((x[0], x[1]) for x in regions)[region]
            pur = pick([(p[0], p[1]) for p in purposes]); purpose = pur[0]
            pdur = dict((p[0], p[2]) for p in purposes)[purpose]
            dep = pick([(d[0], d[1]) for d in depts]); dept = dep[0]
            hour = pick([(12,16),(13,16),(14,11),(15,16),(16,12),(17,10),(18,10),(9,8),(10,9),(11,10)])[0]
            cin = datetime(y, m, dday, hour, rng.randint(0, 59))
            dur = max(3, int(rng.gauss(pdur, pdur * 0.35)))
            nm = rng.choice(names)
            ptype = pick([("Visitor", 70), ("Employee", 20), ("Contractor", 10)])[0]
            prate = None   # no per-visit override: the client's default rate applies
            vseq += 1
            conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                            host_department,purpose,person_type,hourly_rate,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                         (cid, f"VMS-{vseq}", nm, str(rng.randint(60000000, 79999999)),
                          str(rng.randint(800000000, 2699999999)), region, town, dept, purpose, ptype, prate,
                          iso(cin), iso(cin + timedelta(minutes=dur))))
    # a few ongoing visits right now
    ongoing = [("Kgalagadi","Moshupa","Customer Service","Enquiry","Thabang Moremoholo"),
               ("Maseru","Maseru","Finance and Accounts","Tender Collection","Teboho Morai"),
               ("Maseru","Maseru","Human Resource","Statement Collection","Mokeke"),
               ("Maseru","Maseru","Information technology","Tender Collection","Moleboheng Ntai"),
               ("Maseru","Maseru","Procurement","Payment","Ntholeng Lechesa"),
               ("Southern","Gaborone","Legal","Policy cancellation","Alister"),
               ("Southern","Gaborone","Sales","Enquiry","Thapelo Tlale"),
               ("Tsabong","Tsabong","Administration","Lunch delivery","Alister"),
               ("Tsabong","Tsabong","Finance and Accounts","Tender Collection","Alister")]
    for i, (region, town, dept, purpose, nm) in enumerate(ongoing):
        vseq += 1
        cin = now() - timedelta(minutes=rng.randint(2, 95))
        ptype = ["Visitor","Visitor","Visitor","Visitor","Visitor","Employee","Employee","Contractor","Visitor"][i]
        conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                        host_department,purpose,person_type,hourly_rate,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
                     (cid, f"VMS-{vseq}", nm, str(rng.randint(60000000, 79999999)),
                      str(rng.randint(800000000, 2699999999)), region, town, dept, purpose, ptype,
                      None, iso(cin)))
    conn.commit()

# ---------------------------------------------------------------- ingest
FIELD_ALIASES = {
    "visit_id": ["visit_id","visitid","id","visit id","ref","reference"],
    "visitor_name": ["visitor name","visitor_name","visitor","visitor /user name","visitor/user name","user name","name"],
    "contact": ["contact number","contact_number","contact","phone","mobile","cell"],
    "id_number": ["id_number","id number","idnumber","national_id","omang"],
    "region": ["region"],
    "town": ["town","city"],
    "host_department": ["host department","host_department","department","dept"],
    "purpose": ["purpose","purpose_of_visit","purpose of visit"],
    "person_type": ["visit type","person_type","person type","visitor type","entry type","register","type","category","role"],
    "hourly_rate": ["hourly_rate","hourly rate","rate","pay_rate","pay rate","wage","cost_per_hour","cost per hour"],
    "check_in": ["check in date","check in","check_in","checkin","time in","arrival","check in time","date in"],
    "check_out": ["check out date","check out","check_out","checkout","time out","departure","check out time","date out"],
}
VALID_PERSON_TYPES = {"visitor", "employee", "contractor", "staff", "worker"}

# values that mean "nothing was chosen" in an export, not real data
PLACEHOLDERS = {"null", "none", "n/a", "na", "-", "--", "select", "choose", "please select",
                "select one", "unspecified", "undefined", "unknown", "tbd"}

def _norm_key(k):
    k = (k or "").strip().lower().replace("-", " ").replace("_", " ")
    return re.sub(r"\s+", " ", k)

def norm_row(raw):
    low = {}
    for k, v in raw.items():
        key = _norm_key(k)
        low[key] = v.strip() if isinstance(v, str) else v
    out = {}
    for field, aliases in FIELD_ALIASES.items():
        for a in aliases:
            a = _norm_key(a)
            val = low.get(a)
            if val in ("", None): continue
            if isinstance(val, str) and val.strip().lower() in PLACEHOLDERS: continue
            # a column like "Category" often holds something that is not a person type at all
            if field == "person_type" and str(val).strip().lower() not in VALID_PERSON_TYPES:
                continue
            out[field] = val
            break
    return out

def parse_dt(v, dayfirst=None):
    """Accepts ISO-8601 (with milliseconds and/or timezone offset), common regional
    formats (dd/mm and mm/dd), epoch seconds/milliseconds, and Firestore timestamps."""
    if v is None or v == "": return None
    # Firestore / JSON timestamp objects
    if isinstance(v, dict):
        for k in ("_seconds", "seconds", "epochSecond"):
            if k in v:
                try: return datetime.fromtimestamp(int(v[k]))
                except Exception: return None
        for k in ("value", "iso", "timestamp"):
            if k in v: return parse_dt(v[k], dayfirst)
        return None
    # epoch numbers (seconds or milliseconds)
    if isinstance(v, (int, float)):
        n = float(v)
        try: return datetime.fromtimestamp(n / 1000 if n > 1e11 else n)
        except Exception: return None
    s = str(v).strip()
    if not s: return None
    if re.fullmatch(r"\d{10}(\.\d+)?", s):
        try: return datetime.fromtimestamp(float(s))
        except Exception: return None
    if re.fullmatch(r"\d{13}", s):
        try: return datetime.fromtimestamp(int(s) / 1000)
        except Exception: return None
    # ISO-8601, including "2026-07-26T15:39:00.000Z" and "+02:00" offsets
    iso = s.replace("z", "Z")
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return d.replace(tzinfo=None) if d.tzinfo is None else d.astimezone().replace(tzinfo=None)
    except ValueError:
        pass
    # strip trailing Z / offset / fractional seconds, then try known layouts
    t = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", s).strip()
    t = re.sub(r"\.\d+$", "", t)
    day_first = True if dayfirst is None else dayfirst
    dmy = ["%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%d-%m-%Y %H:%M:%S","%d-%m-%Y %H:%M",
           "%d/%m/%Y","%d-%m-%Y"]
    mdy = ["%m/%d/%Y %H:%M:%S","%m/%d/%Y %H:%M","%m-%d-%Y %H:%M:%S","%m-%d-%Y %H:%M",
           "%m/%d/%Y","%m-%d-%Y"]
    fmts = (["%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M","%Y-%m-%d %H:%M",
             "%Y/%m/%d %H:%M:%S","%Y/%m/%d %H:%M"]
            + (dmy + mdy if day_first else mdy + dmy)
            + ["%Y-%m-%d","%d %b %Y %H:%M:%S","%d %B %Y %H:%M:%S"])
    for f in fmts:
        try: return datetime.strptime(t, f)
        except ValueError: pass
    return None

def detect_dayfirst(samples):
    """Decide whether dd/mm or mm/dd is in use by looking at the whole batch.
    Returns True (day first), False (month first), or None if undecidable."""
    day_evidence = month_evidence = 0
    for raw in samples:
        t = str(raw or "").strip()
        m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})", t)
        if not m: continue
        a, b = int(m.group(1)), int(m.group(2))
        if a > 12 and b <= 12: day_evidence += 1
        elif b > 12 and a <= 12: month_evidence += 1
    if month_evidence and not day_evidence: return False
    if day_evidence and not month_evidence: return True
    return None

def synth_visit_id(r):
    """Stable id for sources (e.g. CSV exports) that carry no visit reference.
    Same person + same check-in always yields the same id, so re-uploading the
    file updates rows instead of duplicating them."""
    basis = "|".join(str(r.get(k, "") or "").strip().lower() for k in
                     ("visitor_name", "check_in", "id_number", "contact", "town", "host_department"))
    return "auto-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:20]

def ingest_rows(conn, client_id, rows, source):
    ins = upd = err = 0
    problems = []
    dicts = [x for x in rows if isinstance(x, dict)]
    dayfirst = detect_dayfirst([norm_row(x).get("check_in") for x in dicts])
    for idx, raw in enumerate(rows, 1):
        if not isinstance(raw, dict):
            err += 1; problems.append({"row": idx, "reason": "Row is not a JSON object"}); continue
        if not any(str(v or "").strip() for v in raw.values()):
            continue                      # blank line in a CSV export
        r = norm_row(raw)
        raw_cin = r.get("check_in", "")
        cin = parse_dt(raw_cin, dayfirst)
        vid = str(r.get("visit_id", "") or "").strip()
        if not vid and cin:
            vid = synth_visit_id({**r, "check_in": iso(cin)})
        if not vid or not cin:
            err += 1
            if not cin and raw_cin in ("", None):
                reason = ("No check-in time found. Received columns: "
                          + ", ".join(sorted(str(k) for k in raw.keys())[:12]))
            elif raw_cin in ("", None):
                reason = "Missing 'check_in'"
            else:
                reason = (f"Unrecognised 'check_in' value: {str(raw_cin)[:40]!r}. "
                          "Use ISO-8601, e.g. 2026-07-26T15:39:00")
            problems.append({"row": idx, "visit_id": str(vid)[:40], "reason": reason})
            continue
        if not str(r.get("region", "") or "").strip() and str(r.get("town", "") or "").strip():
            r["region"] = r["town"]
        cout = parse_dt(r.get("check_out", ""), dayfirst)
        ptype = str(r.get("person_type", "") or "Visitor").strip().title() or "Visitor"
        if ptype in ("Staff", "Worker"): ptype = "Employee"
        try:
            rate = float(str(r.get("hourly_rate", "")).replace(",", "").strip() or 0) or None
        except (TypeError, ValueError):
            rate = None
        vals = (r.get("visitor_name",""), r.get("contact",""), r.get("id_number",""),
                r.get("region",""), r.get("town",""), r.get("host_department",""),
                r.get("purpose",""), ptype, rate, iso(cin), iso(cout) if cout else None)
        cur = conn.execute("SELECT id FROM visits WHERE client_id=? AND visit_id=?", (client_id, vid)).fetchone()
        if cur:
            conn.execute("""UPDATE visits SET visitor_name=?,contact=?,id_number=?,region=?,town=?,
                            host_department=?,purpose=?,person_type=?,hourly_rate=?,check_in=?,check_out=? WHERE id=?""", vals + (cur["id"],))
            upd += 1
        else:
            conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                            host_department,purpose,person_type,hourly_rate,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                         (client_id, vid) + vals)
            ins += 1
    note = " | ".join(f"row {p['row']}: {p['reason']}" for p in problems)[:600]
    conn.execute("INSERT INTO ingest_log(client_id,ts,source,rows_in,inserted,updated,errors,note) VALUES(?,?,?,?,?,?,?,?)",
                 (client_id, iso(now()), source, len(rows), ins, upd, err, note))
    conn.commit()
    out = {"received": len(rows), "inserted": ins, "updated": upd, "errors": err}
    if problems: out["problems"] = problems[:20]
    return out

# ---------------------------------------------------------------- stats
def mins(a, b):
    """Duration in minutes, rounded to the nearest minute. Any visit with a
    measurable length counts as at least 1 minute, so short visits are never
    lost from durations or cost."""
    try:
        secs = (datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds()
    except Exception:
        return 0
    if secs <= 0: return 0
    return max(1, int(round(secs / 60.0)))

def _flt_where(cid, flt):
    flt = flt or {}
    where, P = ["client_id=?"], [cid]
    if flt.get("date_from"): where.append("substr(check_in,1,10)>=?"); P.append(flt["date_from"])
    if flt.get("date_to"):   where.append("substr(check_in,1,10)<=?"); P.append(flt["date_to"])
    if flt.get("region"):    where.append("region=?"); P.append(flt["region"])
    if flt.get("department"):where.append("host_department=?"); P.append(flt["department"])
    if flt.get("person_type"):where.append("person_type=?"); P.append(flt["person_type"])
    return " AND ".join(where), P   # note: display_currency is presentation-only, never filters rows

def _flt_from_qs(qs):
    g = lambda k: (qs.get(k, [""])[0] or "").strip()
    return {"date_from": g("date_from"), "date_to": g("date_to"),
            "region": g("region"), "department": g("department"), "person_type": g("person_type"),
            "display_currency": g("display_currency")}

def stats_overview(conn, cid, flt=None):
    flt = flt or {}
    where, P = ["client_id=?"], [cid]
    if flt.get("date_from"): where.append("substr(check_in,1,10)>=?"); P.append(flt["date_from"])
    if flt.get("date_to"):   where.append("substr(check_in,1,10)<=?"); P.append(flt["date_to"])
    if flt.get("region"):    where.append("region=?"); P.append(flt["region"])
    if flt.get("department"):where.append("host_department=?"); P.append(flt["department"])
    if flt.get("person_type"):where.append("person_type=?"); P.append(flt["person_type"])
    W = " AND ".join(where)
    t = now(); today_s = t.strftime("%Y-%m-%d")
    q = lambda sql, *a: conn.execute(sql, tuple(P) + a).fetchone()
    total = q(f"SELECT COUNT(*) c FROM visits WHERE {W}")["c"]
    unique = q(f"""SELECT COUNT(DISTINCT COALESCE(NULLIF(id_number,''), visitor_name||'|'||contact)) c
                  FROM visits WHERE {W}""")["c"]
    today_c = q(f"SELECT COUNT(*) c FROM visits WHERE {W} AND substr(check_in,1,10)=?", today_s)["c"]
    ongoing = q(f"SELECT COUNT(*) c FROM visits WHERE {W} AND check_out IS NULL")["c"]
    days = q(f"SELECT COUNT(DISTINCT substr(check_in,1,10)) c FROM visits WHERE {W}")["c"] or 1
    durs = [mins(r["check_in"], r["check_out"]) for r in
            conn.execute(f"SELECT check_in,check_out FROM visits WHERE {W} AND check_out IS NOT NULL", tuple(P))]
    avg_dur = round(sum(durs) / len(durs)) if durs else 0
    daily = conn.execute(f"""SELECT substr(check_in,1,10) d, COUNT(*) c FROM visits WHERE {W}
                            GROUP BY d ORDER BY c""", tuple(P)).fetchall()
    lo = {"count": daily[0]["c"], "date": daily[0]["d"]} if daily else None
    hi = {"count": daily[-1]["c"], "date": daily[-1]["d"]} if daily else None
    m0 = t.strftime("%Y-%m")
    prev = (t.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    cm = q(f"SELECT COUNT(*) c FROM visits WHERE {W} AND substr(check_in,1,7)=?", m0)["c"]
    pm = q(f"SELECT COUNT(*) c FROM visits WHERE {W} AND substr(check_in,1,7)=?", prev)["c"]
    change = round((cm - pm) / pm * 100, 1) if pm else None
    months = []
    y, mth = t.year, t.month
    for back in range(5, -1, -1):
        yy, mm = y, mth - back
        while mm <= 0: mm += 12; yy -= 1
        key = f"{yy:04d}-{mm:02d}"
        c = q(f"SELECT COUNT(*) c FROM visits WHERE {W} AND substr(check_in,1,7)=?", key)["c"]
        months.append({"month": datetime(yy, mm, 1).strftime("%b %Y"), "count": c})
    curdays = conn.execute(f"""SELECT substr(check_in,1,10) d, COUNT(*) c FROM visits
                              WHERE {W} AND substr(check_in,1,7)=? GROUP BY d ORDER BY d""",
                           tuple(P) + (m0,)).fetchall()
    top = lambda col: [dict(r) for r in conn.execute(
        f"""SELECT {col} label, COUNT(*) c FROM visits WHERE {W} AND {col}!=''
            GROUP BY {col} ORDER BY c DESC LIMIT 5""", tuple(P))]
    opts = lambda col: [r[0] for r in conn.execute(
        f"SELECT DISTINCT {col} FROM visits WHERE client_id=? AND {col}!='' ORDER BY {col}", (cid,))]
    return {"total": total, "unique": unique, "today": today_c, "ongoing": ongoing,
            "avg_per_day": round(total / days), "avg_duration": avg_dur, "highest": hi, "lowest": lo,
            "this_month": {"label": t.strftime("%b %Y"), "count": cm},
            "last_month": {"label": (t.replace(day=1) - timedelta(days=1)).strftime("%b %Y"), "count": pm},
            "change_pct": change, "monthly": months,
            "current_month_daily": [dict(r) for r in curdays],
            "top_regions": top("region"), "top_purposes": top("purpose"),
            "top_departments": top("host_department"), "by_type": top("person_type"),
            "filter_regions": opts("region"), "filter_departments": opts("host_department"),
            "filter_types": opts("person_type")}

def stats_analysis(conn, cid, flt=None):
    W, P = _flt_where(cid, flt)
    rows = conn.execute(f"""SELECT region, town, host_department, purpose, check_in, check_out
                           FROM visits WHERE {W}""", tuple(P)).fetchall()
    def agg(key):
        cnt, dur = {}, {}
        for r in rows:
            k = r[key]
            if not k: continue
            cnt[k] = cnt.get(k, 0) + 1
            if r["check_out"]:
                dur.setdefault(k, []).append(mins(r["check_in"], r["check_out"]))
        return cnt, {k: round(sum(v) / len(v)) for k, v in dur.items() if v}
    dcnt, ddur = agg("host_department")
    pcnt, pdur = agg("purpose")
    rcnt, rdur = agg("region")
    hours = {}
    for r in rows:
        h = int(r["check_in"][11:13]); hours[h] = hours.get(h, 0) + 1
    srt = lambda d, n=5: sorted(d.items(), key=lambda x: -x[1])[:n]
    return {
        "dept_footfall": [{"label": k, "c": v} for k, v in srt(dcnt)],
        "dept_duration": sorted([{"label": k, "c": v} for k, v in ddur.items()
                                 if k in dict(srt(dcnt))], key=lambda x: -x["c"]),
        "purpose_duration": [{"label": k, "c": v} for k, v in srt(pdur)],
        "region_visitors": [{"label": k, "c": v} for k, v in srt(rcnt, 10)],
        "region_duration": [{"label": k, "c": v} for k, v in srt(rdur, 10)],
        "hourly": [{"hour": h, "c": c} for h, c in sorted(hours.items())]}

def stats_live(conn, cid, max_mins):
    max_mins = int(max_mins or 0)
    t = now()
    rows = conn.execute("""SELECT * FROM visits WHERE client_id=? AND check_out IS NULL
                           ORDER BY region, town, check_in""", (cid,)).fetchall()
    ongoing = [{**{k: r[k] for k in ("region","town","host_department","id_number","visitor_name","contact","purpose","person_type")},
                "check_in": r["check_in"], "elapsed": mins(r["check_in"], iso(t))} for r in rows]
    purposes = {}
    for o in ongoing: purposes[o["purpose"]] = purposes.get(o["purpose"], 0) + 1
    exceeding = [o for o in ongoing if max_mins and o["elapsed"] > max_mins]
    return {"ongoing": ongoing, "purposes": [{"label": k, "c": v} for k, v in
            sorted(purposes.items(), key=lambda x: -x[1])], "exceeding": exceeding, "max_mins": max_mins}

EXCHANGERATE_API_KEY = os.environ.get("EXCHANGERATE_API_KEY", "").strip()
_FX_CACHE = {}          # base -> (fetched_at, {code: rate})
_FX_TTL = 12 * 3600

CURRENCIES = [
 "AED","AFN","ALL","AMD","ANG","AOA","ARS","AUD","AWG","AZN","BAM","BBD","BDT","BGN","BHD","BIF",
 "BMD","BND","BOB","BRL","BSD","BTN","BWP","BYN","BZD","CAD","CDF","CHF","CLP","CNY","COP","CRC",
 "CUP","CVE","CZK","DJF","DKK","DOP","DZD","EGP","ERN","ETB","EUR","FJD","FKP","FOK","GBP","GEL",
 "GGP","GHS","GIP","GMD","GNF","GTQ","GYD","HKD","HNL","HRK","HTG","HUF","IDR","ILS","IMP","INR",
 "IQD","IRR","ISK","JEP","JMD","JOD","JPY","KES","KGS","KHR","KID","KMF","KRW","KWD","KYD","KZT",
 "LAK","LBP","LKR","LRD","LSL","LYD","MAD","MDL","MGA","MKD","MMK","MNT","MOP","MRU","MUR","MVR",
 "MWK","MXN","MYR","MZN","NAD","NGN","NIO","NOK","NPR","NZD","OMR","PAB","PEN","PGK","PHP","PKR",
 "PLN","PYG","QAR","RON","RSD","RUB","RWF","SAR","SBD","SCR","SDG","SEK","SGD","SHP","SLE","SOS",
 "SRD","SSP","STN","SYP","SZL","THB","TJS","TMT","TND","TOP","TRY","TTD","TVD","TWD","TZS","UAH",
 "UGX","USD","UYU","UZS","VES","VND","VUV","WST","XAF","XCD","XDR","XOF","XPF","YER","ZAR","ZMW",
 "ZWL"]

def fx_rates(base):
    """{code: rate} for 1 unit of base. Cached 12h. Returns {} when unavailable."""
    base = (base or "USD").upper()
    hit = _FX_CACHE.get(base)
    if hit and (time.time() - hit[0]) < _FX_TTL:
        return hit[1]
    if not EXCHANGERATE_API_KEY:
        return {}
    url = f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/{base}"
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read().decode())
        rates = data.get("conversion_rates") or {}
        if rates:
            _FX_CACHE[base] = (time.time(), rates)
        return rates
    except Exception:
        return {}

def fx_convert(amount, base, target):
    """Returns (converted_amount, effective_currency, converted?)."""
    base = (base or "").upper(); target = (target or "").upper()
    if not target or target == base:
        return amount, base, False
    rates = fx_rates(base)
    rate = rates.get(target)
    if not rate:
        return amount, base, False
    return amount * float(rate), target, True

def stats_labour(conn, cid, flt=None):
    """Cost of employee/contractor time on site: per-visit rate if supplied, else the client default."""
    W, P = _flt_where(cid, flt)
    c = conn.execute("SELECT hourly_rate, currency FROM clients WHERE id=?", (cid,)).fetchone()
    default_rate = float(c["hourly_rate"] or 0) if c else 0.0
    currency = ((c["currency"] if c else "BWP") or "BWP")
    rows = conn.execute(f"""SELECT person_type, host_department, visitor_name, hourly_rate,
                                   check_in, check_out FROM visits WHERE {W}""", tuple(P)).fetchall()
    total_cost = 0.0; total_mins = 0; people = set()
    by_dept, by_type, by_person = {}, {}, {}
    for r in rows:
        if r["person_type"] not in ("Employee", "Contractor"): continue
        if not r["check_out"]: continue
        mn = mins(r["check_in"], r["check_out"])
        rate = float(r["hourly_rate"]) if r["hourly_rate"] not in (None, "") else default_rate
        if rate <= 0: continue
        cost = mn / 60.0 * rate
        total_cost += cost; total_mins += mn; people.add(r["visitor_name"])
        for bucket, key in ((by_dept, r["host_department"] or "Unassigned"),
                            (by_type, r["person_type"]),
                            (by_person, r["visitor_name"] or "Unknown")):
            b = bucket.setdefault(key, {"mins": 0, "cost": 0.0})
            b["mins"] += mn; b["cost"] += cost
    target = ((flt or {}).get("display_currency") or "").upper()
    factor, shown_currency, converted = 1.0, currency, False
    if target and target != currency.upper():
        one, cur_, ok_ = fx_convert(1.0, currency, target)
        if ok_: factor, shown_currency, converted = one, cur_, True
    cv = lambda x: round(x * factor, 2)
    def fmtc(d):
        out = []
        for k, v in d.items():
            hrs = v["mins"] / 60.0
            out.append({"label": k, "mins": v["mins"], "cost": cv(v["cost"]),
                        "rate": round(cv(v["cost"]) / hrs, 2) if hrs else 0})
        return sorted(out, key=lambda x: -x["cost"])
    return {"currency": shown_currency, "base_currency": currency, "converted": converted,
            "fx_available": bool(EXCHANGERATE_API_KEY), "currencies": CURRENCIES,
            "default_rate": default_rate,
            "total_cost": cv(total_cost), "total_hours": round(total_mins / 60.0, 1),
            "people": len(people),
            "avg_cost_per_person": cv(total_cost / len(people)) if people else 0,
            "by_department": fmtc(by_dept)[:8], "by_type": fmtc(by_type), "by_person": fmtc(by_person)[:10]}

def stats_maps(conn, cid, flt=None):
    W, P = _flt_where(cid, flt)
    regs = {}
    for r in conn.execute(f"""SELECT region, town, check_in, check_out FROM visits
                              WHERE {W} AND region!=''""", tuple(P)):
        g = regs.setdefault(r["region"], {"region": r["region"], "visits": 0, "ongoing": 0,
                                          "durs": [], "towns": {}})
        g["visits"] += 1
        g["towns"][r["town"]] = g["towns"].get(r["town"], 0) + 1
        if r["check_out"] is None: g["ongoing"] += 1
        else: g["durs"].append(mins(r["check_in"], r["check_out"]))
    out = []
    for g in regs.values():
        town = max(g["towns"].items(), key=lambda x: x[1])[0] if g["towns"] else ""
        out.append({"region": g["region"], "town": town, "visits": g["visits"],
                    "ongoing": g["ongoing"],
                    "avg_mins": round(sum(g["durs"]) / len(g["durs"])) if g["durs"] else 0})
    out.sort(key=lambda x: -x["visits"])
    for g in out:
        ll = gaz_lookup(g["region"], g["town"])
        if not ll:
            ll = geocode_lookup(conn, g["region"]) or geocode_lookup(conn, g["town"])
        if ll:
            g["lat"], g["lon"] = ll[0], ll[1]
    opts = lambda col: [r[0] for r in conn.execute(
        f"SELECT DISTINCT {col} FROM visits WHERE client_id=? AND {col}!='' ORDER BY {col}", (cid,))]
    return {"regions": out, "filter_regions": opts("region"), "filter_departments": opts("host_department"),
            "filter_types": opts("person_type")}

# ---------------- region gazetteer (used by the PDF map) ----------------
GAZ_PY = {
 # Lesotho
 "maseru":(-29.31,27.48),"berea":(-29.15,27.74),"teyateyaneng":(-29.15,27.74),
 "leribe":(-28.87,28.05),"hlotse":(-28.87,28.05),"mafeteng":(-29.82,27.24),
 "mohale's hoek":(-30.15,27.47),"quthing":(-30.40,27.70),"qacha's nek":(-30.12,28.69),
 "mokhotlong":(-29.29,29.07),"thaba-tseka":(-29.52,28.61),"butha-buthe":(-28.77,28.25),
 "roma":(-29.45,27.72),"mapoteng":(-29.05,28.00),"morija":(-29.62,27.51),
 # Botswana
 "gaborone":(-24.65,25.91),"southern":(-25.03,25.10),"kgalagadi":(-24.70,22.00),
 "tsabong":(-26.02,22.40),"kgatleng":(-24.20,26.20),"mochudi":(-24.42,26.15),
 "moshupa":(-24.77,25.42),"francistown":(-21.17,27.51),"maun":(-19.98,23.42),
 "ghanzi":(-21.70,21.65),"serowe":(-22.39,26.71),"central":(-21.50,26.50),
 "north-east":(-20.90,27.30),"north east":(-20.90,27.30),"north-west":(-19.50,23.00),
 "north west":(-19.50,23.00),"kweneng":(-24.00,25.30),"south-east":(-24.90,25.70),
 "south east":(-24.90,25.70),"selebi-phikwe":(-21.98,27.85),"palapye":(-22.55,27.13),
 "lobatse":(-25.22,25.68),"jwaneng":(-24.60,24.73),"kanye":(-24.98,25.34),
 "kasane":(-17.82,25.15),"molepolole":(-24.41,25.50),"letlhakane":(-21.42,25.59),
 "chobe":(-18.30,24.50),"ngamiland":(-19.50,22.80),"boteti":(-21.40,24.70),
 # Zimbabwe
 "harare":(-17.83,31.05),"bulawayo":(-20.16,28.58),"mutare":(-18.97,32.67),
 "gweru":(-19.45,29.82),"masvingo":(-20.07,30.83),"kwekwe":(-18.93,29.81),
 "chitungwiza":(-18.01,31.08),"kadoma":(-18.33,29.92),"victoria falls":(-17.93,25.83),
 "manicaland":(-19.00,32.30),"mashonaland":(-17.30,31.00),"matabeleland":(-20.00,28.00),
 "midlands":(-19.30,29.70),"masvingo province":(-20.30,31.00),
 # South Africa
 "johannesburg":(-26.20,28.05),"pretoria":(-25.75,28.19),"bloemfontein":(-29.12,26.21),
 "durban":(-29.86,31.02),"cape town":(-33.92,18.42),"gauteng":(-26.20,28.20),
 "western cape":(-33.50,20.00),"eastern cape":(-32.30,26.50),"kwazulu-natal":(-29.00,30.50),
 "free state":(-28.50,26.80),"limpopo":(-23.40,29.50),"mpumalanga":(-25.60,30.50),
 "north west province":(-26.00,25.60),"northern cape":(-29.00,21.50),
 "port elizabeth":(-33.96,25.60),"east london":(-33.02,27.90),"polokwane":(-23.90,29.47),
 "nelspruit":(-25.47,30.97),"kimberley":(-28.74,24.76),"rustenburg":(-25.67,27.24),
 # Neighbours / other
 "windhoek":(-22.56,17.08),"walvis bay":(-22.96,14.51),"maputo":(-25.97,32.57),
 "beira":(-19.84,34.84),"lusaka":(-15.42,28.28),"livingstone":(-17.86,25.86),
 "mbabane":(-26.32,31.13),"manzini":(-26.50,31.38),"blantyre":(-15.79,35.01),
 "lilongwe":(-13.98,33.79),"gaza":(-24.00,33.00),
 # South Africa — metros, townships and larger towns
 "soweto":(-26.27,27.86),"newcastle":(-27.76,29.93),"sandton":(-26.11,28.05),
 "midrand":(-25.99,28.13),"centurion":(-25.86,28.19),"benoni":(-26.19,28.32),
 "boksburg":(-26.21,28.26),"germiston":(-26.22,28.17),"krugersdorp":(-26.10,27.77),
 "roodepoort":(-26.16,27.87),"vereeniging":(-26.67,27.93),"vanderbijlpark":(-26.71,27.84),
 "tembisa":(-25.99,28.23),"alexandra":(-26.10,28.10),"katlehong":(-26.33,28.15),
 "emalahleni":(-25.87,29.23),"witbank":(-25.87,29.23),"middelburg":(-25.77,29.46),
 "secunda":(-26.51,29.20),"ermelo":(-26.53,29.98),"standerton":(-26.95,29.24),
 "klerksdorp":(-26.85,26.66),"potchefstroom":(-26.72,27.10),"mahikeng":(-25.86,25.64),
 "mafikeng":(-25.86,25.64),"brits":(-25.63,27.78),"lichtenburg":(-26.15,26.16),
 "welkom":(-27.98,26.73),"bethlehem":(-28.23,28.31),"sasolburg":(-26.81,27.82),
 "pietermaritzburg":(-29.60,30.38),"richards bay":(-28.78,32.04),"ladysmith":(-28.55,29.78),
 "empangeni":(-28.75,31.89),"vryheid":(-27.77,30.79),"kokstad":(-30.55,29.42),
 "mthatha":(-31.59,28.79),"umtata":(-31.59,28.79),"queenstown":(-31.90,26.88),
 "uitenhage":(-33.76,25.40),"makhanda":(-33.31,26.52),"grahamstown":(-33.31,26.52),
 "george":(-33.96,22.46),"knysna":(-34.04,23.05),"mossel bay":(-34.18,22.15),
 "oudtshoorn":(-33.59,22.20),"paarl":(-33.73,18.96),"stellenbosch":(-33.93,18.86),
 "worcester":(-33.65,19.45),"beaufort west":(-32.36,22.58),"upington":(-28.45,21.26),
 "kuruman":(-27.45,23.43),"vryburg":(-26.96,24.73),"de aar":(-30.65,24.01),
 "polokwane city":(-23.90,29.47),"tzaneen":(-23.83,30.16),"thohoyandou":(-22.95,30.48),
 "musina":(-22.35,30.04),"mokopane":(-24.19,29.01),"bela-bela":(-24.88,28.29),
 "phalaborwa":(-23.94,31.14),"soshanguve":(-25.52,28.11),"mamelodi":(-25.72,28.38),
 "khayelitsha":(-34.04,18.68),"mitchells plain":(-34.03,18.62),"bellville":(-33.90,18.63),
 # Lesotho / Botswana / Zimbabwe extras
 "maputsoe":(-28.89,27.90),"mohale s hoek":(-30.15,27.47),"semonkong":(-29.84,28.06),
 "tlokweng":(-24.66,25.97),"gabane":(-24.66,25.79),"tonota":(-21.44,27.46),
 "orapa":(-21.31,25.37),"sowa":(-20.56,26.22),"chinhoyi":(-17.36,30.20),
 "marondera":(-18.19,31.55),"bindura":(-17.30,31.33),"zvishavane":(-20.33,30.07),
 "hwange":(-18.36,26.50),"redcliff":(-19.03,29.79),"norton":(-17.88,30.70),
 # ---- world: capitals and major cities (so maps work outside Southern Africa) ----
 "london":(51.51,-0.13),"manchester":(53.48,-2.24),"birmingham":(52.49,-1.89),
 "edinburgh":(55.95,-3.19),"dublin":(53.35,-6.26),"paris":(48.86,2.35),"lyon":(45.76,4.84),
 "marseille":(43.30,5.37),"madrid":(40.42,-3.70),"barcelona":(41.39,2.17),"lisbon":(38.72,-9.14),
 "porto":(41.15,-8.61),"rome":(41.90,12.50),"milan":(45.46,9.19),"naples":(40.85,14.27),
 "berlin":(52.52,13.41),"munich":(48.14,11.58),"frankfurt":(50.11,8.68),"hamburg":(53.55,9.99),
 "amsterdam":(52.37,4.90),"rotterdam":(51.92,4.48),"brussels":(50.85,4.35),"vienna":(48.21,16.37),
 "zurich":(47.38,8.54),"geneva":(46.20,6.14),"prague":(50.08,14.44),"warsaw":(52.23,21.01),
 "budapest":(47.50,19.04),"bucharest":(44.43,26.10),"sofia":(42.70,23.32),"athens":(37.98,23.73),
 "istanbul":(41.01,28.98),"ankara":(39.93,32.86),"moscow":(55.76,37.62),"saint petersburg":(59.93,30.34),
 "kyiv":(50.45,30.52),"stockholm":(59.33,18.06),"oslo":(59.91,10.75),"copenhagen":(55.68,12.57),
 "helsinki":(60.17,24.94),"reykjavik":(64.15,-21.94),"dublin city":(53.35,-6.26),
 # Americas
 "new york":(40.71,-74.01),"washington":(38.91,-77.04),"boston":(42.36,-71.06),
 "chicago":(41.88,-87.63),"los angeles":(34.05,-118.24),"san francisco":(37.77,-122.42),
 "seattle":(47.61,-122.33),"miami":(25.76,-80.19),"houston":(29.76,-95.37),"dallas":(32.78,-96.80),
 "atlanta":(33.75,-84.39),"denver":(39.74,-104.99),"phoenix":(33.45,-112.07),"las vegas":(36.17,-115.14),
 "toronto":(43.65,-79.38),"vancouver":(49.28,-123.12),"montreal":(45.50,-73.57),"ottawa":(45.42,-75.70),
 "calgary":(51.05,-114.07),"mexico city":(19.43,-99.13),"guadalajara":(20.67,-103.35),
 "monterrey":(25.69,-100.32),"havana":(23.11,-82.37),"kingston":(17.97,-76.79),
 "panama city":(8.98,-79.52),"bogota":(4.71,-74.07),"medellin":(6.24,-75.58),"lima":(-12.05,-77.04),
 "quito":(-0.18,-78.47),"caracas":(10.48,-66.90),"santiago":(-33.45,-70.67),
 "buenos aires":(-34.60,-58.38),"montevideo":(-34.90,-56.16),"asuncion":(-25.26,-57.58),
 "la paz":(-16.50,-68.15),"sao paulo":(-23.55,-46.63),"rio de janeiro":(-22.91,-43.17),
 "brasilia":(-15.79,-47.88),"salvador":(-12.97,-38.50),"recife":(-8.05,-34.88),
 # Africa
 "cairo":(30.04,31.24),"alexandria":(31.20,29.92),"casablanca":(33.57,-7.59),"rabat":(34.02,-6.84),
 "marrakesh":(31.63,-8.01),"algiers":(36.75,3.06),"tunis":(36.81,10.18),"tripoli":(32.89,13.19),
 "khartoum":(15.50,32.56),"addis ababa":(9.03,38.74),"nairobi":(-1.29,36.82),"mombasa":(-4.04,39.67),
 "kampala":(0.35,32.58),"kigali":(-1.94,30.06),"dar es salaam":(-6.79,39.21),"dodoma":(-6.16,35.75),
 "arusha":(-3.39,36.68),"lagos":(6.52,3.38),"abuja":(9.06,7.50),"kano":(12.00,8.52),
 "port harcourt":(4.82,7.04),"accra":(5.60,-0.19),"kumasi":(6.69,-1.62),"abidjan":(5.36,-4.01),
 "dakar":(14.72,-17.47),"bamako":(12.64,-8.00),"ouagadougou":(12.37,-1.52),"niamey":(13.51,2.11),
 "conakry":(9.64,-13.58),"freetown":(8.48,-13.23),"monrovia":(6.30,-10.80),"lome":(6.13,1.22),
 "cotonou":(6.37,2.42),"douala":(4.05,9.77),"yaounde":(3.85,11.50),"libreville":(0.42,9.47),
 "brazzaville":(-4.27,15.28),"kinshasa":(-4.44,15.27),"lubumbashi":(-11.66,27.48),
 "luanda":(-8.84,13.23),"benguela":(-12.58,13.41),"antananarivo":(-18.88,47.51),
 "port louis":(-20.16,57.50),"victoria seychelles":(-4.62,55.45),"moroni":(-11.70,43.26),
 "djibouti":(11.59,43.15),"mogadishu":(2.05,45.32),"asmara":(15.34,38.93),"juba":(4.85,31.58),
 # Middle East / Asia
 "dubai":(25.20,55.27),"abu dhabi":(24.45,54.38),"doha":(25.29,51.53),"riyadh":(24.71,46.68),
 "jeddah":(21.49,39.19),"kuwait city":(29.38,47.99),"manama":(26.23,50.59),"muscat":(23.59,58.41),
 "amman":(31.95,35.93),"beirut":(33.89,35.50),"damascus":(33.51,36.29),"baghdad":(33.32,44.36),
 "tehran":(35.69,51.39),"jerusalem":(31.77,35.21),"tel aviv":(32.09,34.78),
 "karachi":(24.86,67.01),"lahore":(31.55,74.34),"islamabad":(33.68,73.05),"kabul":(34.53,69.17),
 "delhi":(28.61,77.21),"new delhi":(28.61,77.21),"mumbai":(19.08,72.88),"bangalore":(12.97,77.59),
 "bengaluru":(12.97,77.59),"chennai":(13.08,80.27),"kolkata":(22.57,88.36),"hyderabad":(17.39,78.49),
 "pune":(18.52,73.86),"ahmedabad":(23.02,72.57),"colombo":(6.93,79.86),"kathmandu":(27.72,85.32),
 "dhaka":(23.81,90.41),"yangon":(16.87,96.20),"bangkok":(13.76,100.50),"hanoi":(21.03,105.85),
 "ho chi minh city":(10.82,106.63),"phnom penh":(11.56,104.92),"vientiane":(17.97,102.63),
 "kuala lumpur":(3.14,101.69),"singapore":(1.35,103.82),"jakarta":(-6.21,106.85),
 "surabaya":(-7.26,112.75),"manila":(14.60,120.98),"cebu":(10.32,123.89),
 "beijing":(39.90,116.41),"shanghai":(31.23,121.47),"guangzhou":(23.13,113.26),
 "shenzhen":(22.54,114.06),"chengdu":(30.57,104.07),"hong kong":(22.32,114.17),
 "taipei":(25.03,121.57),"seoul":(37.57,126.98),"busan":(35.18,129.08),"tokyo":(35.68,139.65),
 "osaka":(34.69,135.50),"nagoya":(35.18,136.91),"ulaanbaatar":(47.89,106.91),
 "almaty":(43.24,76.89),"astana":(51.17,71.43),"tashkent":(41.30,69.24),"baku":(40.41,49.87),
 "tbilisi":(41.72,44.79),"yerevan":(40.18,44.51),
 # Oceania
 "sydney":(-33.87,151.21),"melbourne":(-37.81,144.96),"brisbane":(-27.47,153.03),
 "perth":(-31.95,115.86),"adelaide":(-34.93,138.60),"canberra":(-35.28,149.13),
 "auckland":(-36.85,174.76),"wellington":(-41.29,174.78),"christchurch":(-43.53,172.64),
 "suva":(-18.14,178.44),"port moresby":(-9.44,147.18),
 # countries (so a country name alone still plots)
 "south africa":(-29.00,24.00),"lesotho":(-29.60,28.20),"botswana":(-22.30,24.70),
 "zimbabwe":(-19.00,29.80),"namibia":(-22.00,17.00),"mozambique":(-18.70,35.50),
 "zambia":(-13.10,27.80),"malawi":(-13.20,34.30),"eswatini":(-26.50,31.50),
 "kenya":(0.20,37.90),"tanzania":(-6.30,34.80),"uganda":(1.30,32.30),"nigeria":(9.10,8.70),
 "ghana":(7.90,-1.00),"egypt":(26.80,30.80),"morocco":(31.80,-7.10),"ethiopia":(9.10,40.50),
 "united kingdom":(54.00,-2.00),"ireland":(53.10,-8.00),"france":(46.60,2.30),
 "germany":(51.10,10.40),"spain":(40.40,-3.70),"portugal":(39.50,-8.00),"italy":(42.80,12.60),
 "netherlands":(52.10,5.30),"belgium":(50.60,4.60),"switzerland":(46.80,8.20),
 "sweden":(60.10,18.60),"norway":(60.50,8.50),"denmark":(56.20,9.50),"poland":(52.10,19.40),
 "united states":(39.80,-98.60),"usa":(39.80,-98.60),"canada":(56.10,-106.30),
 "mexico":(23.60,-102.50),"brazil":(-14.20,-51.90),"argentina":(-38.40,-63.60),
 "chile":(-35.70,-71.50),"colombia":(4.60,-74.30),"peru":(-9.20,-75.00),
 "india":(20.60,79.00),"china":(35.90,104.20),"japan":(36.20,138.30),"south korea":(35.90,127.80),
 "indonesia":(-0.80,113.90),"malaysia":(4.20,101.90),"thailand":(15.90,101.00),
 "vietnam":(14.10,108.30),"philippines":(12.90,121.80),"pakistan":(30.40,69.30),
 "bangladesh":(23.70,90.40),"saudi arabia":(23.90,45.10),"united arab emirates":(23.40,53.80),
 "turkey":(39.00,35.20),"israel":(31.00,34.90),"australia":(-25.30,133.80),
 "new zealand":(-40.90,174.90),"russia":(61.50,105.30),
 # test data
 "test region":(-24.65,25.91),"test town":(-24.65,25.91),
}

_GAZ_NOISE = ("province","district","region","municipality","metropolitan","metro",
              "council","city of","city","town","area","zone","branch","office")

GEOCODE = os.environ.get("GEOCODE", "1").strip() not in ("0", "false", "no", "off")
_GEO_MEM = {}

def geocode_lookup(conn, place):
    """Resolve any place name in the world via OpenStreetMap, cached permanently.
    Returns (lat, lon) or None. Never raises, never blocks for long."""
    key = (place or "").strip().lower()
    if not key or not GEOCODE: return None
    if key in _GEO_MEM: return _GEO_MEM[key]
    try:
        row = conn.execute("SELECT lat, lon, found FROM geocache WHERE place=?", (key,)).fetchone()
    except Exception:
        row = None
    if row is not None:
        hit = (row["lat"], row["lon"]) if row["found"] else None
        _GEO_MEM[key] = hit
        return hit
    lat = lon = None
    try:
        url = ("https://nominatim.openstreetmap.org/search?format=json&limit=1&q="
               + urllib.parse.quote(place))
        req_ = urllib.request.Request(url, headers={"User-Agent": "StatsPack-Analytics-Lab/1.0"})
        with urllib.request.urlopen(req_, timeout=6) as r:
            data = json.loads(r.read().decode())
        if data:
            lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass                      # offline or rate-limited: fall through, cache the miss briefly
    hit = (lat, lon) if lat is not None else None
    try:
        conn.execute("INSERT INTO geocache(place,lat,lon,found,looked_up_at) VALUES(?,?,?,?,?)",
                     (key, lat, lon, 1 if hit else 0, iso(now())))
        conn.commit()
    except Exception:
        pass
    _GEO_MEM[key] = hit
    return hit

def gaz_lookup(*names):
    """Find coordinates for a region/town, tolerating suffixes like 'Harare Province'."""
    for raw in names:
        k = (raw or "").strip().lower()
        if not k: continue
        if k in GAZ_PY: return GAZ_PY[k]
        cleaned = k
        for noise in _GAZ_NOISE:
            cleaned = cleaned.replace(noise, " ")
        cleaned = " ".join(cleaned.split())
        if cleaned and cleaned in GAZ_PY: return GAZ_PY[cleaned]
        for key in GAZ_PY:
            if key and (key in k or (cleaned and key in cleaned)):
                return GAZ_PY[key]
    return None

# ---------------- minimal PDF writer (pure stdlib) ----------------
def _pdf_escape(t):
    return t.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

class MiniPDF:
    W, H = 595, 842
    def __init__(self):
        self.pages, self.ops, self.y = [], [], self.H - 46
    def _flush(self):
        if self.ops: self.pages.append("\n".join(self.ops)); self.ops = []
    def new_page(self):
        self._flush(); self.y = self.H - 46
    def ensure(self, h):
        if self.y - h < 42: self.new_page()
    def t(self, x, txt, size=10, bold=False, rgb=(0.15, 0.20, 0.24), y=None):
        yy = self.y if y is None else y
        r, g, b = rgb
        txt = str(txt).encode("latin-1", "replace").decode("latin-1")
        self.ops.append(f"BT /F{'2' if bold else '1'} {size} Tf {r:.3f} {g:.3f} {b:.3f} rg "
                        f"1 0 0 1 {x:.1f} {yy:.1f} Tm ({_pdf_escape(txt)}) Tj ET")
    def rect(self, x, y, w, h, rgb):
        r, g, b = rgb
        self.ops.append(f"{r:.3f} {g:.3f} {b:.3f} rg {x:.1f} {y:.1f} {w:.1f} {h:.1f} re f")
    def circle(self, cx, cy, rad, rgb):
        r, g, b = rgb
        k = 0.5523 * rad
        self.ops.append(
            f"{r:.3f} {g:.3f} {b:.3f} rg "
            f"{cx+rad:.1f} {cy:.1f} m "
            f"{cx+rad:.1f} {cy+k:.1f} {cx+k:.1f} {cy+rad:.1f} {cx:.1f} {cy+rad:.1f} c "
            f"{cx-k:.1f} {cy+rad:.1f} {cx-rad:.1f} {cy+k:.1f} {cx-rad:.1f} {cy:.1f} c "
            f"{cx-rad:.1f} {cy-k:.1f} {cx-k:.1f} {cy-rad:.1f} {cx:.1f} {cy-rad:.1f} c "
            f"{cx+k:.1f} {cy-rad:.1f} {cx+rad:.1f} {cy-k:.1f} {cx+rad:.1f} {cy:.1f} c f")
    def down(self, dy): self.y -= dy
    def heading(self, txt):
        self.ensure(40); self.down(10)
        self.rect(40, self.y - 6, self.W - 80, 22, (0.24, 0.34, 0.40))
        self.t(48, txt, 12, True, (1, 1, 1), y=self.y)
        self.down(30)
    def kv(self, x, label, value):
        self.t(x, label, 9.5, False, (0.45, 0.53, 0.60))
        self.t(x + 130, str(value), 10, True)
    def barrow(self, label, val, maxv, total, color, unit="", pct=False):
        self.ensure(18)
        self.t(44, str(label)[:26], 9.5)
        bw = 250 * (val / maxv if maxv else 0)
        self.rect(210, self.y - 2, max(bw, 2), 10, color)
        lab = f"{round(val/total*100)}%" if (pct and total) else f"{val}{unit}"
        self.t(210 + max(bw, 2) + 6, lab, 9.5, True)
        self.down(16)
    def table_header(self, cols):
        self.ensure(20)
        for x, txt in cols: self.t(x, txt, 9, True, (0.45, 0.53, 0.60))
        self.down(14)
    def output(self):
        self._flush()
        objs = []
        n_pages = len(self.pages)
        kids = " ".join(f"{5 + 2*i} 0 R" for i in range(n_pages))
        objs.append("<< /Type /Catalog /Pages 2 0 R >>")
        objs.append(f"<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>")
        objs.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
        objs.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
        for i, content in enumerate(self.pages):
            objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {self.W} {self.H}] "
                        f"/Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {6 + 2*i} 0 R >>")
            data = content.encode("latin-1", "replace")
            objs.append(f"<< /Length {len(data)} >>\nstream\n{content}\nendstream")
        out = b"%PDF-1.4\n"
        offsets = [0]
        for i, o in enumerate(objs, 1):
            offsets.append(len(out))
            out += f"{i} 0 obj\n{o}\nendobj\n".encode("latin-1", "replace")
        xref = len(out)
        out += f"xref\n0 {len(objs)+1}\n0000000000 65535 f \n".encode()
        for off in offsets[1:]:
            out += f"{off:010d} 00000 n \n".encode()
        out += (f"trailer\n<< /Size {len(objs)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF").encode()
        return out

SLATE=(0.24,0.34,0.40); TEAL=(0.36,0.74,0.71); AMBER=(0.88,0.66,0.25); CORAL=(0.87,0.42,0.30)

def build_report(conn, cid, flt, client_name):
    ov = stats_overview(conn, cid, flt)
    an = stats_analysis(conn, cid, flt)
    mp = stats_maps(conn, cid, flt)
    c = conn.execute("SELECT max_visit_mins FROM clients WHERE id=?", (cid,)).fetchone()
    lv = stats_live(conn, cid, c["max_visit_mins"] if c else 60)
    p = MiniPDF()
    p.t(40, "StatsPack Analytics Lab", 20, True, SLATE); p.down(22)
    p.t(40, "Visitor Analytics Report", 13, False, TEAL); p.down(20)
    p.kv(40, "Client", client_name); p.down(14)
    p.kv(40, "Generated", iso(now()).replace("T", " ")); p.down(14)
    fdesc = ", ".join(f"{k.replace('_',' ')}: {v}" for k, v in (flt or {}).items() if v) or "None (all data)"
    p.kv(40, "Filters applied", fdesc); p.down(6)

    p.heading("Key Figures")
    left = [("Total visitors", ov["total"]), ("Unique visitors", ov["unique"]),
            ("Visitors today", ov["today"]), ("Ongoing visits", ov["ongoing"]),
            ("Avg visits / day", ov["avg_per_day"])]
    right = [("Avg visit duration", f"{ov['avg_duration']} mins"),
             ("Highest day", f"{ov['highest']['count']} on {ov['highest']['date']}" if ov["highest"] else "-"),
             ("Lowest day", f"{ov['lowest']['count']} on {ov['lowest']['date']}" if ov["lowest"] else "-"),
             (f"{ov['this_month']['label']}", ov["this_month"]["count"]),
             ("vs last month", f"{ov['change_pct']}%" if ov["change_pct"] is not None else "-")]
    for (l1, v1), (l2, v2) in zip(left, right):
        p.ensure(16); p.kv(44, l1, v1); p.kv(310, l2, v2); p.down(15)

    def bars(title, items, color, unit="", pct=False):
        if not items: return
        p.heading(title)
        mx = max(i["c"] for i in items); tot = sum(i["c"] for i in items)
        for i in items: p.barrow(i["label"], i["c"], mx, tot, color, unit, pct)

    bars("Check-ins by Person Type (Visitors / Employees / Contractors)", ov.get("by_type", []), SLATE, pct=True)
    lbr = stats_labour(conn, cid, flt)
    if lbr["total_cost"] > 0:
        p.heading(f"Workforce Time & Cost ({lbr['currency']})")
        p.ensure(20)
        p.kv(44, "Total cost", f"{lbr['currency']} {lbr['total_cost']:,.2f}")
        p.kv(310, "Hours worked", lbr["total_hours"]); p.down(15)
        p.ensure(20)
        p.kv(44, "People costed", lbr["people"])
        p.kv(310, "Avg per person", f"{lbr['currency']} {lbr['avg_cost_per_person']:,.2f}"); p.down(18)
        if lbr["by_department"]:
            mxc = max(x["cost"] for x in lbr["by_department"])
            for x in lbr["by_department"]:
                p.barrow(x["label"], round(x["cost"]), round(mxc), 0, SLATE, unit=f" {lbr['currency']}")
        if lbr["by_person"]:
            p.down(4); p.table_header([(44, "PERSON"), (250, "HOURS"), (350, f"COST ({lbr['currency']})")])
            for x in lbr["by_person"]:
                p.ensure(15)
                p.t(44, x["label"][:28]); p.t(250, f"{x['mins']/60:.1f}")
                p.t(350, f"{x['cost']:,.2f}", bold=True); p.down(14)

    bars("Top 5 Regions Visited", ov["top_regions"], TEAL, pct=True)
    bars("Top 5 Purposes of Visit", ov["top_purposes"], AMBER, pct=True)
    bars("Top 5 Departments by Visits", ov["top_departments"], CORAL, pct=True)
    bars("Departments taking the most time (avg)", an["dept_duration"], SLATE, unit=" mins")
    bars("Visits taking the most time (avg)", an["purpose_duration"], AMBER, unit=" mins")
    hrs = [{"label": f"{h['hour']:02d}:00", "c": h["c"]} for h in an["hourly"]]
    bars("Footfall by Hour of Day", hrs, TEAL)
    mons = [{"label": m["month"], "c": m["count"]} for m in ov["monthly"]]
    bars("Monthly Footfall (last 6 months)", mons, SLATE)

    p.heading("Regions Overview (map data)")
    p.table_header([(44, "REGION"), (200, "MAIN TOWN"), (330, "VISITS"), (400, "ONGOING"), (470, "AVG MINS")])
    for r in mp["regions"][:20]:
        p.ensure(15)
        p.t(44, r["region"][:24]); p.t(200, r["town"][:20]); p.t(330, r["visits"], bold=True)
        p.t(400, r["ongoing"]); p.t(470, r["avg_mins"])
        p.down(14)

    # ---- regions map (visits bubbles on a southern-Africa projection)
    p.heading("Regions Map - Total Check-ins")
    MH = 250
    p.ensure(MH + 34)
    yb = p.y - MH
    p.rect(44, yb, 507, MH, (0.918, 0.945, 0.957))
    X = lambda lon: 44 + (lon - 14) / 22.0 * 507
    Yc = lambda lat: yb + (lat + 35) / 19.0 * MH
    for gx in range(1, 6):
        p.rect(44 + gx * 507 / 6.0, yb, 0.6, MH, (0.87, 0.91, 0.93))
        p.rect(44, yb + gx * MH / 6.0, 507, 0.6, (0.87, 0.91, 0.93))
    for lab, lon, lat in [("NAMIBIA",16.4,-23.0),("BOTSWANA",23.2,-21.3),("SOUTH AFRICA",22.0,-31.6),
                          ("ZIMBABWE",29.4,-19.2),("LESOTHO",29.6,-30.4),("MOZAMBIQUE",33.0,-18.4)]:
        p.t(X(lon), lab, 7, True, (0.68, 0.75, 0.79), y=Yc(lat))

    _ll = lambda r: (r.get("lat"), r.get("lon")) if r.get("lat") is not None else gaz_lookup(r["region"], r["town"])
    mrows = [r for r in mp["regions"] if _ll(r)]
    unmapped = [r for r in mp["regions"] if not _ll(r)]
    mx = max([r["visits"] for r in mrows], default=1)
    # geometry first so labels can avoid both other labels and the bubbles
    bubbles = []
    for r in sorted(mrows, key=lambda x: -x["visits"]):
        lat, lon = _ll(r)
        rad = 8 + 13 * (r["visits"] / mx) ** 0.5
        bubbles.append((X(lon), Yc(lat), rad, r))
    occupied = [(cx - rad, cy - rad, cx + rad, cy + rad) for cx, cy, rad, _ in bubbles]
    # white ring under each bubble keeps overlapping circles readable
    for cx, cy, rad, _ in bubbles:
        p.circle(cx, cy, rad + 1.8, (1, 1, 1))
    for cx, cy, rad, _ in bubbles:
        p.circle(cx, cy, rad, TEAL)
    # numbers drawn after every circle, so a neighbouring bubble can never cover one
    for cx, cy, rad, r in bubbles:
        num = str(r["visits"])
        p.t(cx - len(num) * 2.4, num, 9, True, (1, 1, 1), y=cy - 3.2)
    for cx, cy, rad, r in bubbles:
        name = r["region"][:20]
        half = len(name) * 2.05
        for dy in (rad + 7, -(rad + 12), rad + 18, -(rad + 23), rad + 29):
            ly = cy + dy
            box = (cx - half, ly - 4, cx + half, ly + 7)
            if not any(box[0] < q[2] and q[0] < box[2] and box[1] < q[3] and q[1] < box[3] for q in occupied):
                occupied.append(box)
                p.t(cx - half, name, 8.5, True, (0.20, 0.29, 0.35), y=ly)
                break
    if not bubbles:
        p.t(60, "No region in this selection could be placed on the map.", 10, True, (0.49,0.56,0.63), y=yb + MH/2 + 6)
        p.t(60, "Region names received: " + (", ".join(r["region"] for r in mp["regions"][:6]) or "none"),
            9, False, (0.49,0.56,0.63), y=yb + MH/2 - 10)
    p.y = yb - 10
    if unmapped:
        p.t(44, "Not shown on map (region not recognised): " +
            ", ".join(f"{r['region']} ({r['visits']})" for r in unmapped[:5]), 8,
            False, (0.49, 0.56, 0.63)); p.down(13)
    p.down(6)

    p.heading(f"Current On-Premises ({len(lv['ongoing'])})")
    if lv["ongoing"]:
        p.table_header([(44, "VISITOR"), (170, "REGION / TOWN"), (300, "DEPARTMENT"), (430, "MINS")])
        for o in lv["ongoing"][:30]:
            p.ensure(15)
            p.t(44, o["visitor_name"][:20]); p.t(170, f"{o['region']} / {o['town']}"[:22])
            p.t(300, o["host_department"][:22]); p.t(430, o["elapsed"], bold=True,
                rgb=CORAL if o["elapsed"] > lv["max_mins"] else (0.15, 0.20, 0.24))
            p.down(14)
        p.down(4)
        p.t(44, f"{len(lv['exceeding'])} visitor(s) exceeding the allowed {lv['max_mins']} minutes.",
            9.5, True, CORAL); p.down(14)
    else:
        p.t(44, "Nobody is on the premises right now.", 10); p.down(14)
    return p.output()

# ---------------------------------------------------------------- embedded frontend (whole UI in this one file)
INDEX_HTML = '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>StatsPack Analytics Lab</title>\n<style>\n:root{\n  --slate:#3d5666; --slate-deep:#35505f; --ink:#25333d; --muted:#7d8fa0;\n  --mint:#cfe8e6; --mint-soft:#e7f4f2; --teal:#5cbcb6; --amber:#e0a83f; --coral:#dd6b4d;\n  --bg:#eef1f4; --card:#ffffff; --line-blue:#7b93f0; --ok:#2f9e77; --bad:#c65644;\n}\n*{box-sizing:border-box;margin:0}\nbody{background:var(--bg);color:var(--ink);\n  font:15px/1.5 "Trebuchet MS","Lato","Segoe UI",system-ui,sans-serif}\nbutton{font:inherit;cursor:pointer}\ninput,select{font:inherit;padding:9px 12px;border:1.5px solid #c4d0d8;border-radius:9px;background:#fff;width:100%;color:var(--ink)}\ninput:focus,select:focus,button:focus-visible{outline:2px solid var(--teal);outline-offset:1px}\na{color:var(--slate)}\n@media (prefers-reduced-motion: reduce){*{transition:none!important;animation:none!important}}\n\n/* ============ app shell ============ */\n.shell{display:grid;grid-template-columns:290px 1fr;min-height:100vh}\naside{background:#fff;box-shadow:2px 0 8px rgba(37,51,61,.06);display:flex;flex-direction:column;\n  padding:14px 14px 18px;position:sticky;top:0;height:100vh;overflow-y:auto}\n.profile{background:#a9d6d5;border-radius:0 0 20px 20px;margin:-14px -14px 0;padding:28px 16px 22px;text-align:center}\n.profile img{height:56px;display:block;margin:0 auto 4px}\n.profile .wm{font-size:13px;font-weight:800;letter-spacing:4px;color:var(--slate)}\n.profile .wm span{color:#8fa8b2}\n.profile .nm{font-size:19px;font-weight:800;color:var(--slate);margin-top:12px}\n.profile .org{font-size:14px;font-weight:700;color:var(--slate);opacity:.85;margin-top:2px}\n.profile .em{font-size:12.5px;color:#5a7481;margin-top:2px;word-break:break-all}\n.navsec{font-size:12px;font-weight:800;letter-spacing:2.5px;color:#9aabb8;margin:22px 10px 8px;\n  display:flex;justify-content:space-between;align-items:center}\n.navsec::after{content:"▾";font-size:9px;color:#c1ccd5}\n.nitem{display:flex;align-items:center;gap:14px;width:calc(100% + 28px);margin-left:-14px;\n  padding:13px 24px;border:0;background:none;color:var(--ink);font-size:16.5px;font-weight:700;text-align:left;\n  border-radius:0}\n.nitem svg{width:22px;height:22px;flex:0 0 22px;fill:none;stroke:var(--amber);stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}\n.nitem:hover{background:var(--mint-soft)}\n.nitem.on{background:var(--slate);color:#fff}\n.nitem.on svg{stroke:#fff}\n.sideout{margin-top:auto;padding-top:18px}\n.sideout .btn{width:100%;border-radius:12px;padding:13px;font-size:16px}\n.vtag{text-align:center;font-size:13px;color:#8aa0ad;margin-top:12px}\n.clientbox{margin:14px 4px 0}\n.clientbox label{font-size:11.5px;font-weight:800;letter-spacing:1.5px;color:#9aabb8;display:block;margin:0 6px 4px}\n\n/* top bar */\n.topbar{background:var(--slate);color:#fff;display:flex;align-items:center;justify-content:center;\n  gap:12px;padding:20px 16px;position:relative}\n.topbar h1{font-size:23px;font-weight:800;display:flex;align-items:center;gap:12px}\n.topbar h1 svg{width:24px;height:24px;fill:none;stroke:#fff;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}\n.burger{display:none;position:absolute;left:14px;top:50%;transform:translateY(-50%);\n  background:none;border:0;color:#fff;font-size:24px;padding:6px}\nmain{padding:26px 28px 40px;max-width:1500px;width:100%}\n\n/* KPI cards — SmartRegister style: icon tile, colored title, underline, footer stats */\n.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px}\n.kpi{background:var(--card);border-radius:16px;box-shadow:0 4px 14px rgba(37,51,61,.07);padding:20px 24px}\n.kpi .ico{width:46px;height:46px;border-radius:12px;display:flex;align-items:center;justify-content:center;margin-bottom:16px}\n.kpi .ico svg{width:24px;height:24px;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}\n.k-slate .ico{background:#e8edf1}.k-slate .ico svg{stroke:var(--slate)}\n.k-teal .ico{background:#e2f4f3}.k-teal .ico svg{stroke:var(--teal)}\n.k-amber .ico{background:#faf1de}.k-amber .ico svg{stroke:var(--amber)}\n.k-coral .ico{background:#fae7e1}.k-coral .ico svg{stroke:var(--coral)}\n.kpi .lbl{font-size:18px;font-weight:800}\n.kpi .rule{height:3px;border-radius:2px;margin:14px 0 16px}\n.kpi .val{font-size:32px;font-weight:800;line-height:1.1}\n.kpi .sub{font-size:12.5px;color:var(--muted);margin-top:4px}\n.kpi .footr{display:flex;justify-content:space-between;align-items:baseline;gap:8px}\n.k-slate .lbl{color:var(--slate)} .k-slate .rule{background:var(--slate)}\n.k-teal  .lbl{color:var(--teal)}  .k-teal  .rule{background:var(--teal)}\n.k-amber .lbl{color:var(--amber)} .k-amber .rule{background:var(--amber)}\n.k-coral .lbl{color:var(--coral)} .k-coral .rule{background:var(--coral)}\n.val.up{color:var(--ok)} .val.down{color:var(--bad)}\n\n/* content cards + glance tables */\n.panel{background:var(--card);border-radius:18px;box-shadow:0 4px 14px rgba(37,51,61,.07);\n  padding:22px 26px;margin-top:24px;min-width:0}\n.panel>h2{font-size:21px;font-weight:800;color:var(--slate);margin-bottom:14px}\n.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:24px;margin-top:24px}\n.grid2 .panel{margin-top:0}\ntable{width:100%;border-collapse:collapse}\nth{text-align:left;padding:10px 12px;font-size:14px;letter-spacing:1px;color:var(--slate);\n   border-bottom:2px solid #e3e9ee;font-weight:800}\ntd{padding:14px 12px;border-bottom:1.5px solid #eef2f5;font-size:15px}\ntd.b{font-weight:800}\ntr:last-child td{border-bottom:0}\n.chip{display:inline-block;background:#e2eaf0;color:var(--slate);border-radius:999px;\n  padding:3px 14px;font-size:13px;font-weight:700;margin-left:8px}\n.score{display:inline-block;background:#d9efe7;color:var(--ok);border-radius:8px;padding:3px 12px;font-weight:800;font-size:14px}\n.score.bad{background:#f7e3dd;color:var(--bad)}\n.empty{color:var(--muted);text-align:center;padding:24px 8px}\n\n.btn{background:var(--slate);color:#fff;border:0;border-radius:10px;padding:10px 18px;font-weight:800}\n.btn.ghost{background:#fff;color:var(--slate);border:1.5px solid var(--slate)}\n.btn.small{padding:6px 12px;font-size:13px;border-radius:8px}\n.btn.warn{background:var(--coral)}\n.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}\n.note{font-size:13px;color:var(--muted)}\ncode,pre{font:12.5px/1.55 ui-monospace,Consolas,monospace;background:#2c3f4b;color:#dbe8f0;border-radius:10px}\npre{padding:13px 15px;overflow:auto}\ncode.inline{background:#e8eef2;color:var(--slate);padding:2px 7px;border-radius:5px}\n.msg{border-radius:10px;padding:10px 13px;font-size:14px;margin:8px 0;display:none}\n.msg.err{display:block;background:#f7e3dd;color:#8a3a28}\n.msg.ok{display:block;background:#d9efe7;color:#1d6c4f}\n.refresh{font-size:13px;color:var(--muted);margin-top:8px}\nsvg text{font-family:"Trebuchet MS","Lato",sans-serif}\n\n/* ============ analytics loader ============ */\n.loader{position:fixed;inset:0;background:rgba(238,241,244,.94);display:grid;place-items:center;z-index:60}\n.loader .box{text-align:center}\n.bars{display:flex;gap:9px;align-items:flex-end;height:74px;justify-content:center}\n.bars i{width:15px;border-radius:5px 5px 2px 2px;animation:grow 1.05s ease-in-out infinite}\n.bars i:nth-child(1){background:var(--slate);animation-delay:0s}\n.bars i:nth-child(2){background:var(--teal);animation-delay:.14s}\n.bars i:nth-child(3){background:var(--amber);animation-delay:.28s}\n.bars i:nth-child(4){background:var(--coral);animation-delay:.42s}\n.bars i:nth-child(5){background:var(--slate);animation-delay:.56s}\n@keyframes grow{0%,100%{height:18%}50%{height:100%}}\n.loader p{margin-top:18px;font-weight:800;color:var(--slate);letter-spacing:1px}\n\n/* ============ login (unchanged look) ============ */\n.login-wrap{min-height:100vh;display:grid;place-items:center;padding:24px;position:relative;\n  background:#dfe4e8 url(\'/login.png\') center/cover no-repeat}\n/* no overlay — background image shows at full clarity */\n.login{position:relative;width:100%;max-width:620px;text-align:center;\n  font-family:"Trebuchet MS","Lato","Segoe UI",sans-serif}\n.login .logo{height:222px;max-width:90%;object-fit:contain;margin:0 auto 6px;display:block}\n.login h1{font-size:30px;font-weight:800;letter-spacing:10px;color:#3d5666;margin:26px 0 30px;\n  text-shadow:0 1px 6px rgba(255,255,255,.85)}\n.login h1 span{color:#5cbcb6}\n.login .field{position:relative;margin:0 0 26px}\n.login .field svg{position:absolute;left:24px;top:50%;transform:translateY(-50%);width:22px;height:22px;\n  stroke:#3d5666;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}\n.login input{width:100%;padding:19px 58px;border:2px solid #35505f;border-radius:999px;\n  background:rgba(255,255,255,.88);font:inherit;font-size:17px;color:#25333d}\n.login input::placeholder{color:#4d6674}\n.login .eye{position:absolute;right:20px;top:50%;transform:translateY(-50%);background:none;border:0;padding:4px;line-height:0}\n.login .eye svg{position:static;transform:none}\n.login .btn{width:100%;padding:18px;border-radius:999px;background:#35505f;font-size:18px;letter-spacing:.5px}\n.login .demo{margin-top:30px;text-align:left;background:rgba(255,255,255,.62);backdrop-filter:blur(3px);\n  border-radius:14px;padding:16px 20px;font-size:14px;color:#25333d;line-height:2}\n.login .demo code{background:rgba(255,255,255,.85);color:#25333d;padding:2px 8px;border-radius:5px;\n  font:13px ui-monospace,Consolas,monospace}\n.login .msg{text-align:left}\n\n/* embed mode: horizontal tab strip instead of the sidebar */\n.tabstrip{background:#fff;border-bottom:1px solid #e3e9ee;display:flex;gap:2px;overflow-x:auto;padding:0 12px;align-items:center}\n.tabstrip button{background:none;border:0;border-bottom:3px solid transparent;padding:13px 14px;\n  color:var(--muted);font-weight:800;white-space:nowrap;font-size:14.5px}\n.tabstrip button.on{color:var(--slate);border-bottom-color:var(--slate)}\n.tabstrip select{width:auto;min-width:170px;margin-left:auto}\n.tabstrip .outlink{margin-left:8px;color:var(--coral);font-weight:800}\n\n/* mobile */\n@media (max-width: 920px){\n  .shell{grid-template-columns:1fr}\n  aside{position:fixed;left:0;top:0;bottom:0;width:290px;z-index:50;transform:translateX(-102%);\n    transition:transform .25s ease;box-shadow:6px 0 24px rgba(0,0,0,.18)}\n  aside.open{transform:none}\n  .burger{display:block}\n  main{padding:18px 14px 30px}\n}\n</style>\n</head>\n<body>\n<div id="app"></div>\n<script>\n"use strict";\nconst $ = (s, el=document) => el.querySelector(s);\nconst S = { token: sessionStorage.getItem("tok") || "", role: sessionStorage.getItem("role") || "",\n            name: sessionStorage.getItem("name") || "", email: sessionStorage.getItem("email") || "",\n            clientName: sessionStorage.getItem("cname") || "",\n            tab: "", clientId: null, clients: [], timer: null,\n            pct: sessionStorage.getItem("pct") === "1",\n            cur: sessionStorage.getItem("cur") || "",\n            f: { date_from:"", date_to:"", region:"", department:"", person_type:"" } };\n\nconst EMBED = new URLSearchParams(location.search).has("embed");\nconst sleep = ms => new Promise(s=>setTimeout(s,ms));\nasync function api(path, body, method, _try=0){\n  let r;\n  try{\n    r = await fetch(path, { method: method || (body ? "POST" : "GET"),\n      headers: { "Content-Type": "application/json", ...(S.token ? { Authorization: "Bearer " + S.token } : {}) },\n      body: body ? JSON.stringify(body) : undefined });\n  }catch(netErr){\n    if(_try < 4){ showLoader("Waking the server — retrying…"); await sleep(3000); return api(path, body, method, _try+1); }\n    throw new Error("Cannot reach the server — check your connection and try again");\n  }\n  let d = null;\n  try{ d = await r.json(); }catch(_){ /* non-JSON = response from the platform edge, not the app */ }\n  if (!r.ok){\n    const infra = d === null || [502,503,504].includes(r.status);\n    if (infra && _try < 4){\n      showLoader("Waking the server — retrying…");\n      await sleep(3000);\n      return api(path, body, method, _try+1);\n    }\n    throw new Error((d && d.error) || ("Request failed (" + r.status + ") — if this persists, redeploy may be in progress; try again in a minute"));\n  }\n  return d || {};\n}\nconst esc = s => String(s ?? "").replace(/[&<>"\']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",\'"\':"&quot;","\'":"&#39;"}[c]));\nconst fmtDT = s => s ? s.replace("T", " ").slice(0, 16) : "—";\n\n/* ---------------- analytics loader ---------------- */\nfunction showLoader(msg){\n  hideLoader();\n  const d = document.createElement("div");\n  d.className = "loader"; d.id = "loader";\n  d.innerHTML = `<div class="box"><div class="bars"><i></i><i></i><i></i><i></i><i></i></div>\n    <p>${esc(msg || "Crunching your numbers…")}</p></div>`;\n  document.body.appendChild(d);\n}\nfunction hideLoader(){ const l = $("#loader"); if (l) l.remove(); }\n\n/* ---------------- icons ---------------- */\nconst IC = {\n  grid:\'<svg viewBox="0 0 24 24"><rect x="3.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.5"/></svg>\',\n  eye:\'<svg viewBox="0 0 24 24"><path d="M1.5 12s4-7 10.5-7 10.5 7 10.5 7-4 7-10.5 7S1.5 12 1.5 12z"/><circle cx="12" cy="12" r="3"/></svg>\',\n  chart:\'<svg viewBox="0 0 24 24"><path d="M4 20V10M10 20V4M16 20v-8M21 20H3"/></svg>\',\n  pulse:\'<svg viewBox="0 0 24 24"><path d="M2.5 12h4l2.5-7 4.5 14 2.5-7h5.5"/></svg>\',\n  plug:\'<svg viewBox="0 0 24 24"><path d="M9 3v5M15 3v5M7 8h10v3a5 5 0 0 1-5 5 5 5 0 0 1-5-5V8zM12 16v5"/></svg>\',\n  pin:\'<svg viewBox="0 0 24 24"><path d="M12 21s-7-6.2-7-11a7 7 0 0 1 14 0c0 4.8-7 11-7 11z"/><circle cx="12" cy="10" r="2.6"/></svg>\',\n  doc:\'<svg viewBox="0 0 24 24"><path d="M6 2.5h8l4 4V21.5H6zM14 2.5v4h4M9 12h6M9 16h6M9 8h2"/></svg>\',\n  key:\'<svg viewBox="0 0 24 24"><circle cx="8" cy="15" r="4.5"/><path d="M11.5 11.5L20 3M16 7l3 3M13.5 9.5l3 3"/></svg>\',\n  home:\'<svg viewBox="0 0 24 24"><path d="M4 11l8-7 8 7M6 9.5V20h12V9.5M10 20v-5h4v5"/></svg>\'\n};\nconst TITLES = { clients:["Overview",IC.grid], overview:["Bird\'s Eye View",IC.eye],\n  analysis:["In-Depth Analysis",IC.chart], live:["Live from the Premises",IC.pulse],\n  maps:["Maps",IC.pin], reports:["Reports",IC.doc], connection:["Data & Connection",IC.plug],\n  security:["Security",IC.key] };\n\n/* ---------------- % / count toggle ---------------- */\nfunction pctToggle(){\n  return `<div class="row" style="justify-content:flex-end;margin-bottom:14px">\n    <span class="note" style="font-weight:700">Chart values:</span>\n    <button class="btn small ${S.pct?"ghost":""}" id="modeCount">Count</button>\n    <button class="btn small ${S.pct?"":"ghost"}" id="modePct">%</button></div>`;\n}\nfunction wireToggle(){\n  const c=$("#modeCount"), p=$("#modePct");\n  if(c) c.onclick=()=>{ S.pct=false; sessionStorage.setItem("pct","0"); draw(); };\n  if(p) p.onclick=()=>{ S.pct=true; sessionStorage.setItem("pct","1"); draw(); };\n}\n/* count-charts (unit === "") honor S.pct; duration charts keep their units */\nconst shareLab=(v,total,unit)=> (S.pct && unit==="" && total>0) ? Math.round(v/total*100)+"%" : v+unit;\n\n/* ---------------- tiny SVG chart kit ---------------- */\nfunction lineChart(series, {w=560, h=230, unit=""}={}){\n  if (!series.length) return \'<div class="empty">No data yet</div>\';\n  const pad = {l:44, r:18, t:22, b:44};\n  const max = Math.max(...series.map(p=>p.v), 1);\n  const X = i => pad.l + i * (w - pad.l - pad.r) / Math.max(series.length - 1, 1);\n  const Y = v => pad.t + (1 - v / max) * (h - pad.t - pad.b);\n  let path = "";\n  series.forEach((p,i)=>{\n    const x=X(i), y=Y(p.v);\n    if(!i){ path=`M${x},${y}`; return; }\n    const px=X(i-1), py=Y(series[i-1].v), cx=(px+x)/2;\n    path += ` C${cx},${py} ${cx},${y} ${x},${y}`;\n  });\n  const tot = series.reduce((a,b)=>a+b.v,0);\n  const dots = series.map((p,i)=>`<circle cx="${X(i)}" cy="${Y(p.v)}" r="4.5" fill="#fff" stroke="#5cbcb6" stroke-width="2.2"/>\n    <text x="${X(i)}" y="${Y(p.v)-10}" font-size="11" text-anchor="middle" fill="#3d5666" font-weight="700">${esc(shareLab(p.v,tot,unit))}</text>`).join("");\n  const labels = series.map((p,i)=>`<text x="${X(i)}" y="${h-16}" font-size="11" text-anchor="middle" fill="#7d8fa0">${esc(p.l)}</text>`).join("");\n  const gy = [0,.5,1].map(f=>{const y=Y(max*f);return `<line x1="${pad.l}" y1="${y}" x2="${w-pad.r}" y2="${y}" stroke="#e3e9ee" stroke-dasharray="3 4"/>\n    <text x="${pad.l-8}" y="${y+4}" font-size="10.5" text-anchor="end" fill="#9aabb8">${Math.round(max*f)}</text>`}).join("");\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">${gy}\n    <path d="${path}" fill="none" stroke="#5cbcb6" stroke-width="2.6"/>${dots}${labels}</svg>`;\n}\nconst PAL = ["#3d5666","#5cbcb6","#e0a83f","#dd6b4d","#8fa8b2","#7b93f0","#b087b4","#54b98d"];\nfunction hbar(items, {w=560, colors=null, unit=""}={}){\n  if (!items.length) return \'<div class="empty">No data yet</div>\';\n  const total = items.reduce((a,b)=>a+b.c,0) || 1;\n  const rowH=38, pad={l:158,r:28,t:6}, h=pad.t+items.length*rowH+8;\n  const max=Math.max(...items.map(i=>i.c),1);\n  const cols = colors || PAL;\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">` + items.map((it,i)=>{\n    const y=pad.t+i*rowH, bw=Math.max((w-pad.l-pad.r)*it.c/max, 28);\n    const lab = shareLab(it.c,total,unit);\n    return `<text x="${pad.l-8}" y="${y+22}" font-size="12.5" text-anchor="end" fill="#25333d" font-weight="700">${esc(String(it.label).slice(0,20))}</text>\n      <rect x="${pad.l}" y="${y+5}" width="${bw}" height="${rowH-13}" rx="5" fill="${cols[i%cols.length]}"/>\n      <text x="${pad.l+bw/2}" y="${y+21}" font-size="11.5" text-anchor="middle" fill="#fff" font-weight="800">${lab}</text>`;\n  }).join("") + "</svg>";\n}\nfunction vbar(items, {w=560,h=250,unit=""}={}){\n  if (!items.length) return \'<div class="empty">No data yet</div>\';\n  const pad={l:46,r:14,t:26,b:66}; const max=Math.max(...items.map(i=>i.c),1);\n  const bw=(w-pad.l-pad.r)/items.length; const tot=items.reduce((a,b)=>a+b.c,0);\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">` + items.map((it,i)=>{\n    const bh=(h-pad.t-pad.b)*it.c/max, x=pad.l+i*bw+bw*0.14, y=h-pad.b-bh;\n    return `<rect x="${x}" y="${y}" width="${bw*0.72}" height="${bh}" rx="5" fill="${PAL[i%PAL.length]}"/>\n      <text x="${x+bw*0.36}" y="${y+18>h-pad.b?y-6:y+20}" font-size="11" text-anchor="middle" fill="${y+18>h-pad.b?\'#25333d\':\'#fff\'}" font-weight="800">${shareLab(it.c,tot,unit)}</text>\n      <text x="${x+bw*0.36}" y="${h-pad.b+14}" font-size="10.5" text-anchor="end" fill="#7d8fa0" transform="rotate(-32 ${x+bw*0.36} ${h-pad.b+14})">${esc(String(it.label).slice(0,16))}</text>`;\n  }).join("") + "</svg>";\n}\nfunction donut(items, {w=560,h=250}={}){\n  if (!items.length) return \'<div class="empty">Nobody is on the premises right now</div>\';\n  const total=items.reduce((a,b)=>a+b.c,0), cx=w/2, cy=h/2, R=Math.min(w,h)/2-26, r=R*0.58;\n  let a0=-Math.PI/2, out="";\n  items.forEach((it,i)=>{\n    const a1=a0+2*Math.PI*it.c/total, big=(a1-a0)>Math.PI?1:0;\n    const p=(a,rr)=>[cx+rr*Math.cos(a),cy+rr*Math.sin(a)];\n    const [x0,y0]=p(a0,R),[x1,y1]=p(a1,R),[x2,y2]=p(a1,r),[x3,y3]=p(a0,r);\n    out+=`<path d="M${x0},${y0} A${R},${R} 0 ${big} 1 ${x1},${y1} L${x2},${y2} A${r},${r} 0 ${big} 0 ${x3},${y3} Z" fill="${PAL[i%PAL.length]}"/>`;\n    const mid=(a0+a1)/2,[lx,ly]=p(mid,R+14);\n    out+=`<text x="${lx}" y="${ly}" font-size="11" text-anchor="${lx>cx?"start":"end"}" fill="#25333d">${esc(it.label)} (${S.pct?Math.round(it.c/total*100)+"%":it.c})</text>`;\n    a0=a1;\n  });\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">${out}\n    <text x="${cx}" y="${cy+6}" font-size="17" font-weight="800" text-anchor="middle" fill="#25333d">${total}</text></svg>`;\n}\n\n/* ---------------- login ---------------- */\nfunction renderLogin(err){\n  const eyeOpen=\'<svg viewBox="0 0 24 24"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"/><circle cx="12" cy="12" r="3"/></svg>\';\n  const eyeOff=\'<svg viewBox="0 0 24 24"><path d="M17.94 17.94A10.9 10.9 0 0 1 12 19c-7 0-11-7-11-7a20.7 20.7 0 0 1 5.06-5.94M9.9 4.24A10.4 10.4 0 0 1 12 4c7 0 11 7 11 7a20.8 20.8 0 0 1-3.22 4.19M1 1l22 22"/></svg>\';\n  document.body.innerHTML = `<div class="login-wrap"><form class="login" id="loginForm">\n    <img class="logo" src="/logo.png" alt="StatsPack" onerror="this.style.display=\'none\'">\n    <h1>ANALYTICS <span>LAB</span></h1>\n    ${err?`<div class="msg err">${esc(err)}</div>`:""}\n    <div class="field">\n      <svg viewBox="0 0 24 24"><rect x="2.5" y="5" width="19" height="14" rx="2.5"/><path d="M3 6.5l9 7 9-7"/></svg>\n      <input id="em" type="email" placeholder="Email" autocomplete="username" aria-label="Email" required>\n    </div>\n    <div class="field">\n      <svg viewBox="0 0 24 24"><rect x="4.5" y="10.5" width="15" height="10" rx="2.5"/><path d="M8 10.5V7.5a4 4 0 0 1 8 0v3"/></svg>\n      <input id="pw" type="password" placeholder="Password" autocomplete="current-password" aria-label="Password" required>\n      <button class="eye" type="button" id="eyeBtn" aria-label="Show password">${eyeOpen}</button>\n    </div>\n    <button class="btn" type="submit">Sign In</button>\n    <p style="position:relative;margin-top:14px;font-size:12px;color:#7d8fa0">Analytics Lab · v23</p>\n  </form></div>`;\n  $("#eyeBtn").onclick = ()=>{ const p=$("#pw"), show=p.type==="password";\n    p.type=show?"text":"password"; $("#eyeBtn").innerHTML=show?eyeOff:eyeOpen;\n    $("#eyeBtn").setAttribute("aria-label", show?"Hide password":"Show password"); };\n  $("#loginForm").addEventListener("submit", async e=>{\n    e.preventDefault();\n    showLoader("Signing you in…");\n    try{\n      const d = await api("/api/login", { email:$("#em").value, password:$("#pw").value });\n      S.token=d.token; S.role=d.role; S.name=d.name; S.email=d.email||""; S.clientName=d.client_name||"";\n      sessionStorage.setItem("tok",d.token); sessionStorage.setItem("role",d.role);\n      sessionStorage.setItem("name",d.name); sessionStorage.setItem("email",d.email||"");\n      sessionStorage.setItem("cname",d.client_name||"");\n      S.tab = d.role==="super" ? "clients" : "overview";\n      await boot();\n    }catch(ex){ hideLoader(); renderLogin(ex.message); }\n  });\n}\n\n/* ---------------- shell ---------------- */\nfunction navBtn(k){ const [label, icon] = TITLES[k];\n  return `<button class="nitem ${S.tab===k?"on":""}" data-t="${k}">${icon}<span>${label}</span></button>`; }\n\nfunction shellEmbed(){\n  const isSuper = S.role==="super";\n  const tabs = [];\n  if (isSuper) tabs.push("clients");\n  tabs.push("overview","analysis","live","maps","reports","connection","security");\n  const [title, icon] = TITLES[S.tab] || ["",""];\n  const picker = isSuper && S.clients.length ? `<select id="clientPick" aria-label="Client">\n      ${S.clients.map(c=>`<option value="${c.id}" ${c.id==S.clientId?"selected":""}>${esc(c.name)}</option>`).join("")}\n    </select>` : "";\n  document.body.innerHTML = `\n    <div class="topbar" style="padding:14px 16px"><h1 style="font-size:19px">${icon}<span>${esc(title)}</span></h1></div>\n    <div class="tabstrip">\n      ${tabs.map(k=>`<button data-t="${k}" class="${S.tab===k?"on":""}">${TITLES[k][0]}</button>`).join("")}\n      ${picker}<button class="outlink" id="out">Sign out</button>\n    </div>\n    <main id="view" style="padding:18px 16px 30px"></main>`;\n  $("#out").onclick = ()=>{ sessionStorage.clear(); location.reload(); };\n  document.querySelectorAll(".tabstrip [data-t]").forEach(b=>b.onclick=()=>{ S.tab=b.dataset.t; draw(); });\n  const cp=$("#clientPick"); if(cp) cp.onchange=()=>{ S.clientId=+cp.value; draw(); };\n}\nfunction shell(){\n  if (EMBED) return shellEmbed();\n  const isSuper = S.role==="super";\n  const org = isSuper ? "StatsPack · HQ"\n    : esc(S.clientName || (S.clients.find(c=>c.id==S.clientId)||{}).name || "Client");\n  const picker = isSuper && S.clients.length ? `<div class="clientbox"><label for="clientPick">VIEWING CLIENT</label>\n      <select id="clientPick">${S.clients.map(c=>`<option value="${c.id}" ${c.id==S.clientId?"selected":""}>${esc(c.name)}${c.active?"":" (suspended)"}</option>`).join("")}</select></div>` : "";\n  const [title, icon] = TITLES[S.tab] || ["",""];\n  document.body.innerHTML = `<div class="shell">\n   <aside id="sidebar">\n    <div class="profile">\n      <img src="/logo.png" alt="" onerror="this.style.display=\'none\'">\n      <div class="wm">STATS<span>PACK</span></div>\n      <div class="nm">${esc(S.name)}</div>\n      <div class="org">${org}</div>\n      <div class="em">${esc(S.email || (isSuper?"StatsPack super user":"Client admin"))}</div>\n    </div>\n    ${picker}\n    ${isSuper?`<div class="navsec">PLATFORM</div>${navBtn("clients")}`:""}\n    <div class="navsec">DASHBOARDS</div>\n    ${navBtn("overview")}${navBtn("analysis")}${navBtn("live")}${navBtn("maps")}\n    <div class="navsec">REPORTS</div>\n    ${navBtn("reports")}\n    <div class="navsec">DATA</div>\n    ${navBtn("connection")}\n    <div class="navsec">ACCOUNT</div>\n    ${navBtn("security")}\n    <div class="sideout"><button class="btn" id="out">Sign out</button>\n      <div class="vtag">Analytics Lab · v23</div></div>\n   </aside>\n   <div>\n    <div class="topbar">\n      <button class="burger" id="burger" aria-label="Menu">☰</button>\n      <h1>${icon}<span>${esc(title)}</span></h1>\n    </div>\n    <main id="view"></main>\n   </div>\n  </div>`;\n  $("#out").onclick = ()=>{ sessionStorage.clear(); location.reload(); };\n  $("#burger").onclick = ()=> $("#sidebar").classList.toggle("open");\n  document.querySelectorAll(".nitem").forEach(b=>b.onclick=()=>{ S.tab=b.dataset.t; $("#sidebar").classList.remove("open"); draw(); });\n  const cp=$("#clientPick"); if(cp) cp.onchange=()=>{ S.clientId=+cp.value; draw(); };\n}\nconst q = () => S.role==="super" ? "?client_id="+S.clientId : "";\nfunction stopTimer(){ if(S.timer){ clearInterval(S.timer); S.timer=null; } }\n\nasync function draw(quiet){\n  stopTimer(); shell();\n  if(!quiet) showLoader();\n  const v=$("#view");\n  try{\n    if (S.tab==="overview") await drawOverview(v);\n    else if (S.tab==="analysis") await drawAnalysis(v);\n    else if (S.tab==="live") await drawLive(v);\n    else if (S.tab==="maps") await drawMaps(v);\n    else if (S.tab==="reports") await drawReports(v);\n    else if (S.tab==="connection") await drawConnection(v);\n    else if (S.tab==="security") await drawSecurity(v);\n    else if (S.tab==="clients") await drawClients(v);\n  }catch(ex){\n    if (/log in/i.test(ex.message)) { hideLoader(); sessionStorage.clear(); return renderLogin("Session expired — sign in again"); }\n    v.innerHTML = `<div class="msg err" style="display:block">${esc(ex.message)}</div>`;\n  }\n  hideLoader();\n}\n\n/* ---------------- KPI card (SmartRegister style) ---------------- */\nfunction kpi(color, icon, label, val, subL, subR){\n  return `<div class="kpi k-${color}"><div class="ico">${icon}</div>\n    <div class="lbl">${label}</div><div class="rule" style="background:var(--${color==="slate"?"slate":color})"></div>\n    <div class="footr"><div class="val">${val}</div><div class="sub">${subR||""}</div></div>\n    ${subL?`<div class="sub">${subL}</div>`:""}</div>`;\n}\n\n/* ---------------- Bird\'s Eye View ---------------- */\nfunction fq(withCurrency){\n  const p = new URLSearchParams();\n  if (S.role==="super") p.set("client_id", S.clientId);\n  for (const k of ["date_from","date_to","region","department","person_type"]) if (S.f[k]) p.set(k, S.f[k]);\n  if (withCurrency && S.cur) p.set("display_currency", S.cur);\n  const qs = p.toString(); return qs ? "?"+qs : "";\n}\nfunction filterBar(regions, depts, withToggle, types){\n  const sel=(id,label,opts,cur)=>`<div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">${label}</label>\n     <select id="${id}" style="min-width:150px"><option value="">All</option>\n     ${opts.map(o=>`<option ${o===cur?"selected":""}>${esc(o)}</option>`).join("")}</select></div>`;\n  return `<div class="panel" style="margin:0 0 20px">\n     <div class="row" style="gap:16px;align-items:flex-end">\n       <div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">START DATE</label>\n         <input type="date" id="fFrom" value="${esc(S.f.date_from)}" style="width:160px"></div>\n       <div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">END DATE</label>\n         <input type="date" id="fTo" value="${esc(S.f.date_to)}" style="width:160px"></div>\n       ${sel("fRegion","REGION",regions,S.f.region)}\n       ${sel("fDept","DEPARTMENT",depts,S.f.department)}\n       ${sel("fType","PERSON TYPE",types||[],S.f.person_type)}\n       <button class="btn" id="fApply">Apply</button>\n       <button class="btn ghost" id="fReset">Reset</button>\n       <div style="flex:1"></div>\n       ${withToggle?`<div class="row"><span class="note" style="font-weight:700">Chart values:</span>\n        <button class="btn small ${S.pct?"ghost":""}" id="modeCount">Count</button>\n        <button class="btn small ${S.pct?"":"ghost"}" id="modePct">%</button></div>`:""}\n     </div></div>`;\n}\nfunction wireFilters(){\n  $("#fApply").onclick = ()=>{ S.f={ date_from:$("#fFrom").value, date_to:$("#fTo").value,\n      region:$("#fRegion").value, department:$("#fDept").value, person_type:$("#fType")?$("#fType").value:"" }; draw(); };\n  $("#fReset").onclick = ()=>{ S.f={date_from:"",date_to:"",region:"",department:"",person_type:""}; draw(); };\n  wireToggle();\n}\nasync function drawOverview(v){\n  const [d, lb] = await Promise.all([api("/api/stats/overview"+fq()), api("/api/stats/labour"+fq(true))]);\n  const ch = d.change_pct;\n  const cmp = ch===null\n    ? kpi("slate", IC.chart, "vs Last Month", "—", "No visits last month", "")\n    : kpi("slate", IC.chart, "vs Last Month",\n        `<span class="${ch<0?"val down":"val up"}" style="font-size:32px">${ch<0?"↓":"↑"} ${Math.abs(ch)}%</span>`,\n        `${esc(d.this_month.label)}: ${d.this_month.count} · ${esc(d.last_month.label)}: ${d.last_month.count}`, "");\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, true, d.filter_types) + `\n   <div class="kpis">\n     ${kpi("slate", IC.grid, "Total Check-ins", d.total, "", "")}\n     ${kpi("teal", IC.eye, "Unique Visitors", d.unique, "", "")}\n     ${kpi("amber", IC.doc, "Today", d.today, "", "")}\n     ${kpi("coral", IC.pulse, "Ongoing Now", d.ongoing, "", "")}\n     ${cmp}\n   </div>\n   <div class="kpis" style="margin-top:20px">\n     ${kpi("teal", IC.chart, "Avg Visits / Day", d.avg_per_day, "", "")}\n     ${kpi("slate", IC.key, "Avg Duration", `${d.avg_duration}<span style="font-size:18px"> mins</span>`, "", "")}\n     ${kpi("coral", IC.pin, "Highest Day", d.highest?d.highest.count:"—", d.highest?"On "+esc(d.highest.date):"", "")}\n     ${kpi("amber", IC.home, "Lowest Day", d.lowest?d.lowest.count:"—", d.lowest?"On "+esc(d.lowest.date):"", "")}\n   </div>\n   ${lb.total_cost<=0 ? `\n   <div class="panel" style="margin-top:20px"><h2>Workforce Time &amp; Cost</h2>\n     <div class="empty" style="text-align:left;line-height:1.7">\n       Nothing to cost in this selection yet. Time is costed only when all three are true:\n       <br>• the person is an <b>Employee</b> or <b>Contractor</b> — visitors are never costed\n       <br>• they have <b>checked out</b> — people still on site are not costed until they leave\n       <br>• a rate applies — currently <b>${esc(lb.base_currency)} ${lb.default_rate}/hour</b>${lb.default_rate>0?"":" (not set yet — set it under Data &amp; Connection)"}\n     </div>\n   </div>` : `\n   <div class="panel" style="margin-top:20px"><h2>Workforce Time &amp; Cost</h2>\n     <div class="row" style="justify-content:space-between;align-items:flex-start;gap:14px">\n       <p class="note" style="flex:1;min-width:240px">Employees and contractors with completed check-outs. Uses the rate\n       SmartRegister sends per person, otherwise the client rate (${esc(lb.base_currency)} ${lb.default_rate}/hour,\n       set under Data &amp; Connection).${lb.converted?` Shown in <b>${esc(lb.currency)}</b> at today\'s rate.`:""}</div>\n       ${lb.fx_available?`<div class="row"><span class="note" style="font-weight:700">Show in</span>\n         <select id="curPick" style="width:auto;min-width:120px" aria-label="Display currency">\n           <option value="" ${!S.cur?"selected":""}>${esc(lb.base_currency)} (client rate)</option>\n           ${(lb.currencies||[]).map(c=>`<option ${c===S.cur?"selected":""}>${c}</option>`).join("")}\n         </select></div>`:""}\n     </div>\n     <div class="kpis" style="margin-top:14px">\n       ${kpi("slate", IC.key, "Total Cost", esc(lb.currency)+" "+lb.total_cost.toLocaleString(), "", "")}\n       ${kpi("teal", IC.pulse, "Hours Worked", lb.total_hours, "", "")}\n       ${kpi("amber", IC.eye, "People", lb.people, "", "")}\n       ${kpi("coral", IC.chart, "Avg / Person", esc(lb.currency)+" "+lb.avg_cost_per_person.toLocaleString(), "", "")}\n     </div>\n     <div class="grid2" style="margin-top:18px">\n       <div><h2 style="font-size:16px">Cost by Department</h2>\n         ${lb.by_department.length?hbar(lb.by_department.map(r=>({label:r.label,c:Math.round(r.cost)}))):\'<div class="empty">No costed time yet</div>\'}</div>\n       <div><h2 style="font-size:16px">Cost by Person (top 10)</h2>\n         ${lb.by_person.length?`<table><thead><tr><th>Name</th><th>Hours</th><th>Rate</th><th>Cost (${esc(lb.currency)})</th></tr></thead>\n           <tbody>${lb.by_person.map(p=>`<tr><td class="b">${esc(p.label)}</td><td>${(p.mins/60).toFixed(2)}</td>\n             <td>${p.rate.toLocaleString()}</td><td>${p.cost.toLocaleString()}</td></tr>`).join("")}</tbody></table>\n           <p class="note" style="margin-top:8px">Rate shown is what was actually applied. It differs from the\n           client rate only when SmartRegister sent an <code>hourly_rate</code> for that person.</p>`:\'<div class="empty">No costed time yet</div>\'}</div>\n     </div>\n   </div>`}\n   <div class="grid2">\n     <div class="panel"><h2>Monthly Footfall: Overall</h2>${lineChart(d.monthly.map(m=>({l:m.month,v:m.count})))}</div>\n     <div class="panel"><h2>Current Month\'s Footfall</h2>${lineChart(d.current_month_daily.map(x=>({l:x.d.slice(8)+" "+d.this_month.label.slice(0,3),v:x.c})))}</div>\n     <div class="panel"><h2>Top 5 Regions Visited</h2>${hbar(d.top_regions.map(r=>({label:r.label,c:r.c})))}</div>\n     <div class="panel"><h2>Top 5 Purposes of Visit</h2>${hbar(d.top_purposes.map(r=>({label:r.label,c:r.c})))}</div>\n     <div class="panel"><h2>Top 5 Departments by Visits</h2>${hbar(d.top_departments.map(r=>({label:r.label,c:r.c})))}</div>\n     <div class="panel"><h2>Check-ins by Person Type</h2>${hbar((d.by_type||[]).map(r=>({label:r.label,c:r.c})))}</div>\n   </div>`;\n  wireFilters();\n  const cp=$("#curPick");\n  if(cp) cp.onchange=()=>{ S.cur=cp.value; sessionStorage.setItem("cur",S.cur); draw(); };\n}\n\n/* ---------------- In-Depth ---------------- */\nasync function drawAnalysis(v){\n  const d = await api("/api/stats/analysis"+q());\n  v.innerHTML = pctToggle() + `\n   <div class="grid2" style="margin-top:0">\n     <div class="panel"><h2>Visitors by Region</h2>${hbar(d.region_visitors)}</div>\n     <div class="panel"><h2>Visit Duration by Region</h2>${hbar(d.region_duration,{unit:" mins"})}</div>\n     <div class="panel"><h2>Departments by Visitor Footfall</h2>${hbar(d.dept_footfall)}</div>\n     <div class="panel"><h2>Departments taking the most time</h2>\n       ${lineChart(d.dept_duration.map(x=>({l:String(x.label).slice(0,12),v:x.c})),{unit:" m"})}</div>\n     <div class="panel"><h2>Visits taking the most time</h2>${vbar(d.purpose_duration,{unit:" mins"})}</div>\n     <div class="panel"><h2>Hours with highest Visitor Footfall</h2>\n       ${lineChart(d.hourly.map(x=>({l:(x.hour%12||12)+(x.hour<12?" AM":" PM"),v:x.c})))}</div>\n   </div>`;\n  wireToggle();\n}\n\n/* ---------------- Live ---------------- */\nasync function drawLive(v){\n  const render = async () => {\n    const d = await api("/api/stats/live"+q());\n    const rows = d.ongoing.map(o=>`<tr><td class="b">${esc(o.visitor_name)}</td>\n      <td><span class="chip" style="margin:0">${esc(o.person_type||"Visitor")}</span></td>\n      <td>${esc(o.region)} · ${esc(o.town)}</td>\n      <td>${esc(o.host_department)}</td><td>${esc(o.purpose)}</td><td>${esc(o.id_number)}</td>\n      <td>${esc(o.contact)}</td><td><span class="score">${o.elapsed} mins</span></td></tr>`).join("");\n    const exc = d.exceeding.map(o=>`<tr><td class="b">${esc(o.visitor_name)}</td><td>${esc(o.region)} · ${esc(o.town)}</td>\n      <td>${esc(o.host_department)}</td><td><span class="chip" style="margin:0">Ongoing</span></td>\n      <td><span class="score bad">${o.elapsed} mins</span></td></tr>`).join("");\n    $("#view").innerHTML = pctToggle() + `\n     <div class="grid2" style="margin-top:0">\n       <div class="panel"><h2>Purpose for Ongoing Visits</h2>${donut(d.purposes)}</div>\n       <div class="panel"><h2>Ongoing Visits by Region</h2>\n         ${hbar(Object.entries(d.ongoing.reduce((a,o)=>{a[o.region]=(a[o.region]||0)+1;return a;},{}))\n                .map(([label,c])=>({label,c})))}</div>\n     </div>\n     <div class="panel"><h2>Current On-Premises Visitors</h2>\n       ${d.ongoing.length?`<div style="overflow-x:auto"><table><thead><tr><th>Name</th><th>Type</th><th>Region · Town</th>\n        <th>Host Department</th><th>Purpose</th><th>ID Number</th><th>Contact</th><th>Elapsed</th></tr></thead>\n        <tbody>${rows}</tbody></table></div>`:\'<div class="empty">Nobody is on the premises right now</div>\'}</div>\n     <div class="panel"><h2>${d.max_mins?`Visitors Exceeding Allowed Duration (${d.max_mins} mins)`:"Visitors Exceeding Allowed Duration"}</h2>\n       ${!d.max_mins?\'<div class="empty">No duration limit set — turn one on under Data &amp; Connection</div>\':d.exceeding.length?`<div style="overflow-x:auto"><table><thead><tr><th>Visitor</th><th>Region · Town</th>\n        <th>Host Department</th><th>Status</th><th>Duration</th></tr></thead>\n        <tbody>${exc}</tbody></table></div>`:\'<div class="empty">No visitor has exceeded the allowed duration</div>\'}</div>\n     <p class="refresh">Auto-refreshes every 15 seconds · Last updated ${new Date().toLocaleTimeString()}</p>`;\n    wireToggle();\n  };\n  await render();\n  S.timer = setInterval(()=>{ if(S.tab==="live") render().catch(()=>{}); }, 15000);\n}\n\n\n/* ---------------- Maps ---------------- */\nconst GAZ = {"abidjan":[5.36,-4.01],"abu dhabi":[24.45,54.38],"abuja":[9.06,7.5],"accra":[5.6,-0.19],"addis ababa":[9.03,38.74],"adelaide":[-34.93,138.6],"ahmedabad":[23.02,72.57],"alexandra":[-26.1,28.1],"alexandria":[31.2,29.92],"algiers":[36.75,3.06],"almaty":[43.24,76.89],"amman":[31.95,35.93],"amsterdam":[52.37,4.9],"ankara":[39.93,32.86],"antananarivo":[-18.88,47.51],"argentina":[-38.4,-63.6],"arusha":[-3.39,36.68],"asmara":[15.34,38.93],"astana":[51.17,71.43],"asuncion":[-25.26,-57.58],"athens":[37.98,23.73],"atlanta":[33.75,-84.39],"auckland":[-36.85,174.76],"australia":[-25.3,133.8],"baghdad":[33.32,44.36],"baku":[40.41,49.87],"bamako":[12.64,-8.0],"bangalore":[12.97,77.59],"bangkok":[13.76,100.5],"bangladesh":[23.7,90.4],"barcelona":[41.39,2.17],"beaufort west":[-32.36,22.58],"beijing":[39.9,116.41],"beira":[-19.84,34.84],"beirut":[33.89,35.5],"bela-bela":[-24.88,28.29],"belgium":[50.6,4.6],"bellville":[-33.9,18.63],"bengaluru":[12.97,77.59],"benguela":[-12.58,13.41],"benoni":[-26.19,28.32],"berea":[-29.15,27.74],"berlin":[52.52,13.41],"bethlehem":[-28.23,28.31],"bindura":[-17.3,31.33],"birmingham":[52.49,-1.89],"blantyre":[-15.79,35.01],"bloemfontein":[-29.12,26.21],"bogota":[4.71,-74.07],"boksburg":[-26.21,28.26],"boston":[42.36,-71.06],"boteti":[-21.4,24.7],"botswana":[-22.3,24.7],"brasilia":[-15.79,-47.88],"brazil":[-14.2,-51.9],"brazzaville":[-4.27,15.28],"brisbane":[-27.47,153.03],"brits":[-25.63,27.78],"brussels":[50.85,4.35],"bucharest":[44.43,26.1],"budapest":[47.5,19.04],"buenos aires":[-34.6,-58.38],"bulawayo":[-20.16,28.58],"busan":[35.18,129.08],"butha-buthe":[-28.77,28.25],"cairo":[30.04,31.24],"calgary":[51.05,-114.07],"canada":[56.1,-106.3],"canberra":[-35.28,149.13],"cape town":[-33.92,18.42],"caracas":[10.48,-66.9],"casablanca":[33.57,-7.59],"cebu":[10.32,123.89],"central":[-21.5,26.5],"centurion":[-25.86,28.19],"chengdu":[30.57,104.07],"chennai":[13.08,80.27],"chicago":[41.88,-87.63],"chile":[-35.7,-71.5],"china":[35.9,104.2],"chinhoyi":[-17.36,30.2],"chitungwiza":[-18.01,31.08],"chobe":[-18.3,24.5],"christchurch":[-43.53,172.64],"colombia":[4.6,-74.3],"colombo":[6.93,79.86],"conakry":[9.64,-13.58],"copenhagen":[55.68,12.57],"cotonou":[6.37,2.42],"dakar":[14.72,-17.47],"dallas":[32.78,-96.8],"damascus":[33.51,36.29],"dar es salaam":[-6.79,39.21],"de aar":[-30.65,24.01],"delhi":[28.61,77.21],"denmark":[56.2,9.5],"denver":[39.74,-104.99],"dhaka":[23.81,90.41],"djibouti":[11.59,43.15],"dodoma":[-6.16,35.75],"doha":[25.29,51.53],"douala":[4.05,9.77],"dubai":[25.2,55.27],"dublin":[53.35,-6.26],"dublin city":[53.35,-6.26],"durban":[-29.86,31.02],"east london":[-33.02,27.9],"eastern cape":[-32.3,26.5],"edinburgh":[55.95,-3.19],"egypt":[26.8,30.8],"emalahleni":[-25.87,29.23],"empangeni":[-28.75,31.89],"ermelo":[-26.53,29.98],"eswatini":[-26.5,31.5],"ethiopia":[9.1,40.5],"france":[46.6,2.3],"francistown":[-21.17,27.51],"frankfurt":[50.11,8.68],"free state":[-28.5,26.8],"freetown":[8.48,-13.23],"gabane":[-24.66,25.79],"gaborone":[-24.65,25.91],"gauteng":[-26.2,28.2],"gaza":[-24.0,33.0],"geneva":[46.2,6.14],"george":[-33.96,22.46],"germany":[51.1,10.4],"germiston":[-26.22,28.17],"ghana":[7.9,-1.0],"ghanzi":[-21.7,21.65],"grahamstown":[-33.31,26.52],"guadalajara":[20.67,-103.35],"guangzhou":[23.13,113.26],"gweru":[-19.45,29.82],"hamburg":[53.55,9.99],"hanoi":[21.03,105.85],"harare":[-17.83,31.05],"havana":[23.11,-82.37],"helsinki":[60.17,24.94],"hlotse":[-28.87,28.05],"ho chi minh city":[10.82,106.63],"hong kong":[22.32,114.17],"houston":[29.76,-95.37],"hwange":[-18.36,26.5],"hyderabad":[17.39,78.49],"india":[20.6,79.0],"indonesia":[-0.8,113.9],"ireland":[53.1,-8.0],"islamabad":[33.68,73.05],"israel":[31.0,34.9],"istanbul":[41.01,28.98],"italy":[42.8,12.6],"jakarta":[-6.21,106.85],"japan":[36.2,138.3],"jeddah":[21.49,39.19],"jerusalem":[31.77,35.21],"johannesburg":[-26.2,28.05],"juba":[4.85,31.58],"jwaneng":[-24.6,24.73],"kabul":[34.53,69.17],"kadoma":[-18.33,29.92],"kampala":[0.35,32.58],"kano":[12.0,8.52],"kanye":[-24.98,25.34],"karachi":[24.86,67.01],"kasane":[-17.82,25.15],"kathmandu":[27.72,85.32],"katlehong":[-26.33,28.15],"kenya":[0.2,37.9],"kgalagadi":[-24.7,22.0],"kgatleng":[-24.2,26.2],"khartoum":[15.5,32.56],"khayelitsha":[-34.04,18.68],"kigali":[-1.94,30.06],"kimberley":[-28.74,24.76],"kingston":[17.97,-76.79],"kinshasa":[-4.44,15.27],"klerksdorp":[-26.85,26.66],"knysna":[-34.04,23.05],"kokstad":[-30.55,29.42],"kolkata":[22.57,88.36],"krugersdorp":[-26.1,27.77],"kuala lumpur":[3.14,101.69],"kumasi":[6.69,-1.62],"kuruman":[-27.45,23.43],"kuwait city":[29.38,47.99],"kwazulu-natal":[-29.0,30.5],"kwekwe":[-18.93,29.81],"kweneng":[-24.0,25.3],"kyiv":[50.45,30.52],"la paz":[-16.5,-68.15],"ladysmith":[-28.55,29.78],"lagos":[6.52,3.38],"lahore":[31.55,74.34],"las vegas":[36.17,-115.14],"leribe":[-28.87,28.05],"lesotho":[-29.6,28.2],"letlhakane":[-21.42,25.59],"libreville":[0.42,9.47],"lichtenburg":[-26.15,26.16],"lilongwe":[-13.98,33.79],"lima":[-12.05,-77.04],"limpopo":[-23.4,29.5],"lisbon":[38.72,-9.14],"livingstone":[-17.86,25.86],"lobatse":[-25.22,25.68],"lome":[6.13,1.22],"london":[51.51,-0.13],"los angeles":[34.05,-118.24],"luanda":[-8.84,13.23],"lubumbashi":[-11.66,27.48],"lusaka":[-15.42,28.28],"lyon":[45.76,4.84],"madrid":[40.42,-3.7],"mafeteng":[-29.82,27.24],"mafikeng":[-25.86,25.64],"mahikeng":[-25.86,25.64],"makhanda":[-33.31,26.52],"malawi":[-13.2,34.3],"malaysia":[4.2,101.9],"mamelodi":[-25.72,28.38],"manama":[26.23,50.59],"manchester":[53.48,-2.24],"manicaland":[-19.0,32.3],"manila":[14.6,120.98],"manzini":[-26.5,31.38],"mapoteng":[-29.05,28.0],"maputo":[-25.97,32.57],"maputsoe":[-28.89,27.9],"marondera":[-18.19,31.55],"marrakesh":[31.63,-8.01],"marseille":[43.3,5.37],"maseru":[-29.31,27.48],"mashonaland":[-17.3,31.0],"masvingo":[-20.07,30.83],"masvingo province":[-20.3,31.0],"matabeleland":[-20.0,28.0],"maun":[-19.98,23.42],"mbabane":[-26.32,31.13],"medellin":[6.24,-75.58],"melbourne":[-37.81,144.96],"mexico":[23.6,-102.5],"mexico city":[19.43,-99.13],"miami":[25.76,-80.19],"middelburg":[-25.77,29.46],"midlands":[-19.3,29.7],"midrand":[-25.99,28.13],"milan":[45.46,9.19],"mitchells plain":[-34.03,18.62],"mochudi":[-24.42,26.15],"mogadishu":[2.05,45.32],"mohale s hoek":[-30.15,27.47],"mohale\'s hoek":[-30.15,27.47],"mokhotlong":[-29.29,29.07],"mokopane":[-24.19,29.01],"molepolole":[-24.41,25.5],"mombasa":[-4.04,39.67],"monrovia":[6.3,-10.8],"monterrey":[25.69,-100.32],"montevideo":[-34.9,-56.16],"montreal":[45.5,-73.57],"morija":[-29.62,27.51],"morocco":[31.8,-7.1],"moroni":[-11.7,43.26],"moscow":[55.76,37.62],"moshupa":[-24.77,25.42],"mossel bay":[-34.18,22.15],"mozambique":[-18.7,35.5],"mpumalanga":[-25.6,30.5],"mthatha":[-31.59,28.79],"mumbai":[19.08,72.88],"munich":[48.14,11.58],"muscat":[23.59,58.41],"musina":[-22.35,30.04],"mutare":[-18.97,32.67],"nagoya":[35.18,136.91],"nairobi":[-1.29,36.82],"namibia":[-22.0,17.0],"naples":[40.85,14.27],"nelspruit":[-25.47,30.97],"netherlands":[52.1,5.3],"new delhi":[28.61,77.21],"new york":[40.71,-74.01],"new zealand":[-40.9,174.9],"newcastle":[-27.76,29.93],"ngamiland":[-19.5,22.8],"niamey":[13.51,2.11],"nigeria":[9.1,8.7],"north east":[-20.9,27.3],"north west":[-19.5,23.0],"north west province":[-26.0,25.6],"north-east":[-20.9,27.3],"north-west":[-19.5,23.0],"northern cape":[-29.0,21.5],"norton":[-17.88,30.7],"norway":[60.5,8.5],"orapa":[-21.31,25.37],"osaka":[34.69,135.5],"oslo":[59.91,10.75],"ottawa":[45.42,-75.7],"ouagadougou":[12.37,-1.52],"oudtshoorn":[-33.59,22.2],"paarl":[-33.73,18.96],"pakistan":[30.4,69.3],"palapye":[-22.55,27.13],"panama city":[8.98,-79.52],"paris":[48.86,2.35],"perth":[-31.95,115.86],"peru":[-9.2,-75.0],"phalaborwa":[-23.94,31.14],"philippines":[12.9,121.8],"phnom penh":[11.56,104.92],"phoenix":[33.45,-112.07],"pietermaritzburg":[-29.6,30.38],"poland":[52.1,19.4],"polokwane":[-23.9,29.47],"polokwane city":[-23.9,29.47],"port elizabeth":[-33.96,25.6],"port harcourt":[4.82,7.04],"port louis":[-20.16,57.5],"port moresby":[-9.44,147.18],"porto":[41.15,-8.61],"portugal":[39.5,-8.0],"potchefstroom":[-26.72,27.1],"prague":[50.08,14.44],"pretoria":[-25.75,28.19],"pune":[18.52,73.86],"qacha\'s nek":[-30.12,28.69],"queenstown":[-31.9,26.88],"quito":[-0.18,-78.47],"quthing":[-30.4,27.7],"rabat":[34.02,-6.84],"recife":[-8.05,-34.88],"redcliff":[-19.03,29.79],"reykjavik":[64.15,-21.94],"richards bay":[-28.78,32.04],"rio de janeiro":[-22.91,-43.17],"riyadh":[24.71,46.68],"roma":[-29.45,27.72],"rome":[41.9,12.5],"roodepoort":[-26.16,27.87],"rotterdam":[51.92,4.48],"russia":[61.5,105.3],"rustenburg":[-25.67,27.24],"saint petersburg":[59.93,30.34],"salvador":[-12.97,-38.5],"san francisco":[37.77,-122.42],"sandton":[-26.11,28.05],"santiago":[-33.45,-70.67],"sao paulo":[-23.55,-46.63],"sasolburg":[-26.81,27.82],"saudi arabia":[23.9,45.1],"seattle":[47.61,-122.33],"secunda":[-26.51,29.2],"selebi-phikwe":[-21.98,27.85],"semonkong":[-29.84,28.06],"seoul":[37.57,126.98],"serowe":[-22.39,26.71],"shanghai":[31.23,121.47],"shenzhen":[22.54,114.06],"singapore":[1.35,103.82],"sofia":[42.7,23.32],"soshanguve":[-25.52,28.11],"south africa":[-29.0,24.0],"south east":[-24.9,25.7],"south korea":[35.9,127.8],"south-east":[-24.9,25.7],"southern":[-25.03,25.1],"sowa":[-20.56,26.22],"soweto":[-26.27,27.86],"spain":[40.4,-3.7],"standerton":[-26.95,29.24],"stellenbosch":[-33.93,18.86],"stockholm":[59.33,18.06],"surabaya":[-7.26,112.75],"suva":[-18.14,178.44],"sweden":[60.1,18.6],"switzerland":[46.8,8.2],"sydney":[-33.87,151.21],"taipei":[25.03,121.57],"tanzania":[-6.3,34.8],"tashkent":[41.3,69.24],"tbilisi":[41.72,44.79],"tehran":[35.69,51.39],"tel aviv":[32.09,34.78],"tembisa":[-25.99,28.23],"test region":[-24.65,25.91],"test town":[-24.65,25.91],"teyateyaneng":[-29.15,27.74],"thaba-tseka":[-29.52,28.61],"thailand":[15.9,101.0],"thohoyandou":[-22.95,30.48],"tlokweng":[-24.66,25.97],"tokyo":[35.68,139.65],"tonota":[-21.44,27.46],"toronto":[43.65,-79.38],"tripoli":[32.89,13.19],"tsabong":[-26.02,22.4],"tunis":[36.81,10.18],"turkey":[39.0,35.2],"tzaneen":[-23.83,30.16],"uganda":[1.3,32.3],"uitenhage":[-33.76,25.4],"ulaanbaatar":[47.89,106.91],"umtata":[-31.59,28.79],"united arab emirates":[23.4,53.8],"united kingdom":[54.0,-2.0],"united states":[39.8,-98.6],"upington":[-28.45,21.26],"usa":[39.8,-98.6],"vancouver":[49.28,-123.12],"vanderbijlpark":[-26.71,27.84],"vereeniging":[-26.67,27.93],"victoria falls":[-17.93,25.83],"victoria seychelles":[-4.62,55.45],"vienna":[48.21,16.37],"vientiane":[17.97,102.63],"vietnam":[14.1,108.3],"vryburg":[-26.96,24.73],"vryheid":[-27.77,30.79],"walvis bay":[-22.96,14.51],"warsaw":[52.23,21.01],"washington":[38.91,-77.04],"welkom":[-27.98,26.73],"wellington":[-41.29,174.78],"western cape":[-33.5,20.0],"windhoek":[-22.56,17.08],"witbank":[-25.87,29.23],"worcester":[-33.65,19.45],"yangon":[16.87,96.2],"yaounde":[3.85,11.5],"yerevan":[40.18,44.51],"zambia":[-13.1,27.8],"zimbabwe":[-19.0,29.8],"zurich":[47.38,8.54],"zvishavane":[-20.33,30.07]};\nconst GAZ_NOISE=["province","district","region","municipality","metropolitan","metro","council","city of","city","town","area","zone","branch","office"];\nfunction gazFind(raw){\n  const k=(raw||"").trim().toLowerCase();\n  if(!k) return null;\n  if(GAZ[k]) return GAZ[k];\n  let c=k; GAZ_NOISE.forEach(n=>{ c=c.split(n).join(" "); });\n  c=c.replace(/\\s+/g," ").trim();\n  if(c && GAZ[c]) return GAZ[c];\n  for(const key in GAZ){ if(key && (k.includes(key) || (c && c.includes(key)))) return GAZ[key]; }\n  return null;\n}\nfunction locate(r){\n  if (r.lat !== undefined && r.lat !== null) return [r.lat, r.lon];\n  return gazFind(r.region) || gazFind(r.town) || null;\n}\nfunction ensureLeaflet(cb){\n  if (window.L) return cb(true);\n  if (!navigator.onLine) return cb(false);\n  const css=document.createElement("link"); css.rel="stylesheet";\n  css.href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(css);\n  const js=document.createElement("script");\n  js.src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";\n  let done=false; const fin=ok=>{ if(!done){done=true; cb(ok&&!!window.L);} };\n  js.onload=()=>fin(true); js.onerror=()=>fin(false);\n  setTimeout(()=>fin(false), 5000);\n  document.head.appendChild(js);\n}\nfunction svgBubbleMap(rows, metric, color, unit){\n  const pts0=rows.map(r=>locate(r)).filter(Boolean);\n  let lo1=14, lo2=36, la1=-35, la2=-16;                    // default: southern Africa\n  if(pts0.length){\n    const lats=pts0.map(p=>p[0]), lons=pts0.map(p=>p[1]);\n    const padLa=Math.max(4,(Math.max(...lats)-Math.min(...lats))*0.35);\n    const padLo=Math.max(4,(Math.max(...lons)-Math.min(...lons))*0.35);\n    la1=Math.max(-85,Math.min(...lats)-padLa); la2=Math.min(85,Math.max(...lats)+padLa);\n    lo1=Math.max(-180,Math.min(...lons)-padLo); lo2=Math.min(180,Math.max(...lons)+padLo);\n  }\n  const w=560,hh=320;\n  const X=lon=>(lon-lo1)/(lo2-lo1)*w, Y=lat=>(la2-lat)/(la2-la1)*hh;\n  const pts=rows.map(r=>({r, ll:locate(r)})).filter(p=>p.ll);\n  const missing=rows.filter(r=>!locate(r));\n  const max=Math.max(...pts.map(p=>p.r[metric]),1);\n  const labels=[["NAMIBIA",17.2,-22.5],["BOTSWANA",23.6,-21.6],["SOUTH AFRICA",23.5,-31.2],\n    ["ZIMBABWE",29.6,-19.0],["LESOTHO",29.3,-30.3],["MOZAMBIQUE",33.6,-18.5],["ESWATINI",32.3,-26.4]]\n    .filter(([t,lon,lat])=>lon>=lo1&&lon<=lo2&&lat>=la1&&lat<=la2);\n  let out=`<svg viewBox="0 0 ${w} ${hh}" role="img" style="width:100%;height:auto;background:#eaf1f4;border-radius:12px">`;\n  for(let g=1; g<6; g++){ out+=`<line x1="${g*w/6}" y1="0" x2="${g*w/6}" y2="${hh}" stroke="#dde7ec"/>\n    <line x1="0" y1="${g*hh/6}" x2="${w}" y2="${g*hh/6}" stroke="#dde7ec"/>`; }\n  labels.forEach(([t,lon,lat])=>{ out+=`<text x="${X(lon)}" y="${Y(lat)}" font-size="11" letter-spacing="2" fill="#a9bcc6" font-weight="700">${t}</text>`; });\n  const tot=rows.reduce((a,b)=>a+b[metric],0);\n  pts.sort((a,b)=>b.r[metric]-a.r[metric]).forEach(p=>{\n    const [lat,lon]=p.ll, v=p.r[metric], rad=9+22*Math.sqrt(v/max);\n    const lab=(S.pct&&unit===""&&tot)?Math.round(v/tot*100)+"%":v+unit;\n    out+=`<circle cx="${X(lon)}" cy="${Y(lat)}" r="${rad}" fill="${color}" fill-opacity=".78" stroke="#fff" stroke-width="2"/>\n      <text x="${X(lon)}" y="${Y(lat)+4}" font-size="11" text-anchor="middle" fill="#fff" font-weight="800">${lab}</text>\n      <text x="${X(lon)}" y="${Y(lat)-rad-4}" font-size="10.5" text-anchor="middle" fill="#3d5666" font-weight="700">${esc(p.r.region)}</text>`;\n  });\n  out+="</svg>";\n  if(missing.length) out+=`<p class="note" style="margin-top:8px">Not on map (unknown location): ${missing.map(r=>esc(r.region)+" ("+r[metric]+unit+")").join(", ")}</p>`;\n  return out;\n}\nfunction unplacedNote(rows, metric, unit){\n  const miss = rows.filter(r=>!locate(r));\n  if(!miss.length) return "";\n  return `<p class="note" style="margin-top:8px;color:#dd6b4d">Not shown on map (region not recognised):\n    ${miss.map(r=>esc(r.region)+" ("+r[metric]+unit+")").join(", ")}</p>`;\n}\nfunction leafletMap(el, rows, metric, colorHex, unit){\n  const map=L.map(el,{scrollWheelZoom:false, attributionControl:true});\n  const satellite = L.tileLayer(\n    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",\n    {maxZoom:17, attribution:"Imagery &copy; Esri"});\n  const streets = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",\n    {maxZoom:17, attribution:"&copy; OpenStreetMap"});\n  const labels = L.tileLayer(\n    "https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",\n    {maxZoom:17, opacity:.9});\n  satellite.addTo(map); labels.addTo(map);          // satellite is the default view\n  L.control.layers({"Satellite": satellite, "Streets": streets}, {"Place names": labels},\n                   {position:"topright"}).addTo(map);\n  const pts=rows.map(r=>({r, ll:locate(r)})).filter(p=>p.ll);\n  const max=Math.max(...pts.map(p=>p.r[metric]),1);\n  const tot=rows.reduce((a,b)=>a+b[metric],0);\n  const group=[];\n  pts.forEach(p=>{\n    const v=p.r[metric], rad=10+24*Math.sqrt(v/max);\n    const lab=(S.pct&&unit===""&&tot)?Math.round(v/tot*100)+"%":v+unit;\n    const mk=L.circleMarker(p.ll,{radius:rad,color:"#fff",weight:2.5,fillColor:colorHex,fillOpacity:.85})\n      .bindTooltip(`<b>${esc(p.r.region)}</b> — ${lab}`,{permanent:false}).addTo(map);\n    group.push(mk.getLatLng());\n  });\n  if(group.length) map.fitBounds(L.latLngBounds(group).pad(0.45));\n  else map.setView([-26,25],5);\n}\nasync function drawMaps(v){\n  const d = await api("/api/stats/maps"+fq());\n  const rows = d.regions;\n  if(!rows.length){\n    v.innerHTML = filterBar(d.filter_regions, d.filter_departments, true, d.filter_types) +\n      `<div class="panel"><h2>Maps</h2><div class="empty">No check-ins in this selection yet —\n       maps appear once SmartRegister sends visits with a region or town.</div></div>`;\n    wireFilters(); return;\n  }\n  const defs=[["m1","Total Visits by Region","visits","#5cbcb6",""],\n              ["m2","Active / Ongoing Visits by Region","ongoing","#dd6b4d",""],\n              ["m3","Regions with Highest Visit Duration","avg_mins","#e0a83f"," mins"]];\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, true, d.filter_types) +\n    defs.map(([id,title])=>`<div class="panel"><h2>${title}</h2>\n      <div id="${id}" style="height:340px;border-radius:12px;overflow:hidden"></div></div>`).join("");\n  wireFilters();\n  ensureLeaflet(ok=>{\n    defs.forEach(([id,_t,metric,color,unit])=>{\n      const el=$("#"+id); if(!el) return;\n      const data = metric==="ongoing" ? rows.filter(r=>r.ongoing>0) :\n                   metric==="avg_mins" ? [...rows].sort((a,b)=>b.avg_mins-a.avg_mins) : rows;\n      if(ok){ el.style.height="340px"; leafletMap(el, data, metric, color, unit);\n              el.insertAdjacentHTML("afterend", unplacedNote(data, metric, unit)); }\n      else { el.style.height="auto"; el.innerHTML = svgBubbleMap(data, metric, color, unit); }\n    });\n  });\n}\n\n/* ---------------- Reports ---------------- */\nasync function drawReports(v){\n  const [d, lb] = await Promise.all([api("/api/stats/overview"+fq()), api("/api/stats/labour"+fq(true))]);\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, false, d.filter_types) + `\n   <div class="grid2" style="margin-top:0">\n    <div class="panel"><h2>Download full report (PDF)</h2>\n      <p class="note">One PDF containing every report: key figures, check-ins by person type (visitors,\n      employees, contractors), top regions / purposes / departments, duration analysis, hourly and monthly\n      footfall, the <b>regions map</b>, the regions table, and the current on-premises list.\n      The filters above are applied to the whole report.</p>\n      <div class="msg" id="repMsg"></div>\n      <button class="btn" id="repBtn" style="margin-top:12px;padding:13px 22px">Download report (PDF)</button>\n    </div>\n    <div class="panel"><h2>What this report will cover</h2>\n      <table><tbody>\n        <tr><td class="b">Period</td><td>${esc(S.f.date_from||"Start of data")} → ${esc(S.f.date_to||"Today")}</td></tr>\n        <tr><td class="b">Region</td><td>${esc(S.f.region||"All regions")}</td></tr>\n        <tr><td class="b">Department</td><td>${esc(S.f.department||"All departments")}</td></tr>\n        <tr><td class="b">Visits in scope</td><td>${d.total}</td></tr>\n        <tr><td class="b">Unique visitors</td><td>${d.unique}</td></tr>\n        <tr><td class="b">Ongoing right now</td><td>${d.ongoing}</td></tr>\n      </tbody></table>\n    </div>\n   </div>`;\n  wireFilters();\n  $("#repBtn").onclick = async ()=>{\n    const m=$("#repMsg"), b=$("#repBtn"); b.disabled=true; b.textContent="Building report…";\n    try{\n      const r = await fetch("/api/reports/pdf"+fq(), {headers:{Authorization:"Bearer "+S.token}});\n      if(!r.ok){ const e=await r.json().catch(()=>({})); throw new Error(e.error||"Report failed"); }\n      const blob = await r.blob();\n      const a=document.createElement("a"); a.href=URL.createObjectURL(blob);\n      a.download="StatsPack-Visitor-Report.pdf"; a.click(); URL.revokeObjectURL(a.href);\n      m.className="msg ok"; m.textContent="Report downloaded";\n    }catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n    b.disabled=false; b.textContent="Download report (PDF)";\n  };\n}\n\n\n/* ---------------- Security ---------------- */\nasync function drawSecurity(v){\n  v.innerHTML = `\n   <div class="grid2" style="margin-top:0">\n    <div class="panel"><h2>Change password</h2>\n      <p class="note">Applies to your login (${esc(S.email||S.name)}). Use at least 6 characters —\n      longer passphrases are stronger.</p>\n      <div class="msg" id="pwMsg"></div>\n      <div style="max-width:340px">\n        <label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">CURRENT PASSWORD</label>\n        <input type="password" id="pwCur" autocomplete="current-password" style="margin:4px 0 12px">\n        <label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">NEW PASSWORD</label>\n        <input type="password" id="pwNew" autocomplete="new-password" style="margin:4px 0 12px">\n        <label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">CONFIRM NEW PASSWORD</label>\n        <input type="password" id="pwNew2" autocomplete="new-password" style="margin:4px 0 14px">\n        <button class="btn" id="pwBtn">Update password</button>\n      </div>\n    </div>\n    <div class="panel"><h2>Good practice</h2>\n      <table><tbody>\n        <tr><td class="b">First sign-in</td><td>Change any initially issued password immediately.</td></tr>\n        <tr><td class="b">API keys</td><td>Treat client API keys like passwords. Rotate a key from the\n          Client Console if it may have leaked — the old key stops working instantly.</td></tr>\n        <tr><td class="b">Test vs production</td><td>Use a separate client (and key) for test\n          environments so it can be revoked without touching production.</td></tr>\n      </tbody></table>\n    </div>\n   </div>`;\n  $("#pwBtn").onclick = async ()=>{\n    const m=$("#pwMsg"), cur=$("#pwCur").value, nw=$("#pwNew").value, nw2=$("#pwNew2").value;\n    if(nw!==nw2){ m.className="msg err"; m.textContent="New passwords do not match"; return; }\n    try{\n      await api("/api/account/password",{current:cur,new_password:nw});\n      m.className="msg ok"; m.textContent="Password updated";\n      $("#pwCur").value=$("#pwNew").value=$("#pwNew2").value="";\n    }catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n}\n\n/* ---------------- Data & Connection ---------------- */\nasync function drawConnection(v){\n  const d = await api("/api/connection"+q());\n  const base = location.origin;\n  v.innerHTML = `\n   <div class="grid2" style="margin-top:0">\n    <div class="panel"><h2>Connect SmartRegister (real-time)</h2>\n      <p class="note">Point the SmartRegister real-time export at this endpoint. Every check-in and check-out it sends\n      appears here instantly. Re-sending the same <code class="inline">visit_id</code> updates that visit —\n      that\'s how a checkout lands.</p>\n      <p style="margin:12px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">API key</p>\n      <div class="row"><code class="inline" id="apiKey" style="word-break:break-all">${esc(d.api_key)}</code>\n        <button class="btn small ghost" id="copyKey">Copy</button></div>\n      <p style="margin:14px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">Push a visit (JSON)</p>\n<pre>curl -X POST ${base}/ingest/visits \\\\\n  -H "X-API-Key: ${esc(d.api_key)}" \\\\\n  -H "Content-Type: application/json" \\\\\n  -d \'{"visit_id":"VMS-2041","visitor_name":"Thabo M.",\n       "region":"Maseru","town":"Maseru",\n       "host_department":"Customer Service",\n       "purpose":"Enquiry","check_in":"2026-07-19T09:05:00"}\'</pre>\n      <p style="margin:12px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">Or stream the VMS CSV export as-is</p>\n<pre>curl -X POST ${base}/ingest/visits \\\\\n  -H "X-API-Key: ${esc(d.api_key)}" \\\\\n  -H "Content-Type: text/csv" \\\\\n  --data-binary @vms_export.csv</pre>\n      <p class="note">Test the link any time: <code class="inline">GET ${base}/ingest/ping</code> with the same key.</p>\n    </div>\n    <div style="min-width:0">\n     <div class="panel" style="margin-top:0"><h2>Backfill: upload a CSV export</h2>\n      <p class="note">For history that predates the live connection. Headers are matched flexibly\n      (visit_id, visitor name, region, town, department, purpose, check_in, check_out…).</p>\n      <div class="msg" id="upMsg"></div>\n      <input type="file" id="csvFile" accept=".csv,text/csv" aria-label="CSV file" style="margin-top:8px">\n      <button class="btn" id="upBtn" style="margin-top:12px">Upload CSV</button>\n     </div>\n     <div class="panel"><h2>Employee hourly rate</h2>\n      <p class="note">Default rate used to cost employee and contractor time on site. SmartRegister can\n      override it per person by sending an <code>hourly_rate</code> field with the visit.</p>\n      <div class="row" style="margin-top:10px">\n        <select id="curCode" style="width:auto;min-width:110px" aria-label="Currency">\n          ${(d.currencies||[]).map(c=>`<option ${c===(d.currency||"BWP")?"selected":""}>${c}</option>`).join("")}\n        </select>\n        <input id="rateVal" type="number" min="0" step="0.01" value="${d.hourly_rate||0}" style="width:130px"> / hour\n        <button class="btn small" id="saveRate">Save</button></div>\n      <div class="msg" id="rateMsg"></div>\n     </div>\n     <div class="panel"><h2>Allowed visit duration <span class="chip">Optional</span></h2>\n      <p class="note">Ongoing visits longer than this appear under "Visitors Exceeding Allowed Duration".\n      Leave it at <b>0</b> to switch the limit off entirely — nothing will be flagged.</p>\n      <div class="row" style="margin-top:10px"><input id="maxMins" type="number" min="0" max="720" value="${d.max_visit_mins}" style="width:110px"> mins\n      <button class="btn small" id="saveMins">Save</button></div>\n      <div class="msg" id="minsMsg"></div>\n     </div>\n     <div class="panel"><h2>Recent data received</h2>\n      ${d.log.length?`<div style="overflow-x:auto"><table><thead><tr><th>When</th><th>Source</th><th>Rows</th><th>New</th><th>Upd</th><th>Err</th></tr></thead>\n       <tbody>${d.log.map(l=>`<tr><td>${fmtDT(l.ts)}</td><td>${esc(l.source)}</td><td>${l.rows_in}</td>\n        <td>${l.inserted}</td><td>${l.updated}</td><td>${l.errors?`<span class="score bad">${l.errors}</span>`:0}</td></tr>\n        ${l.note?`<tr><td colspan="6" style="padding-top:0;color:#dd6b4d;font-size:12px">${esc(l.note)}</td></tr>`:""}`).join("")}</tbody></table></div>`\n       :\'<div class="empty">Nothing received yet — connect SmartRegister or upload a CSV</div>\'}\n     </div>\n    </div>\n   </div>`;\n  $("#copyKey").onclick = ()=>{ navigator.clipboard.writeText(d.api_key); $("#copyKey").textContent="Copied"; };\n  $("#saveMins").onclick = async ()=>{\n    const m=$("#minsMsg");\n    try{ await api("/api/connection/settings"+q(),{max_visit_mins:+$("#maxMins").value}); m.className="msg ok"; m.textContent="Saved"; }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  $("#saveRate").onclick = async ()=>{\n    const m=$("#rateMsg");\n    try{ await api("/api/connection/settings"+q(),{max_visit_mins:+$("#maxMins").value,\n           hourly_rate:+$("#rateVal").value, currency:$("#curCode").value});\n         m.className="msg ok"; m.textContent="Saved"; }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  $("#upBtn").onclick = async ()=>{\n    const f=$("#csvFile").files[0], m=$("#upMsg");\n    if(!f){ m.className="msg err"; m.textContent="Choose a CSV file first"; return; }\n    try{\n      const text = await f.text();\n      const r = await api("/api/connection/upload"+q(), {csv:text});\n      m.className="msg ok"; m.textContent=`Received ${r.received} rows — ${r.inserted} new, ${r.updated} updated, ${r.errors} errors`;\n    }catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n}\n\n/* ---------------- Super: Overview (Client console) ---------------- */\nasync function drawClients(v){\n  const d = await api("/api/super/clients");\n  S.clients = d.clients;\n  const totals = d.clients.reduce((a,c)=>({v:a.v+c.visits,o:a.o+c.ongoing,ad:a.ad+c.admins}),{v:0,o:0,ad:0});\n  v.innerHTML = `\n   <div class="kpis">\n    ${kpi("slate", IC.grid, "Clients", d.clients.length, "", "")}\n    ${kpi("teal", IC.chart, "Total Visits", totals.v, "", "")}\n    ${kpi("amber", IC.pulse, "On Premises Now", totals.o, "", "")}\n    ${kpi("coral", IC.eye, "Client Admins", totals.ad, "", "")}\n   </div>\n   <div class="panel"><h2>Clients at a glance</h2>\n    <div class="msg" id="cMsg"></div>\n    <div style="overflow-x:auto"><table><thead><tr><th>Client</th><th>Visits</th><th>On premises</th>\n     <th>Admins</th><th>Last data received</th><th>API key</th><th></th></tr></thead><tbody>\n    ${d.clients.map(c=>`<tr>\n      <td class="b">${esc(c.name)}<span class="chip">${c.active?"Active":"Suspended"}</span></td>\n      <td>${c.visits}</td><td>${c.ongoing}</td><td>${c.admins}</td>\n      <td>${fmtDT(c.last_ingest)}</td>\n      <td><code class="inline" style="word-break:break-all">${esc(c.api_key)}</code></td>\n      <td style="white-space:nowrap">\n        <button class="btn small ghost" data-view="${c.id}">Dashboards</button>\n        <button class="btn small ghost" data-admins="${c.id}" data-name="${esc(c.name)}">Admins</button>\n        <button class="btn small ghost" data-rotate="${c.id}">Rotate key</button>\n        <button class="btn small ${c.active?"warn":""}" data-toggle="${c.id}">${c.active?"Suspend":"Reactivate"}</button>\n      </td></tr>`).join("")}\n    </tbody></table></div>\n    <div class="row" style="margin-top:16px"><input id="cName" placeholder="New client / company name" style="max-width:320px">\n     <button class="btn" id="cAdd">Create client</button></div>\n    <p class="note" style="margin-top:8px">Creating a client issues an API key. Give the key to the client\'s VMS\n     installation, then add an admin login so they can see their dashboards.</p>\n   </div>\n   <div id="adminPanel"></div>`;\n  $("#cAdd").onclick = async ()=>{\n    const m=$("#cMsg");\n    try{ await api("/api/super/clients",{name:$("#cName").value}); draw(); }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  v.querySelectorAll("[data-view]").forEach(b=>b.onclick=()=>{ S.clientId=+b.dataset.view; S.tab="overview"; draw(); });\n  v.querySelectorAll("[data-toggle]").forEach(b=>b.onclick=async()=>{ await api(`/api/super/clients/${b.dataset.toggle}/toggle`,{},"POST"); draw(); });\n  v.querySelectorAll("[data-rotate]").forEach(b=>b.onclick=async()=>{\n    if(confirm("Rotate this client\'s API key? The old key stops working immediately.")){\n      await api(`/api/super/clients/${b.dataset.rotate}/rotate_key`,{},"POST"); draw(); } });\n  v.querySelectorAll("[data-admins]").forEach(b=>b.onclick=async()=>{\n    const t=b.textContent; b.textContent="Opening…"; b.disabled=true;\n    try{ await showAdmins(+b.dataset.admins, b.dataset.name); }\n    catch(ex){ $("#cMsg").className="msg err"; $("#cMsg").textContent=ex.message; }\n    b.textContent=t; b.disabled=false;\n  });\n}\nasync function showAdmins(cid, cname){\n  const d = await api(`/api/super/clients/${cid}/admins`);\n  $("#adminPanel").innerHTML = `\n   <div class="panel"><h2>Admin logins — ${esc(cname)}</h2>\n    <div class="msg" id="aMsg"></div>\n    <div class="row"><input id="aName" placeholder="Name" style="max-width:180px">\n     <input id="aEmail" type="email" placeholder="Email" style="max-width:230px">\n     <input id="aPw" type="password" placeholder="Password (6+ chars)" style="max-width:200px">\n     <button class="btn" id="aAdd">Add admin</button></div>\n    ${d.admins.length?`<table style="margin-top:12px"><thead><tr><th>Name</th><th>Email</th><th>Status</th><th></th></tr></thead><tbody>\n     ${d.admins.map(a=>`<tr><td class="b">${esc(a.name)}</td><td>${esc(a.email)}</td>\n      <td><span class="chip" style="margin:0">${a.active?"Active":"Disabled"}</span></td>\n      <td><button class="btn small ghost" data-tg="${a.id}">${a.active?"Disable":"Enable"}</button></td></tr>`).join("")}\n    </tbody></table>`:\'<div class="empty">No admin logins yet — add the first one above</div>\'}\n   </div>`;\n  $("#aAdd").onclick = async ()=>{\n    const m=$("#aMsg");\n    try{ await api(`/api/super/clients/${cid}/admins`,{name:$("#aName").value,email:$("#aEmail").value,password:$("#aPw").value});\n         showAdmins(cid,cname); }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  document.querySelectorAll("[data-tg]").forEach(b=>b.onclick=async()=>{ await api(`/api/super/admins/${b.dataset.tg}/toggle`,{},"POST"); showAdmins(cid,cname); });\n  const panel = $("#adminPanel");\n  if (panel){\n    panel.scrollIntoView({behavior:"smooth", block:"center"});\n    const card = panel.querySelector(".panel");\n    if (card){\n      card.style.transition = "box-shadow .5s ease";\n      card.style.boxShadow = "0 0 0 3px var(--teal)";\n      setTimeout(()=>{ card.style.boxShadow = ""; }, 1400);\n    }\n  }\n}\n\n/* ---------------- boot ---------------- */\nasync function boot(){\n  if(!S.token){ hideLoader(); return renderLogin(); }\n  if(S.role==="super"){\n    try{ const d = await api("/api/super/clients"); S.clients=d.clients;\n         if(!S.clientId && d.clients.length) S.clientId=d.clients[0].id;\n         if(!S.tab) S.tab="clients";\n    }catch(ex){ hideLoader(); sessionStorage.clear(); return renderLogin(); }\n  } else if(!S.tab || S.tab==="clients"){ S.tab="overview"; }\n  await draw(true);\n}\nboot();\n</script>\n</body>\n</html>\n'

# ---------------------------------------------------------------- http
ROUTES = []
def route(method, pattern):
    def deco(fn):
        ROUTES.append((method, re.compile("^" + pattern + "$"), fn)); return fn
    return deco

class Ctx:
    def __init__(s, conn, user, client, qs, body): s.conn, s.user, s.client, s.qs, s.body = conn, user, client, qs, body

def auth_user(conn, headers):
    tok = (headers.get("Authorization") or "").replace("Bearer ", "").strip()
    if not tok: return None
    r = conn.execute("""SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id
                        WHERE s.token=? AND u.active=1""", (tok,)).fetchone()
    return r

def scoped_client(ctx):
    """Which client's data is being viewed. Admin -> own. Super -> ?client_id=N."""
    if ctx.user["role"] == "admin":
        return ctx.user["client_id"]
    cid = ctx.qs.get("client_id", [None])[0]
    return int(cid) if cid else None

# ---- auth
@route("POST", r"/api/login")
def login(ctx):
    email = (ctx.body.get("email") or "").strip().lower()
    u = ctx.conn.execute("SELECT * FROM users WHERE lower(email)=?", (email,)).fetchone()
    if not u or hpw(ctx.body.get("password", ""), u["salt"]) != u["pw"]:
        return 401, {"error": "Wrong email or password"}
    if not u["active"]: return 403, {"error": "Account disabled"}
    if u["client_id"]:
        c = ctx.conn.execute("SELECT active FROM clients WHERE id=?", (u["client_id"],)).fetchone()
        if not c or not c["active"]: return 403, {"error": "Client account suspended — contact StatsPack"}
    tok = secrets.token_hex(24)
    ctx.conn.execute("INSERT INTO sessions(token,user_id,created_at) VALUES(?,?,?)", (tok, u["id"], iso(now())))
    ctx.conn.commit()
    cname = ""
    if u["client_id"]:
        cr = ctx.conn.execute("SELECT name FROM clients WHERE id=?", (u["client_id"],)).fetchone()
        cname = cr["name"] if cr else ""
    return 200, {"token": tok, "role": u["role"], "name": u["name"], "email": u["email"],
                 "client_id": u["client_id"], "client_name": cname}

@route("POST", r"/api/logout")
def logout(ctx):
    return 200, {"ok": True}

# ---- dashboards (admin sees own client; super passes ?client_id=)
@route("GET", r"/api/stats/overview")
def api_overview(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_overview(ctx.conn, cid, _flt_from_qs(ctx.qs))

@route("GET", r"/api/stats/analysis")
def api_analysis(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_analysis(ctx.conn, cid, _flt_from_qs(ctx.qs))

@route("GET", r"/api/stats/live")
def api_live(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT max_visit_mins FROM clients WHERE id=?", (cid,)).fetchone()
    return 200, stats_live(ctx.conn, cid, c["max_visit_mins"] if c else 60)

@route("GET", r"/api/stats/maps")
def api_maps(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_maps(ctx.conn, cid, _flt_from_qs(ctx.qs))

@route("GET", r"/api/stats/labour")
def api_labour(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_labour(ctx.conn, cid, _flt_from_qs(ctx.qs))

@route("GET", r"/api/reports/pdf")
def api_report(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT name FROM clients WHERE id=?", (cid,)).fetchone()
    pdf = build_report(ctx.conn, cid, _flt_from_qs(ctx.qs), c["name"] if c else "Client")
    return 200, pdf, "application/pdf"

@route("POST", r"/api/account/password")
def api_change_password(ctx):
    cur, new = ctx.body.get("current") or "", ctx.body.get("new_password") or ""
    u = ctx.user
    if hpw(cur, u["salt"]) != u["pw"]:
        return 400, {"error": "Current password is incorrect"}
    if len(new) < 6:
        return 400, {"error": "New password must be at least 6 characters"}
    salt = secrets.token_hex(8)
    ctx.conn.execute("UPDATE users SET salt=?, pw=? WHERE id=?", (salt, hpw(new, salt), u["id"]))
    ctx.conn.commit()
    return 200, {"ok": True}

# ---- client admin: connection page
@route("GET", r"/api/connection")
def api_connection(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    log = ctx.conn.execute("""SELECT ts,source,rows_in,inserted,updated,errors,note FROM ingest_log
                              WHERE client_id=? ORDER BY id DESC LIMIT 15""", (cid,)).fetchall()
    return 200, {"name": c["name"], "api_key": c["api_key"], "max_visit_mins": c["max_visit_mins"],
                 "hourly_rate": c["hourly_rate"], "currency": c["currency"],
                 "currencies": CURRENCIES,
                 "log": [dict(r) for r in log]}

@route("POST", r"/api/connection/upload")
def api_upload_csv(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    text = ctx.body.get("csv", "")
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows: return 400, {"error": "No data rows found in the CSV"}
    return 200, ingest_rows(ctx.conn, cid, rows, "manual CSV upload")

@route("POST", r"/api/connection/settings")
def api_conn_settings(ctx):
    cid = scoped_client(ctx)
    try: m = int(ctx.body.get("max_visit_mins") or 0)
    except (TypeError, ValueError): m = 0
    if m and not (5 <= m <= 720):
        return 400, {"error": "Allowed duration must be between 5 and 720 minutes, or 0 for no limit"}
    ctx.conn.execute("UPDATE clients SET max_visit_mins=? WHERE id=?", (m, cid))
    if "hourly_rate" in ctx.body:
        try: hr = max(0.0, float(ctx.body.get("hourly_rate") or 0))
        except (TypeError, ValueError): hr = 0.0
        cur_ = (str(ctx.body.get("currency") or "BWP").strip().upper() or "BWP")[:6]
        ctx.conn.execute("UPDATE clients SET hourly_rate=?, currency=? WHERE id=?", (hr, cur_, cid))
    ctx.conn.commit()
    return 200, {"ok": True}

# ---- super: manage clients
def require_super(ctx):
    return ctx.user["role"] == "super"

@route("GET", r"/api/super/clients")
def super_clients(ctx):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    rows = ctx.conn.execute("""
      SELECT c.*, (SELECT COUNT(*) FROM visits v WHERE v.client_id=c.id) visits,
             (SELECT COUNT(*) FROM visits v WHERE v.client_id=c.id AND v.check_out IS NULL) ongoing,
             (SELECT COUNT(*) FROM users u WHERE u.client_id=c.id) admins,
             (SELECT MAX(ts) FROM ingest_log l WHERE l.client_id=c.id) last_ingest
      FROM clients c ORDER BY c.name""").fetchall()
    return 200, {"clients": [dict(r) for r in rows]}

@route("POST", r"/api/super/clients")
def super_add_client(ctx):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    name = (ctx.body.get("name") or "").strip()
    if not name: return 400, {"error": "Client name is required"}
    try:
        ctx.conn.execute("INSERT INTO clients(name,api_key,active,max_visit_mins,created_at) VALUES(?,?,1,60,?)",
                         (name, "spk_live_" + secrets.token_hex(16), iso(now())))
        ctx.conn.commit()
    except sqlite3.IntegrityError:
        return 400, {"error": "A client with that name already exists"}
    return 200, {"ok": True}

@route("POST", r"/api/super/clients/(\d+)/toggle")
def super_toggle(ctx, cid):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    ctx.conn.execute("UPDATE clients SET active=1-active WHERE id=?", (int(cid),))
    ctx.conn.commit()
    return 200, {"ok": True}

@route("POST", r"/api/super/clients/(\d+)/rotate_key")
def super_rotate(ctx, cid):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    key = "spk_live_" + secrets.token_hex(16)
    ctx.conn.execute("UPDATE clients SET api_key=? WHERE id=?", (key, int(cid)))
    ctx.conn.commit()
    return 200, {"api_key": key}

@route("GET", r"/api/super/clients/(\d+)/admins")
def super_admins(ctx, cid):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    rows = ctx.conn.execute("SELECT id,name,email,active,created_at FROM users WHERE client_id=? ORDER BY name",
                            (int(cid),)).fetchall()
    return 200, {"admins": [dict(r) for r in rows]}

@route("POST", r"/api/super/clients/(\d+)/admins")
def super_add_admin(ctx, cid):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    name, email, pw = (ctx.body.get("name") or "").strip(), (ctx.body.get("email") or "").strip().lower(), ctx.body.get("password") or ""
    if not (name and email and len(pw) >= 6):
        return 400, {"error": "Name, email and a password of 6+ characters are required"}
    salt = secrets.token_hex(8)
    try:
        ctx.conn.execute("INSERT INTO users(client_id,name,email,salt,pw,role,active,created_at) VALUES(?,?,?,?,?, 'admin',1,?)",
                         (int(cid), name, email, salt, hpw(pw, salt), iso(now())))
        ctx.conn.commit()
    except sqlite3.IntegrityError:
        return 400, {"error": "That email is already registered"}
    return 200, {"ok": True}

@route("POST", r"/api/super/admins/(\d+)/toggle")
def super_toggle_admin(ctx, uid):
    if not require_super(ctx): return 403, {"error": "StatsPack super user only"}
    ctx.conn.execute("UPDATE users SET active=1-active WHERE id=? AND role='admin'", (int(uid),))
    ctx.conn.commit()
    return 200, {"ok": True}

# ---------------------------------------------------------------- ingest endpoints (API-key auth, no session)
def client_by_key(conn, headers, qs):
    key = headers.get("X-API-Key") or qs.get("api_key", [""])[0]
    if not key: return None, (401, {"error": "Missing API key: send X-API-Key header"})
    c = conn.execute("SELECT * FROM clients WHERE api_key=?", (key,)).fetchone()
    if not c: return None, (401, {"error": "Unknown API key"})
    if not c["active"]: return None, (403, {"error": "Client is suspended — contact StatsPack"})
    return c, None

class Handler(BaseHTTPRequestHandler):
    server_version = "AnalyticsLab/1.0"
    def log_message(self, *a): pass

    def _send(self, code, payload, ctype="application/json"):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self): self._send(200, {"ok": True})

    def _serve_static(self, path):
        if path in ("/", "/index.html"):
            body = INDEX_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            return self.wfile.write(body)
        fp = os.path.normpath(os.path.join(STATIC, path.lstrip("/")))
        if not fp.startswith(STATIC) or not os.path.isfile(fp):
            # branding images may be uploaded to the repo root instead of static/
            root = os.path.dirname(os.path.abspath(__file__))
            alt = os.path.join(root, os.path.basename(path))
            if os.path.basename(path) in ("logo.png", "login.png", "favicon.ico") and os.path.isfile(alt):
                fp = alt
            else:
                return self._send(404, {"error": "Not found"})
        ctypes = {".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml",
                  ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".ico": "image/x-icon"}
        ext = os.path.splitext(fp)[1]
        with open(fp, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctypes.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        # never let browsers serve a stale UI after a redeploy
        self.send_header("Cache-Control", "no-cache" if ext == ".html" else "public, max-age=300")
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method):
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        ln = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(ln) if ln else b""

        with LOCK:
            conn = connect()
            try:
                # ---- ingest (API key)
                if u.path == "/ingest/ping":
                    c, e = client_by_key(conn, self.headers, qs)
                    return self._send(*e) if e else self._send(200, {"ok": True, "client": c["name"]})
                if u.path == "/ingest/visits" and method == "POST":
                    c, e = client_by_key(conn, self.headers, qs)
                    if e: return self._send(*e)
                    ctype = (self.headers.get("Content-Type") or "").lower()
                    text = raw.decode("utf-8", "replace")
                    if "json" in ctype:
                        try: data = json.loads(text or "[]")
                        except json.JSONDecodeError: return self._send(400, {"error": "Body is not valid JSON"})
                        rows = data if isinstance(data, list) else [data]
                        src = "SmartRegister push (JSON)"
                    else:
                        rows = list(csv.DictReader(io.StringIO(text)))
                        src = "SmartRegister push (CSV)"
                    if not rows: return self._send(400, {"error": "No rows in request body"})
                    return self._send(200, ingest_rows(conn, c["id"], rows, src))

                # ---- app API (session auth)
                if u.path.startswith("/api/"):
                    body = {}
                    if raw:
                        try: body = json.loads(raw.decode("utf-8", "replace"))
                        except json.JSONDecodeError: return self._send(400, {"error": "Invalid JSON"})
                    user = auth_user(conn, self.headers)
                    if u.path != "/api/login" and not user:
                        return self._send(401, {"error": "Please log in"})
                    for m, pat, fn in ROUTES:
                        mt = pat.match(u.path)
                        if m == method and mt:
                            ctx = Ctx(conn, user, None, qs, body)
                            res = fn(ctx, *mt.groups())
                            if len(res) == 3:
                                code, payload, ctype = res
                                return self._send(code, payload, ctype)
                            code, payload = res
                            return self._send(code, payload)
                    return self._send(404, {"error": "Unknown API route"})

                # ---- static
                if method == "GET": return self._serve_static(u.path)
                return self._send(404, {"error": "Not found"})
            finally:
                conn.close()

    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")

def migrate(conn):
    cols = table_columns(conn, "visits")
    if cols and "person_type" not in cols:
        conn.execute("ALTER TABLE visits ADD COLUMN person_type TEXT NOT NULL DEFAULT 'Visitor'")
        conn.commit()
    if cols and "hourly_rate" not in cols:
        conn.execute("ALTER TABLE visits ADD COLUMN hourly_rate REAL")
        conn.commit()
    ccols = table_columns(conn, "clients")
    if ccols and "hourly_rate" not in ccols:
        conn.execute("ALTER TABLE clients ADD COLUMN hourly_rate REAL NOT NULL DEFAULT 0")
        conn.execute("ALTER TABLE clients ADD COLUMN currency TEXT NOT NULL DEFAULT 'BWP'")
        conn.commit()

def main():
    try:
        conn = connect()
    except SystemExit:
        raise
    except Exception as e:
        raise SystemExit(
            f"Could not connect to the database.\n{type(e).__name__}: {e}\n"
            "If using Neon, check DATABASE_URL (host, password, and sslmode=require).\n"
            "Unset DATABASE_URL to fall back to local SQLite storage.")
    conn.executescript(SCHEMA)
    migrate(conn)
    seed(conn)
    conn.close()
    print(f"StatsPack Analytics Lab  ->  http://localhost:{PORT}")
    print("  Super user : admin@statspack.co.ls / super123")
    print("  Client demo: admin@demo.client / admin123")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
