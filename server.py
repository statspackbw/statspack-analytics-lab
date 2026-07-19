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
import json, os, re, csv, io, sqlite3, hashlib, secrets, threading
from datetime import datetime, timedelta, date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT", 8460))          # Render injects PORT automatically
# On Render, attach a persistent disk and set DB_PATH=/var/data/analyticslab.db
# so the database survives deploys/restarts. Locally it defaults to the project folder.
DB = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "analyticslab.db"))
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
LOCK = threading.Lock()

# ---------------------------------------------------------------- helpers
def now(): return datetime.now().replace(microsecond=0)
def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S")
def hpw(pw, salt): return hashlib.sha256((salt + pw).encode()).hexdigest()

def connect():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

SCHEMA = """
CREATE TABLE IF NOT EXISTS clients(
  id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, api_key TEXT NOT NULL UNIQUE,
  active INTEGER NOT NULL DEFAULT 1, max_visit_mins INTEGER NOT NULL DEFAULT 60,
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
  check_in TEXT NOT NULL, check_out TEXT,
  UNIQUE(client_id, visit_id));
CREATE INDEX IF NOT EXISTS ix_visits_client_in ON visits(client_id, check_in);
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
    conn.execute("INSERT INTO clients(name,api_key,active,max_visit_mins,created_at) VALUES(?,?,1,60,?)",
                 ("Demo Client (Botswana Insurance)", "spk_live_" + secrets.token_hex(16), t))
    cid = conn.execute("SELECT id FROM clients WHERE name LIKE 'Demo%'").fetchone()["id"]
    add_user(cid, "Demo Admin", "admin@demo.client", "admin123", "admin")

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
            vseq += 1
            conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                            host_department,purpose,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                         (cid, f"VMS-{vseq}", nm, str(rng.randint(60000000, 79999999)),
                          str(rng.randint(800000000, 2699999999)), region, town, dept, purpose,
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
        conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                        host_department,purpose,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,NULL)""",
                     (cid, f"VMS-{vseq}", nm, str(rng.randint(60000000, 79999999)),
                      str(rng.randint(800000000, 2699999999)), region, town, dept, purpose, iso(cin)))
    conn.commit()

# ---------------------------------------------------------------- ingest
FIELD_ALIASES = {
    "visit_id": ["visit_id","visitid","id","visit id","ref","reference"],
    "visitor_name": ["visitor_name","visitor","name","visitor /user name","visitor/user name","user name","visitor name"],
    "contact": ["contact","contact_number","contact number","phone","mobile"],
    "id_number": ["id_number","id number","idnumber","national_id","omang"],
    "region": ["region"],
    "town": ["town","city"],
    "host_department": ["host_department","host department","department","dept"],
    "purpose": ["purpose","purpose_of_visit","purpose of visit"],
    "check_in": ["check_in","check in","checkin","time_in","time in","arrival","check_in_time"],
    "check_out": ["check_out","check out","checkout","time_out","time out","departure","check_out_time"],
}
def norm_row(raw):
    low = {re.sub(r"\s+", " ", (k or "").strip().lower()): (v or "").strip() for k, v in raw.items()}
    out = {}
    for field, aliases in FIELD_ALIASES.items():
        for a in aliases:
            if a in low and low[a] != "":
                out[field] = low[a]; break
    return out

def parse_dt(s):
    if not s: return None
    s = s.strip().replace("Z", "")
    fmts = ["%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M","%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M","%d/%m/%Y %H:%M:%S","%d-%m-%Y %H:%M","%Y/%m/%d %H:%M"]
    for f in fmts:
        try: return datetime.strptime(s, f)
        except ValueError: pass
    return None

def ingest_rows(conn, client_id, rows, source):
    ins = upd = err = 0
    for raw in rows:
        r = norm_row(raw) if isinstance(raw, dict) else {}
        cin = parse_dt(r.get("check_in", ""))
        vid = r.get("visit_id", "")
        if not vid or not cin:
            err += 1; continue
        cout = parse_dt(r.get("check_out", ""))
        vals = (r.get("visitor_name",""), r.get("contact",""), r.get("id_number",""),
                r.get("region",""), r.get("town",""), r.get("host_department",""),
                r.get("purpose",""), iso(cin), iso(cout) if cout else None)
        cur = conn.execute("SELECT id FROM visits WHERE client_id=? AND visit_id=?", (client_id, vid)).fetchone()
        if cur:
            conn.execute("""UPDATE visits SET visitor_name=?,contact=?,id_number=?,region=?,town=?,
                            host_department=?,purpose=?,check_in=?,check_out=? WHERE id=?""", vals + (cur["id"],))
            upd += 1
        else:
            conn.execute("""INSERT INTO visits(client_id,visit_id,visitor_name,contact,id_number,region,town,
                            host_department,purpose,check_in,check_out) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                         (client_id, vid) + vals)
            ins += 1
    conn.execute("INSERT INTO ingest_log(client_id,ts,source,rows_in,inserted,updated,errors) VALUES(?,?,?,?,?,?,?)",
                 (client_id, iso(now()), source, len(rows), ins, upd, err))
    conn.commit()
    return {"received": len(rows), "inserted": ins, "updated": upd, "errors": err}

# ---------------------------------------------------------------- stats
def mins(a, b):
    try: return max(0, int((datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds() // 60))
    except Exception: return 0

def stats_overview(conn, cid):
    t = now(); today_s = t.strftime("%Y-%m-%d")
    q = lambda sql, *a: conn.execute(sql, a).fetchone()
    total = q("SELECT COUNT(*) c FROM visits WHERE client_id=?", cid)["c"]
    unique = q("""SELECT COUNT(DISTINCT COALESCE(NULLIF(id_number,''), visitor_name||'|'||contact)) c
                  FROM visits WHERE client_id=?""", cid)["c"]
    today_c = q("SELECT COUNT(*) c FROM visits WHERE client_id=? AND substr(check_in,1,10)=?", cid, today_s)["c"]
    ongoing = q("SELECT COUNT(*) c FROM visits WHERE client_id=? AND check_out IS NULL", cid)["c"]
    days = q("SELECT COUNT(DISTINCT substr(check_in,1,10)) c FROM visits WHERE client_id=?", cid)["c"] or 1
    durs = [mins(r["check_in"], r["check_out"]) for r in
            conn.execute("SELECT check_in,check_out FROM visits WHERE client_id=? AND check_out IS NOT NULL", (cid,))]
    avg_dur = round(sum(durs) / len(durs)) if durs else 0
    daily = conn.execute("""SELECT substr(check_in,1,10) d, COUNT(*) c FROM visits WHERE client_id=?
                            GROUP BY d ORDER BY c""", (cid,)).fetchall()
    lo = {"count": daily[0]["c"], "date": daily[0]["d"]} if daily else None
    hi = {"count": daily[-1]["c"], "date": daily[-1]["d"]} if daily else None
    # month over month
    m0 = t.strftime("%Y-%m")
    prev = (t.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    cm = q("SELECT COUNT(*) c FROM visits WHERE client_id=? AND substr(check_in,1,7)=?", cid, m0)["c"]
    pm = q("SELECT COUNT(*) c FROM visits WHERE client_id=? AND substr(check_in,1,7)=?", cid, prev)["c"]
    change = round((cm - pm) / pm * 100, 1) if pm else None
    # monthly series (6 months)
    months = []
    y, m = t.year, t.month
    for back in range(5, -1, -1):
        yy, mm = y, m - back
        while mm <= 0: mm += 12; yy -= 1
        key = f"{yy:04d}-{mm:02d}"
        c = q("SELECT COUNT(*) c FROM visits WHERE client_id=? AND substr(check_in,1,7)=?", cid, key)["c"]
        months.append({"month": datetime(yy, mm, 1).strftime("%b %Y"), "count": c})
    # current month daily series
    curdays = conn.execute("""SELECT substr(check_in,1,10) d, COUNT(*) c FROM visits
                              WHERE client_id=? AND substr(check_in,1,7)=? GROUP BY d ORDER BY d""",
                           (cid, m0)).fetchall()
    top = lambda col: [dict(r) for r in conn.execute(
        f"""SELECT {col} label, COUNT(*) c FROM visits WHERE client_id=? AND {col}!=''
            GROUP BY {col} ORDER BY c DESC LIMIT 5""", (cid,))]
    return {"total": total, "unique": unique, "today": today_c, "ongoing": ongoing,
            "avg_per_day": round(total / days), "avg_duration": avg_dur, "highest": hi, "lowest": lo,
            "this_month": {"label": t.strftime("%b %Y"), "count": cm},
            "last_month": {"label": (t.replace(day=1) - timedelta(days=1)).strftime("%b %Y"), "count": pm},
            "change_pct": change, "monthly": months,
            "current_month_daily": [dict(r) for r in curdays],
            "top_regions": top("region"), "top_purposes": top("purpose")}

def stats_analysis(conn, cid):
    rows = conn.execute("""SELECT region, town, host_department, purpose, check_in, check_out
                           FROM visits WHERE client_id=?""", (cid,)).fetchall()
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
    t = now()
    rows = conn.execute("""SELECT * FROM visits WHERE client_id=? AND check_out IS NULL
                           ORDER BY region, town, check_in""", (cid,)).fetchall()
    ongoing = [{**{k: r[k] for k in ("region","town","host_department","id_number","visitor_name","contact","purpose")},
                "check_in": r["check_in"], "elapsed": mins(r["check_in"], iso(t))} for r in rows]
    purposes = {}
    for o in ongoing: purposes[o["purpose"]] = purposes.get(o["purpose"], 0) + 1
    exceeding = [o for o in ongoing if o["elapsed"] > max_mins]
    return {"ongoing": ongoing, "purposes": [{"label": k, "c": v} for k, v in
            sorted(purposes.items(), key=lambda x: -x[1])], "exceeding": exceeding, "max_mins": max_mins}

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
    return 200, {"token": tok, "role": u["role"], "name": u["name"], "client_id": u["client_id"]}

@route("POST", r"/api/logout")
def logout(ctx):
    return 200, {"ok": True}

# ---- dashboards (admin sees own client; super passes ?client_id=)
@route("GET", r"/api/stats/overview")
def api_overview(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_overview(ctx.conn, cid)

@route("GET", r"/api/stats/analysis")
def api_analysis(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    return 200, stats_analysis(ctx.conn, cid)

@route("GET", r"/api/stats/live")
def api_live(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT max_visit_mins FROM clients WHERE id=?", (cid,)).fetchone()
    return 200, stats_live(ctx.conn, cid, c["max_visit_mins"] if c else 60)

# ---- client admin: connection page
@route("GET", r"/api/connection")
def api_connection(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    log = ctx.conn.execute("""SELECT ts,source,rows_in,inserted,updated,errors FROM ingest_log
                              WHERE client_id=? ORDER BY id DESC LIMIT 15""", (cid,)).fetchall()
    return 200, {"name": c["name"], "api_key": c["api_key"], "max_visit_mins": c["max_visit_mins"],
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
    m = int(ctx.body.get("max_visit_mins", 60))
    if m < 5 or m > 720: return 400, {"error": "Allowed duration must be 5–720 minutes"}
    ctx.conn.execute("UPDATE clients SET max_visit_mins=? WHERE id=?", (m, cid))
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
        if path == "/": path = "/index.html"
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
                        src = "VMS push (JSON)"
                    else:
                        rows = list(csv.DictReader(io.StringIO(text)))
                        src = "VMS push (CSV)"
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
                            code, payload = fn(ctx, *mt.groups())
                            return self._send(code, payload)
                    return self._send(404, {"error": "Unknown API route"})

                # ---- static
                if method == "GET": return self._serve_static(u.path)
                return self._send(404, {"error": "Not found"})
            finally:
                conn.close()

    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")

def main():
    conn = connect()
    conn.executescript(SCHEMA)
    seed(conn)
    conn.close()
    print(f"StatsPack Analytics Lab  ->  http://localhost:{PORT}")
    print("  Super user : admin@statspack.co.ls / super123")
    print("  Client demo: admin@demo.client / admin123")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
