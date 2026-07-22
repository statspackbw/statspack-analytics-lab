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

def _flt_where(cid, flt):
    flt = flt or {}
    where, P = ["client_id=?"], [cid]
    if flt.get("date_from"): where.append("substr(check_in,1,10)>=?"); P.append(flt["date_from"])
    if flt.get("date_to"):   where.append("substr(check_in,1,10)<=?"); P.append(flt["date_to"])
    if flt.get("region"):    where.append("region=?"); P.append(flt["region"])
    if flt.get("department"):where.append("host_department=?"); P.append(flt["department"])
    return " AND ".join(where), P

def _flt_from_qs(qs):
    g = lambda k: (qs.get(k, [""])[0] or "").strip()
    return {"date_from": g("date_from"), "date_to": g("date_to"),
            "region": g("region"), "department": g("department")}

def stats_overview(conn, cid, flt=None):
    flt = flt or {}
    where, P = ["client_id=?"], [cid]
    if flt.get("date_from"): where.append("substr(check_in,1,10)>=?"); P.append(flt["date_from"])
    if flt.get("date_to"):   where.append("substr(check_in,1,10)<=?"); P.append(flt["date_to"])
    if flt.get("region"):    where.append("region=?"); P.append(flt["region"])
    if flt.get("department"):where.append("host_department=?"); P.append(flt["department"])
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
            "top_departments": top("host_department"),
            "filter_regions": opts("region"), "filter_departments": opts("host_department")}

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
    opts = lambda col: [r[0] for r in conn.execute(
        f"SELECT DISTINCT {col} FROM visits WHERE client_id=? AND {col}!='' ORDER BY {col}", (cid,))]
    return {"regions": out, "filter_regions": opts("region"), "filter_departments": opts("host_department")}

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

    p.heading(f"Current On-Premises Visitors ({len(lv['ongoing'])})")
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
INDEX_HTML = '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>StatsPack Analytics Lab</title>\n<style>\n:root{\n  --slate:#3d5666; --slate-deep:#35505f; --ink:#25333d; --muted:#7d8fa0;\n  --mint:#cfe8e6; --mint-soft:#e7f4f2; --teal:#5cbcb6; --amber:#e0a83f; --coral:#dd6b4d;\n  --bg:#eef1f4; --card:#ffffff; --line-blue:#7b93f0; --ok:#2f9e77; --bad:#c65644;\n}\n*{box-sizing:border-box;margin:0}\nbody{background:var(--bg);color:var(--ink);\n  font:15px/1.5 "Trebuchet MS","Lato","Segoe UI",system-ui,sans-serif}\nbutton{font:inherit;cursor:pointer}\ninput,select{font:inherit;padding:9px 12px;border:1.5px solid #c4d0d8;border-radius:9px;background:#fff;width:100%;color:var(--ink)}\ninput:focus,select:focus,button:focus-visible{outline:2px solid var(--teal);outline-offset:1px}\na{color:var(--slate)}\n@media (prefers-reduced-motion: reduce){*{transition:none!important;animation:none!important}}\n\n/* ============ app shell ============ */\n.shell{display:grid;grid-template-columns:290px 1fr;min-height:100vh}\naside{background:#fff;box-shadow:2px 0 8px rgba(37,51,61,.06);display:flex;flex-direction:column;\n  padding:14px 14px 18px;position:sticky;top:0;height:100vh;overflow-y:auto}\n.profile{background:var(--mint);border-radius:16px;padding:22px 14px 18px;text-align:center}\n.profile img{height:56px;display:block;margin:0 auto 4px}\n.profile .wm{font-size:13px;font-weight:800;letter-spacing:4px;color:var(--slate)}\n.profile .wm span{color:#8fa8b2}\n.profile .nm{font-size:19px;font-weight:800;color:var(--slate);margin-top:12px}\n.profile .org{font-size:14px;font-weight:700;color:var(--slate);opacity:.85;margin-top:2px}\n.profile .em{font-size:12.5px;color:#5a7481;margin-top:2px;word-break:break-all}\n.navsec{font-size:12px;font-weight:800;letter-spacing:2.5px;color:#9aabb8;margin:22px 10px 8px;\n  display:flex;justify-content:space-between;align-items:center}\n.navsec::after{content:"▾";font-size:9px;color:#c1ccd5}\n.nitem{display:flex;align-items:center;gap:14px;width:calc(100% + 28px);margin-left:-14px;\n  padding:13px 24px;border:0;background:none;color:var(--ink);font-size:16.5px;font-weight:700;text-align:left;\n  border-radius:0}\n.nitem svg{width:22px;height:22px;flex:0 0 22px;fill:none;stroke:var(--amber);stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}\n.nitem:hover{background:var(--mint-soft)}\n.nitem.on{background:var(--slate);color:#fff}\n.nitem.on svg{stroke:#fff}\n.sideout{margin-top:auto;padding-top:18px}\n.sideout .btn{width:100%;border-radius:12px;padding:13px;font-size:16px}\n.vtag{text-align:center;font-size:13px;color:#8aa0ad;margin-top:12px}\n.clientbox{margin:14px 4px 0}\n.clientbox label{font-size:11.5px;font-weight:800;letter-spacing:1.5px;color:#9aabb8;display:block;margin:0 6px 4px}\n\n/* top bar */\n.topbar{background:var(--slate);color:#fff;display:flex;align-items:center;justify-content:center;\n  gap:12px;padding:20px 16px;position:relative}\n.topbar h1{font-size:23px;font-weight:800;display:flex;align-items:center;gap:12px}\n.topbar h1 svg{width:24px;height:24px;fill:none;stroke:#fff;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}\n.burger{display:none;position:absolute;left:14px;top:50%;transform:translateY(-50%);\n  background:none;border:0;color:#fff;font-size:24px;padding:6px}\nmain{padding:26px 28px 40px;max-width:1500px;width:100%}\n\n/* KPI cards — colored label + underline + big number */\n.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:20px}\n.kpi{background:var(--card);border-radius:16px;box-shadow:0 4px 14px rgba(37,51,61,.07);padding:20px 24px}\n.kpi .lbl{font-size:17px;font-weight:800}\n.kpi .rule{height:3px;border-radius:2px;margin:12px 0 18px}\n.kpi .val{font-size:36px;font-weight:800;line-height:1.1}\n.kpi .sub{font-size:12.5px;color:var(--muted);margin-top:4px}\n.k-slate .lbl{color:var(--slate)} .k-slate .rule{background:var(--slate)}\n.k-teal  .lbl{color:var(--teal)}  .k-teal  .rule{background:var(--teal)}\n.k-amber .lbl{color:var(--amber)} .k-amber .rule{background:var(--amber)}\n.k-coral .lbl{color:var(--coral)} .k-coral .rule{background:var(--coral)}\n.val.up{color:var(--ok)} .val.down{color:var(--bad)}\n\n/* content cards + glance tables */\n.panel{background:var(--card);border-radius:18px;box-shadow:0 4px 14px rgba(37,51,61,.07);\n  padding:22px 26px;margin-top:24px;min-width:0}\n.panel>h2{font-size:21px;font-weight:800;color:var(--slate);margin-bottom:14px}\n.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:24px;margin-top:24px}\n.grid2 .panel{margin-top:0}\ntable{width:100%;border-collapse:collapse}\nth{text-align:left;padding:10px 12px;font-size:14px;letter-spacing:1px;color:var(--slate);\n   border-bottom:2px solid #e3e9ee;font-weight:800}\ntd{padding:14px 12px;border-bottom:1.5px solid #eef2f5;font-size:15px}\ntd.b{font-weight:800}\ntr:last-child td{border-bottom:0}\n.chip{display:inline-block;background:#e2eaf0;color:var(--slate);border-radius:999px;\n  padding:3px 14px;font-size:13px;font-weight:700;margin-left:8px}\n.score{display:inline-block;background:#d9efe7;color:var(--ok);border-radius:8px;padding:3px 12px;font-weight:800;font-size:14px}\n.score.bad{background:#f7e3dd;color:var(--bad)}\n.empty{color:var(--muted);text-align:center;padding:24px 8px}\n\n.btn{background:var(--slate);color:#fff;border:0;border-radius:10px;padding:10px 18px;font-weight:800}\n.btn.ghost{background:#fff;color:var(--slate);border:1.5px solid var(--slate)}\n.btn.small{padding:6px 12px;font-size:13px;border-radius:8px}\n.btn.warn{background:var(--coral)}\n.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}\n.note{font-size:13px;color:var(--muted)}\ncode,pre{font:12.5px/1.55 ui-monospace,Consolas,monospace;background:#2c3f4b;color:#dbe8f0;border-radius:10px}\npre{padding:13px 15px;overflow:auto}\ncode.inline{background:#e8eef2;color:var(--slate);padding:2px 7px;border-radius:5px}\n.msg{border-radius:10px;padding:10px 13px;font-size:14px;margin:8px 0;display:none}\n.msg.err{display:block;background:#f7e3dd;color:#8a3a28}\n.msg.ok{display:block;background:#d9efe7;color:#1d6c4f}\n.refresh{font-size:13px;color:var(--muted);margin-top:8px}\nsvg text{font-family:"Trebuchet MS","Lato",sans-serif}\n\n/* ============ analytics loader ============ */\n.loader{position:fixed;inset:0;background:rgba(238,241,244,.94);display:grid;place-items:center;z-index:60}\n.loader .box{text-align:center}\n.bars{display:flex;gap:9px;align-items:flex-end;height:74px;justify-content:center}\n.bars i{width:15px;border-radius:5px 5px 2px 2px;animation:grow 1.05s ease-in-out infinite}\n.bars i:nth-child(1){background:var(--slate);animation-delay:0s}\n.bars i:nth-child(2){background:var(--teal);animation-delay:.14s}\n.bars i:nth-child(3){background:var(--amber);animation-delay:.28s}\n.bars i:nth-child(4){background:var(--coral);animation-delay:.42s}\n.bars i:nth-child(5){background:var(--slate);animation-delay:.56s}\n@keyframes grow{0%,100%{height:18%}50%{height:100%}}\n.loader p{margin-top:18px;font-weight:800;color:var(--slate);letter-spacing:1px}\n\n/* ============ login (unchanged look) ============ */\n.login-wrap{min-height:100vh;display:grid;place-items:center;padding:24px;position:relative;\n  background:#dfe4e8 url(\'/login.png\') center/cover no-repeat}\n.login-wrap::before{content:"";position:absolute;inset:0;background:rgba(244,246,248,.72)}\n.login{position:relative;width:100%;max-width:620px;text-align:center;\n  font-family:"Trebuchet MS","Lato","Segoe UI",sans-serif}\n.login .logo{height:222px;max-width:90%;object-fit:contain;margin:0 auto 6px;display:block}\n.login h1{font-size:30px;font-weight:800;letter-spacing:10px;color:#3d5666;margin:26px 0 30px}\n.login h1 span{color:#5cbcb6}\n.login .field{position:relative;margin:0 0 26px}\n.login .field svg{position:absolute;left:24px;top:50%;transform:translateY(-50%);width:22px;height:22px;\n  stroke:#3d5666;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}\n.login input{width:100%;padding:19px 58px;border:2px solid #35505f;border-radius:999px;\n  background:rgba(255,255,255,.55);font:inherit;font-size:17px;color:#25333d}\n.login input::placeholder{color:#4d6674}\n.login .eye{position:absolute;right:20px;top:50%;transform:translateY(-50%);background:none;border:0;padding:4px;line-height:0}\n.login .eye svg{position:static;transform:none}\n.login .btn{width:100%;padding:18px;border-radius:999px;background:#35505f;font-size:18px;letter-spacing:.5px}\n.login .demo{margin-top:30px;text-align:left;background:rgba(255,255,255,.62);backdrop-filter:blur(3px);\n  border-radius:14px;padding:16px 20px;font-size:14px;color:#25333d;line-height:2}\n.login .demo code{background:rgba(255,255,255,.85);color:#25333d;padding:2px 8px;border-radius:5px;\n  font:13px ui-monospace,Consolas,monospace}\n.login .msg{text-align:left}\n\n/* mobile */\n@media (max-width: 920px){\n  .shell{grid-template-columns:1fr}\n  aside{position:fixed;left:0;top:0;bottom:0;width:290px;z-index:50;transform:translateX(-102%);\n    transition:transform .25s ease;box-shadow:6px 0 24px rgba(0,0,0,.18)}\n  aside.open{transform:none}\n  .burger{display:block}\n  main{padding:18px 14px 30px}\n}\n</style>\n</head>\n<body>\n<div id="app"></div>\n<script>\n"use strict";\nconst $ = (s, el=document) => el.querySelector(s);\nconst S = { token: sessionStorage.getItem("tok") || "", role: sessionStorage.getItem("role") || "",\n            name: sessionStorage.getItem("name") || "", email: sessionStorage.getItem("email") || "",\n            tab: "", clientId: null, clients: [], timer: null,\n            pct: sessionStorage.getItem("pct") === "1",\n            f: { date_from:"", date_to:"", region:"", department:"" } };\n\nasync function api(path, body, method){\n  const r = await fetch(path, { method: method || (body ? "POST" : "GET"),\n    headers: { "Content-Type": "application/json", ...(S.token ? { Authorization: "Bearer " + S.token } : {}) },\n    body: body ? JSON.stringify(body) : undefined });\n  const d = await r.json().catch(() => ({}));\n  if (!r.ok) throw new Error(d.error || ("Request failed (" + r.status + ")"));\n  return d;\n}\nconst esc = s => String(s ?? "").replace(/[&<>"\']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",\'"\':"&quot;","\'":"&#39;"}[c]));\nconst fmtDT = s => s ? s.replace("T", " ").slice(0, 16) : "—";\n\n/* ---------------- analytics loader ---------------- */\nfunction showLoader(msg){\n  hideLoader();\n  const d = document.createElement("div");\n  d.className = "loader"; d.id = "loader";\n  d.innerHTML = `<div class="box"><div class="bars"><i></i><i></i><i></i><i></i><i></i></div>\n    <p>${esc(msg || "Crunching your numbers…")}</p></div>`;\n  document.body.appendChild(d);\n}\nfunction hideLoader(){ const l = $("#loader"); if (l) l.remove(); }\n\n/* ---------------- icons ---------------- */\nconst IC = {\n  grid:\'<svg viewBox="0 0 24 24"><rect x="3.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.5"/></svg>\',\n  eye:\'<svg viewBox="0 0 24 24"><path d="M1.5 12s4-7 10.5-7 10.5 7 10.5 7-4 7-10.5 7S1.5 12 1.5 12z"/><circle cx="12" cy="12" r="3"/></svg>\',\n  chart:\'<svg viewBox="0 0 24 24"><path d="M4 20V10M10 20V4M16 20v-8M21 20H3"/></svg>\',\n  pulse:\'<svg viewBox="0 0 24 24"><path d="M2.5 12h4l2.5-7 4.5 14 2.5-7h5.5"/></svg>\',\n  plug:\'<svg viewBox="0 0 24 24"><path d="M9 3v5M15 3v5M7 8h10v3a5 5 0 0 1-5 5 5 5 0 0 1-5-5V8zM12 16v5"/></svg>\',\n  pin:\'<svg viewBox="0 0 24 24"><path d="M12 21s-7-6.2-7-11a7 7 0 0 1 14 0c0 4.8-7 11-7 11z"/><circle cx="12" cy="10" r="2.6"/></svg>\',\n  doc:\'<svg viewBox="0 0 24 24"><path d="M6 2.5h8l4 4V21.5H6zM14 2.5v4h4M9 12h6M9 16h6M9 8h2"/></svg>\',\n  home:\'<svg viewBox="0 0 24 24"><path d="M4 11l8-7 8 7M6 9.5V20h12V9.5M10 20v-5h4v5"/></svg>\'\n};\nconst TITLES = { clients:["Overview",IC.grid], overview:["Bird\'s Eye View",IC.eye],\n  analysis:["In-Depth Analysis",IC.chart], live:["Live from the Premises",IC.pulse],\n  maps:["Maps",IC.pin], reports:["Reports",IC.doc], connection:["Data & Connection",IC.plug] };\n\n/* ---------------- % / count toggle ---------------- */\nfunction pctToggle(){\n  return `<div class="row" style="justify-content:flex-end;margin-bottom:14px">\n    <span class="note" style="font-weight:700">Chart values:</span>\n    <button class="btn small ${S.pct?"ghost":""}" id="modeCount">Count</button>\n    <button class="btn small ${S.pct?"":"ghost"}" id="modePct">%</button></div>`;\n}\nfunction wireToggle(){\n  const c=$("#modeCount"), p=$("#modePct");\n  if(c) c.onclick=()=>{ S.pct=false; sessionStorage.setItem("pct","0"); draw(); };\n  if(p) p.onclick=()=>{ S.pct=true; sessionStorage.setItem("pct","1"); draw(); };\n}\n/* count-charts (unit === "") honor S.pct; duration charts keep their units */\nconst shareLab=(v,total,unit)=> (S.pct && unit==="" && total>0) ? Math.round(v/total*100)+"%" : v+unit;\n\n/* ---------------- tiny SVG chart kit ---------------- */\nfunction lineChart(series, {w=560, h=230, unit=""}={}){\n  if (!series.length) return \'<div class="empty">No data yet</div>\';\n  const pad = {l:44, r:18, t:22, b:44};\n  const max = Math.max(...series.map(p=>p.v), 1);\n  const X = i => pad.l + i * (w - pad.l - pad.r) / Math.max(series.length - 1, 1);\n  const Y = v => pad.t + (1 - v / max) * (h - pad.t - pad.b);\n  let path = "";\n  series.forEach((p,i)=>{\n    const x=X(i), y=Y(p.v);\n    if(!i){ path=`M${x},${y}`; return; }\n    const px=X(i-1), py=Y(series[i-1].v), cx=(px+x)/2;\n    path += ` C${cx},${py} ${cx},${y} ${x},${y}`;\n  });\n  const tot = series.reduce((a,b)=>a+b.v,0);\n  const dots = series.map((p,i)=>`<circle cx="${X(i)}" cy="${Y(p.v)}" r="4.5" fill="#fff" stroke="#5cbcb6" stroke-width="2.2"/>\n    <text x="${X(i)}" y="${Y(p.v)-10}" font-size="11" text-anchor="middle" fill="#3d5666" font-weight="700">${esc(shareLab(p.v,tot,unit))}</text>`).join("");\n  const labels = series.map((p,i)=>`<text x="${X(i)}" y="${h-16}" font-size="11" text-anchor="middle" fill="#7d8fa0">${esc(p.l)}</text>`).join("");\n  const gy = [0,.5,1].map(f=>{const y=Y(max*f);return `<line x1="${pad.l}" y1="${y}" x2="${w-pad.r}" y2="${y}" stroke="#e3e9ee" stroke-dasharray="3 4"/>\n    <text x="${pad.l-8}" y="${y+4}" font-size="10.5" text-anchor="end" fill="#9aabb8">${Math.round(max*f)}</text>`}).join("");\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">${gy}\n    <path d="${path}" fill="none" stroke="#5cbcb6" stroke-width="2.6"/>${dots}${labels}</svg>`;\n}\nconst PAL = ["#3d5666","#5cbcb6","#e0a83f","#dd6b4d","#8fa8b2","#7b93f0","#b087b4","#54b98d"];\nfunction hbar(items, {w=560, colors=null, unit=""}={}){\n  if (!items.length) return \'<div class="empty">No data yet</div>\';\n  const total = items.reduce((a,b)=>a+b.c,0) || 1;\n  const rowH=38, pad={l:158,r:28,t:6}, h=pad.t+items.length*rowH+8;\n  const max=Math.max(...items.map(i=>i.c),1);\n  const cols = colors || PAL;\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">` + items.map((it,i)=>{\n    const y=pad.t+i*rowH, bw=Math.max((w-pad.l-pad.r)*it.c/max, 28);\n    const lab = shareLab(it.c,total,unit);\n    return `<text x="${pad.l-8}" y="${y+22}" font-size="12.5" text-anchor="end" fill="#25333d" font-weight="700">${esc(String(it.label).slice(0,20))}</text>\n      <rect x="${pad.l}" y="${y+5}" width="${bw}" height="${rowH-13}" rx="5" fill="${cols[i%cols.length]}"/>\n      <text x="${pad.l+bw/2}" y="${y+21}" font-size="11.5" text-anchor="middle" fill="#fff" font-weight="800">${lab}</text>`;\n  }).join("") + "</svg>";\n}\nfunction vbar(items, {w=560,h=250,unit=""}={}){\n  if (!items.length) return \'<div class="empty">No data yet</div>\';\n  const pad={l:46,r:14,t:26,b:66}; const max=Math.max(...items.map(i=>i.c),1);\n  const bw=(w-pad.l-pad.r)/items.length; const tot=items.reduce((a,b)=>a+b.c,0);\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">` + items.map((it,i)=>{\n    const bh=(h-pad.t-pad.b)*it.c/max, x=pad.l+i*bw+bw*0.14, y=h-pad.b-bh;\n    return `<rect x="${x}" y="${y}" width="${bw*0.72}" height="${bh}" rx="5" fill="${PAL[i%PAL.length]}"/>\n      <text x="${x+bw*0.36}" y="${y+18>h-pad.b?y-6:y+20}" font-size="11" text-anchor="middle" fill="${y+18>h-pad.b?\'#25333d\':\'#fff\'}" font-weight="800">${shareLab(it.c,tot,unit)}</text>\n      <text x="${x+bw*0.36}" y="${h-pad.b+14}" font-size="10.5" text-anchor="end" fill="#7d8fa0" transform="rotate(-32 ${x+bw*0.36} ${h-pad.b+14})">${esc(String(it.label).slice(0,16))}</text>`;\n  }).join("") + "</svg>";\n}\nfunction donut(items, {w=560,h=250}={}){\n  if (!items.length) return \'<div class="empty">Nobody is on the premises right now</div>\';\n  const total=items.reduce((a,b)=>a+b.c,0), cx=w/2, cy=h/2, R=Math.min(w,h)/2-26, r=R*0.58;\n  let a0=-Math.PI/2, out="";\n  items.forEach((it,i)=>{\n    const a1=a0+2*Math.PI*it.c/total, big=(a1-a0)>Math.PI?1:0;\n    const p=(a,rr)=>[cx+rr*Math.cos(a),cy+rr*Math.sin(a)];\n    const [x0,y0]=p(a0,R),[x1,y1]=p(a1,R),[x2,y2]=p(a1,r),[x3,y3]=p(a0,r);\n    out+=`<path d="M${x0},${y0} A${R},${R} 0 ${big} 1 ${x1},${y1} L${x2},${y2} A${r},${r} 0 ${big} 0 ${x3},${y3} Z" fill="${PAL[i%PAL.length]}"/>`;\n    const mid=(a0+a1)/2,[lx,ly]=p(mid,R+14);\n    out+=`<text x="${lx}" y="${ly}" font-size="11" text-anchor="${lx>cx?"start":"end"}" fill="#25333d">${esc(it.label)} (${S.pct?Math.round(it.c/total*100)+"%":it.c})</text>`;\n    a0=a1;\n  });\n  return `<svg viewBox="0 0 ${w} ${h}" role="img" style="width:100%;height:auto">${out}\n    <text x="${cx}" y="${cy+6}" font-size="17" font-weight="800" text-anchor="middle" fill="#25333d">${total}</text></svg>`;\n}\n\n/* ---------------- login ---------------- */\nfunction renderLogin(err){\n  const eyeOpen=\'<svg viewBox="0 0 24 24"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"/><circle cx="12" cy="12" r="3"/></svg>\';\n  const eyeOff=\'<svg viewBox="0 0 24 24"><path d="M17.94 17.94A10.9 10.9 0 0 1 12 19c-7 0-11-7-11-7a20.7 20.7 0 0 1 5.06-5.94M9.9 4.24A10.4 10.4 0 0 1 12 4c7 0 11 7 11 7a20.8 20.8 0 0 1-3.22 4.19M1 1l22 22"/></svg>\';\n  document.body.innerHTML = `<div class="login-wrap"><form class="login" id="loginForm">\n    <img class="logo" src="/logo.png" alt="StatsPack" onerror="this.style.display=\'none\'">\n    <h1>ANALYTICS <span>LAB</span></h1>\n    ${err?`<div class="msg err">${esc(err)}</div>`:""}\n    <div class="field">\n      <svg viewBox="0 0 24 24"><rect x="2.5" y="5" width="19" height="14" rx="2.5"/><path d="M3 6.5l9 7 9-7"/></svg>\n      <input id="em" type="email" placeholder="Email" autocomplete="username" aria-label="Email" required>\n    </div>\n    <div class="field">\n      <svg viewBox="0 0 24 24"><rect x="4.5" y="10.5" width="15" height="10" rx="2.5"/><path d="M8 10.5V7.5a4 4 0 0 1 8 0v3"/></svg>\n      <input id="pw" type="password" placeholder="Password" autocomplete="current-password" aria-label="Password" required>\n      <button class="eye" type="button" id="eyeBtn" aria-label="Show password">${eyeOpen}</button>\n    </div>\n    <button class="btn" type="submit">Sign In</button>\n    <p style="position:relative;margin-top:14px;font-size:12px;color:#7d8fa0">Analytics Lab · v6</p>\n    <div class="demo"><b>Demo accounts</b> (password after the slash)<br>\n      Super user — <code>admin@statspack.co.ls / super123</code><br>\n      Client admin — <code>admin@demo.client / admin123</code></div>\n  </form></div>`;\n  $("#eyeBtn").onclick = ()=>{ const p=$("#pw"), show=p.type==="password";\n    p.type=show?"text":"password"; $("#eyeBtn").innerHTML=show?eyeOff:eyeOpen;\n    $("#eyeBtn").setAttribute("aria-label", show?"Hide password":"Show password"); };\n  $("#loginForm").addEventListener("submit", async e=>{\n    e.preventDefault();\n    showLoader("Signing you in…");\n    try{\n      const d = await api("/api/login", { email:$("#em").value, password:$("#pw").value });\n      S.token=d.token; S.role=d.role; S.name=d.name; S.email=d.email||"";\n      sessionStorage.setItem("tok",d.token); sessionStorage.setItem("role",d.role);\n      sessionStorage.setItem("name",d.name); sessionStorage.setItem("email",d.email||"");\n      S.tab = d.role==="super" ? "clients" : "overview";\n      await boot();\n    }catch(ex){ hideLoader(); renderLogin(ex.message); }\n  });\n}\n\n/* ---------------- shell ---------------- */\nfunction navBtn(k){ const [label, icon] = TITLES[k];\n  return `<button class="nitem ${S.tab===k?"on":""}" data-t="${k}">${icon}<span>${label}</span></button>`; }\n\nfunction shell(){\n  const isSuper = S.role==="super";\n  const org = isSuper ? "StatsPack · HQ"\n    : esc((S.clients.find(c=>c.id==S.clientId)||{}).name || "Client");\n  const picker = isSuper && S.clients.length ? `<div class="clientbox"><label for="clientPick">VIEWING CLIENT</label>\n      <select id="clientPick">${S.clients.map(c=>`<option value="${c.id}" ${c.id==S.clientId?"selected":""}>${esc(c.name)}${c.active?"":" (suspended)"}</option>`).join("")}</select></div>` : "";\n  const [title, icon] = TITLES[S.tab] || ["",""];\n  document.body.innerHTML = `<div class="shell">\n   <aside id="sidebar">\n    <div class="profile">\n      <img src="/logo.png" alt="" onerror="this.style.display=\'none\'">\n      <div class="wm">STATS<span>PACK</span></div>\n      <div class="nm">${esc(S.name)}</div>\n      <div class="org">${org}</div>\n      <div class="em">${esc(S.email || (isSuper?"StatsPack super user":"Client admin"))}</div>\n    </div>\n    ${picker}\n    ${isSuper?`<div class="navsec">PLATFORM</div>${navBtn("clients")}`:""}\n    <div class="navsec">DASHBOARDS</div>\n    ${navBtn("overview")}${navBtn("analysis")}${navBtn("live")}${navBtn("maps")}\n    <div class="navsec">REPORTS</div>\n    ${navBtn("reports")}\n    <div class="navsec">DATA</div>\n    ${navBtn("connection")}\n    <div class="sideout"><button class="btn" id="out">Sign out</button>\n      <div class="vtag">Analytics Lab · v6</div></div>\n   </aside>\n   <div>\n    <div class="topbar">\n      <button class="burger" id="burger" aria-label="Menu">☰</button>\n      <h1>${icon}<span>${esc(title)}</span></h1>\n    </div>\n    <main id="view"></main>\n   </div>\n  </div>`;\n  $("#out").onclick = ()=>{ sessionStorage.clear(); location.reload(); };\n  $("#burger").onclick = ()=> $("#sidebar").classList.toggle("open");\n  document.querySelectorAll(".nitem").forEach(b=>b.onclick=()=>{ S.tab=b.dataset.t; $("#sidebar").classList.remove("open"); draw(); });\n  const cp=$("#clientPick"); if(cp) cp.onchange=()=>{ S.clientId=+cp.value; draw(); };\n}\nconst q = () => S.role==="super" ? "?client_id="+S.clientId : "";\nfunction stopTimer(){ if(S.timer){ clearInterval(S.timer); S.timer=null; } }\n\nasync function draw(quiet){\n  stopTimer(); shell();\n  if(!quiet) showLoader();\n  const v=$("#view");\n  try{\n    if (S.tab==="overview") await drawOverview(v);\n    else if (S.tab==="analysis") await drawAnalysis(v);\n    else if (S.tab==="live") await drawLive(v);\n    else if (S.tab==="maps") await drawMaps(v);\n    else if (S.tab==="reports") await drawReports(v);\n    else if (S.tab==="connection") await drawConnection(v);\n    else if (S.tab==="clients") await drawClients(v);\n  }catch(ex){\n    if (/log in/i.test(ex.message)) { hideLoader(); sessionStorage.clear(); return renderLogin("Session expired — sign in again"); }\n    v.innerHTML = `<div class="msg err" style="display:block">${esc(ex.message)}</div>`;\n  }\n  hideLoader();\n}\n\n/* ---------------- Bird\'s Eye View ---------------- */\nfunction fq(){\n  const p = new URLSearchParams();\n  if (S.role==="super") p.set("client_id", S.clientId);\n  for (const k of ["date_from","date_to","region","department"]) if (S.f[k]) p.set(k, S.f[k]);\n  const qs = p.toString(); return qs ? "?"+qs : "";\n}\nfunction filterBar(regions, depts, withToggle){\n  const sel=(id,label,opts,cur)=>`<div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">${label}</label>\n     <select id="${id}" style="min-width:150px"><option value="">All</option>\n     ${opts.map(o=>`<option ${o===cur?"selected":""}>${esc(o)}</option>`).join("")}</select></div>`;\n  return `<div class="panel" style="margin:0 0 20px">\n     <div class="row" style="gap:16px;align-items:flex-end">\n       <div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">START DATE</label>\n         <input type="date" id="fFrom" value="${esc(S.f.date_from)}" style="width:160px"></div>\n       <div><label style="font-size:11.5px;font-weight:800;letter-spacing:1px;color:#9aabb8">END DATE</label>\n         <input type="date" id="fTo" value="${esc(S.f.date_to)}" style="width:160px"></div>\n       ${sel("fRegion","REGION",regions,S.f.region)}\n       ${sel("fDept","DEPARTMENT",depts,S.f.department)}\n       <button class="btn" id="fApply">Apply</button>\n       <button class="btn ghost" id="fReset">Reset</button>\n       <div style="flex:1"></div>\n       ${withToggle?`<div class="row"><span class="note" style="font-weight:700">Chart values:</span>\n        <button class="btn small ${S.pct?"ghost":""}" id="modeCount">Count</button>\n        <button class="btn small ${S.pct?"":"ghost"}" id="modePct">%</button></div>`:""}\n     </div></div>`;\n}\nfunction wireFilters(){\n  $("#fApply").onclick = ()=>{ S.f={ date_from:$("#fFrom").value, date_to:$("#fTo").value,\n      region:$("#fRegion").value, department:$("#fDept").value }; draw(); };\n  $("#fReset").onclick = ()=>{ S.f={date_from:"",date_to:"",region:"",department:""}; draw(); };\n  wireToggle();\n}\nasync function drawOverview(v){\n  const d = await api("/api/stats/overview"+fq());\n  const ch = d.change_pct;\n  const cmp = ch===null ? `<div class="kpi k-slate"><div class="lbl">vs Last Month</div><div class="rule"></div>\n      <div class="val">—</div><div class="sub">No visits last month</div></div>`\n    : `<div class="kpi k-slate"><div class="lbl">vs Last Month</div><div class="rule"></div>\n       <div class="val ${ch<0?"down":"up"}">${ch<0?"↓":"↑"} ${Math.abs(ch)}%</div>\n       <div class="sub">${esc(d.this_month.label)}: ${d.this_month.count} · ${esc(d.last_month.label)}: ${d.last_month.count}</div></div>`;\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, true) + `\n   <div class="kpis">\n     <div class="kpi k-slate"><div class="lbl">Total Visitors</div><div class="rule"></div><div class="val">${d.total}</div></div>\n     <div class="kpi k-teal"><div class="lbl">Unique Visitors</div><div class="rule"></div><div class="val">${d.unique}</div></div>\n     <div class="kpi k-amber"><div class="lbl">Visitors Today</div><div class="rule"></div><div class="val">${d.today}</div></div>\n     <div class="kpi k-coral"><div class="lbl">Ongoing Visits</div><div class="rule"></div><div class="val">${d.ongoing}</div></div>\n     ${cmp}\n   </div>\n   <div class="kpis" style="margin-top:20px">\n     <div class="kpi k-teal"><div class="lbl">Avg Visits / Day</div><div class="rule"></div><div class="val">${d.avg_per_day}</div></div>\n     <div class="kpi k-slate"><div class="lbl">Avg Visit Duration</div><div class="rule"></div><div class="val">${d.avg_duration}<span style="font-size:19px"> mins</span></div></div>\n     <div class="kpi k-coral"><div class="lbl">Highest Visits</div><div class="rule"></div>\n       <div class="val">${d.highest?d.highest.count:"—"}</div><div class="sub">${d.highest?"On "+esc(d.highest.date):""}</div></div>\n     <div class="kpi k-amber"><div class="lbl">Lowest Visits</div><div class="rule"></div>\n       <div class="val">${d.lowest?d.lowest.count:"—"}</div><div class="sub">${d.lowest?"On "+esc(d.lowest.date):""}</div></div>\n   </div>\n   <div class="grid2">\n     <div class="panel"><h2>Monthly Footfall: Overall</h2>${lineChart(d.monthly.map(m=>({l:m.month,v:m.count})))}</div>\n     <div class="panel"><h2>Current Month\'s Footfall</h2>${lineChart(d.current_month_daily.map(x=>({l:x.d.slice(8)+" "+d.this_month.label.slice(0,3),v:x.c})))}</div>\n     <div class="panel"><h2>Top 5 Regions Visited</h2>${hbar(d.top_regions.map(r=>({label:r.label,c:r.c})))}</div>\n     <div class="panel"><h2>Top 5 Purposes of Visit</h2>${hbar(d.top_purposes.map(r=>({label:r.label,c:r.c})))}</div>\n     <div class="panel"><h2>Top 5 Departments by Visits</h2>${hbar(d.top_departments.map(r=>({label:r.label,c:r.c})))}</div>\n   </div>`;\n  wireFilters();\n}\n\n/* ---------------- In-Depth ---------------- */\nasync function drawAnalysis(v){\n  const d = await api("/api/stats/analysis"+q());\n  v.innerHTML = pctToggle() + `\n   <div class="grid2" style="margin-top:0">\n     <div class="panel"><h2>Regions and Visitors</h2>${hbar(d.region_visitors)}</div>\n     <div class="panel"><h2>Regions with high Visit Duration</h2>${hbar(d.region_duration,{unit:" mins"})}</div>\n     <div class="panel"><h2>Top 5 Departments by Visitor Footfall</h2>${hbar(d.dept_footfall)}</div>\n     <div class="panel"><h2>Departments taking the most time</h2>\n       ${lineChart(d.dept_duration.map(x=>({l:String(x.label).slice(0,12),v:x.c})),{unit:" m"})}</div>\n     <div class="panel"><h2>Visits taking the most time</h2>${vbar(d.purpose_duration,{unit:" mins"})}</div>\n     <div class="panel"><h2>Hours with highest Visitor Footfall</h2>\n       ${lineChart(d.hourly.map(x=>({l:(x.hour%12||12)+(x.hour<12?" AM":" PM"),v:x.c})))}</div>\n   </div>`;\n  wireToggle();\n}\n\n/* ---------------- Live ---------------- */\nasync function drawLive(v){\n  const render = async () => {\n    const d = await api("/api/stats/live"+q());\n    const rows = d.ongoing.map(o=>`<tr><td class="b">${esc(o.visitor_name)}</td><td>${esc(o.region)} · ${esc(o.town)}</td>\n      <td>${esc(o.host_department)}</td><td>${esc(o.purpose)}</td><td>${esc(o.id_number)}</td>\n      <td>${esc(o.contact)}</td><td><span class="score">${o.elapsed} mins</span></td></tr>`).join("");\n    const exc = d.exceeding.map(o=>`<tr><td class="b">${esc(o.visitor_name)}</td><td>${esc(o.region)} · ${esc(o.town)}</td>\n      <td>${esc(o.host_department)}</td><td><span class="chip" style="margin:0">Ongoing</span></td>\n      <td><span class="score bad">${o.elapsed} mins</span></td></tr>`).join("");\n    $("#view").innerHTML = pctToggle() + `\n     <div class="grid2" style="margin-top:0">\n       <div class="panel"><h2>Purpose for Ongoing Visits</h2>${donut(d.purposes)}</div>\n       <div class="panel"><h2>Ongoing Visits by Region</h2>\n         ${hbar(Object.entries(d.ongoing.reduce((a,o)=>{a[o.region]=(a[o.region]||0)+1;return a;},{}))\n                .map(([label,c])=>({label,c})))}</div>\n     </div>\n     <div class="panel"><h2>Current On-Premises Visitors</h2>\n       ${d.ongoing.length?`<div style="overflow-x:auto"><table><thead><tr><th>Visitor</th><th>Region · Town</th>\n        <th>Host Department</th><th>Purpose</th><th>ID Number</th><th>Contact</th><th>Elapsed</th></tr></thead>\n        <tbody>${rows}</tbody></table></div>`:\'<div class="empty">Nobody is on the premises right now</div>\'}</div>\n     <div class="panel"><h2>Visitors Exceeding Allowed Duration (${d.max_mins} mins)</h2>\n       ${d.exceeding.length?`<div style="overflow-x:auto"><table><thead><tr><th>Visitor</th><th>Region · Town</th>\n        <th>Host Department</th><th>Status</th><th>Duration</th></tr></thead>\n        <tbody>${exc}</tbody></table></div>`:\'<div class="empty">No visitor has exceeded the allowed duration</div>\'}</div>\n     <p class="refresh">Auto-refreshes every 15 seconds · Last updated ${new Date().toLocaleTimeString()}</p>`;\n    wireToggle();\n  };\n  await render();\n  S.timer = setInterval(()=>{ if(S.tab==="live") render().catch(()=>{}); }, 15000);\n}\n\n\n/* ---------------- Maps ---------------- */\nconst GAZ = {"maseru":[-29.31,27.48],"berea":[-29.15,27.74],"teyateyaneng":[-29.15,27.74],\n "leribe":[-28.87,28.05],"hlotse":[-28.87,28.05],"mafeteng":[-29.82,27.24],"mohale\'s hoek":[-30.15,27.47],\n "quthing":[-30.40,27.70],"qacha\'s nek":[-30.12,28.69],"mokhotlong":[-29.29,29.07],\n "thaba-tseka":[-29.52,28.61],"butha-buthe":[-28.77,28.25],"roma":[-29.45,27.72],\n "gaborone":[-24.65,25.91],"southern":[-25.03,25.10],"kgalagadi":[-24.70,22.00],\n "tsabong":[-26.02,22.40],"kgatleng":[-24.20,26.20],"mochudi":[-24.42,26.15],"moshupa":[-24.77,25.42],\n "francistown":[-21.17,27.51],"maun":[-19.98,23.42],"ghanzi":[-21.70,21.65],"serowe":[-22.39,26.71],\n "central":[-21.50,26.50],"north-east":[-20.90,27.30],"north east":[-20.90,27.30],\n "north-west":[-19.50,23.00],"north west":[-19.50,23.00],"kweneng":[-24.00,25.30],\n "south-east":[-24.90,25.70],"south east":[-24.90,25.70],"selebi-phikwe":[-21.98,27.85],\n "palapye":[-22.55,27.13],"lobatse":[-25.22,25.68],"jwaneng":[-24.60,24.73],"kanye":[-24.98,25.34],\n "kasane":[-17.82,25.15],"molepolole":[-24.41,25.50],"letlhakane":[-21.42,25.59],\n "johannesburg":[-26.20,28.05],"pretoria":[-25.75,28.19],"bloemfontein":[-29.12,26.21],\n "durban":[-29.86,31.02],"cape town":[-33.92,18.42],"maputo":[-25.97,32.57],\n "harare":[-17.83,31.05],"windhoek":[-22.56,17.08]};\nfunction locate(r){\n  const k1=(r.region||"").trim().toLowerCase(), k2=(r.town||"").trim().toLowerCase();\n  return GAZ[k1] || GAZ[k2] || null;\n}\nfunction ensureLeaflet(cb){\n  if (window.L) return cb(true);\n  if (!navigator.onLine) return cb(false);\n  const css=document.createElement("link"); css.rel="stylesheet";\n  css.href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"; document.head.appendChild(css);\n  const js=document.createElement("script");\n  js.src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";\n  let done=false; const fin=ok=>{ if(!done){done=true; cb(ok&&!!window.L);} };\n  js.onload=()=>fin(true); js.onerror=()=>fin(false);\n  setTimeout(()=>fin(false), 5000);\n  document.head.appendChild(js);\n}\nfunction svgBubbleMap(rows, metric, color, unit){\n  const w=560,hh=320, lo1=14, lo2=36, la1=-35, la2=-16;\n  const X=lon=>(lon-lo1)/(lo2-lo1)*w, Y=lat=>(la2-lat)/(la2-la1)*hh;\n  const pts=rows.map(r=>({r, ll:locate(r)})).filter(p=>p.ll);\n  const missing=rows.filter(r=>!locate(r));\n  const max=Math.max(...pts.map(p=>p.r[metric]),1);\n  const labels=[["NAMIBIA",17.2,-22.5],["BOTSWANA",23.6,-21.6],["SOUTH AFRICA",23.5,-31.2],\n    ["ZIMBABWE",29.6,-19.0],["LESOTHO",29.3,-30.3],["MOZAMBIQUE",33.6,-18.5],["ESWATINI",32.3,-26.4]];\n  let out=`<svg viewBox="0 0 ${w} ${hh}" role="img" style="width:100%;height:auto;background:#eaf1f4;border-radius:12px">`;\n  for(let g=1; g<6; g++){ out+=`<line x1="${g*w/6}" y1="0" x2="${g*w/6}" y2="${hh}" stroke="#dde7ec"/>\n    <line x1="0" y1="${g*hh/6}" x2="${w}" y2="${g*hh/6}" stroke="#dde7ec"/>`; }\n  labels.forEach(([t,lon,lat])=>{ out+=`<text x="${X(lon)}" y="${Y(lat)}" font-size="11" letter-spacing="2" fill="#a9bcc6" font-weight="700">${t}</text>`; });\n  const tot=rows.reduce((a,b)=>a+b[metric],0);\n  pts.sort((a,b)=>b.r[metric]-a.r[metric]).forEach(p=>{\n    const [lat,lon]=p.ll, v=p.r[metric], rad=9+22*Math.sqrt(v/max);\n    const lab=(S.pct&&unit===""&&tot)?Math.round(v/tot*100)+"%":v+unit;\n    out+=`<circle cx="${X(lon)}" cy="${Y(lat)}" r="${rad}" fill="${color}" fill-opacity=".78" stroke="#fff" stroke-width="2"/>\n      <text x="${X(lon)}" y="${Y(lat)+4}" font-size="11" text-anchor="middle" fill="#fff" font-weight="800">${lab}</text>\n      <text x="${X(lon)}" y="${Y(lat)-rad-4}" font-size="10.5" text-anchor="middle" fill="#3d5666" font-weight="700">${esc(p.r.region)}</text>`;\n  });\n  out+="</svg>";\n  if(missing.length) out+=`<p class="note" style="margin-top:8px">Not on map (unknown location): ${missing.map(r=>esc(r.region)+" ("+r[metric]+unit+")").join(", ")}</p>`;\n  return out;\n}\nfunction leafletMap(el, rows, metric, colorHex, unit){\n  const map=L.map(el,{scrollWheelZoom:false, attributionControl:true});\n  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",\n    {maxZoom:12, attribution:"&copy; OpenStreetMap"}).addTo(map);\n  const pts=rows.map(r=>({r, ll:locate(r)})).filter(p=>p.ll);\n  const max=Math.max(...pts.map(p=>p.r[metric]),1);\n  const tot=rows.reduce((a,b)=>a+b[metric],0);\n  const group=[];\n  pts.forEach(p=>{\n    const v=p.r[metric], rad=10+24*Math.sqrt(v/max);\n    const lab=(S.pct&&unit===""&&tot)?Math.round(v/tot*100)+"%":v+unit;\n    const mk=L.circleMarker(p.ll,{radius:rad,color:"#fff",weight:2,fillColor:colorHex,fillOpacity:.8})\n      .bindTooltip(`<b>${esc(p.r.region)}</b> — ${lab}`,{permanent:false}).addTo(map);\n    group.push(mk.getLatLng());\n  });\n  if(group.length) map.fitBounds(L.latLngBounds(group).pad(0.45));\n  else map.setView([-26,25],5);\n}\nasync function drawMaps(v){\n  const d = await api("/api/stats/maps"+fq());\n  const rows = d.regions;\n  const defs=[["m1","Map 1 — Total Visits by Region","visits","#5cbcb6",""],\n              ["m2","Map 2 — Active / Ongoing Visits by Region","ongoing","#dd6b4d",""],\n              ["m3","Map 3 — Regions with Highest Visit Duration","avg_mins","#e0a83f"," mins"]];\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, true) +\n    defs.map(([id,title])=>`<div class="panel"><h2>${title}</h2>\n      <div id="${id}" style="height:340px;border-radius:12px;overflow:hidden"></div></div>`).join("");\n  wireFilters();\n  ensureLeaflet(ok=>{\n    defs.forEach(([id,_t,metric,color,unit])=>{\n      const el=$("#"+id); if(!el) return;\n      const data = metric==="ongoing" ? rows.filter(r=>r.ongoing>0) :\n                   metric==="avg_mins" ? [...rows].sort((a,b)=>b.avg_mins-a.avg_mins) : rows;\n      if(ok){ el.style.height="340px"; leafletMap(el, data, metric, color, unit); }\n      else { el.style.height="auto"; el.innerHTML = svgBubbleMap(data, metric, color, unit); }\n    });\n  });\n}\n\n/* ---------------- Reports ---------------- */\nasync function drawReports(v){\n  const d = await api("/api/stats/overview"+fq());\n  v.innerHTML = filterBar(d.filter_regions, d.filter_departments, false) + `\n   <div class="grid2" style="margin-top:0">\n    <div class="panel"><h2>Download full report (PDF)</h2>\n      <p class="note">One PDF containing every report: key figures, top regions / purposes / departments,\n      duration analysis, hourly and monthly footfall, the regions overview behind the maps, and the current\n      on-premises list. The filters above are applied to the whole report.</p>\n      <div class="msg" id="repMsg"></div>\n      <button class="btn" id="repBtn" style="margin-top:12px;padding:13px 22px">Download report (PDF)</button>\n    </div>\n    <div class="panel"><h2>What this report will cover</h2>\n      <table><tbody>\n        <tr><td class="b">Period</td><td>${esc(S.f.date_from||"Start of data")} → ${esc(S.f.date_to||"Today")}</td></tr>\n        <tr><td class="b">Region</td><td>${esc(S.f.region||"All regions")}</td></tr>\n        <tr><td class="b">Department</td><td>${esc(S.f.department||"All departments")}</td></tr>\n        <tr><td class="b">Visits in scope</td><td>${d.total}</td></tr>\n        <tr><td class="b">Unique visitors</td><td>${d.unique}</td></tr>\n        <tr><td class="b">Ongoing right now</td><td>${d.ongoing}</td></tr>\n      </tbody></table>\n    </div>\n   </div>`;\n  wireFilters();\n  $("#repBtn").onclick = async ()=>{\n    const m=$("#repMsg"), b=$("#repBtn"); b.disabled=true; b.textContent="Building report…";\n    try{\n      const r = await fetch("/api/reports/pdf"+fq(), {headers:{Authorization:"Bearer "+S.token}});\n      if(!r.ok){ const e=await r.json().catch(()=>({})); throw new Error(e.error||"Report failed"); }\n      const blob = await r.blob();\n      const a=document.createElement("a"); a.href=URL.createObjectURL(blob);\n      a.download="StatsPack-Visitor-Report.pdf"; a.click(); URL.revokeObjectURL(a.href);\n      m.className="msg ok"; m.textContent="Report downloaded";\n    }catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n    b.disabled=false; b.textContent="Download report (PDF)";\n  };\n}\n\n/* ---------------- Data & Connection ---------------- */\nasync function drawConnection(v){\n  const d = await api("/api/connection"+q());\n  const base = location.origin;\n  v.innerHTML = `\n   <div class="grid2" style="margin-top:0">\n    <div class="panel"><h2>Connect the VMS (real-time)</h2>\n      <p class="note">Point the VMS real-time export at this endpoint. Every check-in and check-out it sends\n      appears here instantly. Re-sending the same <code class="inline">visit_id</code> updates that visit —\n      that\'s how a checkout lands.</p>\n      <p style="margin:12px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">API key</p>\n      <div class="row"><code class="inline" id="apiKey" style="word-break:break-all">${esc(d.api_key)}</code>\n        <button class="btn small ghost" id="copyKey">Copy</button></div>\n      <p style="margin:14px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">Push a visit (JSON)</p>\n<pre>curl -X POST ${base}/ingest/visits \\\\\n  -H "X-API-Key: ${esc(d.api_key)}" \\\\\n  -H "Content-Type: application/json" \\\\\n  -d \'{"visit_id":"VMS-2041","visitor_name":"Thabo M.",\n       "region":"Maseru","town":"Maseru",\n       "host_department":"Customer Service",\n       "purpose":"Enquiry","check_in":"2026-07-19T09:05:00"}\'</pre>\n      <p style="margin:12px 0 4px;font-weight:800;font-size:14px;color:var(--slate)">Or stream the VMS CSV export as-is</p>\n<pre>curl -X POST ${base}/ingest/visits \\\\\n  -H "X-API-Key: ${esc(d.api_key)}" \\\\\n  -H "Content-Type: text/csv" \\\\\n  --data-binary @vms_export.csv</pre>\n      <p class="note">Test the link any time: <code class="inline">GET ${base}/ingest/ping</code> with the same key.</p>\n    </div>\n    <div style="min-width:0">\n     <div class="panel" style="margin-top:0"><h2>Backfill: upload a CSV export</h2>\n      <p class="note">For history that predates the live connection. Headers are matched flexibly\n      (visit_id, visitor name, region, town, department, purpose, check_in, check_out…).</p>\n      <div class="msg" id="upMsg"></div>\n      <input type="file" id="csvFile" accept=".csv,text/csv" aria-label="CSV file" style="margin-top:8px">\n      <button class="btn" id="upBtn" style="margin-top:12px">Upload CSV</button>\n     </div>\n     <div class="panel"><h2>Allowed visit duration</h2>\n      <p class="note">Ongoing visits longer than this appear under "Visitors Exceeding Allowed Duration".</p>\n      <div class="row" style="margin-top:10px"><input id="maxMins" type="number" min="5" max="720" value="${d.max_visit_mins}" style="width:110px"> mins\n      <button class="btn small" id="saveMins">Save</button></div>\n      <div class="msg" id="minsMsg"></div>\n     </div>\n     <div class="panel"><h2>Recent data received</h2>\n      ${d.log.length?`<div style="overflow-x:auto"><table><thead><tr><th>When</th><th>Source</th><th>Rows</th><th>New</th><th>Upd</th><th>Err</th></tr></thead>\n       <tbody>${d.log.map(l=>`<tr><td>${fmtDT(l.ts)}</td><td>${esc(l.source)}</td><td>${l.rows_in}</td>\n        <td>${l.inserted}</td><td>${l.updated}</td><td>${l.errors?`<span class="score bad">${l.errors}</span>`:0}</td></tr>`).join("")}</tbody></table></div>`\n       :\'<div class="empty">Nothing received yet — connect the VMS or upload a CSV</div>\'}\n     </div>\n    </div>\n   </div>`;\n  $("#copyKey").onclick = ()=>{ navigator.clipboard.writeText(d.api_key); $("#copyKey").textContent="Copied"; };\n  $("#saveMins").onclick = async ()=>{\n    const m=$("#minsMsg");\n    try{ await api("/api/connection/settings"+q(),{max_visit_mins:+$("#maxMins").value}); m.className="msg ok"; m.textContent="Saved"; }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  $("#upBtn").onclick = async ()=>{\n    const f=$("#csvFile").files[0], m=$("#upMsg");\n    if(!f){ m.className="msg err"; m.textContent="Choose a CSV file first"; return; }\n    try{\n      const text = await f.text();\n      const r = await api("/api/connection/upload"+q(), {csv:text});\n      m.className="msg ok"; m.textContent=`Received ${r.received} rows — ${r.inserted} new, ${r.updated} updated, ${r.errors} errors`;\n    }catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n}\n\n/* ---------------- Super: Overview (Client console) ---------------- */\nasync function drawClients(v){\n  const d = await api("/api/super/clients");\n  S.clients = d.clients;\n  const totals = d.clients.reduce((a,c)=>({v:a.v+c.visits,o:a.o+c.ongoing,ad:a.ad+c.admins}),{v:0,o:0,ad:0});\n  v.innerHTML = `\n   <div class="kpis">\n    <div class="kpi k-slate"><div class="lbl">Clients</div><div class="rule"></div><div class="val">${d.clients.length}</div></div>\n    <div class="kpi k-teal"><div class="lbl">Total Visits</div><div class="rule"></div><div class="val">${totals.v}</div></div>\n    <div class="kpi k-amber"><div class="lbl">On Premises Now</div><div class="rule"></div><div class="val">${totals.o}</div></div>\n    <div class="kpi k-coral"><div class="lbl">Client Admins</div><div class="rule"></div><div class="val">${totals.ad}</div></div>\n   </div>\n   <div class="panel"><h2>Clients at a glance</h2>\n    <div class="msg" id="cMsg"></div>\n    <div style="overflow-x:auto"><table><thead><tr><th>Client</th><th>Visits</th><th>On premises</th>\n     <th>Admins</th><th>Last data received</th><th>API key</th><th></th></tr></thead><tbody>\n    ${d.clients.map(c=>`<tr>\n      <td class="b">${esc(c.name)}<span class="chip">${c.active?"Active":"Suspended"}</span></td>\n      <td>${c.visits}</td><td>${c.ongoing}</td><td>${c.admins}</td>\n      <td>${fmtDT(c.last_ingest)}</td>\n      <td><code class="inline" style="word-break:break-all">${esc(c.api_key)}</code></td>\n      <td style="white-space:nowrap">\n        <button class="btn small ghost" data-view="${c.id}">Dashboards</button>\n        <button class="btn small ghost" data-admins="${c.id}" data-name="${esc(c.name)}">Admins</button>\n        <button class="btn small ghost" data-rotate="${c.id}">Rotate key</button>\n        <button class="btn small ${c.active?"warn":""}" data-toggle="${c.id}">${c.active?"Suspend":"Reactivate"}</button>\n      </td></tr>`).join("")}\n    </tbody></table></div>\n    <div class="row" style="margin-top:16px"><input id="cName" placeholder="New client / company name" style="max-width:320px">\n     <button class="btn" id="cAdd">Create client</button></div>\n    <p class="note" style="margin-top:8px">Creating a client issues an API key. Give the key to the client\'s VMS\n     installation, then add an admin login so they can see their dashboards.</p>\n   </div>\n   <div id="adminPanel"></div>`;\n  $("#cAdd").onclick = async ()=>{\n    const m=$("#cMsg");\n    try{ await api("/api/super/clients",{name:$("#cName").value}); draw(); }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  v.querySelectorAll("[data-view]").forEach(b=>b.onclick=()=>{ S.clientId=+b.dataset.view; S.tab="overview"; draw(); });\n  v.querySelectorAll("[data-toggle]").forEach(b=>b.onclick=async()=>{ await api(`/api/super/clients/${b.dataset.toggle}/toggle`,{},"POST"); draw(); });\n  v.querySelectorAll("[data-rotate]").forEach(b=>b.onclick=async()=>{\n    if(confirm("Rotate this client\'s API key? The old key stops working immediately.")){\n      await api(`/api/super/clients/${b.dataset.rotate}/rotate_key`,{},"POST"); draw(); } });\n  v.querySelectorAll("[data-admins]").forEach(b=>b.onclick=()=>showAdmins(+b.dataset.admins, b.dataset.name));\n}\nasync function showAdmins(cid, cname){\n  const d = await api(`/api/super/clients/${cid}/admins`);\n  $("#adminPanel").innerHTML = `\n   <div class="panel"><h2>Admin logins — ${esc(cname)}</h2>\n    <div class="msg" id="aMsg"></div>\n    <div class="row"><input id="aName" placeholder="Name" style="max-width:180px">\n     <input id="aEmail" type="email" placeholder="Email" style="max-width:230px">\n     <input id="aPw" type="password" placeholder="Password (6+ chars)" style="max-width:200px">\n     <button class="btn" id="aAdd">Add admin</button></div>\n    ${d.admins.length?`<table style="margin-top:12px"><thead><tr><th>Name</th><th>Email</th><th>Status</th><th></th></tr></thead><tbody>\n     ${d.admins.map(a=>`<tr><td class="b">${esc(a.name)}</td><td>${esc(a.email)}</td>\n      <td><span class="chip" style="margin:0">${a.active?"Active":"Disabled"}</span></td>\n      <td><button class="btn small ghost" data-tg="${a.id}">${a.active?"Disable":"Enable"}</button></td></tr>`).join("")}\n    </tbody></table>`:\'<div class="empty">No admin logins yet — add the first one above</div>\'}\n   </div>`;\n  $("#aAdd").onclick = async ()=>{\n    const m=$("#aMsg");\n    try{ await api(`/api/super/clients/${cid}/admins`,{name:$("#aName").value,email:$("#aEmail").value,password:$("#aPw").value});\n         showAdmins(cid,cname); }\n    catch(ex){ m.className="msg err"; m.textContent=ex.message; }\n  };\n  document.querySelectorAll("[data-tg]").forEach(b=>b.onclick=async()=>{ await api(`/api/super/admins/${b.dataset.tg}/toggle`,{},"POST"); showAdmins(cid,cname); });\n}\n\n/* ---------------- boot ---------------- */\nasync function boot(){\n  if(!S.token){ hideLoader(); return renderLogin(); }\n  if(S.role==="super"){\n    try{ const d = await api("/api/super/clients"); S.clients=d.clients;\n         if(!S.clientId && d.clients.length) S.clientId=d.clients[0].id;\n         if(!S.tab) S.tab="clients";\n    }catch(ex){ hideLoader(); sessionStorage.clear(); return renderLogin(); }\n  } else if(!S.tab || S.tab==="clients"){ S.tab="overview"; }\n  await draw(true);\n}\nboot();\n</script>\n</body>\n</html>\n'

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
    return 200, {"token": tok, "role": u["role"], "name": u["name"], "email": u["email"], "client_id": u["client_id"]}

@route("POST", r"/api/logout")
def logout(ctx):
    return 200, {"ok": True}

# ---- dashboards (admin sees own client; super passes ?client_id=)
@route("GET", r"/api/stats/overview")
def api_overview(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    g = lambda k: (ctx.qs.get(k, [""])[0] or "").strip()
    flt = {"date_from": g("date_from"), "date_to": g("date_to"),
           "region": g("region"), "department": g("department")}
    return 200, stats_overview(ctx.conn, cid, flt)

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

@route("GET", r"/api/reports/pdf")
def api_report(ctx):
    cid = scoped_client(ctx)
    if not cid: return 400, {"error": "client_id required"}
    c = ctx.conn.execute("SELECT name FROM clients WHERE id=?", (cid,)).fetchone()
    pdf = build_report(ctx.conn, cid, _flt_from_qs(ctx.qs), c["name"] if c else "Client")
    return 200, pdf, "application/pdf"

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
