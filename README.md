# StatsPack Analytics Lab

The analytical companion to the **StatsPack Visitor Management System (VMS)**, built on the same
zero-dependency stack as EduTrack 360: pure Python 3 standard library + SQLite + vanilla HTML/JS.
One command to run, nothing to install.

## Run it

```bash
python3 server.py          # -> http://localhost:8460
```

The database (`analyticslab.db`) is created and seeded with a demo client on first run.

| Login | Email | Password |
|---|---|---|
| StatsPack super user | admin@statspack.co.ls | super123 |
| Demo client admin | admin@demo.client | admin123 |

## How the VMS connects (the "just connect" part)

Instead of exporting CSV files somewhere, the VMS **pushes each event as it happens** to a
per-client API endpoint. This is better than file-based exchange: no polling, no duplicates,
checkout updates arrive naturally, and a suspended client's key stops working instantly.

1. The super user creates a client → an **API key** is issued (`spk_live_…`).
2. Point the VMS's real-time export hook at:

```
POST /ingest/visits
X-API-Key: <client key>
```

3. The body can be **JSON** (single object or array) *or* the VMS's **existing CSV export
   verbatim** (`Content-Type: text/csv`) — headers are matched flexibly
   (`Visit ID`, `Visitor /User Name`, `Host Department`, `Check In`… all recognised, as are
   `dd/mm/yyyy hh:mm` dates).
4. **Checkout** = re-send the same `visit_id` with `check_out` filled in. The visit updates in place.
5. Test the link any time: `GET /ingest/ping` with the same key.

A one-time **CSV backfill upload** lives in the client's *Data & Connection* tab for history that
predates the live connection.

### Try the live connection

`vms_simulator.py` stands in for the real VMS — it replays `sample_export.csv` event by event:

```bash
python3 vms_simulator.py --key spk_live_XXXX --delay 3
```

Open **Live from the Premises** while it runs (auto-refreshes every 15 s) and watch visitors
check in and out.

## What's on the dashboards (mirrors the VMS Analytical Summary)

* **Bird's Eye View** — total / unique / today / ongoing visitors, average visits per day,
  average visit duration, highest & lowest day, month-vs-last-month %, 6-month footfall line,
  current-month daily line, Top 5 regions, Top 5 purposes.
* **In-Depth Analysis** — regions by visitors and by duration, Top 5 departments by footfall,
  departments by time taken, purposes taking the most time, hourly footfall curve.
* **Live from the Premises** — ongoing-visit donut by purpose, ongoing visits by region,
  the current on-premises visitor table, and **Visitors Exceeding Allowed Duration**
  (threshold configurable per client in Data & Connection).

## Users & roles

* **Super user (StatsPack)** — the Client Console: create clients, issue/rotate API keys,
  suspend/reactivate clients (blocks both their logins *and* their ingest immediately),
  create/disable client admin logins, see per-client visit counts and last-data-received,
  and open any client's dashboards read-through.
* **Client admin** — their own three dashboards plus Data & Connection (API key, connection
  recipes, CSV backfill, allowed-duration setting, ingest log). Strictly scoped to their tenant.

## Files

```
server.py           THE ENTIRE APP in one file — backend, API, ingest, seed,
                    and the whole frontend (HTML/CSS/JS embedded inside it)
vms_simulator.py    plays a CSV into the ingest API like a live VMS
sample_export.csv   demo export used by the simulator
logo.png            your logo (shown on login + sidebar) — upload to repo root
login.png           login background photo — upload to repo root
```

**Updating the app now means updating ONE file: `server.py`.** There is no static
folder anymore — the UI is embedded, so a single GitHub edit + commit deploys everything.

## Notes for production

Dev conveniences to change before deploying anywhere shared: run behind HTTPS, move the port and
any secrets to env vars, and back up `analyticslab.db` (or swap SQLite for Postgres — the SQL is
deliberately plain).


## Deploy: GitHub → Render (step by step)

### 1. Put the code on GitHub
1. Go to **github.com → New repository**, name it `statspack-analytics-lab`, keep it **Private**, create it.
2. On the new repo page choose **"uploading an existing file"**, drag in the **contents** of this zip
   (server.py, static/, render.yaml, requirements.txt, README.md, .gitignore, vms_simulator.py,
   sample_export.csv — not the zip itself), and **Commit changes**.
   *(Or with git installed: `git init && git add . && git commit -m "first" && git remote add origin <repo-url> && git push -u origin main`.)*

### 2. Deploy on Render
1. Sign in at **render.com** (you can sign in with GitHub) and connect your GitHub account.
2. Click **New → Blueprint**, pick the `statspack-analytics-lab` repo. Render reads `render.yaml`
   and pre-fills everything: Python runtime, `python3 server.py` start command, a **1 GB persistent
   disk** mounted at `/var/data`, and `DB_PATH` pointing at it. Click **Apply / Deploy**.
3. Wait for the build to go **Live**. Your backend URL is
   `https://statspack-analytics-lab.onrender.com` (Render shows the exact one at the top).

*No-blueprint alternative:* **New → Web Service** → pick the repo → Runtime **Python 3** →
Build `pip install -r requirements.txt` → Start `python3 server.py` → (paid plans) add a Disk
mounted at `/var/data` and env var `DB_PATH=/var/data/analyticslab.db` → Create.

### 3. First run on the live URL
1. Open the URL → sign in as the super user (`admin@statspack.co.ls / super123`).
2. **Immediately change the seeded passwords**: create your real client + admin logins in the
   Client Console; the demo logins are public knowledge from this README.
3. Create a client → copy its API key from the Client Console.

### 4. Connect the VMS to the live backend
Point the VMS real-time export at the Render URL:
```
POST https://<your-app>.onrender.com/ingest/visits
X-API-Key: spk_live_…
```
Test from any machine: `curl https://<your-app>.onrender.com/ingest/ping -H "X-API-Key: spk_live_…"`
You can also demo it remotely: `python3 vms_simulator.py --key spk_live_… --url https://<your-app>.onrender.com`

### Render notes
* **Free plan:** works for testing, but it has **no persistent disk** (data resets on each deploy)
  and the service sleeps after inactivity (first request takes ~30 s to wake). If you use free,
  delete the `disk:` block and the `DB_PATH` env var from `render.yaml` first.
* **Starter plan + disk** (as configured) keeps `analyticslab.db` across deploys and stays awake.
* HTTPS is automatic on Render; every future `git push` to the repo redeploys automatically.
