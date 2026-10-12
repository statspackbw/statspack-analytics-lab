# StatsPack Analytics Lab

The analytical companion to **SmartRegister** (formerly the StatsPack Visitor Management System) — covering visitors, employees and contractors, built on the same
zero-dependency stack as EduTrack 360: pure Python 3 standard library + SQLite + vanilla HTML/JS.
One command to run, nothing to install.

## Run it

```bash
python3 server.py          # -> http://localhost:8460
```

The database (`analyticslab.db`) is created and seeded with a demo client on first run.

On first start the app seeds one super-user and one demo client-admin login. **The initial
credentials are printed only in the server console/logs —
they are deliberately not published here. Sign in, then change both passwords immediately under
**Account → Security** in the sidebar.

## How the VMS connects (the "just connect" part)

Instead of exporting CSV files somewhere, SmartRegister **pushes each event as it happens** to a
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


## Stable API keys without persistent storage

If the app runs without persistent storage, the database re-seeds on every restart, which
regenerates the seeded clients' API keys. To keep them stable, set environment variables
on the host before starting the app:

* `TEST_API_KEY` — pins the "SmartRegister Test Environment" client's key
* `DEMO_API_KEY` — optionally pins the demo client's key

The seeded logins and these keys then survive every restart. Visit data still requires a
persistent `DB_PATH` location to survive.

## Deploy (any host that runs Python 3)

Analytics Lab is one file with zero dependencies. To run it anywhere:

```bash
PORT=8460 DB_PATH=/var/data/analyticslab.db python3 server.py
```

On a Linux VPS, keep it running permanently with systemd (see HANDOVER-FOR-MAX.md for the
unit file) and put nginx or Caddy in front for HTTPS. Point SmartRegister's export at:

```
POST https://analytics.YOUR-DOMAIN.com/ingest/visits
X-API-Key: <client api key>
```

Test from any machine: `curl https://analytics.YOUR-DOMAIN.com/ingest/ping -H "X-API-Key: …"`
You can also replay demo data: `python3 vms_simulator.py --key … --url https://analytics.YOUR-DOMAIN.com`

Requirements for production: a persistent `DB_PATH` location so data survives restarts,
and HTTPS in front of the app.
