# Analytics Lab — Handover & Integration Instructions (for Max / SmartRegister team)

Analytics Lab is the analytics companion for SmartRegister. This package is the complete
application: **one Python file (`server.py`)** containing the backend, the ingest API, and the
entire web interface. No frameworks, no dependencies — it runs anywhere Python 3.8+ runs.

Goal of this handover: Analytics Lab appears **inside SmartRegister, under the Report menu**,
on your own domain.

---

## 1. Where to host it (read this first — it matters)

Analytics Lab is a **long-running Python server process**. That has one hard consequence:

> **name.com shared web hosting cannot run it.** name.com hosting serves static sites and
> PHP; it does not run persistent Python processes. This is a limitation of that hosting
> product, not of Analytics Lab.

That does NOT block putting Analytics Lab "inside SmartRegister" — the SmartRegister web app
itself talks to Firebase, which also isn't on name.com's servers. The same pattern applies
here: the **domain stays at name.com**, the app runs on a Python-capable host, and a
subdomain points at it (Section 3). Users only ever see your domain.

Pick one:

**Option A — any VPS or Python-capable host you already use (recommended)** (DigitalOcean, Hetzner,
PythonAnywhere, a company server, etc.):

```bash
# upload server.py, logo.png, login.png to a folder, then:
PORT=8460 DB_PATH=/var/data/analyticslab.db python3 server.py
```

To keep it running permanently on a Linux VPS, a minimal systemd unit:

```ini
# /etc/systemd/system/analyticslab.service
[Unit]
Description=Analytics Lab
After=network.target
[Service]
WorkingDirectory=/opt/analyticslab
Environment=PORT=8460
Environment=DB_PATH=/opt/analyticslab/data/analyticslab.db
ExecStart=/usr/bin/python3 /opt/analyticslab/server.py
Restart=always
[Install]
WantedBy=multi-user.target
```
`systemctl enable --now analyticslab`, then put nginx/Caddy in front for HTTPS.

**Option B — upgrade the name.com account to a VPS product, if they offer one to you.**
Then follow Option A on it. Plain shared hosting will not work.

Environment variables (all optional):

| Variable | Purpose |
|---|---|
| `PORT` | Listen port (default 8460) |
| `DB_PATH` | Where the SQLite database lives (default: next to server.py) |
| `TEST_API_KEY` | Pins the seeded test client's API key so it survives restarts |
| `DEMO_API_KEY` | Same for the demo client |

---

## 2. Putting it under SmartRegister's "Report" menu

Analytics Lab is **iframe-friendly** — it sends no frame-blocking headers, so it can be
embedded directly. Add a menu item under **Report** (e.g. "Analytics Lab") that opens an
embedded view of the Analytics Lab URL.

**SmartRegister web (Flutter web)** — register an iframe view and show it when the menu
item is selected:

```dart
import 'dart:ui_web' as ui_web;
import 'package:flutter/widgets.dart';

// once, at startup:
ui_web.platformViewRegistry.registerViewFactory('analytics-lab', (int viewId) {
  final el = IFrameElement()
    ..src = 'https://analytics.YOUR-DOMAIN.com/?embed=1'   // see Section 3
    ..style.border = 'none'
    ..style.width = '100%'
    ..style.height = '100%';
  return el;
});

// in the Report > Analytics Lab page:
const HtmlElementView(viewType: 'analytics-lab')
```

**SmartRegister mobile apps** — use `webview_flutter`:

```dart
WebViewWidget(controller: WebViewController()
  ..setJavaScriptMode(JavaScriptMode.unrestricted)
  ..loadRequest(Uri.parse('https://analytics.YOUR-DOMAIN.com/?embed=1')))
```

**Any plain HTML admin panel** — one line:

```html
<iframe src="https://analytics.YOUR-DOMAIN.com/?embed=1"
        style="border:0;width:100%;height:100vh"></iframe>
```

**Use `?embed=1` on the iframe URL** (e.g. `https://analytics.YOUR-DOMAIN.com/?embed=1`).
In embed mode Analytics Lab hides its own sidebar and shows a slim horizontal tab strip
instead, so it doesn't duplicate SmartRegister's navigation. Its styling (teal profile
block, slate top bar, icon stat cards) matches the SmartRegister dashboard, so it reads
as part of the product. Without the parameter you get the full standalone interface.

One browser note: Safari restricts storage inside cross-site iframes, which can make the
embedded login loop. Serving Analytics Lab on a **subdomain of the same domain** as
SmartRegister (Section 3) avoids this — another reason to do the DNS step.

Users sign in to Analytics Lab inside the frame with their client-admin login. Each
SmartRegister customer gets their own Analytics Lab client (own login + own API key),
so they only ever see their own data.

## 3. Make it live on your own domain (name.com DNS)

So the embedded URL is yours rather than a hosting provider's:

1. name.com dashboard → your domain → **DNS Records**.
2. Add an **A record**: host `analytics`, answer = your VPS's IP address
   (or a **CNAME** to the host's domain name if your provider gives you one).
3. On the server, configure `analytics.YOUR-DOMAIN.com` in nginx or Caddy with a
   Let's Encrypt certificate so the app is served over HTTPS.
4. Point the iframe at `https://analytics.YOUR-DOMAIN.com`.

## 4. Connecting SmartRegister's data feed (unchanged)

- `POST /ingest/visits` with header `X-API-Key: <client key>` — JSON object, JSON array,
  or raw CSV body. Timestamps accept ISO-8601 (with milliseconds/timezones), epoch
  seconds/millis, and Firestore timestamp objects.
- Re-sending the same `visit_id` with `check_out` set closes the visit (idempotent).
- Optional `person_type` field: Visitor / Employee / Contractor (aliases like `type`,
  `category`, `role` are accepted; missing defaults to Visitor).
- Errors come back self-explained in a `problems` array, and appear under
  **Data & Connection → Recent data received**.
- Health check: `GET /ingest/ping` with the same key.

## 5. Logins & keys

Seeded on first run (change passwords under Account → Security once on persistent storage):

| Role | Email | Password |
|---|---|---|
| StatsPack super user | admin@statspack.co.ls | super123 |
| SmartRegister test admin | vms@test.client | vms123 |
| Demo client admin | admin@demo.client | admin123 |

API keys are shown per client under **Data & Connection** (client admin) or in the
**Client Console** (super user). Without a persistent disk the database re-seeds on
restart — pin the key with `TEST_API_KEY` if needed, and treat persistent storage as a
prerequisite for production.

## 6. Logins, database and environment file — common questions

**Where is the database file?** There isn't one to ship, and you don't need one.
Analytics Lab creates `analyticslab.db` itself the first time it starts, and seeds the
login accounts in code at that moment. Just run the app and the accounts below exist.
Set `DB_PATH` (Section 6b) so the file lands somewhere persistent.

**Where are the login credentials stored?** They are seeded by the application, not
shipped in a file. See the table in Section 5. Change them under **Account → Security**
after first sign-in.

**Is there an env file?** `.env.example` is included as a template. Nothing in it is
required — the app runs with no variables set. Note the app reads real environment
variables, it does not parse a `.env` file by itself, so either export them in your
systemd unit / hosting panel, or `set -a; . ./.env; set +a` before starting.

### 6a. Using Neon (Postgres) for permanent storage — recommended

Set `DATABASE_URL` to the Neon connection string and Analytics Lab stores everything in
Neon instead of a local file, so data survives restarts, redeploys and server rebuilds:

```bash
pip install "psycopg[binary]"
export DATABASE_URL="postgresql://user:pass@ep-xxxx.region.aws.neon.tech/dbname?sslmode=require"
python3 server.py
```

Notes:
* Keep `sslmode=require` in the string — Neon requires TLS.
* The app creates its own tables on first connect; no SQL to run by hand.
* `DB_PATH` is ignored when `DATABASE_URL` is set.
* Leave `DATABASE_URL` unset and the app uses SQLite exactly as before, with no driver
  needed. The switch is one variable — nothing else in the app changes.
* Neon's free tier suspends an idle database; the first query after idle takes a few
  seconds to wake it. This is normal and only affects the first request.

### 6b. Environment variables

| Variable | Required | What it does |
|---|---|---|
| `PORT` | no | Listen port. Default 8460. |
| `DATABASE_URL` | recommended | Postgres/Neon connection string. When set, all data lives in Neon permanently and `DB_PATH` is ignored. Needs `pip install "psycopg[binary]"`. |
| `DB_PATH` | if not using Neon | Where the SQLite file lives. Without a persistent path, all data is lost on restart. |
| `TEST_API_KEY` | no | Pins the SmartRegister test client's ingest key so it survives restarts. |
| `DEMO_API_KEY` | no | Same for the demo client. |
| `EXCHANGERATE_API_KEY` | no | Enables live currency conversion of workforce costs. |

Production start:

```bash
PORT=8460 DB_PATH=/var/data/analyticslab.db python3 server.py
```

### 6c. Employee time & cost

SmartRegister now registers **visitors, employees and contractors**. For employees and
contractors, Analytics Lab costs the time they spend on site:

* Send an optional **`hourly_rate`** field with the visit (aliases accepted: `rate`,
  `pay_rate`, `wage`, `cost_per_hour`). This is the per-person rate.
* If no rate is sent, the client's **default hourly rate** is used — set it under
  **Data & Connection → Employee hourly rate**, along with the currency.
* Cost = time between `check_in` and `check_out` × rate. Only **completed** visits are
  costed; people still on site are not, and **visitors are never costed** even if a rate
  is sent.
* With `EXCHANGERATE_API_KEY` set, costs can be displayed in any world currency at
  today's rate; without it they show in the client's own currency.
* Results appear in the **Workforce Time & Cost** panel on Bird's Eye View (total cost,
  hours, people, average per person, cost by department, cost per person) and in the
  PDF report. All dashboard filters apply.

**Images.** `logo.png` (sign-in logo and sidebar mark) and `login.png` (sign-in page
background) live in the same folder as `server.py`. The app serves them from there, or
from a `static/` sub-folder if you prefer. Every other graphic in the interface is inline
SVG inside `server.py`, so these two files are the only image assets.

## 7. Files in this package

```
server.py           the entire application (backend + API + UI)
logo.png            sign-in logo / sidebar mark — keep next to server.py
login.png           sign-in page background — keep next to server.py
.env.example        environment variable template (all values optional)
requirements.txt    only needed if you use Neon/Postgres (psycopg driver)
vms_simulator.py    replays sample_export.csv into the API like a live SmartRegister
sample_export.csv   demo data for the simulator
README.md           general documentation
```
