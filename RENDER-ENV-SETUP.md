# Render — environment variables to set

Render does **not** read a `.env` file. Add these in the dashboard:

> your service (`statspack-analytics-lab-3`) → **Environment** → **Add Environment Variable**

Saving triggers a redeploy. Nothing here is required to boot, but with Neon set you get
permanent data, and with the key pinned you stop having to resend the API key after restarts.

---

## Set these three

| Key | Value | Why |
|---|---|---|
| `DATABASE_URL` | your Neon connection string | **Permanent storage.** Without it data is wiped on restarts and redeploys. |
| `TEST_API_KEY` | `spk_test_e431c4caf327871e15d80977d62b36b8` | Pins the SmartRegister client's key so it survives restarts. Send this value to Max. |
| `DEMO_API_KEY` | `spk_live_77824593f9c35c62684d5a2932cd570b` | Same for the demo client used in presentations. |

The two keys above were randomly generated for you. Treat them as passwords. If either
ever leaks, change the variable's value and the old key stops working immediately.

### DATABASE_URL — where to get it

Neon dashboard → your project → **Connection string** → copy the *pooled* connection string.
It looks like:

```
postgresql://user:password@ep-xxxx-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

Keep `sslmode=require` in the string — Neon rejects unencrypted connections.

---

## Do NOT set these on Render

| Key | Reason |
|---|---|
| `PORT` | Render injects its own. Setting it manually breaks routing. |
| `DB_PATH` | Only used for local SQLite storage. Ignored once `DATABASE_URL` is set. |

---

## One extra step for Neon

Neon needs the Postgres driver, which is listed in `requirements.txt`. Render installs it
automatically during the build, so there is nothing to run by hand — just make sure
`requirements.txt` is committed alongside `server.py`.

If `DATABASE_URL` is set but the driver is missing, the app stops with a clear message
telling you exactly that, rather than failing silently.

---

## Checking it worked

After the deploy finishes:

1. Open the app. The sign-in page should read **Analytics Lab · v14**.
2. Sign in as super user → **Data & Connection**. The API key shown should match the
   `TEST_API_KEY` value above.
3. Add a visit (or let SmartRegister push one), then **restart the service from the Render
   dashboard**. If the visit is still there afterwards, Neon is working.

Step 3 is the one that actually proves permanent storage. Do it before the presentations.

---

## Custom domain (analytics.statspack.africa)

1. name.com → statspack.africa → **DNS Records** → add a **CNAME**:
   host `analytics`, value `statspack-analytics-lab-3.onrender.com`
2. Render → service → **Settings → Custom Domains** → add `analytics.statspack.africa`.
   Render verifies DNS and issues HTTPS automatically.

Until that finishes, the custom domain shows a certificate warning — expected, not a fault.
Both addresses serve the same app.
