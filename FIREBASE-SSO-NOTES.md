# Firebase SSO — deployment notes

Max's Firebase handoff is merged into the current build. SmartRegister posts the signed-in
user's Firebase ID token to the iframe; Analytics Lab verifies it and signs the user in.

## Environment variables

| Variable | Needed | Purpose |
|---|---|---|
| `FLUTTER_APP_ORIGIN` | yes, for SSO | Exact origin(s) of the SmartRegister web app. No path, no trailing slash. Several may be listed separated by commas, so test and production can both be allowed, e.g. `https://vms.test.statspackbw.org,https://vms.statspackbw.org`. Messages from any other origin are ignored. |
| `FIREBASE_PROJECT_ID` | usually | `statspack-vms-testing` |
| `GOOGLE_APPLICATION_CREDENTIALS` | host-dependent | Path to the mounted Firebase service-account file. On Render use a **Secret File** and point this at its path. |

## Two corrections to ANALYTICSLAB_TODO.md

1. **`requirements.txt` must keep `psycopg[binary]` as well as `firebase-admin`.**
   The version supplied dropped psycopg. Without it the app cannot connect to Neon and
   refuses to start.

2. **Storage is Neon, not a local SQLite file.** The TODO says to keep `analyticslab.db`
   on persistent storage and set `DB_PATH`. That applies only when running on SQLite.
   With `DATABASE_URL` set, Neon is used and `DB_PATH` is ignored — no local database
   file exists to preserve.

## Accounts

SSO matches on **email**, compared case-insensitively. For each SmartRegister user who
should reach the lab there must be an active Analytics Lab account with the same email,
and their client must be active. No account is created automatically by this route: an
unmatched email is rejected with a clear message. Firebase emails must be verified.

## Checking it

- Outside the iframe, the normal login page still works (verified).
- Inside the iframe, the handoff only runs when the URL has `embed` and the message comes
  from `FLUTTER_APP_ORIGIN` and from the parent window. Anything else is ignored.
- If Firebase credentials are missing, the endpoint fails closed with a clear message and
  no session is issued. The rest of the app keeps working.
