#!/usr/bin/env python3
"""
VMS connector simulator — stands in for the real Visitor Management System.

It replays a CSV export into StatsPack Analytics Lab exactly the way the live
VMS would: one push per event, check-in first, checkout a little later
(by re-sending the same visit_id with check_out filled in).

Usage:
    python3 vms_simulator.py --key spk_live_XXXX [--file sample_export.csv]
                             [--url http://localhost:8460] [--delay 3]

Watch the "Live from the Premises" tab while it runs — visitors appear,
then check out, in real time.
"""
import argparse, csv, json, time, urllib.request, urllib.error, sys

def push(url, key, payload):
    req = urllib.request.Request(url + "/ingest/visits", json.dumps(payload).encode(),
                                 {"Content-Type": "application/json", "X-API-Key": key})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")
    except urllib.error.URLError as e:
        print(f"Cannot reach {url} — is server.py running? ({e.reason})"); sys.exit(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", required=True, help="Client API key from Analytics Lab")
    ap.add_argument("--file", default="sample_export.csv")
    ap.add_argument("--url", default="http://localhost:8460")
    ap.add_argument("--delay", type=float, default=3.0, help="Seconds between events")
    a = ap.parse_args()

    # connectivity check first
    req = urllib.request.Request(a.url + "/ingest/ping", headers={"X-API-Key": a.key})
    try:
        with urllib.request.urlopen(req) as r:
            print("Connected:", json.loads(r.read())["client"])
    except urllib.error.HTTPError as e:
        print("Key rejected:", json.loads(e.read() or b"{}").get("error")); sys.exit(1)

    rows = list(csv.DictReader(open(a.file, newline="", encoding="utf-8-sig")))
    print(f"Replaying {len(rows)} visits from {a.file} (one event every {a.delay:g}s)\n")

    for r in rows:
        checkout = r.get("check_out", "")
        checkin_row = dict(r); checkin_row["check_out"] = ""
        _, res = push(a.url, a.key, checkin_row)
        print(f"  check-in  {r.get('visit_id')} — {r.get('visitor_name') or r.get('name','')} "
              f"({r.get('purpose','')})  ->  {res}")
        time.sleep(a.delay)
        if checkout:
            _, res = push(a.url, a.key, r)
            print(f"  check-out {r.get('visit_id')}  ->  {res}")
            time.sleep(a.delay / 2)
    print("\nDone — every event should now be on the dashboards.")

if __name__ == "__main__":
    main()
