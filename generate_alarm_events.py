"""
Synthetic alarm signal generator.

Simulates a fleet of residential/commercial security panels sending sensor
events (door contacts, motion, glass break, smoke, keypad arm/disarm, low
battery, tamper, comm failures, supervision heartbeats).

All data is fake. Modeled on real-world alarm system behavior.

Writes newline-delimited JSON batch files into an output folder so a streaming
reader (Databricks Auto Loader / Spark Structured Streaming) can pick them up
as they land.

Usage:
    python generate_alarm_events.py --out ./landing --sites 200 --batches 24
    python generate_alarm_events.py --out ./landing --batches 0 --interval 10   # run forever, new batch every 10s
"""

import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

CITIES = [
    "Minneapolis", "St. Paul", "Bloomington", "Lakeville", "Burnsville",
    "Eagan", "Edina", "Plymouth", "Maple Grove", "Woodbury", "Apple Valley",
]
FIRMWARE = ["4.2.1", "4.2.3", "4.3.0", "5.0.0-beta"]

# device_type -> (min, max) count per site
DEVICE_MIX = {
    "door_contact": (2, 4),
    "window_contact": (0, 4),
    "motion": (1, 3),
    "glass_break": (0, 1),
    "smoke": (1, 2),
    "keypad": (1, 1),
}


# ---------------------------------------------------------------- fleet setup
def build_fleet(n_sites, rng):
    sites, devices = [], []
    for i in range(n_sites):
        site_id = f"SITE-{i:05d}"
        panel_id = f"PNL-{rng.randint(100000, 999999)}"
        site = {
            "site_id": site_id,
            "panel_id": panel_id,
            "city": rng.choice(CITIES),
            "site_type": rng.choices(["residential", "commercial"], [0.8, 0.2])[0],
            "install_date": (datetime(2020, 1, 1) + timedelta(days=rng.randint(0, 2100))).date().isoformat(),
            "firmware": rng.choice(FIRMWARE),
            # a few "problem" panels drop comms much more often
            "comm_fail_rate": 0.15 if rng.random() < 0.05 else 0.002,  # chance per hour
            # some households are prone to user-error false alarms
            "false_alarm_prone": rng.random() < 0.12,
            "armed_state": "disarmed",
        }
        sites.append(site)
        zone = 1
        for dtype, (lo, hi) in DEVICE_MIX.items():
            for _ in range(rng.randint(lo, hi)):
                devices.append({
                    "device_id": f"DEV-{uuid.uuid4().hex[:10].upper()}",
                    "site_id": site_id,
                    "panel_id": panel_id,
                    "device_type": dtype,
                    "zone": zone,
                    "battery_pct": rng.uniform(8, 25) if rng.random() < 0.1 else rng.uniform(55, 100),
                    # % battery lost per simulated hour
                    "drain_per_hr": rng.uniform(0.005, 0.03) * (4 if rng.random() < 0.06 else 1),
                })
                zone += 1
    return sites, devices


# ---------------------------------------------------------------- events
def make_event(site, dev, event_type, ts, rng, armed_state=None):
    return {
        "event_id": str(uuid.uuid4()),
        "site_id": site["site_id"],
        "panel_id": site["panel_id"],
        "device_id": dev["device_id"] if dev else None,
        "device_type": dev["device_type"] if dev else "panel",
        "zone": dev["zone"] if dev else 0,
        "event_type": event_type,
        "armed_state": armed_state or site["armed_state"],
        "battery_pct": round(dev["battery_pct"], 1) if dev else None,
        "signal_dbm": rng.randint(-95, -40),
        "firmware": site["firmware"],
        "event_ts": ts.isoformat(),
    }


def dirty(event, rng):
    """Inject the kind of junk real telemetry has, so the silver layer has work to do."""
    r = rng.random()
    if r < 0.01:
        event["device_id"] = None                       # missing key
    elif r < 0.02:
        event["battery_pct"] = rng.choice([-1, 255])    # sensor garbage
    elif r < 0.03:
        event["event_ts"] = event["event_ts"][:19].replace("T", " ")  # wrong ts format
    elif r < 0.035:
        event["event_type"] = event["event_type"].upper()  # inconsistent casing
    return event


def simulate_hour(sites, devices_by_site, start, rng):
    """Generate ~1 hour of activity across the fleet."""
    events = []
    for site in sites:
        devs = devices_by_site[site["site_id"]]
        keypad = next(d for d in devs if d["device_type"] == "keypad")
        doors = [d for d in devs if d["device_type"] == "door_contact"]
        motions = [d for d in devs if d["device_type"] == "motion"]

        def t(minutes):
            return start + timedelta(minutes=minutes, seconds=rng.randint(0, 59))

        # battery drain + low battery / supervision
        for d in devs:
            d["battery_pct"] = max(0.0, d["battery_pct"] - d["drain_per_hr"])
            if d["battery_pct"] < 20 and rng.random() < 0.3:
                events.append(make_event(site, d, "low_battery", t(rng.randint(0, 59)), rng))
            if rng.random() < 0.25:
                events.append(make_event(site, d, "supervision", t(rng.randint(0, 59)), rng))
            # simulate battery replacement after it goes very low
            if d["battery_pct"] < 3 and rng.random() < 0.2:
                d["battery_pct"] = 100.0
                events.append(make_event(site, d, "battery_replaced", t(rng.randint(0, 59)), rng))

        # comm failures
        if rng.random() < site["comm_fail_rate"]:
            m = rng.randint(0, 50)
            events.append(make_event(site, None, "comm_fail", t(m), rng))
            events.append(make_event(site, None, "comm_restore", t(m + rng.randint(1, 9)), rng))

        # normal daily life: door open/close, motion while disarmed
        hour = start.hour
        active = 6 <= hour <= 22
        if active and site["armed_state"] == "disarmed":
            for _ in range(rng.randint(0, 4)):
                d, m = rng.choice(doors), rng.randint(0, 58)
                events.append(make_event(site, d, "open", t(m), rng))
                events.append(make_event(site, d, "close", t(m + 1), rng))
            for _ in range(rng.randint(0, 6)):
                events.append(make_event(site, rng.choice(motions), "motion", t(rng.randint(0, 59)), rng))

        # arming / disarming
        if site["armed_state"] == "disarmed" and rng.random() < (0.25 if hour in (7, 8, 22, 23) else 0.03):
            mode = "armed_away" if hour in (7, 8) else "armed_stay"
            events.append(make_event(site, keypad, "arm", t(rng.randint(0, 10)), rng, armed_state=mode))
            site["armed_state"] = mode
        elif site["armed_state"] != "disarmed" and rng.random() < (0.3 if hour in (6, 7, 17, 18) else 0.04):
            m = rng.randint(10, 55)
            d = rng.choice(doors)
            events.append(make_event(site, d, "open", t(m), rng))
            events.append(make_event(site, keypad, "disarm", t(m + 0.5), rng, armed_state="disarmed"))
            site["armed_state"] = "disarmed"

        # FALSE ALARM pattern: armed, motion trips with no door entry, user cancels fast
        if site["armed_state"] != "disarmed" and rng.random() < (0.10 if site["false_alarm_prone"] else 0.005):
            m = rng.randint(0, 55)
            mo = rng.choice(motions)
            events.append(make_event(site, mo, "motion", t(m), rng))
            events.append(make_event(site, mo, "alarm", t(m), rng))
            events.append(make_event(site, keypad, "alarm_cancel",
                                     start + timedelta(minutes=m, seconds=rng.randint(15, 90)),
                                     rng, armed_state="disarmed"))
            site["armed_state"] = "disarmed"

        # REAL ALARM pattern (rare): armed_away, door opens, no disarm
        if site["armed_state"] == "armed_away" and rng.random() < 0.0008:
            m = rng.randint(0, 50)
            d = rng.choice(doors)
            events.append(make_event(site, d, "open", t(m), rng))
            events.append(make_event(site, d, "alarm", t(m + 1), rng))
            events.append(make_event(site, None, "dispatch", t(m + 3), rng))

        # smoke / glass break / tamper (rare)
        for d in devs:
            if d["device_type"] == "smoke" and rng.random() < 0.0005:
                events.append(make_event(site, d, "smoke_alarm", t(rng.randint(0, 59)), rng))
            if d["device_type"] == "glass_break" and rng.random() < 0.0008:
                events.append(make_event(site, d, "glass_break", t(rng.randint(0, 59)), rng))
            if rng.random() < 0.0003:
                events.append(make_event(site, d, "tamper", t(rng.randint(0, 59)), rng))

    events = [dirty(e, rng) for e in events]
    # duplicate ~1% (panels re-send when they don't get an ack)
    events += [dict(e) for e in rng.sample(events, k=max(1, len(events) // 100))]
    rng.shuffle(events)
    return events


# ---------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="./landing", help="output folder (local path or /Volumes/... in Databricks)")
    p.add_argument("--sites", type=int, default=200)
    p.add_argument("--batches", type=int, default=24, help="hours to simulate; 0 = run forever")
    p.add_argument("--interval", type=float, default=0, help="seconds to sleep between batches (streaming demo)")
    p.add_argument("--start", default=None, help="simulation start, e.g. 2026-11-01T00:00:00")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    rng = random.Random(args.seed)
    events_dir = os.path.join(args.out, "events")
    os.makedirs(events_dir, exist_ok=True)

    sites, devices = build_fleet(args.sites, rng)
    devices_by_site = {}
    for d in devices:
        devices_by_site.setdefault(d["site_id"], []).append(d)

    # dimension files (static reference data)
    for name, rows, cols in [
        ("sites.csv", sites, ["site_id", "panel_id", "city", "site_type", "install_date", "firmware"]),
        ("devices.csv", devices, ["device_id", "site_id", "panel_id", "device_type", "zone"]),
    ]:
        with open(os.path.join(args.out, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    start = (datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc) if args.start
             else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=args.batches or 1))

    n, total = 0, 0
    while args.batches == 0 or n < args.batches:
        hour_start = start + timedelta(hours=n)
        events = simulate_hour(sites, devices_by_site, hour_start, rng)
        fname = os.path.join(events_dir, f"events_{hour_start:%Y%m%d_%H%M}.json")
        with open(fname, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        total += len(events)
        n += 1
        print(f"batch {n}: {len(events):,} events -> {fname}")
        if args.interval:
            time.sleep(args.interval)

    print(f"done: {len(sites)} sites, {len(devices)} devices, {total:,} events")


if __name__ == "__main__":
    main()
