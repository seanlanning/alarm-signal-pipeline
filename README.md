# Alarm Signal Streaming Pipeline

A streaming data pipeline on **Databricks** that ingests security-panel telemetry (door contacts, motion, glass break, smoke, keypad, battery, comms) and turns it into answers a security operations team needs:

- **Which devices need a battery soon?** Projects days until 15% from each device's drain rate
- **Which alarms were likely false?** Flags alarms that were cancelled fast with no entry, plus sites with repeat false alarms
- **Which panels keep dropping comms?** Measures failure frequency and outage length per panel

> All data is **synthetic**, produced by a generator I wrote. It's modeled on real-world alarm system behavior from my experience as a low-voltage security technician. No real customer or company data is used.

## Architecture

```
generate_alarm_events.py ──► Volume (JSON batches)
                                   │  Auto Loader (streaming, availableNow)
                                   ▼
                           BRONZE  bronze_events            raw strings, source file + ingest time
                                   │  foreachBatch: clean → validate → MERGE on event_id
                                   ▼
                           SILVER  silver_events            typed, deduped
                                   silver_quarantine        bad rows kept with a reason
                                   silver_sites / devices   reference data
                                   │  SQL
                                   ▼
                           GOLD    gold_device_battery_health
                                   gold_false_alarms / gold_site_false_alarm_summary
                                   gold_panel_comm_health
```

## What the generator simulates
- 200 sites (residential and commercial across the Twin Cities metro), about 2,000 devices
- Daily routines: arming at night and in the morning, disarming on entry, door and motion activity
- **False alarm pattern:** armed system, motion trips with no door entry, user cancels within about 90 seconds
- **Real alarm pattern (rare):** armed away, door opens, no disarm, dispatch
- Battery drain (with some fast-draining "bad" devices), low-battery signals, replacements
- A few problem panels with frequent comm failures
- **Dirty data on purpose:** missing IDs, impossible battery values (-1, 255), mixed timestamp formats, inconsistent casing, and about 1% duplicate re-sends

## Data engineering concepts shown
Auto Loader incremental ingestion · Medallion architecture · Schema-on-read bronze · Data quality rules and quarantine · Idempotent `MERGE` dedupe · Structured Streaming checkpoints · Window functions (`LEAD`) · Regression-based forecasting in SQL (`REGR_SLOPE`) · Event-correlation joins

## Sample results (48 simulated hours)
| Metric | Value |
|---|---|
| Raw events ingested | 58,819 |
| Quarantined rows | 576 |
| Clean events in silver | 57,691 |
| Devices needing a battery | 214 |
| Alarms flagged as likely false | ~70% |
| Panels needing a comm service check | 8 |

## Run it (Databricks Free Edition)
1. Push this repo to GitHub.
2. In Databricks: **Workspace → Create → Git folder** → paste the repo URL.
3. Open `alarm_pipeline` and click **Run all** (serverless compute is fine).
4. To simulate new data arriving, change `--start` in cell 1 to a later date and run all again. Auto Loader only processes the new files.

Run the generator locally:
```bash
python generate_alarm_events.py --out ./landing --sites 200 --batches 48
python generate_alarm_events.py --out ./landing --batches 0 --interval 10   # continuous stream
```

## Next steps
- Databricks AI/BI dashboard on the gold tables
- Schedule as a Databricks Job (hourly)
- Rewrite as a Lakeflow Declarative Pipeline with expectations
- Join with public weather data (do storms drive comm failures?)
