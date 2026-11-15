# Databricks notebook source
# MAGIC %md
# MAGIC # Alarm Signal Streaming Pipeline
# MAGIC Synthetic security-panel telemetry → **Bronze** (raw, Auto Loader) → **Silver** (cleaned, deduped) → **Gold** (business answers).
# MAGIC
# MAGIC **Gold tables answer:**
# MAGIC 1. Which devices need a battery soon? (`gold_device_battery_health`)
# MAGIC 2. Which alarms were likely false, and which sites keep having them? (`gold_false_alarms`, `gold_site_false_alarm_summary`)
# MAGIC 3. Which panels keep dropping communication? (`gold_panel_comm_health`)
# MAGIC
# MAGIC _All data is synthetic. Modeled on real-world alarm system behavior._

# COMMAND ----------

# MAGIC %md ## 0. Setup

# COMMAND ----------

CATALOG = "workspace"   # Databricks Free Edition default catalog
SCHEMA  = "alarm"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

LANDING     = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
EVENTS_PATH = f"{LANDING}/events"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# MAGIC %md ## 1. Generate synthetic events
# MAGIC Runs `generate_alarm_events.py` (same folder) and writes hourly JSON batches into the landing volume.
# MAGIC Re-run this cell with a later `--start` to simulate new data arriving, then re-run the pipeline: Auto Loader only picks up the new files.

# COMMAND ----------

import sys
import generate_alarm_events as gen

sys.argv = ["gen", "--out", LANDING, "--sites", "200", "--batches", "48", "--start", "2026-11-01T00:00:00"]
gen.main()

# COMMAND ----------

# MAGIC %md ## 2. Bronze: raw ingest with Auto Loader
# MAGIC Everything lands as strings, exactly as sent. No cleaning here, so we can always replay.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "false")          # keep raw strings
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_schema")
    .load(EVENTS_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(bronze_stream.writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
    .trigger(availableNow=True)                              # process what's new, then stop
    .toTable("bronze_events")
    .awaitTermination())

display(spark.sql("SELECT COUNT(*) AS bronze_rows FROM bronze_events"))

# COMMAND ----------

# MAGIC %md ## 3. Silver: clean, validate, dedupe
# MAGIC - Normalize `event_type` casing
# MAGIC - Parse two timestamp formats
# MAGIC - Null out impossible battery readings (<0 or >100)
# MAGIC - Send unusable rows to a **quarantine** table instead of silently dropping them
# MAGIC - Dedupe panel re-sends on `event_id` with an idempotent `MERGE`

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_events (
  event_id STRING, site_id STRING, panel_id STRING, device_id STRING, device_type STRING,
  zone INT, event_type STRING, armed_state STRING, battery_pct DOUBLE, signal_dbm INT,
  firmware STRING, event_ts TIMESTAMP, _ingested_at TIMESTAMP
)""")

def clean(df):
    return (df
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("event_ts", F.expr(
            "coalesce(try_to_timestamp(event_ts), try_to_timestamp(event_ts, 'yyyy-MM-dd HH:mm:ss'))"))
        .withColumn("battery_pct", F.expr("try_cast(battery_pct AS DOUBLE)"))
        .withColumn("battery_pct", F.when(F.col("battery_pct").between(0, 100), F.col("battery_pct")))
        .withColumn("zone", F.expr("try_cast(zone AS INT)"))
        .withColumn("signal_dbm", F.expr("try_cast(signal_dbm AS INT)"))
    )

def upsert_silver(batch_df, batch_id):
    cleaned = clean(batch_df)
    bad = F.col("event_ts").isNull() | (F.col("device_id").isNull() & (F.col("device_type") != "panel"))

    (cleaned.filter(bad)
        .withColumn("_quarantine_reason", F.when(F.col("event_ts").isNull(), "bad_timestamp")
                                           .otherwise("missing_device_id"))
        .write.mode("append").saveAsTable("silver_quarantine"))

    good = (cleaned.filter(~bad)
        .dropDuplicates(["event_id"])
        .select("event_id", "site_id", "panel_id", "device_id", "device_type", "zone", "event_type",
                "armed_state", "battery_pct", "signal_dbm", "firmware", "event_ts", "_ingested_at"))

    (DeltaTable.forName(batch_df.sparkSession, "silver_events").alias("t")
        .merge(good.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute())

(spark.readStream.table("bronze_events")
    .writeStream
    .foreachBatch(upsert_silver)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver")
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

# reference data
spark.read.option("header", True).csv(f"{LANDING}/sites.csv").write.mode("overwrite").saveAsTable("silver_sites")
spark.read.option("header", True).csv(f"{LANDING}/devices.csv").write.mode("overwrite").saveAsTable("silver_devices")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality check: what got cleaned / quarantined
# MAGIC SELECT 'bronze'     AS layer, COUNT(*) AS rows FROM bronze_events
# MAGIC UNION ALL SELECT 'silver',     COUNT(*) FROM silver_events
# MAGIC UNION ALL SELECT 'quarantine', COUNT(*) FROM silver_quarantine
# MAGIC UNION ALL SELECT 'duplicates removed',
# MAGIC   (SELECT COUNT(*) FROM bronze_events) - (SELECT COUNT(*) FROM silver_events) - (SELECT COUNT(*) FROM silver_quarantine)

# COMMAND ----------

# MAGIC %md ## 4. Gold: business tables

# COMMAND ----------

# MAGIC %md ### 4a. Device battery health
# MAGIC Uses battery readings since the last replacement, fits a drain rate (`regr_slope`), and projects days until 15%.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_device_battery_health AS
# MAGIC WITH last_replace AS (
# MAGIC   SELECT device_id, MAX(event_ts) AS replaced_at
# MAGIC   FROM silver_events WHERE event_type = 'battery_replaced' GROUP BY device_id
# MAGIC ),
# MAGIC readings AS (
# MAGIC   SELECT e.* FROM silver_events e
# MAGIC   LEFT JOIN last_replace r USING (device_id)
# MAGIC   WHERE e.battery_pct IS NOT NULL AND e.device_type <> 'panel'
# MAGIC     AND (r.replaced_at IS NULL OR e.event_ts >= r.replaced_at)
# MAGIC ),
# MAGIC stats AS (
# MAGIC   SELECT device_id, site_id, device_type, zone,
# MAGIC     MAX_BY(battery_pct, event_ts)                                AS battery_now,
# MAGIC     MAX(event_ts)                                                AS last_reading,
# MAGIC     -REGR_SLOPE(battery_pct, UNIX_TIMESTAMP(event_ts)) * 86400   AS drain_pct_per_day,
# MAGIC     COUNT(*)                                                     AS readings
# MAGIC   FROM readings GROUP BY device_id, site_id, device_type, zone
# MAGIC )
# MAGIC SELECT s.*, st.city,
# MAGIC   ROUND(CASE WHEN drain_pct_per_day > 0 THEN (battery_now - 15) / drain_pct_per_day END, 1) AS days_to_15pct,
# MAGIC   CASE
# MAGIC     WHEN battery_now < 20 THEN 'REPLACE NOW'
# MAGIC     WHEN drain_pct_per_day > 0 AND (battery_now - 15) / drain_pct_per_day < 30 THEN 'REPLACE SOON'
# MAGIC     ELSE 'OK'
# MAGIC   END AS battery_status
# MAGIC FROM stats s JOIN silver_sites st USING (site_id)
# MAGIC WHERE readings >= 3;
# MAGIC
# MAGIC SELECT battery_status, COUNT(*) AS devices FROM gold_device_battery_health GROUP BY 1 ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %md ### 4b. False alarm detection
# MAGIC **Likely false alarm** = an alarm that was cancelled at the keypad within 2 minutes **and** had no door opening in the 2 minutes before it (nobody actually came in).
# MAGIC Many cities fine repeat false alarms, so sites with 2+ are worth a callback or a service visit.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_false_alarms AS
# MAGIC SELECT a.event_id, a.site_id, a.device_id, a.device_type, a.zone, a.event_ts AS alarm_ts,
# MAGIC   MIN(c.event_ts) AS cancel_ts,
# MAGIC   TIMESTAMPDIFF(SECOND, a.event_ts, MIN(c.event_ts)) AS seconds_to_cancel,
# MAGIC   COUNT(d.event_id) > 0 AS door_opened_before,
# MAGIC   MIN(c.event_ts) IS NOT NULL AND COUNT(d.event_id) = 0 AS likely_false_alarm
# MAGIC FROM silver_events a
# MAGIC LEFT JOIN silver_events c
# MAGIC   ON c.site_id = a.site_id AND c.event_type = 'alarm_cancel'
# MAGIC  AND c.event_ts BETWEEN a.event_ts AND a.event_ts + INTERVAL 2 MINUTES
# MAGIC LEFT JOIN silver_events d
# MAGIC   ON d.site_id = a.site_id AND d.event_type = 'open'
# MAGIC  AND d.event_ts BETWEEN a.event_ts - INTERVAL 2 MINUTES AND a.event_ts
# MAGIC WHERE a.event_type = 'alarm'
# MAGIC GROUP BY a.event_id, a.site_id, a.device_id, a.device_type, a.zone, a.event_ts;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_site_false_alarm_summary AS
# MAGIC SELECT f.site_id, s.city, s.site_type,
# MAGIC   COUNT(*)                                   AS total_alarms,
# MAGIC   SUM(CAST(likely_false_alarm AS INT))       AS likely_false_alarms,
# MAGIC   ROUND(AVG(CASE WHEN likely_false_alarm THEN seconds_to_cancel END), 0) AS avg_seconds_to_cancel,
# MAGIC   MODE(CASE WHEN likely_false_alarm THEN device_type END)                AS top_trigger_device,
# MAGIC   SUM(CAST(likely_false_alarm AS INT)) >= 2  AS needs_followup
# MAGIC FROM gold_false_alarms f JOIN silver_sites s USING (site_id)
# MAGIC GROUP BY f.site_id, s.city, s.site_type;
# MAGIC
# MAGIC SELECT * FROM gold_site_false_alarm_summary ORDER BY likely_false_alarms DESC LIMIT 15;

# COMMAND ----------

# MAGIC %md ### 4c. Panel communication health
# MAGIC Pairs each `comm_fail` with the next `comm_restore` to measure outage length. Panels with frequent drops may need a cellular module, antenna or power check.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_panel_comm_health AS
# MAGIC WITH comm AS (
# MAGIC   SELECT panel_id, site_id, firmware, event_type, event_ts,
# MAGIC     LEAD(event_type) OVER (PARTITION BY panel_id ORDER BY event_ts) AS next_type,
# MAGIC     LEAD(event_ts)   OVER (PARTITION BY panel_id ORDER BY event_ts) AS next_ts
# MAGIC   FROM silver_events WHERE event_type IN ('comm_fail', 'comm_restore')
# MAGIC ),
# MAGIC outages AS (
# MAGIC   SELECT panel_id, site_id, firmware, event_ts AS fail_ts,
# MAGIC     CASE WHEN next_type = 'comm_restore' THEN TIMESTAMPDIFF(SECOND, event_ts, next_ts) / 60.0 END AS outage_min
# MAGIC   FROM comm WHERE event_type = 'comm_fail'
# MAGIC )
# MAGIC SELECT o.panel_id, o.site_id, s.city, o.firmware,
# MAGIC   COUNT(*)                         AS comm_failures,
# MAGIC   ROUND(AVG(outage_min), 1)        AS avg_outage_min,
# MAGIC   ROUND(SUM(outage_min), 1)        AS total_outage_min,
# MAGIC   MAX(fail_ts)                     AS last_failure,
# MAGIC   COUNT(*) >= 3                    AS needs_service
# MAGIC FROM outages o JOIN silver_sites s USING (site_id)
# MAGIC GROUP BY o.panel_id, o.site_id, s.city, o.firmware;
# MAGIC
# MAGIC SELECT * FROM gold_panel_comm_health ORDER BY comm_failures DESC LIMIT 15;

# COMMAND ----------

# MAGIC %md ## 5. Fleet KPIs (one-line summary for a dashboard)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(DISTINCT site_id) FROM silver_sites)                                   AS sites,
# MAGIC   (SELECT COUNT(*) FROM silver_events)                                                 AS events_processed,
# MAGIC   (SELECT COUNT(*) FROM gold_device_battery_health WHERE battery_status <> 'OK')       AS batteries_to_replace,
# MAGIC   (SELECT ROUND(100 * AVG(CAST(likely_false_alarm AS INT)), 1) FROM gold_false_alarms) AS false_alarm_pct,
# MAGIC   (SELECT COUNT(*) FROM gold_site_false_alarm_summary WHERE needs_followup)             AS sites_needing_followup,
# MAGIC   (SELECT COUNT(*) FROM gold_panel_comm_health WHERE needs_service)                    AS panels_needing_service
