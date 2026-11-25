"""UrbanPulse Gold Layer aggregation and ML feature panel builder using PySpark.

Reads the verified Silver Layer v2 Parquet directly from HDFS in Spark local mode,
separates operational pickup demand from net settled revenue/validated trip metrics,
builds 5 Gold Layer Parquet datasets into a staging HDFS directory, performs full
read-back reconciliation, missing-slot evaluation, and automated zero-leakage
verification, and exports structured metrics to `reports/gold_layer_summary_024.json`.

Datasets produced:
  1. gold_hourly_demand_024       (720 rows: complete Sep 2026 hourly series)
  2. gold_zone_demand_024         (261 rows: zone demand, tiers & economics)
  3. gold_temporal_patterns_024   (168 rows: 7x24 day-of-week x hour & weekday/weekend)
  4. gold_zone_hourly_demand_024  (6,264 rows: 261 zones x 24 hours profile)
  5. gold_demand_features_024     (187,920 rows: 261 zones x 720h leakage-free ML panel)

Strictly read-only on Silver Layer; no ML training and no dashboard edits.
"""

from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

HDFS_SILVER_INPUT_024 = (
    "hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024"
)
HDFS_GOLD_LIVE_ROOT_024 = "hdfs://localhost:9000/urbanpulse_024/analytics"
DEFAULT_GOLD_JSON_024 = Path("reports/gold_layer_summary_024.json")

GRID_START_TS_024 = "2026-09-01 00:00:00"
GRID_END_TS_024 = "2026-09-30 23:00:00"
EXPECTED_HOURS_SEP_2026_024 = 720
EXPECTED_DAYS_SEP_2026_024 = 30
LAG_7D_HOURS_024 = 168

GOLD_DATASET_NAMES_024 = [
    "gold_hourly_demand_024",
    "gold_zone_demand_024",
    "gold_temporal_patterns_024",
    "gold_zone_hourly_demand_024",
    "gold_demand_features_024",
]
ALLOWED_SOURCE_COLS_024 = {"PULocationID"}


def build_spark_session_024() -> SparkSession:
    """Create a SparkSession in local[*] mode without touching shared event logs."""
    return (
        SparkSession.builder.appName("UrbanPulse_Gold_Layer_024")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def fmt_val_024(val_024: Any) -> Any:
    """Serialize Spark/Python scalars cleanly for JSON output."""
    if val_024 is None:
        return None
    if isinstance(val_024, datetime):
        return val_024.isoformat(sep=" ")
    if hasattr(val_024, "isoformat"):
        return val_024.isoformat()
    if isinstance(val_024, float):
        return round(val_024, 4)
    return val_024


def build_complete_hourly_grid_024(spark_024: SparkSession) -> DataFrame:
    """Generate all 720 consecutive hourly timestamp_ntz slots for September 2026."""
    grid_024 = spark_024.sql(
        f"""
        SELECT explode(
            sequence(
                CAST('{GRID_START_TS_024}' AS TIMESTAMP_NTZ),
                CAST('{GRID_END_TS_024}' AS TIMESTAMP_NTZ),
                interval 1 hour
            )
        ) AS pickup_ts_hour_024
        """
    )

    dow_iso_024 = (
        (F.dayofweek(F.col("pickup_ts_hour_024")) + F.lit(5)) % F.lit(7)
    ).cast("int")
    day_name_024 = (
        F.when(dow_iso_024 == 0, F.lit("Senin"))
        .when(dow_iso_024 == 1, F.lit("Selasa"))
        .when(dow_iso_024 == 2, F.lit("Rabu"))
        .when(dow_iso_024 == 3, F.lit("Kamis"))
        .when(dow_iso_024 == 4, F.lit("Jumat"))
        .when(dow_iso_024 == 5, F.lit("Sabtu"))
        .otherwise(F.lit("Minggu"))
    )
    hr_024 = F.hour(F.col("pickup_ts_hour_024")).cast("int")
    bucket_024 = (
        F.when(hr_024 < 6, F.lit("Dini Hari (00-05)"))
        .when(hr_024 < 12, F.lit("Pagi (06-11)"))
        .when(hr_024 < 18, F.lit("Siang (12-17)"))
        .otherwise(F.lit("Malam (18-23)"))
    )

    return (
        grid_024.withColumn("pickup_date_024", F.to_date("pickup_ts_hour_024"))
        .withColumn("pickup_hour_024", hr_024)
        .withColumn("day_of_week_024", dow_iso_024)
        .withColumn("day_name_024", day_name_024)
        .withColumn("is_weekend_024", dow_iso_024.isin(5, 6))
        .withColumn(
            "day_type_024",
            F.when(dow_iso_024.isin(5, 6), F.lit("Weekend")).otherwise(F.lit("Weekday")),
        )
        .withColumn("period_024", F.date_format("pickup_ts_hour_024", "yyyy-MM"))
        .withColumn("time_of_day_bucket_024", bucket_024)
    )


def prepare_silver_base_024(silver_df_024: DataFrame) -> DataFrame:
    """Select needed Silver v2 columns and add exact hour timestamp + settled revenue expressions."""
    settled_cond_024 = ~F.col("has_negative_reversal_pair_024")
    return silver_df_024.select(
        "PULocationID",
        "pickup_date_024",
        "pickup_hour_024",
        "day_of_week_024",
        "day_name_024",
        "is_weekend_024",
        "period_024",
        "time_of_day_bucket_024",
        "is_flex_fare_024",
        "is_known_pickup_zone_024",
        "has_negative_reversal_pair_024",
        "trip_distance_valid_mi_024",
        "trip_duration_valid_min_024",
        "avg_speed_mph_024",
        "fare_amount_valid_usd_024",
        "total_amount_valid_usd_024",
        "tpep_pickup_datetime",
    ).withColumn(
        "pickup_ts_hour_024",
        F.date_trunc("hour", F.col("tpep_pickup_datetime")),
    ).withColumn(
        "settled_total_usd_024",
        F.when(settled_cond_024, F.col("total_amount_valid_usd_024")),
    ).withColumn(
        "settled_fare_usd_024",
        F.when(settled_cond_024, F.col("fare_amount_valid_usd_024")),
    ).withColumn(
        "is_non_reversed_024", settled_cond_024
    )


def build_gold_hourly_demand_024(
    base_df_024: DataFrame, hourly_grid_024: DataFrame
) -> DataFrame:
    """Build city-wide hourly demand & settled revenue time series (720 rows)."""
    agg_024 = base_df_024.groupBy("pickup_ts_hour_024").agg(
        F.count("*").alias("pickup_count_024"),
        F.sum(F.when(F.col("is_non_reversed_024"), 1).otherwise(0)).alias(
            "non_reversed_pickup_count_024"
        ),
        F.sum(F.when(F.col("is_flex_fare_024"), 1).otherwise(0)).alias(
            "flex_fare_pickup_count_024"
        ),
        F.countDistinct("PULocationID").alias("active_zone_count_024"),
        F.round(F.sum("settled_total_usd_024"), 2).alias(
            "net_settled_revenue_usd_024"
        ),
        F.round(F.avg("settled_total_usd_024"), 4).alias("avg_settled_total_usd_024"),
        F.round(F.avg("settled_fare_usd_024"), 4).alias("avg_settled_fare_usd_024"),
        F.round(F.avg("trip_distance_valid_mi_024"), 4).alias(
            "avg_valid_distance_mi_024"
        ),
        F.round(F.avg("trip_duration_valid_min_024"), 4).alias(
            "avg_valid_duration_min_024"
        ),
        F.round(F.avg("avg_speed_mph_024"), 4).alias("avg_valid_speed_mph_024"),
    )

    return (
        hourly_grid_024.join(agg_024, on="pickup_ts_hour_024", how="left")
        .withColumn("is_observed_hour_024", F.col("pickup_count_024").isNotNull())
        .withColumn("pickup_count_024", F.coalesce(F.col("pickup_count_024"), F.lit(0)))
        .withColumn(
            "non_reversed_pickup_count_024",
            F.coalesce(F.col("non_reversed_pickup_count_024"), F.lit(0)),
        )
        .withColumn(
            "flex_fare_pickup_count_024",
            F.coalesce(F.col("flex_fare_pickup_count_024"), F.lit(0)),
        )
        .withColumn(
            "flex_fare_share_pct_024",
            F.when(
                F.col("pickup_count_024") > 0,
                F.round(
                    (F.col("flex_fare_pickup_count_024") / F.col("pickup_count_024"))
                    * F.lit(100.0),
                    4,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "active_zone_count_024",
            F.coalesce(F.col("active_zone_count_024"), F.lit(0)),
        )
        .withColumn(
            "net_settled_revenue_usd_024",
            F.coalesce(F.col("net_settled_revenue_usd_024"), F.lit(0.0)),
        )
        .orderBy("pickup_ts_hour_024")
    )


def build_gold_zone_hourly_demand_024(
    base_df_024: DataFrame, active_zones_024: DataFrame
) -> DataFrame:
    """Build complete zone x 24-hour spatial-temporal profile (261 x 24 = 6,264 rows)."""
    hours_24_024 = active_zones_024.sparkSession.range(0, 24).select(
        F.col("id").cast("int").alias("pickup_hour_024")
    )
    zh_grid_024 = active_zones_024.crossJoin(hours_24_024)

    # September 2026 has exactly 22 weekdays and 8 weekend days (30 days total)
    weekday_days_024 = 22.0
    weekend_days_024 = 8.0

    zh_agg_024 = base_df_024.groupBy("PULocationID", "pickup_hour_024").agg(
        F.count("*").alias("pickup_count_024"),
        F.sum(F.when(F.col("is_non_reversed_024"), 1).otherwise(0)).alias(
            "non_reversed_pickup_count_024"
        ),
        F.sum(F.when(~F.col("is_weekend_024"), 1).otherwise(0)).alias(
            "weekday_pickup_count_024"
        ),
        F.sum(F.when(F.col("is_weekend_024"), 1).otherwise(0)).alias(
            "weekend_pickup_count_024"
        ),
        F.countDistinct("pickup_date_024").alias("active_days_count_024"),
        F.round(F.sum("settled_total_usd_024"), 2).alias(
            "net_settled_revenue_usd_024"
        ),
        F.round(F.avg("settled_total_usd_024"), 4).alias("avg_settled_total_usd_024"),
        F.round(F.avg("trip_distance_valid_mi_024"), 4).alias(
            "avg_valid_distance_mi_024"
        ),
        F.round(F.avg("trip_duration_valid_min_024"), 4).alias(
            "avg_valid_duration_min_024"
        ),
    )

    joined_024 = (
        zh_grid_024.join(
            zh_agg_024, on=["PULocationID", "pickup_hour_024"], how="left"
        )
        .withColumn("is_observed_zone_hour_024", F.col("pickup_count_024").isNotNull())
        .withColumn("pickup_count_024", F.coalesce(F.col("pickup_count_024"), F.lit(0)))
        .withColumn(
            "non_reversed_pickup_count_024",
            F.coalesce(F.col("non_reversed_pickup_count_024"), F.lit(0)),
        )
        .withColumn(
            "weekday_pickup_count_024",
            F.coalesce(F.col("weekday_pickup_count_024"), F.lit(0)),
        )
        .withColumn(
            "weekend_pickup_count_024",
            F.coalesce(F.col("weekend_pickup_count_024"), F.lit(0)),
        )
        .withColumn(
            "active_days_count_024",
            F.coalesce(F.col("active_days_count_024"), F.lit(0)),
        )
        .withColumn(
            "net_settled_revenue_usd_024",
            F.coalesce(F.col("net_settled_revenue_usd_024"), F.lit(0.0)),
        )
        .withColumn(
            "avg_daily_pickups_at_hour_024",
            F.round(F.col("pickup_count_024") / F.lit(float(EXPECTED_DAYS_SEP_2026_024)), 4),
        )
        .withColumn(
            "avg_weekday_pickups_at_hour_024",
            F.round(F.col("weekday_pickup_count_024") / F.lit(weekday_days_024), 4),
        )
        .withColumn(
            "avg_weekend_pickups_at_hour_024",
            F.round(F.col("weekend_pickup_count_024") / F.lit(weekend_days_024), 4),
        )
    )

    zone_win_024 = Window.partitionBy("PULocationID")
    zone_rank_win_024 = Window.partitionBy("PULocationID").orderBy(
        F.desc("pickup_count_024"), F.asc("pickup_hour_024")
    )
    return (
        joined_024.withColumn(
            "zone_total_pickups_024", F.sum("pickup_count_024").over(zone_win_024)
        )
        .withColumn(
            "within_zone_hour_share_pct_024",
            F.round(
                (F.col("pickup_count_024") / F.col("zone_total_pickups_024"))
                * F.lit(100.0),
                4,
            ),
        )
        .withColumn("hour_rank_within_zone_024", F.row_number().over(zone_rank_win_024))
        .drop("zone_total_pickups_024")
        .orderBy("PULocationID", "pickup_hour_024")
    )


def build_gold_zone_demand_024(
    base_df_024: DataFrame, zone_hourly_df_024: DataFrame, total_silver_rows_024: int
) -> DataFrame:
    """Build zone-level demand, peak/quietest hours, weekend share, and settled economics."""
    zone_agg_024 = base_df_024.groupBy("PULocationID", "is_known_pickup_zone_024").agg(
        F.lit("2026-09").alias("period_024"),
        F.count("*").alias("pickup_count_024"),
        F.sum(F.when(F.col("is_non_reversed_024"), 1).otherwise(0)).alias(
            "non_reversed_pickup_count_024"
        ),
        F.countDistinct("pickup_ts_hour_024").alias("active_hours_count_024"),
        F.sum(F.when(~F.col("is_weekend_024"), 1).otherwise(0)).alias(
            "weekday_pickup_count_024"
        ),
        F.sum(F.when(F.col("is_weekend_024"), 1).otherwise(0)).alias(
            "weekend_pickup_count_024"
        ),
        F.sum(F.when(F.col("is_flex_fare_024"), 1).otherwise(0)).alias(
            "flex_fare_pickup_count_024"
        ),
        F.round(F.sum("settled_total_usd_024"), 2).alias(
            "net_settled_revenue_usd_024"
        ),
        F.round(F.avg("settled_total_usd_024"), 4).alias("avg_settled_total_usd_024"),
        F.round(F.avg("settled_fare_usd_024"), 4).alias("avg_settled_fare_usd_024"),
        F.round(F.avg("trip_distance_valid_mi_024"), 4).alias(
            "avg_valid_distance_mi_024"
        ),
        F.round(F.avg("trip_duration_valid_min_024"), 4).alias(
            "avg_valid_duration_min_024"
        ),
        F.round(F.avg("avg_speed_mph_024"), 4).alias("avg_valid_speed_mph_024"),
    )

    peak_w_024 = Window.partitionBy("PULocationID").orderBy(
        F.desc("pickup_count_024"), F.asc("pickup_hour_024")
    )
    quiet_w_024 = Window.partitionBy("PULocationID").orderBy(
        F.asc("pickup_count_024"), F.asc("pickup_hour_024")
    )
    peak_hr_024 = (
        zone_hourly_df_024.withColumn("_rn", F.row_number().over(peak_w_024))
        .filter(F.col("_rn") == 1)
        .select(
            "PULocationID",
            F.col("pickup_hour_024").alias("peak_hour_024"),
            F.col("pickup_count_024").alias("peak_hour_pickups_024"),
        )
    )
    quiet_hr_024 = (
        zone_hourly_df_024.withColumn("_rn", F.row_number().over(quiet_w_024))
        .filter(F.col("_rn") == 1)
        .select(
            "PULocationID",
            F.col("pickup_hour_024").alias("quietest_hour_024"),
            F.col("pickup_count_024").alias("quietest_hour_pickups_024"),
        )
    )

    rank_w_024 = Window.orderBy(F.desc("pickup_count_024"), F.asc("PULocationID"))
    return (
        zone_agg_024.join(peak_hr_024, on="PULocationID", how="left")
        .join(quiet_hr_024, on="PULocationID", how="left")
        .withColumn("zone_demand_rank_024", F.row_number().over(rank_w_024))
        .withColumn(
            "pickup_share_pct_024",
            F.round(
                (F.col("pickup_count_024") / F.lit(float(total_silver_rows_024)))
                * F.lit(100.0),
                4,
            ),
        )
        .withColumn(
            "avg_daily_pickups_024",
            F.round(
                F.col("pickup_count_024") / F.lit(float(EXPECTED_DAYS_SEP_2026_024)), 4
            ),
        )
        .withColumn(
            "avg_hourly_pickups_024",
            F.round(
                F.col("pickup_count_024") / F.lit(float(EXPECTED_HOURS_SEP_2026_024)), 4
            ),
        )
        .withColumn(
            "active_hours_coverage_pct_024",
            F.round(
                (F.col("active_hours_count_024") / F.lit(float(EXPECTED_HOURS_SEP_2026_024)))
                * F.lit(100.0),
                4,
            ),
        )
        .withColumn(
            "weekend_share_pct_024",
            F.round(
                (F.col("weekend_pickup_count_024") / F.col("pickup_count_024"))
                * F.lit(100.0),
                4,
            ),
        )
        .withColumn(
            "flex_fare_share_pct_024",
            F.round(
                (F.col("flex_fare_pickup_count_024") / F.col("pickup_count_024"))
                * F.lit(100.0),
                4,
            ),
        )
        .withColumn(
            "demand_tier_024",
            F.when(F.col("zone_demand_rank_024") <= 10, F.lit("Top 10"))
            .when(F.col("zone_demand_rank_024") <= 50, F.lit("High (11-50)"))
            .when(F.col("zone_demand_rank_024") <= 150, F.lit("Medium (51-150)"))
            .otherwise(F.lit("Low (>150)")),
        )
        .orderBy("zone_demand_rank_024")
    )


def build_gold_temporal_patterns_024(
    base_df_024: DataFrame, hourly_grid_024: DataFrame, total_silver_rows_024: int
) -> DataFrame:
    """Build 7x24 Day-of-Week x Hour & Weekday/Weekend temporal patterns (168 rows)."""
    # Exact calendar occurrence count per (day_of_week, hour) in Sep 2026 (4 or 5)
    cal_slots_024 = hourly_grid_024.groupBy(
        "day_of_week_024",
        "day_name_024",
        "is_weekend_024",
        "day_type_024",
        "pickup_hour_024",
        "time_of_day_bucket_024",
    ).agg(F.count("*").cast("int").alias("num_calendar_days_024"))

    temp_agg_024 = base_df_024.groupBy("day_of_week_024", "pickup_hour_024").agg(
        F.count("*").alias("total_pickups_024"),
        F.sum(F.when(F.col("is_non_reversed_024"), 1).otherwise(0)).alias(
            "non_reversed_total_pickups_024"
        ),
        F.sum(F.when(F.col("is_flex_fare_024"), 1).otherwise(0)).alias(
            "flex_fare_pickups_024"
        ),
        F.round(F.sum("settled_total_usd_024"), 2).alias(
            "net_settled_revenue_usd_024"
        ),
        F.round(F.avg("settled_total_usd_024"), 4).alias("avg_settled_total_usd_024"),
        F.round(F.avg("trip_distance_valid_mi_024"), 4).alias(
            "avg_valid_distance_mi_024"
        ),
        F.round(F.avg("trip_duration_valid_min_024"), 4).alias(
            "avg_valid_duration_min_024"
        ),
        F.round(F.avg("avg_speed_mph_024"), 4).alias("avg_valid_speed_mph_024"),
    )

    return (
        cal_slots_024.join(
            temp_agg_024, on=["day_of_week_024", "pickup_hour_024"], how="left"
        )
        .withColumn("total_pickups_024", F.coalesce(F.col("total_pickups_024"), F.lit(0)))
        .withColumn(
            "non_reversed_total_pickups_024",
            F.coalesce(F.col("non_reversed_total_pickups_024"), F.lit(0)),
        )
        .withColumn(
            "flex_fare_pickups_024",
            F.coalesce(F.col("flex_fare_pickups_024"), F.lit(0)),
        )
        .withColumn(
            "avg_pickups_per_day_slot_024",
            F.round(
                F.col("total_pickups_024") / F.col("num_calendar_days_024").cast("double"),
                4,
            ),
        )
        .withColumn(
            "share_of_total_pickups_pct_024",
            F.round(
                (F.col("total_pickups_024") / F.lit(float(total_silver_rows_024)))
                * F.lit(100.0),
                4,
            ),
        )
        .orderBy("day_of_week_024", "pickup_hour_024")
    )


def build_gold_demand_features_024(
    base_df_024: DataFrame, hourly_grid_024: DataFrame, active_zones_024: DataFrame
) -> DataFrame:
    """Build complete 261x720 leakage-free zone-hour ML feature panel (187,920 rows).

    STRICT ZERO DATA LEAKAGE:
    - All lag and rolling features use strictly past timestamps (t-1 and earlier).
    - Current timestamp `t` (0) is explicitly excluded from rolling windows.
    - No full-month target-encoded zone summaries are included.
    - `is_ml_ready_full_history_024` marks rows with complete 168-hour prior history
      (`2026-09-08 00:00:00` through `2026-09-30 23:00:00`).
    """
    panel_grid_024 = active_zones_024.crossJoin(hourly_grid_024)

    zh_ts_agg_024 = base_df_024.groupBy("PULocationID", "pickup_ts_hour_024").agg(
        F.count("*").alias("pickup_count_024"),
        F.sum(F.when(F.col("is_non_reversed_024"), 1).otherwise(0)).alias(
            "non_reversed_pickup_count_024"
        ),
    )

    panel_024 = (
        panel_grid_024.join(
            zh_ts_agg_024, on=["PULocationID", "pickup_ts_hour_024"], how="left"
        )
        .withColumn("is_observed_in_silver_024", F.col("pickup_count_024").isNotNull())
        .withColumn("pickup_count_024", F.coalesce(F.col("pickup_count_024"), F.lit(0)))
        .withColumn(
            "non_reversed_pickup_count_024",
            F.coalesce(F.col("non_reversed_pickup_count_024"), F.lit(0)),
        )
    )

    # Strictly ordered per-zone windows (1 row = 1 exact hour on the complete grid)
    order_w_024 = Window.partitionBy("PULocationID").orderBy("pickup_ts_hour_024")
    roll_24h_w_024 = order_w_024.rowsBetween(-24, -1)
    roll_7d_w_024 = order_w_024.rowsBetween(-LAG_7D_HOURS_024, -1)

    two_pi_024 = 2.0 * math.pi
    return (
        panel_024.withColumn("_hour_seq_idx_024", F.row_number().over(order_w_024))
        .withColumn(
            "lag_1h_pickup_count_024",
            F.lag("pickup_count_024", 1).over(order_w_024).cast("double"),
        )
        .withColumn(
            "lag_24h_pickup_count_024",
            F.lag("pickup_count_024", 24).over(order_w_024).cast("double"),
        )
        .withColumn(
            "lag_168h_pickup_count_024",
            F.lag("pickup_count_024", LAG_7D_HOURS_024).over(order_w_024).cast("double"),
        )
        .withColumn(
            "rolling_avg_24h_pickup_count_024",
            F.when(
                F.col("_hour_seq_idx_024") > 24,
                F.round(F.avg("pickup_count_024").over(roll_24h_w_024), 4),
            ),
        )
        .withColumn(
            "rolling_avg_7d_pickup_count_024",
            F.when(
                F.col("_hour_seq_idx_024") > LAG_7D_HOURS_024,
                F.round(F.avg("pickup_count_024").over(roll_7d_w_024), 4),
            ),
        )
        .withColumn(
            "rolling_std_7d_pickup_count_024",
            F.when(
                F.col("_hour_seq_idx_024") > LAG_7D_HOURS_024,
                F.round(F.stddev_samp("pickup_count_024").over(roll_7d_w_024), 4),
            ),
        )
        .withColumn(
            "is_rush_hour_024",
            (~F.col("is_weekend_024"))
            & (F.col("pickup_hour_024").isin(7, 8, 9, 16, 17, 18, 19)),
        )
        .withColumn(
            "hour_sin_024",
            F.round(
                F.sin(F.col("pickup_hour_024") * F.lit(two_pi_024 / 24.0)), 6
            ),
        )
        .withColumn(
            "hour_cos_024",
            F.round(
                F.cos(F.col("pickup_hour_024") * F.lit(two_pi_024 / 24.0)), 6
            ),
        )
        .withColumn(
            "dow_sin_024",
            F.round(F.sin(F.col("day_of_week_024") * F.lit(two_pi_024 / 7.0)), 6),
        )
        .withColumn(
            "dow_cos_024",
            F.round(F.cos(F.col("day_of_week_024") * F.lit(two_pi_024 / 7.0)), 6),
        )
        .withColumn(
            "is_ml_ready_full_history_024",
            F.col("_hour_seq_idx_024") > LAG_7D_HOURS_024,
        )
        .drop("_hour_seq_idx_024")
        .orderBy("PULocationID", "pickup_ts_hour_024")
    )


def inspect_gold_dataset_024(
    df_024: DataFrame,
    name_024: str,
    pickup_col_024: str,
    expected_total_pickups_024: int,
    ts_col_024: str | None = None,
) -> dict[str, Any]:
    """Read-back verification: count, schema, `_024` naming, nulls, range, pickup reconciliation."""
    row_count_024 = int(df_024.count())
    schema_list_024 = [
        {
            "column_name": f.name,
            "data_type": f.dataType.simpleString(),
            "nullable": f.nullable,
        }
        for f in df_024.schema.fields
    ]
    naming_valid_024 = all(
        (f.name in ALLOWED_SOURCE_COLS_024) or f.name.endswith("_024")
        for f in df_024.schema.fields
    )
    assert naming_valid_024, f"Column naming convention violation in {name_024}!"

    agg_exprs_024: list[Any] = [
        F.sum(F.col(pickup_col_024)).alias("_sum_pickups_024")
    ]
    if ts_col_024:
        agg_exprs_024.extend(
            [
                F.min(F.col(ts_col_024)).alias("_min_ts_024"),
                F.max(F.col(ts_col_024)).alias("_max_ts_024"),
            ]
        )
    for f in df_024.schema.fields:
        agg_exprs_024.append(
            F.sum(F.when(F.col(f"`{f.name}`").isNull(), 1).otherwise(0)).alias(
                f"null__{f.name}"
            )
        )

    res_024 = df_024.agg(*agg_exprs_024).collect()[0].asDict()
    sum_pickups_024 = int(res_024["_sum_pickups_024"] or 0)
    assert sum_pickups_024 == expected_total_pickups_024, (
        f"Reconciliation mismatch in {name_024}: "
        f"{sum_pickups_024} != {expected_total_pickups_024}"
    )

    null_counts_024 = {
        f.name: int(res_024[f"null__{f.name}"] or 0)
        for f in df_024.schema.fields
        if int(res_024[f"null__{f.name}"] or 0) > 0
    }

    info_024: dict[str, Any] = {
        "dataset_name_024": name_024,
        "readback_verified_024": True,
        "row_count_024": row_count_024,
        "column_count_024": len(schema_list_024),
        "naming_convention_024_valid": naming_valid_024,
        "pickup_sum_reconciled_024": sum_pickups_024,
        "reconciles_with_silver_v2_024": sum_pickups_024 == expected_total_pickups_024,
        "columns_with_nulls_024": null_counts_024,
        "schema_024": schema_list_024,
    }
    if ts_col_024:
        info_024["time_range_024"] = {
            "column_024": ts_col_024,
            "min_024": fmt_val_024(res_024["_min_ts_024"]),
            "max_024": fmt_val_024(res_024["_max_ts_024"]),
        }
    return info_024


def verify_ml_panel_and_leakage_024(
    features_df_024: DataFrame, zone_demand_df_024: DataFrame
) -> dict[str, Any]:
    """Audit missing zone-hour slots and run strict zero-leakage assertions on ML features."""
    panel_stats_024 = (
        features_df_024.agg(
            F.count("*").alias("total_panel_rows_024"),
            F.sum(F.when(F.col("is_observed_in_silver_024"), 1).otherwise(0)).alias(
                "observed_zone_hours_024"
            ),
            F.sum(F.when(~F.col("is_observed_in_silver_024"), 1).otherwise(0)).alias(
                "missing_zero_filled_zone_hours_024"
            ),
            F.sum(F.when(F.col("is_ml_ready_full_history_024"), 1).otherwise(0)).alias(
                "ml_ready_rows_024"
            ),
            F.sum(
                F.when(
                    F.col("is_ml_ready_full_history_024")
                    & (
                        F.col("lag_1h_pickup_count_024").isNull()
                        | F.col("lag_24h_pickup_count_024").isNull()
                        | F.col("lag_168h_pickup_count_024").isNull()
                        | F.col("rolling_avg_24h_pickup_count_024").isNull()
                        | F.col("rolling_avg_7d_pickup_count_024").isNull()
                        | F.col("rolling_std_7d_pickup_count_024").isNull()
                    ),
                    1,
                ).otherwise(0)
            ).alias("ml_ready_any_feature_null_024"),
            F.sum(
                F.when(
                    (
                        F.col("pickup_ts_hour_024")
                        == F.lit(GRID_START_TS_024).cast("timestamp_ntz")
                    )
                    & (
                        F.col("lag_1h_pickup_count_024").isNotNull()
                        | F.col("lag_24h_pickup_count_024").isNotNull()
                        | F.col("rolling_avg_7d_pickup_count_024").isNotNull()
                    ),
                    1,
                ).otherwise(0)
            ).alias("first_hour_leakage_violations_024"),
            F.sum(
                F.when(
                    (
                        F.col("pickup_ts_hour_024")
                        < F.lit("2026-09-08 00:00:00").cast("timestamp_ntz")
                    )
                    & F.col("rolling_avg_7d_pickup_count_024").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("warmup_7d_leakage_violations_024"),
        )
        .collect()[0]
        .asDict()
    )

    assert int(panel_stats_024["ml_ready_any_feature_null_024"]) == 0, (
        "ML-ready rows contain unexpected NULL lag/rolling features!"
    )
    assert int(panel_stats_024["first_hour_leakage_violations_024"]) == 0, (
        "First hour contains non-null lag/rolling values (leakage)!"
    )
    assert int(panel_stats_024["warmup_7d_leakage_violations_024"]) == 0, (
        "First 168 hours contain non-null 7-day rolling values!"
    )

    # Spot-verify exact arithmetic mean of strictly past [t-168 .. t-1] excluding t
    spot_zone_024 = 237
    spot_ts_024 = "2026-09-15 18:00:00"
    spot_row_024 = (
        features_df_024.filter(
            (F.col("PULocationID") == spot_zone_024)
            & (F.col("pickup_ts_hour_024") == F.lit(spot_ts_024).cast("timestamp_ntz"))
        )
        .select(
            "pickup_count_024",
            "lag_1h_pickup_count_024",
            "lag_24h_pickup_count_024",
            "lag_168h_pickup_count_024",
            "rolling_avg_7d_pickup_count_024",
        )
        .collect()[0]
    )
    manual_past_024 = (
        features_df_024.filter(
            (F.col("PULocationID") == spot_zone_024)
            & (
                F.col("pickup_ts_hour_024")
                >= F.lit("2026-09-08 18:00:00").cast("timestamp_ntz")
            )
            & (F.col("pickup_ts_hour_024") < F.lit(spot_ts_024).cast("timestamp_ntz"))
        )
        .agg(
            F.count("*").alias("cnt"),
            F.avg("pickup_count_024").alias("manual_avg"),
        )
        .collect()[0]
    )
    assert int(manual_past_024["cnt"]) == 168
    stored_roll7d_024 = float(spot_row_024["rolling_avg_7d_pickup_count_024"])
    manual_roll7d_024 = round(float(manual_past_024["manual_avg"]), 4)
    assert abs(stored_roll7d_024 - manual_roll7d_024) < 1e-3, (
        f"Rolling 7d mismatch: {stored_roll7d_024} vs {manual_roll7d_024}"
    )

    # Sparsity by zone demand tier
    tier_sparsity_024 = [
        {
            "demand_tier_024": r["demand_tier_024"],
            "zone_count_024": int(r["z_cnt"]),
            "total_zone_hours_024": int(r["tot_zh"]),
            "observed_zone_hours_024": int(r["obs_zh"]),
            "zero_filled_pct_024": round(
                ((int(r["tot_zh"]) - int(r["obs_zh"])) / int(r["tot_zh"])) * 100.0, 2
            ),
        }
        for r in features_df_024.join(
            zone_demand_df_024.select("PULocationID", "demand_tier_024"),
            on="PULocationID",
            how="left",
        )
        .groupBy("demand_tier_024")
        .agg(
            F.countDistinct("PULocationID").alias("z_cnt"),
            F.count("*").alias("tot_zh"),
            F.sum(F.when(F.col("is_observed_in_silver_024"), 1).otherwise(0)).alias(
                "obs_zh"
            ),
        )
        .orderBy("demand_tier_024")
        .collect()
    ]

    tot_p_024 = int(panel_stats_024["total_panel_rows_024"])
    obs_p_024 = int(panel_stats_024["observed_zone_hours_024"])
    miss_p_024 = int(panel_stats_024["missing_zero_filled_zone_hours_024"])

    return {
        "total_panel_rows_024": tot_p_024,
        "observed_zone_hours_024": obs_p_024,
        "observed_pct_024": round((obs_p_024 / tot_p_024) * 100.0, 4),
        "missing_zero_filled_zone_hours_024": miss_p_024,
        "missing_zero_filled_pct_024": round((miss_p_024 / tot_p_024) * 100.0, 4),
        "ml_ready_rows_full_7d_history_024": int(panel_stats_024["ml_ready_rows_024"]),
        "ml_ready_time_range_024": ["2026-09-08 00:00:00", GRID_END_TS_024],
        "zero_leakage_assertions_passed_024": True,
        "spot_check_zone_237_2026_09_15_18h_024": {
            "actual_pickup_count_t_024": int(spot_row_024["pickup_count_024"]),
            "lag_1h_024": float(spot_row_024["lag_1h_pickup_count_024"]),
            "lag_24h_024": float(spot_row_024["lag_24h_pickup_count_024"]),
            "lag_168h_024": float(spot_row_024["lag_168h_pickup_count_024"]),
            "stored_rolling_avg_7d_024": stored_roll7d_024,
            "manual_past_168h_mean_excl_t_024": manual_roll7d_024,
            "matches_exact_past_only_024": True,
        },
        "sparsity_by_demand_tier_024": tier_sparsity_024,
    }


def extract_initial_insights_024(
    hourly_df_024: DataFrame,
    zone_df_024: DataFrame,
    temporal_df_024: DataFrame,
    zone_hourly_df_024: DataFrame,
) -> dict[str, Any]:
    """Extract evidence-based BI insights directly from verified Gold datasets."""
    # City-wide hourly profile (averaged across 30 days)
    hr_profile_024 = [
        {
            "pickup_hour_024": int(r["pickup_hour_024"]),
            "total_pickups_024": int(r["tot"]),
            "avg_pickups_per_day_024": round(float(r["avg_day"]), 2),
            "net_settled_revenue_usd_024": round(float(r["rev"]), 2),
        }
        for r in hourly_df_024.groupBy("pickup_hour_024")
        .agg(
            F.sum("pickup_count_024").alias("tot"),
            F.avg("pickup_count_024").alias("avg_day"),
            F.sum("net_settled_revenue_usd_024").alias("rev"),
        )
        .orderBy(F.desc("tot"))
        .collect()
    ]
    missing_city_hours_024 = hourly_df_024.filter(~F.col("is_observed_hour_024")).count()

    # Single highest & lowest actual calendar hour in September 2026
    busiest_slot_024 = (
        hourly_df_024.orderBy(F.desc("pickup_count_024"))
        .select(
            "pickup_ts_hour_024",
            "day_name_024",
            "pickup_count_024",
            "net_settled_revenue_usd_024",
        )
        .first()
    )
    quietest_slot_024 = (
        hourly_df_024.orderBy(F.asc("pickup_count_024"))
        .select(
            "pickup_ts_hour_024",
            "day_name_024",
            "pickup_count_024",
            "net_settled_revenue_usd_024",
        )
        .first()
    )

    # Top 10 & Bottom 10 zones
    top10_zones_024 = [
        {
            "rank_024": int(r["zone_demand_rank_024"]),
            "PULocationID": int(r["PULocationID"]),
            "pickup_count_024": int(r["pickup_count_024"]),
            "pickup_share_pct_024": float(r["pickup_share_pct_024"]),
            "peak_hour_024": int(r["peak_hour_024"]),
            "weekend_share_pct_024": float(r["weekend_share_pct_024"]),
            "net_settled_revenue_usd_024": float(r["net_settled_revenue_usd_024"]),
            "avg_settled_total_usd_024": float(r["avg_settled_total_usd_024"]),
        }
        for r in zone_df_024.orderBy("zone_demand_rank_024").limit(10).collect()
    ]
    bottom10_zones_024 = [
        {
            "rank_024": int(r["zone_demand_rank_024"]),
            "PULocationID": int(r["PULocationID"]),
            "pickup_count_024": int(r["pickup_count_024"]),
            "active_hours_count_024": int(r["active_hours_count_024"]),
            "net_settled_revenue_usd_024": float(r["net_settled_revenue_usd_024"]),
        }
        for r in zone_df_024.orderBy(F.desc("zone_demand_rank_024")).limit(10).collect()
    ]

    top10_share_024 = round(sum(z["pickup_share_pct_024"] for z in top10_zones_024), 4)
    top20_share_024 = round(
        float(
            zone_df_024.filter(F.col("zone_demand_rank_024") <= 20)
            .agg(F.sum("pickup_share_pct_024"))
            .first()[0]
        ),
        4,
    )

    # Weekday vs Weekend (normalized per calendar day: 22 weekdays, 8 weekend days)
    day_type_summary_024 = [
        {
            "day_type_024": r["day_type_024"],
            "calendar_days_024": 22 if r["day_type_024"] == "Weekday" else 8,
            "total_pickups_024": int(r["tot_p"]),
            "avg_pickups_per_day_024": round(
                int(r["tot_p"]) / (22.0 if r["day_type_024"] == "Weekday" else 8.0), 2
            ),
            "avg_pickups_per_hour_024": round(
                int(r["tot_p"])
                / ((22.0 if r["day_type_024"] == "Weekday" else 8.0) * 24.0),
                2,
            ),
            "net_settled_revenue_usd_024": round(float(r["rev"]), 2),
        }
        for r in temporal_df_024.groupBy("day_type_024")
        .agg(
            F.sum("total_pickups_024").alias("tot_p"),
            F.sum("net_settled_revenue_usd_024").alias("rev"),
        )
        .orderBy("day_type_024")
        .collect()
    ]

    # Peak hour for Weekday vs Weekend separately
    wd_peak_024 = (
        temporal_df_024.filter(F.col("day_type_024") == "Weekday")
        .groupBy("pickup_hour_024")
        .agg(F.sum("total_pickups_024").alias("tot"))
        .orderBy(F.desc("tot"))
        .first()
    )
    we_peak_024 = (
        temporal_df_024.filter(F.col("day_type_024") == "Weekend")
        .groupBy("pickup_hour_024")
        .agg(F.sum("total_pickups_024").alias("tot"))
        .orderBy(F.desc("tot"))
        .first()
    )

    # Day of week ranking (normalized per calendar occurrence)
    dow_summary_024 = [
        {
            "day_of_week_024": int(r["day_of_week_024"]),
            "day_name_024": r["day_name_024"],
            "num_calendar_days_024": int(r["cal_days"]),
            "total_pickups_024": int(r["tot_p"]),
            "avg_pickups_per_day_024": round(int(r["tot_p"]) / float(r["cal_days"]), 2),
        }
        for r in temporal_df_024.groupBy(
            "day_of_week_024", "day_name_024"
        )
        .agg(
            F.max("num_calendar_days_024").alias("cal_days"),
            F.sum("total_pickups_024").alias("tot_p"),
        )
        .orderBy("day_of_week_024")
        .collect()
    ]

    # Top 10 Zone x Hour hotspots
    top10_hotspots_024 = [
        {
            "PULocationID": int(r["PULocationID"]),
            "pickup_hour_024": int(r["pickup_hour_024"]),
            "pickup_count_024": int(r["pickup_count_024"]),
            "avg_daily_pickups_at_hour_024": float(r["avg_daily_pickups_at_hour_024"]),
            "within_zone_hour_share_pct_024": float(r["within_zone_hour_share_pct_024"]),
        }
        for r in zone_hourly_df_024.orderBy(F.desc("pickup_count_024")).limit(10).collect()
    ]

    totals_row_024 = (
        hourly_df_024.agg(
            F.sum("pickup_count_024").alias("tot_pickups"),
            F.sum("non_reversed_pickup_count_024").alias("tot_non_rev"),
            F.sum("flex_fare_pickup_count_024").alias("tot_flex"),
            F.round(F.sum("net_settled_revenue_usd_024"), 2).alias("tot_rev"),
        )
        .collect()[0]
    )

    return {
        "overall_kpis_024": {
            "total_operational_pickups_024": int(totals_row_024["tot_pickups"]),
            "total_non_reversed_pickups_024": int(totals_row_024["tot_non_rev"]),
            "reversal_paired_pickups_excluded_from_revenue_024": int(
                totals_row_024["tot_pickups"]
            )
            - int(totals_row_024["tot_non_rev"]),
            "total_flex_fare_pickups_024": int(totals_row_024["tot_flex"]),
            "flex_fare_share_pct_024": round(
                (int(totals_row_024["tot_flex"]) / int(totals_row_024["tot_pickups"]))
                * 100.0,
                4,
            ),
            "total_net_settled_revenue_usd_024": float(totals_row_024["tot_rev"]),
            "avg_daily_pickups_024": round(
                int(totals_row_024["tot_pickups"]) / 30.0, 2
            ),
            "avg_hourly_pickups_024": round(
                int(totals_row_024["tot_pickups"]) / 720.0, 2
            ),
            "missing_city_hours_out_of_720_024": int(missing_city_hours_024),
        },
        "peak_and_quiet_hours_024": {
            "peak_hour_024": hr_profile_024[0],
            "quietest_hour_024": hr_profile_024[-1],
            "top5_hours_024": hr_profile_024[:5],
            "bottom5_hours_024": list(reversed(hr_profile_024[-5:])),
            "busiest_calendar_hour_slot_024": {
                "pickup_ts_hour_024": fmt_val_024(busiest_slot_024["pickup_ts_hour_024"]),
                "day_name_024": busiest_slot_024["day_name_024"],
                "pickup_count_024": int(busiest_slot_024["pickup_count_024"]),
                "net_settled_revenue_usd_024": float(
                    busiest_slot_024["net_settled_revenue_usd_024"]
                ),
            },
            "quietest_calendar_hour_slot_024": {
                "pickup_ts_hour_024": fmt_val_024(
                    quietest_slot_024["pickup_ts_hour_024"]
                ),
                "day_name_024": quietest_slot_024["day_name_024"],
                "pickup_count_024": int(quietest_slot_024["pickup_count_024"]),
                "net_settled_revenue_usd_024": float(
                    quietest_slot_024["net_settled_revenue_usd_024"]
                ),
            },
        },
        "zone_insights_024": {
            "top10_combined_share_pct_024": top10_share_024,
            "top20_combined_share_pct_024": top20_share_024,
            "top10_zones_024": top10_zones_024,
            "bottom10_zones_024": bottom10_zones_024,
        },
        "weekday_vs_weekend_024": {
            "summary_024": day_type_summary_024,
            "weekday_peak_hour_024": int(wd_peak_024["pickup_hour_024"]),
            "weekend_peak_hour_024": int(we_peak_024["pickup_hour_024"]),
            "day_of_week_breakdown_024": dow_summary_024,
        },
        "top10_zone_hour_hotspots_024": top10_hotspots_024,
    }


def run_gold_layer_build_024(
    silver_input_024: str = HDFS_SILVER_INPUT_024,
    output_root_024: str = HDFS_GOLD_LIVE_ROOT_024,
    output_json_024: Path = DEFAULT_GOLD_JSON_024,
) -> dict[str, Any]:
    """Build all 5 Gold Layer Parquet datasets, verify read-back & zero-leakage, and save JSON."""
    if silver_input_024.rstrip("/") == output_root_024.rstrip("/"):
        raise ValueError("Refusing to write Gold output onto Silver input path.")

    t0_024 = time.time()
    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    try:
        silver_df_024 = spark_024.read.parquet(silver_input_024)
        base_df_024 = prepare_silver_base_024(silver_df_024).cache()
        total_silver_rows_024 = int(base_df_024.count())

        hourly_grid_024 = build_complete_hourly_grid_024(spark_024).cache()
        active_zones_024 = (
            base_df_024.select("PULocationID", "is_known_pickup_zone_024")
            .distinct()
            .cache()
        )

        # Build all 5 Gold DataFrames
        gold_hourly_024 = build_gold_hourly_demand_024(base_df_024, hourly_grid_024)
        gold_zone_hourly_024 = build_gold_zone_hourly_demand_024(
            base_df_024, active_zones_024
        )
        gold_zone_024 = build_gold_zone_demand_024(
            base_df_024, gold_zone_hourly_024, total_silver_rows_024
        )
        gold_temporal_024 = build_gold_temporal_patterns_024(
            base_df_024, hourly_grid_024, total_silver_rows_024
        )
        gold_features_024 = build_gold_demand_features_024(
            base_df_024, hourly_grid_024, active_zones_024
        )

        # Write to target/staging root (1 file for small BI tables, 4 files for 187k ML panel)
        out_root_024 = output_root_024.rstrip("/")
        write_specs_024 = [
            ("gold_hourly_demand_024", gold_hourly_024, 1, "pickup_count_024", "pickup_ts_hour_024"),
            ("gold_zone_demand_024", gold_zone_024, 1, "pickup_count_024", None),
            ("gold_temporal_patterns_024", gold_temporal_024, 1, "total_pickups_024", None),
            ("gold_zone_hourly_demand_024", gold_zone_hourly_024, 1, "pickup_count_024", None),
            ("gold_demand_features_024", gold_features_024, 4, "pickup_count_024", "pickup_ts_hour_024"),
        ]

        dataset_verifications_024 = {}
        readback_dfs_024 = {}
        for ds_name_024, df_024, n_parts_024, pcol_024, tscol_024 in write_specs_024:
            ds_path_024 = f"{out_root_024}/{ds_name_024}"
            df_024.coalesce(n_parts_024).write.mode("overwrite").parquet(ds_path_024)
            rb_df_024 = spark_024.read.parquet(ds_path_024)
            readback_dfs_024[ds_name_024] = rb_df_024
            dataset_verifications_024[ds_name_024] = inspect_gold_dataset_024(
                rb_df_024, ds_name_024, pcol_024, total_silver_rows_024, tscol_024
            )
            dataset_verifications_024[ds_name_024]["hdfs_staged_path_024"] = ds_path_024
            dataset_verifications_024[ds_name_024]["hdfs_live_path_024"] = (
                f"{HDFS_GOLD_LIVE_ROOT_024}/{ds_name_024}"
            )

        # ML panel missing-slot & zero-leakage audit
        ml_audit_024 = verify_ml_panel_and_leakage_024(
            readback_dfs_024["gold_demand_features_024"],
            readback_dfs_024["gold_zone_demand_024"],
        )

        # Initial BI insights from verified read-back Gold tables
        insights_024 = extract_initial_insights_024(
            readback_dfs_024["gold_hourly_demand_024"],
            readback_dfs_024["gold_zone_demand_024"],
            readback_dfs_024["gold_temporal_patterns_024"],
            readback_dfs_024["gold_zone_hourly_demand_024"],
        )

        base_df_024.unpersist()
        hourly_grid_024.unpersist()
        active_zones_024.unpersist()

        elapsed_sec_024 = round(time.time() - t0_024, 2)
        summary_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "script_024": "spark/processing/build_gold_layer_024.py",
                "hdfs_silver_input_024": silver_input_024,
                "hdfs_written_root_024": out_root_024,
                "hdfs_live_root_024": HDFS_GOLD_LIVE_ROOT_024,
                "spark_version_024": spark_024.version,
                "spark_master_024": spark_024.sparkContext.master,
                "silver_v2_source_rows_024": total_silver_rows_024,
                "executed_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": elapsed_sec_024,
            },
            "population_definitions_024": {
                "operational_pickup_demand_024": (
                    "All 3,860,852 verified Silver v2 rows (Sep 2026 paid trips, "
                    "including Flex Fare, VendorID 6/7, and flagged telemetry anomalies). "
                    "Companion column `non_reversed_pickup_count_024` also reported "
                    "excluding the 13,018 reversal-paired rows."
                ),
                "net_settled_revenue_and_economics_024": (
                    "Excludes `has_negative_reversal_pair_024 = true` (13,018 rows) "
                    "and uses validated non-outlier columns (`total_amount_valid_usd_024`, "
                    "`fare_amount_valid_usd_024`, `trip_distance_valid_mi_024`, "
                    "`trip_duration_valid_min_024`, `avg_speed_mph_024`)."
                ),
                "zone_reference_status_024": (
                    "taxi_zone_lookup.csv is not yet present in data/reference/ or HDFS "
                    "/urbanpulse_024/raw/reference/; original `PULocationID` is preserved "
                    "without fabricating zone names."
                ),
            },
            "dataset_verifications_024": dataset_verifications_024,
            "ml_feature_panel_audit_024": ml_audit_024,
            "initial_insights_024": insights_024,
        }

        output_json_024.parent.mkdir(parents=True, exist_ok=True)
        with output_json_024.open("w", encoding="utf-8") as fh_024:
            json.dump(summary_024, fh_024, indent=2, ensure_ascii=False)

        print("=== URBANPULSE GOLD LAYER BUILD SUMMARY (_024) ===")
        for ds_name_024 in GOLD_DATASET_NAMES_024:
            dv_024 = dataset_verifications_024[ds_name_024]
            print(
                f"  {ds_name_024:<30}: rows={dv_024['row_count_024']:>7,}, "
                f"cols={dv_024['column_count_024']:>2}, "
                f"sum_pickups={dv_024['pickup_sum_reconciled_024']:,}"
            )
        print(
            "ML Panel Audit : "
            f"observed={ml_audit_024['observed_zone_hours_024']:,} "
            f"({ml_audit_024['observed_pct_024']}%), "
            f"zero_filled={ml_audit_024['missing_zero_filled_zone_hours_024']:,} "
            f"({ml_audit_024['missing_zero_filled_pct_024']}%), "
            f"ml_ready={ml_audit_024['ml_ready_rows_full_7d_history_024']:,}, "
            f"zero_leakage={ml_audit_024['zero_leakage_assertions_passed_024']}"
        )
        print(
            "Net Settled Rev: "
            f"${insights_024['overall_kpis_024']['total_net_settled_revenue_usd_024']:,.2f}"
        )
        print(f"Elapsed Time   : {elapsed_sec_024} s")
        print(f"Saved JSON     : {output_json_024}")
        return summary_024
    finally:
        spark_024.stop()


if __name__ == "__main__":
    run_gold_layer_build_024(
        silver_input_024=os.environ.get(
            "URBANPULSE_SILVER_INPUT_024", HDFS_SILVER_INPUT_024
        ),
        output_root_024=os.environ.get(
            "URBANPULSE_GOLD_OUTPUT_ROOT_024", HDFS_GOLD_LIVE_ROOT_024
        ),
        output_json_024=Path(
            os.environ.get("URBANPULSE_GOLD_JSON_024", str(DEFAULT_GOLD_JSON_024))
        ),
    )
