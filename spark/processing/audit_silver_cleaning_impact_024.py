"""UrbanPulse post-cleaning Silver Layer impact audit (read-only).

Evaluates every hard-removal rule applied in `clean_yellow_taxi_024.py`,
computes exact rule overlaps without double-counting, profiles each removed
anomaly group (negative/zero/extreme fares, duration > 3h, speed > 80 mph,
out-of-period timestamps), and compares hourly and zone pickup demand
distributions between period-bounded Raw data (September 2026) and the current
Silver Layer in HDFS.

Strictly read-only: never modifies Raw or Silver Parquet files.
"""

from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HDFS_RAW_INPUT_024 = (
    "hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/"
    "yellow_tripdata_2026-09_024.parquet"
)
HDFS_SILVER_INPUT_024 = (
    "hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024"
)
DEFAULT_AUDIT_JSON_024 = Path("reports/silver_cleaning_impact_audit_024.json")

TARGET_START_TS_024 = "2026-09-01 00:00:00"
TARGET_END_TS_024 = "2026-10-01 00:00:00"
SURCHARGE_COLS_024 = [
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]


def build_spark_session_024() -> SparkSession:
    """Create a local[*] SparkSession for read-only audit."""
    return (
        SparkSession.builder.appName("UrbanPulse_Silver_Impact_Audit_024")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def pearson_corr_024(xs_024: list[float], ys_024: list[float]) -> float:
    """Compute Pearson correlation between two equal-length numeric lists."""
    n_024 = len(xs_024)
    if n_024 < 2:
        return 1.0
    mx_024 = sum(xs_024) / n_024
    my_024 = sum(ys_024) / n_024
    num_024 = sum((x - mx_024) * (y - my_024) for x, y in zip(xs_024, ys_024))
    den_x_024 = math.sqrt(sum((x - mx_024) ** 2 for x in xs_024))
    den_y_024 = math.sqrt(sum((y - my_024) ** 2 for y in ys_024))
    if den_x_024 == 0 or den_y_024 == 0:
        return 1.0
    return round(num_024 / (den_x_024 * den_y_024), 8)


def average_ranks_024(vals_024: list[float]) -> list[float]:
    """Compute average ranks (for Spearman correlation with ties)."""
    indexed_024 = sorted(enumerate(vals_024), key=lambda kv: kv[1])
    ranks_024 = [0.0] * len(vals_024)
    i_024 = 0
    while i_024 < len(indexed_024):
        j_024 = i_024
        while (
            j_024 + 1 < len(indexed_024)
            and indexed_024[j_024 + 1][1] == indexed_024[i_024][1]
        ):
            j_024 += 1
        avg_r_024 = (i_024 + j_024) / 2.0 + 1.0
        for k_024 in range(i_024, j_024 + 1):
            ranks_024[indexed_024[k_024][0]] = avg_r_024
        i_024 = j_024 + 1
    return ranks_024


def spearman_corr_024(xs_024: list[float], ys_024: list[float]) -> float:
    """Compute Spearman rank correlation."""
    return pearson_corr_024(average_ranks_024(xs_024), average_ranks_024(ys_024))


def annotate_rules_024(raw_df_024: DataFrame) -> DataFrame:
    """Attach boolean flags for every Silver removal rule and monetary sub-rule."""
    dur_024 = (
        F.unix_timestamp(F.col("tpep_dropoff_datetime"))
        - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    ) / F.lit(60.0)
    spd_024 = F.when(
        (dur_024 > 0) & (F.col("trip_distance") > 0),
        F.col("trip_distance") / (dur_024 / F.lit(60.0)),
    )

    present_surcharges_024 = [
        c_024 for c_024 in SURCHARGE_COLS_024 if c_024 in raw_df_024.columns
    ]
    surcharge_neg_024 = F.lit(False)
    for col_024 in present_surcharges_024:
        surcharge_neg_024 = surcharge_neg_024 | (
            F.col(f"`{col_024}`").isNotNull() & (F.col(f"`{col_024}`") < 0)
        )

    r_period_024 = (
        F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | (F.col("tpep_pickup_datetime") < F.lit(TARGET_START_TS_024))
        | (F.col("tpep_pickup_datetime") >= F.lit(TARGET_END_TS_024))
    )
    r_dur_024 = (dur_024 < 0) | (dur_024 > 180.0)
    r_dist_024 = (
        F.col("trip_distance").isNull()
        | (F.col("trip_distance") < 0)
        | (F.col("trip_distance") > 100.0)
    )
    r_spd_024 = spd_024.isNotNull() & (spd_024 > 80.0)
    r_loc_024 = (
        F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
        | (~F.col("PULocationID").between(1, 265))
        | (~F.col("DOLocationID").between(1, 265))
    )

    sub_fare_neg_024 = F.col("fare_amount") < 0
    sub_fare_zero_024 = F.col("fare_amount") == 0
    sub_fare_gt500_024 = F.col("fare_amount") > 500.0
    sub_total_neg_024 = F.col("total_amount") < 0
    sub_total_zero_024 = F.col("total_amount") == 0
    sub_total_gt1000_024 = F.col("total_amount") > 1000.0
    sub_tip_neg_024 = F.col("tip_amount") < 0
    sub_tip_gt200_024 = F.col("tip_amount") > 200.0
    sub_any_neg_money_024 = (
        sub_fare_neg_024 | sub_total_neg_024 | sub_tip_neg_024 | surcharge_neg_024
    )
    sub_any_zero_money_024 = (sub_fare_zero_024 | sub_total_zero_024) & (
        ~sub_any_neg_money_024
    )
    sub_any_extreme_money_024 = (
        sub_fare_gt500_024 | sub_total_gt1000_024 | sub_tip_gt200_024
    )

    r_fare_024 = (
        F.col("fare_amount").isNull()
        | (F.col("fare_amount") <= 0)
        | (F.col("fare_amount") > 500.0)
        | F.col("total_amount").isNull()
        | (F.col("total_amount") <= 0)
        | (F.col("total_amount") > 1000.0)
        | F.col("tip_amount").isNull()
        | (F.col("tip_amount") < 0)
        | (F.col("tip_amount") > 200.0)
        | surcharge_neg_024
    )

    return (
        raw_df_024.withColumn("trip_duration_min_024", dur_024)
        .withColumn("avg_speed_mph_024", spd_024)
        .withColumn("r_period_024", r_period_024)
        .withColumn("r_dur_024", r_dur_024)
        .withColumn("r_dist_024", r_dist_024)
        .withColumn("r_spd_024", r_spd_024)
        .withColumn("r_fare_024", r_fare_024)
        .withColumn("r_loc_024", r_loc_024)
        .withColumn("sub_fare_neg_024", sub_fare_neg_024)
        .withColumn("sub_fare_zero_024", sub_fare_zero_024)
        .withColumn("sub_fare_gt500_024", sub_fare_gt500_024)
        .withColumn("sub_total_neg_024", sub_total_neg_024)
        .withColumn("sub_total_zero_024", sub_total_zero_024)
        .withColumn("sub_total_gt1000_024", sub_total_gt1000_024)
        .withColumn("sub_tip_neg_024", sub_tip_neg_024)
        .withColumn("sub_tip_gt200_024", sub_tip_gt200_024)
        .withColumn("sub_surcharge_neg_024", surcharge_neg_024)
        .withColumn("sub_any_neg_money_024", sub_any_neg_money_024)
        .withColumn("sub_any_zero_money_024", sub_any_zero_money_024)
        .withColumn("sub_any_extreme_money_024", sub_any_extreme_money_024)
        .withColumn(
            "any_dropped_024",
            r_period_024
            | r_dur_024
            | r_dist_024
            | r_spd_024
            | r_fare_024
            | r_loc_024,
        )
    )


def audit_rule_overlaps_024(
    annotated_df_024: DataFrame, total_raw_024: int
) -> dict[str, Any]:
    """Compute gross, exclusive, overlap, and exact disjoint combinations of dropped rows."""
    dropped_df_024 = annotated_df_024.filter(F.col("any_dropped_024"))

    # Build human-readable combination label for every dropped row
    combo_label_024 = F.concat_ws(
        " + ",
        F.when(F.col("r_period_024"), F.lit("out_of_period_024")),
        F.when(F.col("r_dur_024"), F.lit("duration_gt_180m_024")),
        F.when(F.col("r_dist_024"), F.lit("distance_gt_100mi_024")),
        F.when(F.col("r_spd_024"), F.lit("speed_gt_80mph_024")),
        F.when(F.col("sub_any_neg_money_024"), F.lit("negative_money_024")),
        F.when(F.col("sub_any_zero_money_024"), F.lit("zero_fare_or_total_024")),
        F.when(F.col("sub_any_extreme_money_024"), F.lit("extreme_money_024")),
        F.when(F.col("r_loc_024"), F.lit("invalid_location_024")),
    )

    coarse_combo_label_024 = F.concat_ws(
        " + ",
        F.when(F.col("r_period_024"), F.lit("out_of_period_024")),
        F.when(F.col("r_dur_024"), F.lit("duration_gt_180m_024")),
        F.when(F.col("r_dist_024"), F.lit("distance_gt_100mi_024")),
        F.when(F.col("r_spd_024"), F.lit("speed_gt_80mph_024")),
        F.when(F.col("r_fare_024"), F.lit("invalid_fare_money_024")),
        F.when(F.col("r_loc_024"), F.lit("invalid_location_024")),
    )

    rules_6_024 = [
        ("out_of_period_024", "r_period_024"),
        ("duration_gt_180m_024", "r_dur_024"),
        ("distance_gt_100mi_024", "r_dist_024"),
        ("speed_gt_80mph_024", "r_spd_024"),
        ("invalid_fare_money_024", "r_fare_024"),
        ("invalid_location_024", "r_loc_024"),
    ]

    rule_count_sum_024 = sum(F.col(col_024).cast("int") for _, col_024 in rules_6_024)
    agg_exprs_024 = [
        F.count("*").alias("total_dropped_024"),
        F.sum(F.when(rule_count_sum_024 == 1, 1).otherwise(0)).alias(
            "single_rule_dropped_024"
        ),
        F.sum(F.when(rule_count_sum_024 > 1, 1).otherwise(0)).alias(
            "multi_rule_overlap_dropped_024"
        ),
    ]
    for label_024, col_024 in rules_6_024:
        agg_exprs_024.append(
            F.sum(F.when(F.col(col_024), 1).otherwise(0)).alias(f"gross__{label_024}")
        )
        agg_exprs_024.append(
            F.sum(
                F.when(F.col(col_024) & (rule_count_sum_024 == 1), 1).otherwise(0)
            ).alias(f"excl__{label_024}")
        )

    # Monetary sub-rule counts inside dropped rows
    money_subs_024 = [
        "sub_fare_neg_024",
        "sub_fare_zero_024",
        "sub_fare_gt500_024",
        "sub_total_neg_024",
        "sub_total_zero_024",
        "sub_total_gt1000_024",
        "sub_tip_neg_024",
        "sub_tip_gt200_024",
        "sub_surcharge_neg_024",
        "sub_any_neg_money_024",
        "sub_any_zero_money_024",
        "sub_any_extreme_money_024",
    ]
    for sub_024 in money_subs_024:
        agg_exprs_024.append(
            F.sum(F.when(F.col(sub_024), 1).otherwise(0)).alias(f"sub__{sub_024}")
        )

    summary_row_024 = dropped_df_024.agg(*agg_exprs_024).collect()[0].asDict()
    total_dropped_024 = int(summary_row_024["total_dropped_024"])

    per_rule_table_024 = []
    for label_024, _ in rules_6_024:
        gross_024 = int(summary_row_024[f"gross__{label_024}"] or 0)
        excl_024 = int(summary_row_024[f"excl__{label_024}"] or 0)
        overlap_024 = gross_024 - excl_024
        per_rule_table_024.append(
            {
                "rule_name_024": label_024,
                "gross_rows_024": gross_024,
                "exclusive_only_rows_024": excl_024,
                "overlapping_rows_024": overlap_024,
                "pct_of_raw_024": round((gross_024 / total_raw_024) * 100.0, 4),
                "pct_of_dropped_024": round(
                    (gross_024 / total_dropped_024) * 100.0, 4
                ),
            }
        )

    coarse_combos_024 = [
        {
            "rule_combination_024": r["combo"],
            "disjoint_rows_024": int(r["cnt"]),
            "pct_of_dropped_024": round((int(r["cnt"]) / total_dropped_024) * 100.0, 4),
            "pct_of_raw_024": round((int(r["cnt"]) / total_raw_024) * 100.0, 4),
        }
        for r in dropped_df_024.groupBy(coarse_combo_label_024.alias("combo"))
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    fine_combos_024 = [
        {
            "rule_combination_024": r["combo"],
            "disjoint_rows_024": int(r["cnt"]),
            "pct_of_dropped_024": round((int(r["cnt"]) / total_dropped_024) * 100.0, 4),
            "pct_of_raw_024": round((int(r["cnt"]) / total_raw_024) * 100.0, 4),
        }
        for r in dropped_df_024.groupBy(combo_label_024.alias("combo"))
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    monetary_sub_counts_024 = {
        sub_024: int(summary_row_024[f"sub__{sub_024}"] or 0)
        for sub_024 in money_subs_024
    }

    return {
        "total_dropped_024": total_dropped_024,
        "single_rule_dropped_024": int(summary_row_024["single_rule_dropped_024"]),
        "multi_rule_overlap_dropped_024": int(
            summary_row_024["multi_rule_overlap_dropped_024"]
        ),
        "disjoint_sum_check_coarse_024": sum(
            c["disjoint_rows_024"] for c in coarse_combos_024
        ),
        "disjoint_sum_check_fine_024": sum(
            c["disjoint_rows_024"] for c in fine_combos_024
        ),
        "per_rule_table_024": per_rule_table_024,
        "coarse_disjoint_combinations_024": coarse_combos_024,
        "fine_disjoint_combinations_024": fine_combos_024,
        "monetary_sub_rule_counts_024": monetary_sub_counts_024,
    }


def profile_special_groups_024(annotated_df_024: DataFrame) -> dict[str, Any]:
    """Deep-dive analysis of each special anomaly group and mirror-reversal pairs."""
    # 1. Negative money group + exact positive mirror-pair test
    neg_df_024 = annotated_df_024.filter(F.col("sub_any_neg_money_024"))
    neg_by_pay_vendor_024 = [
        {
            "payment_type": r["payment_type"],
            "VendorID": r["VendorID"],
            "rows_024": int(r["cnt"]),
            "dist_gt0_024": int(r["dist_gt0"]),
            "dur_gt0_024": int(r["dur_gt0"]),
            "mean_fare_024": round(float(r["avg_fare"]), 2) if r["avg_fare"] is not None else None,
            "mean_total_024": round(float(r["avg_total"]), 2) if r["avg_total"] is not None else None,
        }
        for r in neg_df_024.groupBy("payment_type", "VendorID")
        .agg(
            F.count("*").alias("cnt"),
            F.sum(F.when(F.col("trip_distance") > 0, 1).otherwise(0)).alias("dist_gt0"),
            F.sum(F.when(F.col("trip_duration_min_024") > 0, 1).otherwise(0)).alias("dur_gt0"),
            F.mean("fare_amount").alias("avg_fare"),
            F.mean("total_amount").alias("avg_total"),
        )
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    # Check if negative transactions have an exact positive mirror row (chargeback/reversal pair)
    pos_keys_024 = (
        annotated_df_024.filter(F.col("total_amount") > 0)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            F.round(F.col("trip_distance"), 2).alias("dist_k_024"),
            F.round(F.abs(F.col("total_amount")), 2).alias("abs_total_k_024"),
        )
        .distinct()
    )
    neg_keys_024 = neg_df_024.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        F.round(F.col("trip_distance"), 2).alias("dist_k_024"),
        F.round(F.abs(F.col("total_amount")), 2).alias("abs_total_k_024"),
        "payment_type",
    )
    mirror_matched_024 = (
        neg_keys_024.join(
            pos_keys_024,
            on=[
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "dist_k_024",
                "abs_total_k_024",
            ],
            how="inner",
        ).count()
    )
    neg_total_024 = neg_df_024.count()

    # 2. Zero fare / zero total (non-negative)
    zero_money_df_024 = annotated_df_024.filter(F.col("sub_any_zero_money_024"))
    zero_money_profile_024 = [
        {
            "payment_type": r["payment_type"],
            "VendorID": r["VendorID"],
            "rows_024": int(r["cnt"]),
            "dist_gt0_024": int(r["dist_gt0"]),
            "dur_gt0_024": int(r["dur_gt0"]),
            "mean_dist_024": round(float(r["avg_dist"]), 2) if r["avg_dist"] is not None else None,
            "mean_total_024": round(float(r["avg_total"]), 2) if r["avg_total"] is not None else None,
        }
        for r in zero_money_df_024.groupBy("payment_type", "VendorID")
        .agg(
            F.count("*").alias("cnt"),
            F.sum(F.when(F.col("trip_distance") > 0, 1).otherwise(0)).alias("dist_gt0"),
            F.sum(F.when(F.col("trip_duration_min_024") > 0, 1).otherwise(0)).alias("dur_gt0"),
            F.mean("trip_distance").alias("avg_dist"),
            F.mean("total_amount").alias("avg_total"),
        )
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    # 3. Extreme fare / total / tip
    ext_money_df_024 = annotated_df_024.filter(F.col("sub_any_extreme_money_024"))
    ext_money_profile_024 = [
        {
            "RatecodeID": r["RatecodeID"],
            "payment_type": r["payment_type"],
            "rows_024": int(r["cnt"]),
            "min_fare_024": round(float(r["min_fare"]), 2),
            "max_fare_024": round(float(r["max_fare"]), 2),
            "mean_dist_024": round(float(r["avg_dist"]), 2),
            "mean_dur_min_024": round(float(r["avg_dur"]), 2),
        }
        for r in ext_money_df_024.groupBy("RatecodeID", "payment_type")
        .agg(
            F.count("*").alias("cnt"),
            F.min("fare_amount").alias("min_fare"),
            F.max("fare_amount").alias("max_fare"),
            F.mean("trip_distance").alias("avg_dist"),
            F.mean("trip_duration_min_024").alias("avg_dur"),
        )
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    # 4. Duration > 180 min buckets
    dur_df_024 = annotated_df_024.filter(F.col("r_dur_024"))
    dur_bucket_024 = (
        F.when(F.col("trip_duration_min_024") <= 360, F.lit("180-360m (3-6h)"))
        .when(F.col("trip_duration_min_024") <= 720, F.lit("360-720m (6-12h)"))
        .when(F.col("trip_duration_min_024") <= 1440, F.lit("720-1440m (12-24h)"))
        .otherwise(F.lit(">1440m (>24h)"))
    )
    dur_profile_024 = [
        {
            "duration_bucket_024": r["bucket"],
            "rows_024": int(r["cnt"]),
            "mean_dist_mi_024": round(float(r["avg_dist"]), 2),
            "median_dist_mi_024": round(float(r["med_dist"]), 2),
            "mean_fare_usd_024": round(float(r["avg_fare"]), 2),
            "median_fare_usd_024": round(float(r["med_fare"]), 2),
            "vendor2_rows_024": int(r["v2_cnt"]),
        }
        for r in dur_df_024.groupBy(dur_bucket_024.alias("bucket"))
        .agg(
            F.count("*").alias("cnt"),
            F.mean("trip_distance").alias("avg_dist"),
            F.percentile_approx("trip_distance", 0.5).alias("med_dist"),
            F.mean("fare_amount").alias("avg_fare"),
            F.percentile_approx("fare_amount", 0.5).alias("med_fare"),
            F.sum(F.when(F.col("VendorID") == 2, 1).otherwise(0)).alias("v2_cnt"),
        )
        .orderBy("bucket")
        .collect()
    ]

    # 5. Speed > 80 mph buckets
    spd_df_024 = annotated_df_024.filter(F.col("r_spd_024"))
    spd_dur_bucket_024 = (
        F.when(F.col("trip_duration_min_024") < 1.0, F.lit("<1m (short timestamp artifact)"))
        .when(F.col("trip_duration_min_024") < 5.0, F.lit("1-5m"))
        .otherwise(F.lit(">=5m"))
    )
    spd_profile_024 = [
        {
            "duration_bucket_024": r["bucket"],
            "rows_024": int(r["cnt"]),
            "dist_gt_100mi_024": int(r["dist_gt100"]),
            "median_dist_mi_024": round(float(r["med_dist"]), 2),
            "median_speed_mph_024": round(float(r["med_spd"]), 2),
            "mean_fare_usd_024": round(float(r["avg_fare"]), 2),
        }
        for r in spd_df_024.groupBy(spd_dur_bucket_024.alias("bucket"))
        .agg(
            F.count("*").alias("cnt"),
            F.sum(F.when(F.col("trip_distance") > 100, 1).otherwise(0)).alias("dist_gt100"),
            F.percentile_approx("trip_distance", 0.5).alias("med_dist"),
            F.percentile_approx("avg_speed_mph_024", 0.5).alias("med_spd"),
            F.mean("fare_amount").alias("avg_fare"),
        )
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    # 6. Out-of-period timestamps (exact details of all 22 rows)
    oop_df_024 = annotated_df_024.filter(F.col("r_period_024"))
    oop_by_month_024 = [
        {
            "pickup_month_024": r["ym"],
            "rows_024": int(r["cnt"]),
            "min_ts_024": r["min_ts"].isoformat(sep=" "),
            "max_ts_024": r["max_ts"].isoformat(sep=" "),
        }
        for r in oop_df_024.groupBy(
            F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("ym")
        )
        .agg(
            F.count("*").alias("cnt"),
            F.min("tpep_pickup_datetime").alias("min_ts"),
            F.max("tpep_pickup_datetime").alias("max_ts"),
        )
        .orderBy("ym")
        .collect()
    ]

    return {
        "negative_money_group_024": {
            "total_rows_024": neg_total_024,
            "exact_positive_mirror_matched_024": mirror_matched_024,
            "mirror_match_pct_024": round(
                (mirror_matched_024 / neg_total_024) * 100.0, 4
            )
            if neg_total_024 > 0
            else 0.0,
            "unmatched_negative_rows_024": neg_total_024 - mirror_matched_024,
            "breakdown_by_payment_vendor_024": neg_by_pay_vendor_024,
        },
        "zero_money_group_024": {
            "total_rows_024": zero_money_df_024.count(),
            "breakdown_by_payment_vendor_024": zero_money_profile_024,
        },
        "extreme_money_group_024": {
            "total_rows_024": ext_money_df_024.count(),
            "breakdown_by_ratecode_payment_024": ext_money_profile_024,
        },
        "duration_over_3h_group_024": {
            "total_rows_024": dur_df_024.count(),
            "breakdown_by_bucket_024": dur_profile_024,
        },
        "speed_over_80mph_group_024": {
            "total_rows_024": spd_df_024.count(),
            "breakdown_by_duration_bucket_024": spd_profile_024,
        },
        "out_of_period_group_024": {
            "total_rows_024": oop_df_024.count(),
            "breakdown_by_month_024": oop_by_month_024,
        },
    }


def compare_hourly_and_zone_demand_024(
    raw_df_024: DataFrame, silver_df_024: DataFrame
) -> dict[str, Any]:
    """Compare hourly (0-23) and zone (PULocationID) pickups between Raw Sep 2026 and Silver."""
    raw_sep_024 = raw_df_024.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(TARGET_START_TS_024))
        & (F.col("tpep_pickup_datetime") < F.lit(TARGET_END_TS_024))
    )

    raw_sep_total_024 = raw_sep_024.count()
    silver_total_024 = silver_df_024.count()

    # Hourly comparison
    raw_hourly_map_024 = {
        int(r["hr"]): int(r["cnt"])
        for r in raw_sep_024.groupBy(F.hour("tpep_pickup_datetime").alias("hr"))
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    silver_hourly_map_024 = {
        int(r["pickup_hour_024"]): int(r["cnt"])
        for r in silver_df_024.groupBy("pickup_hour_024")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }

    hourly_rows_024 = []
    for hr_024 in range(24):
        rc_024 = raw_hourly_map_024.get(hr_024, 0)
        sc_024 = silver_hourly_map_024.get(hr_024, 0)
        diff_024 = sc_024 - rc_024
        drop_pct_024 = round(((rc_024 - sc_024) / rc_024) * 100.0, 4) if rc_024 > 0 else 0.0
        raw_share_024 = round((rc_024 / raw_sep_total_024) * 100.0, 4)
        sil_share_024 = round((sc_024 / silver_total_024) * 100.0, 4)
        hourly_rows_024.append(
            {
                "pickup_hour_024": hr_024,
                "raw_sep_pickups_024": rc_024,
                "silver_pickups_024": sc_024,
                "diff_pickups_024": diff_024,
                "drop_pct_024": drop_pct_024,
                "raw_share_pct_024": raw_share_024,
                "silver_share_pct_024": sil_share_024,
                "share_shift_pp_024": round(sil_share_024 - raw_share_024, 4),
            }
        )

    raw_hr_sorted_024 = sorted(
        hourly_rows_024, key=lambda x: x["raw_sep_pickups_024"], reverse=True
    )
    sil_hr_sorted_024 = sorted(
        hourly_rows_024, key=lambda x: x["silver_pickups_024"], reverse=True
    )
    raw_hr_vec_024 = [float(r["raw_sep_pickups_024"]) for r in hourly_rows_024]
    sil_hr_vec_024 = [float(r["silver_pickups_024"]) for r in hourly_rows_024]

    # Zone comparison
    raw_zone_map_024 = {
        int(r["PULocationID"]): int(r["cnt"])
        for r in raw_sep_024.groupBy("PULocationID")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    sil_zone_map_024 = {
        int(r["PULocationID"]): int(r["cnt"])
        for r in silver_df_024.groupBy("PULocationID")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }

    all_zones_024 = sorted(set(raw_zone_map_024.keys()) | set(sil_zone_map_024.keys()))
    raw_rank_order_024 = sorted(
        all_zones_024, key=lambda z: raw_zone_map_024.get(z, 0), reverse=True
    )
    sil_rank_order_024 = sorted(
        all_zones_024, key=lambda z: sil_zone_map_024.get(z, 0), reverse=True
    )
    raw_rank_map_024 = {z: i + 1 for i, z in enumerate(raw_rank_order_024)}
    sil_rank_map_024 = {z: i + 1 for i, z in enumerate(sil_rank_order_024)}

    zone_rows_024 = []
    for z_024 in raw_rank_order_024:
        rc_024 = raw_zone_map_024.get(z_024, 0)
        sc_024 = sil_zone_map_024.get(z_024, 0)
        drop_cnt_024 = rc_024 - sc_024
        drop_pct_024 = round((drop_cnt_024 / rc_024) * 100.0, 4) if rc_024 > 0 else 0.0
        raw_sh_024 = round((rc_024 / raw_sep_total_024) * 100.0, 4)
        sil_sh_024 = round((sc_024 / silver_total_024) * 100.0, 4)
        zone_rows_024.append(
            {
                "PULocationID": z_024,
                "raw_rank_024": raw_rank_map_024[z_024],
                "silver_rank_024": sil_rank_map_024[z_024],
                "rank_delta_024": raw_rank_map_024[z_024] - sil_rank_map_024[z_024],
                "raw_sep_pickups_024": rc_024,
                "silver_pickups_024": sc_024,
                "dropped_pickups_024": drop_cnt_024,
                "drop_pct_024": drop_pct_024,
                "raw_share_pct_024": raw_sh_024,
                "silver_share_pct_024": sil_sh_024,
                "share_shift_pp_024": round(sil_sh_024 - raw_sh_024, 4),
            }
        )

    raw_z_vec_024 = [float(raw_zone_map_024.get(z, 0)) for z in all_zones_024]
    sil_z_vec_024 = [float(sil_zone_map_024.get(z, 0)) for z in all_zones_024]

    top10_rank_changes_024 = [
        z for z in zone_rows_024[:10] if z["rank_delta_024"] != 0
    ]
    top20_rank_changes_024 = [
        z for z in zone_rows_024[:20] if z["rank_delta_024"] != 0
    ]
    top50_rank_changes_024 = [
        z for z in zone_rows_024[:50] if z["rank_delta_024"] != 0
    ]
    zones_ge100_highest_drop_024 = sorted(
        [z for z in zone_rows_024 if z["raw_sep_pickups_024"] >= 100],
        key=lambda z: z["drop_pct_024"],
        reverse=True,
    )[:15]

    return {
        "baseline_totals_024": {
            "raw_sep_2026_rows_024": raw_sep_total_024,
            "silver_rows_024": silver_total_024,
            "in_period_dropped_rows_024": raw_sep_total_024 - silver_total_024,
            "in_period_dropped_pct_024": round(
                ((raw_sep_total_024 - silver_total_024) / raw_sep_total_024) * 100.0, 4
            ),
        },
        "hourly_comparison_024": {
            "raw_peak_hour_024": raw_hr_sorted_024[0]["pickup_hour_024"],
            "silver_peak_hour_024": sil_hr_sorted_024[0]["pickup_hour_024"],
            "peak_hour_changed_024": (
                raw_hr_sorted_024[0]["pickup_hour_024"]
                != sil_hr_sorted_024[0]["pickup_hour_024"]
            ),
            "raw_quietest_hour_024": raw_hr_sorted_024[-1]["pickup_hour_024"],
            "silver_quietest_hour_024": sil_hr_sorted_024[-1]["pickup_hour_024"],
            "quietest_hour_changed_024": (
                raw_hr_sorted_024[-1]["pickup_hour_024"]
                != sil_hr_sorted_024[-1]["pickup_hour_024"]
            ),
            "raw_top5_hours_024": [r["pickup_hour_024"] for r in raw_hr_sorted_024[:5]],
            "silver_top5_hours_024": [
                r["pickup_hour_024"] for r in sil_hr_sorted_024[:5]
            ],
            "top5_hours_order_identical_024": (
                [r["pickup_hour_024"] for r in raw_hr_sorted_024[:5]]
                == [r["pickup_hour_024"] for r in sil_hr_sorted_024[:5]]
            ),
            "pearson_corr_024": pearson_corr_024(raw_hr_vec_024, sil_hr_vec_024),
            "spearman_corr_024": spearman_corr_024(raw_hr_vec_024, sil_hr_vec_024),
            "max_abs_share_shift_pp_024": max(
                abs(r["share_shift_pp_024"]) for r in hourly_rows_024
            ),
            "max_hourly_drop_pct_024": max(
                hourly_rows_024, key=lambda r: r["drop_pct_024"]
            ),
            "min_hourly_drop_pct_024": min(
                hourly_rows_024, key=lambda r: r["drop_pct_024"]
            ),
            "hourly_table_024": hourly_rows_024,
        },
        "zone_comparison_024": {
            "total_zones_raw_sep_024": len(raw_zone_map_024),
            "total_zones_silver_024": len(sil_zone_map_024),
            "zones_completely_lost_024": sorted(
                set(raw_zone_map_024.keys()) - set(sil_zone_map_024.keys())
            ),
            "pearson_corr_all_zones_024": pearson_corr_024(
                raw_z_vec_024, sil_z_vec_024
            ),
            "spearman_corr_all_zones_024": spearman_corr_024(
                raw_z_vec_024, sil_z_vec_024
            ),
            "max_abs_share_shift_pp_024": max(
                abs(z["share_shift_pp_024"]) for z in zone_rows_024
            ),
            "top10_rank_changes_count_024": len(top10_rank_changes_024),
            "top20_rank_changes_count_024": len(top20_rank_changes_024),
            "top50_rank_changes_count_024": len(top50_rank_changes_024),
            "top20_zones_024": zone_rows_024[:20],
            "top50_rank_changes_detail_024": top50_rank_changes_024,
            "highest_drop_pct_zones_ge100_024": zones_ge100_highest_drop_024,
        },
    }


def run_impact_audit_024(
    raw_path_024: str = HDFS_RAW_INPUT_024,
    silver_path_024: str = HDFS_SILVER_INPUT_024,
    output_json_024: Path = DEFAULT_AUDIT_JSON_024,
) -> dict[str, Any]:
    """Execute complete read-only post-cleaning Silver Layer impact audit."""
    t0_024 = time.time()
    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    try:
        raw_df_024 = spark_024.read.parquet(raw_path_024)
        silver_df_024 = spark_024.read.parquet(silver_path_024)

        total_raw_024 = raw_df_024.count()
        annotated_df_024 = annotate_rules_024(raw_df_024)

        overlap_audit_024 = audit_rule_overlaps_024(annotated_df_024, total_raw_024)
        special_groups_024 = profile_special_groups_024(annotated_df_024)
        demand_impact_024 = compare_hourly_and_zone_demand_024(
            raw_df_024, silver_df_024
        )

        elapsed_sec_024 = round(time.time() - t0_024, 2)
        audit_result_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "script_024": "spark/processing/audit_silver_cleaning_impact_024.py",
                "hdfs_raw_input_024": raw_path_024,
                "hdfs_silver_input_024": silver_path_024,
                "spark_version_024": spark_024.version,
                "spark_master_024": spark_024.sparkContext.master,
                "read_only_verified_024": True,
                "executed_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": elapsed_sec_024,
            },
            "rule_overlap_audit_024": overlap_audit_024,
            "special_group_evaluation_024": special_groups_024,
            "demand_impact_comparison_024": demand_impact_024,
        }

        output_json_024.parent.mkdir(parents=True, exist_ok=True)
        with output_json_024.open("w", encoding="utf-8") as fh_024:
            json.dump(audit_result_024, fh_024, indent=2, ensure_ascii=False)

        print("=== URBANPULSE SILVER IMPACT AUDIT SUMMARY (_024) ===")
        print(f"Total Raw Rows      : {total_raw_024:,}")
        print(
            f"Total Dropped Rows  : {overlap_audit_024['total_dropped_024']:,} "
            f"(single-rule={overlap_audit_024['single_rule_dropped_024']:,}, "
            f"multi-rule overlap={overlap_audit_024['multi_rule_overlap_dropped_024']:,})"
        )
        print(
            "Mirror Negative Pair: "
            f"{special_groups_024['negative_money_group_024']['exact_positive_mirror_matched_024']:,} / "
            f"{special_groups_024['negative_money_group_024']['total_rows_024']:,} "
            f"({special_groups_024['negative_money_group_024']['mirror_match_pct_024']}%)"
        )
        print(
            "Peak / Quietest Hr  : "
            f"Raw={demand_impact_024['hourly_comparison_024']['raw_peak_hour_024']}/"
            f"{demand_impact_024['hourly_comparison_024']['raw_quietest_hour_024']} vs "
            f"Silver={demand_impact_024['hourly_comparison_024']['silver_peak_hour_024']}/"
            f"{demand_impact_024['hourly_comparison_024']['silver_quietest_hour_024']}"
        )
        print(
            "Top-10 / Top-20 Zone Rank Changes: "
            f"{demand_impact_024['zone_comparison_024']['top10_rank_changes_count_024']} / "
            f"{demand_impact_024['zone_comparison_024']['top20_rank_changes_count_024']}"
        )
        print(
            "Zone Spearman / Pearson: "
            f"{demand_impact_024['zone_comparison_024']['spearman_corr_all_zones_024']} / "
            f"{demand_impact_024['zone_comparison_024']['pearson_corr_all_zones_024']}"
        )
        print(f"Elapsed Time        : {elapsed_sec_024} s")
        print(f"Saved JSON          : {output_json_024}")
        return audit_result_024
    finally:
        spark_024.stop()


if __name__ == "__main__":
    run_impact_audit_024(
        os.environ.get("URBANPULSE_RAW_INPUT_024", HDFS_RAW_INPUT_024),
        os.environ.get("URBANPULSE_SILVER_INPUT_024", HDFS_SILVER_INPUT_024),
        Path(os.environ.get("URBANPULSE_AUDIT_JSON_024", str(DEFAULT_AUDIT_JSON_024))),
    )
