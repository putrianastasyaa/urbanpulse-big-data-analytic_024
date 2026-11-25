"""UrbanPulse Silver Layer v2 cleaning and feature derivation using PySpark.

Implements the audit-driven Silver Layer v2 design (from
`reports/silver_cleaning_impact_audit_024.md`):
- Separates core pickup-event data quality filtering from analysis-specific
  metric filtering.
- Preserves potentially valid pickups that exhibit metric/telemetry anomalies
  (zero fare with positive total such as VendorID=6 Flex Fare, duration >180m,
  speed >80 mph, distance >100 mi, high fares $500-$2,000, negative sub-components
  with positive total, Flex Fare passenger_count NULL, zero duration VendorID=7,
  zero distance Flex Fare, and zones 264/265) via explicit `_024` flags and
  clean analytical `*_valid_*_024` metric columns.
- Flags retained positive rows that have an exact mirror negative reversal in
  Raw (`has_negative_reversal_pair_024`) so Gold Layer financial analyses can
  exclude reversals explicitly.
- Supports safe staging writes (`URBANPULSE_SILVER_OUTPUT_024`) so replacement
  of the live HDFS Silver directory occurs only after full read-back
  verification passes.
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
HDFS_SILVER_OUTPUT_024 = (
    "hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024"
)
DEFAULT_OUTPUT_JSON_024 = Path("reports/silver_cleaning_summary_024.json")

# Hard-removal thresholds (strictly out-of-period, non-paid, or proven corrupt)
TARGET_START_TS_024 = "2026-09-01 00:00:00"
TARGET_END_TS_024 = "2026-10-01 00:00:00"
CORRUPT_MAX_AMOUNT_USD_024 = 2000.0

# Analysis-flagging thresholds (preserved in Silver, nulled in *_valid_*_024 metrics)
FLAG_MAX_DURATION_MIN_024 = 180.0
FLAG_MAX_DISTANCE_MI_024 = 100.0
FLAG_MAX_SPEED_MPH_024 = 80.0
FLAG_HIGH_FARE_USD_024 = 500.0
FLAG_HIGH_TOTAL_USD_024 = 1000.0
FLAG_HIGH_TIP_USD_024 = 200.0
SILVER_OUTPUT_PARTITIONS_024 = 8

SURCHARGE_COLUMNS_024 = [
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]


def build_spark_session_024() -> SparkSession:
    """Create a SparkSession in local[*] mode without writing to shared event logs."""
    return (
        SparkSession.builder.appName("UrbanPulse_Silver_Cleaning_v2_024")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def format_value_024(value_024: Any) -> Any:
    """Convert scalar values into clean JSON-serializable primitives."""
    if value_024 is None:
        return None
    if isinstance(value_024, datetime):
        return value_024.isoformat(sep=" ")
    if isinstance(value_024, float):
        return round(value_024, 6)
    return value_024


def pearson_corr_024(xs_024: list[float], ys_024: list[float]) -> float:
    """Compute Pearson correlation."""
    n_024 = len(xs_024)
    if n_024 < 2:
        return 1.0
    mx_024 = sum(xs_024) / n_024
    my_024 = sum(ys_024) / n_024
    num_024 = sum((x - mx_024) * (y - my_024) for x, y in zip(xs_024, ys_024))
    dx_024 = math.sqrt(sum((x - mx_024) ** 2 for x in xs_024))
    dy_024 = math.sqrt(sum((y - my_024) ** 2 for y in ys_024))
    if dx_024 == 0 or dy_024 == 0:
        return 1.0
    return round(num_024 / (dx_024 * dy_024), 8)


def average_ranks_024(vals_024: list[float]) -> list[float]:
    """Compute average ranks for Spearman rank correlation."""
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


def build_v2_conditions_024(raw_df_024: DataFrame) -> dict[str, Any]:
    """Define Silver v2 hard-removal predicates and telemetry/monetary flag expressions."""
    existing_cols_024 = set(raw_df_024.columns)
    present_surcharges_024 = [
        c_024 for c_024 in SURCHARGE_COLUMNS_024 if c_024 in existing_cols_024
    ]

    duration_expr_024 = (
        F.unix_timestamp(F.col("tpep_dropoff_datetime"))
        - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    ) / F.lit(60.0)
    raw_speed_expr_024 = F.when(
        (duration_expr_024 > 0) & (F.col("trip_distance") > 0),
        F.col("trip_distance") / (duration_expr_024 / F.lit(60.0)),
    )

    surcharge_neg_cond_024 = F.lit(False)
    for col_024 in present_surcharges_024:
        surcharge_neg_cond_024 = surcharge_neg_cond_024 | (
            F.col(f"`{col_024}`").isNotNull() & (F.col(f"`{col_024}`") < 0)
        )

    return {
        "duration_expr_024": duration_expr_024,
        "raw_speed_expr_024": raw_speed_expr_024,
        "surcharge_neg_cond_024": surcharge_neg_cond_024,
        # Hard-removal rules (Silver v2)
        "rule_out_of_period_024": (
            F.col("tpep_pickup_datetime").isNull()
            | F.col("tpep_dropoff_datetime").isNull()
            | (F.col("tpep_pickup_datetime") < F.lit(TARGET_START_TS_024))
            | (F.col("tpep_pickup_datetime") >= F.lit(TARGET_END_TS_024))
        ),
        "rule_invalid_location_024": (
            F.col("PULocationID").isNull()
            | F.col("DOLocationID").isNull()
            | (~F.col("PULocationID").between(1, 265))
            | (~F.col("DOLocationID").between(1, 265))
        ),
        "rule_negative_telemetry_024": (
            (duration_expr_024 < 0)
            | F.col("trip_distance").isNull()
            | (F.col("trip_distance") < 0)
        ),
        "rule_non_positive_total_024": (
            F.col("total_amount").isNull() | (F.col("total_amount") <= 0)
        ),
        "rule_corrupt_extreme_amount_024": (
            F.col("fare_amount").isNull()
            | (F.col("fare_amount") > F.lit(CORRUPT_MAX_AMOUNT_USD_024))
            | (F.col("total_amount") > F.lit(CORRUPT_MAX_AMOUNT_USD_024))
        ),
        # v1-only rules that are NOW RELAXED TO FLAGS in v2 (for recovery tracking)
        "v1_only_zero_fare_024": (
            (F.col("fare_amount") == 0) & (F.col("total_amount") > 0)
        ),
        "v1_only_neg_component_paid_024": (
            (F.col("total_amount") > 0)
            & (
                (F.col("fare_amount") < 0)
                | (F.col("tip_amount") < 0)
                | surcharge_neg_cond_024
            )
        ),
        "v1_only_extreme_duration_024": (
            duration_expr_024 > F.lit(FLAG_MAX_DURATION_MIN_024)
        ),
        "v1_only_extreme_distance_024": (
            F.col("trip_distance") > F.lit(FLAG_MAX_DISTANCE_MI_024)
        ),
        "v1_only_extreme_speed_024": (
            raw_speed_expr_024.isNotNull()
            & (raw_speed_expr_024 > F.lit(FLAG_MAX_SPEED_MPH_024))
        ),
        "v1_only_high_fare_500_2000_024": (
            (F.col("total_amount") > 0)
            & (F.col("total_amount") <= F.lit(CORRUPT_MAX_AMOUNT_USD_024))
            & (F.col("fare_amount") <= F.lit(CORRUPT_MAX_AMOUNT_USD_024))
            & (
                (F.col("fare_amount") > F.lit(FLAG_HIGH_FARE_USD_024))
                | (F.col("total_amount") > F.lit(FLAG_HIGH_TOTAL_USD_024))
                | (F.col("tip_amount") > F.lit(FLAG_HIGH_TIP_USD_024))
            )
        ),
    }


def extract_negative_reversal_keys_024(raw_df_024: DataFrame) -> DataFrame:
    """Build distinct lookup keys of raw negative-total rows (mirror reversal detection)."""
    return (
        raw_df_024.filter(F.col("total_amount") < 0)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            F.round(F.col("trip_distance"), 2).alias("_rev_dist_024"),
            F.round(F.abs(F.col("total_amount")), 2).alias("_rev_abs_total_024"),
        )
        .distinct()
        .withColumn("_has_neg_rev_024", F.lit(True))
    )


def evaluate_v2_cleaning_and_recovery_024(
    raw_df_024: DataFrame, conds_024: dict[str, Any]
) -> dict[str, Any]:
    """Measure v2 removal rules, exact overlaps, non-positive total split, and v1->v2 recovery."""
    v2_drop_024 = (
        conds_024["rule_out_of_period_024"]
        | conds_024["rule_invalid_location_024"]
        | conds_024["rule_negative_telemetry_024"]
        | conds_024["rule_non_positive_total_024"]
        | conds_024["rule_corrupt_extreme_amount_024"]
    )
    v2_keep_024 = ~v2_drop_024

    v1_relaxed_any_024 = (
        conds_024["v1_only_zero_fare_024"]
        | conds_024["v1_only_neg_component_paid_024"]
        | conds_024["v1_only_extreme_duration_024"]
        | conds_024["v1_only_extreme_distance_024"]
        | conds_024["v1_only_extreme_speed_024"]
        | conds_024["v1_only_high_fare_500_2000_024"]
    )

    def cnt_024(expr_024: Any, alias_024: str) -> Any:
        return F.sum(F.when(expr_024, 1).otherwise(0)).alias(alias_024)

    # Also compute exact mirror match count among dropped negative-total rows
    pos_keys_024 = (
        raw_df_024.filter(F.col("total_amount") > 0)
        .select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            F.round(F.col("trip_distance"), 2).alias("_k_dist_024"),
            F.round(F.abs(F.col("total_amount")), 2).alias("_k_total_024"),
        )
        .distinct()
    )
    neg_total_in_period_024 = raw_df_024.filter(
        (~conds_024["rule_out_of_period_024"]) & (F.col("total_amount") < 0)
    )
    neg_mirror_matched_024 = (
        neg_total_in_period_024.select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            F.round(F.col("trip_distance"), 2).alias("_k_dist_024"),
            F.round(F.abs(F.col("total_amount")), 2).alias("_k_total_024"),
        )
        .join(
            pos_keys_024,
            on=[
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "_k_dist_024",
                "_k_total_024",
            ],
            how="inner",
        )
        .count()
    )

    row_024 = (
        raw_df_024.agg(
            F.count("*").alias("raw_total_rows_024"),
            cnt_024(conds_024["rule_out_of_period_024"], "drop_out_of_period_024"),
            cnt_024(conds_024["rule_invalid_location_024"], "drop_invalid_location_024"),
            cnt_024(
                conds_024["rule_negative_telemetry_024"],
                "drop_negative_telemetry_024",
            ),
            cnt_024(
                conds_024["rule_non_positive_total_024"],
                "drop_non_positive_total_024",
            ),
            cnt_024(
                (~conds_024["rule_out_of_period_024"]) & (F.col("total_amount") < 0),
                "drop_in_period_neg_total_024",
            ),
            cnt_024(
                (~conds_024["rule_out_of_period_024"]) & (F.col("total_amount") == 0),
                "drop_in_period_zero_total_024",
            ),
            cnt_024(
                conds_024["rule_corrupt_extreme_amount_024"],
                "drop_corrupt_extreme_amount_024",
            ),
            cnt_024(v2_drop_024, "v2_total_dropped_024"),
            cnt_024(v2_keep_024, "v2_retained_rows_024"),
            # Recovered rows vs v1 (rows kept in v2 that v1 had dropped)
            cnt_024(v2_keep_024 & v1_relaxed_any_024, "recovered_total_vs_v1_024"),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_zero_fare_024"],
                "recovered_zero_fare_024",
            ),
            cnt_024(
                v2_keep_024
                & conds_024["v1_only_zero_fare_024"]
                & (F.col("VendorID") == 6)
                & (F.col("payment_type") == 0),
                "recovered_vendor6_flex_zero_fare_024",
            ),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_neg_component_paid_024"],
                "recovered_neg_component_paid_024",
            ),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_extreme_duration_024"],
                "recovered_extreme_duration_024",
            ),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_extreme_distance_024"],
                "recovered_extreme_distance_024",
            ),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_extreme_speed_024"],
                "recovered_extreme_speed_024",
            ),
            cnt_024(
                v2_keep_024 & conds_024["v1_only_high_fare_500_2000_024"],
                "recovered_high_fare_500_2000_024",
            ),
            # Segment preservation
            cnt_024(F.col("payment_type") == 0, "raw_flex_fare_024"),
            cnt_024(v2_keep_024 & (F.col("payment_type") == 0), "v2_flex_fare_024"),
            cnt_024(F.col("VendorID") == 7, "raw_vendor7_024"),
            cnt_024(v2_keep_024 & (F.col("VendorID") == 7), "v2_vendor7_024"),
            cnt_024(F.col("VendorID") == 6, "raw_vendor6_024"),
            cnt_024(v2_keep_024 & (F.col("VendorID") == 6), "v2_vendor6_024"),
        )
        .collect()[0]
        .asDict()
    )

    total_024 = int(row_024["raw_total_rows_024"])
    v2_retained_024 = int(row_024["v2_retained_rows_024"])
    v2_dropped_024 = int(row_024["v2_total_dropped_024"])
    in_period_neg_024 = int(row_024["drop_in_period_neg_total_024"])
    in_period_zero_024 = int(row_024["drop_in_period_zero_total_024"])

    def pct_raw_024(val_024: int) -> float:
        return round((val_024 / total_024) * 100.0, 4) if total_024 > 0 else 0.0

    # Disjoint combinations of v2 dropped rows
    drop_combo_024 = F.concat_ws(
        " + ",
        F.when(conds_024["rule_out_of_period_024"], F.lit("out_of_period_024")),
        F.when(conds_024["rule_invalid_location_024"], F.lit("invalid_location_024")),
        F.when(
            conds_024["rule_negative_telemetry_024"],
            F.lit("negative_telemetry_024"),
        ),
        F.when(
            conds_024["rule_non_positive_total_024"],
            F.lit("non_positive_total_amount_024"),
        ),
        F.when(
            conds_024["rule_corrupt_extreme_amount_024"],
            F.lit("corrupt_extreme_amount_gt_2000_024"),
        ),
    )
    dropped_combos_024 = [
        {
            "rule_combination_024": r["combo"],
            "disjoint_rows_024": int(r["cnt"]),
            "pct_of_dropped_024": round((int(r["cnt"]) / v2_dropped_024) * 100.0, 4),
            "pct_of_raw_024": pct_raw_024(int(r["cnt"])),
        }
        for r in raw_df_024.filter(v2_drop_024)
        .groupBy(drop_combo_024.alias("combo"))
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .collect()
    ]

    raw_flex_024 = int(row_024["raw_flex_fare_024"])
    v2_flex_024 = int(row_024["v2_flex_fare_024"])
    raw_v7_024 = int(row_024["raw_vendor7_024"])
    v2_v7_024 = int(row_024["v2_vendor7_024"])
    raw_v6_024 = int(row_024["raw_vendor6_024"])
    v2_v6_024 = int(row_024["v2_vendor6_024"])

    return {
        "raw_total_rows_024": total_024,
        "silver_v2_retained_rows_024": v2_retained_024,
        "silver_v2_dropped_rows_024": v2_dropped_024,
        "retention_pct_024": pct_raw_024(v2_retained_024),
        "dropped_pct_024": pct_raw_024(v2_dropped_024),
        "v2_hard_removal_rules_024": {
            "out_of_period_sept_2026_024": {
                "count_024": int(row_024["drop_out_of_period_024"]),
                "pct_of_raw_024": pct_raw_024(int(row_024["drop_out_of_period_024"])),
            },
            "invalid_location_outside_1_265_024": {
                "count_024": int(row_024["drop_invalid_location_024"]),
                "pct_of_raw_024": pct_raw_024(
                    int(row_024["drop_invalid_location_024"])
                ),
            },
            "negative_duration_or_distance_024": {
                "count_024": int(row_024["drop_negative_telemetry_024"]),
                "pct_of_raw_024": pct_raw_024(
                    int(row_024["drop_negative_telemetry_024"])
                ),
            },
            "non_positive_total_amount_024": {
                "count_024": int(row_024["drop_non_positive_total_024"]),
                "pct_of_raw_024": pct_raw_024(
                    int(row_024["drop_non_positive_total_024"])
                ),
                "in_period_negative_total_024": in_period_neg_024,
                "in_period_negative_mirror_matched_024": neg_mirror_matched_024,
                "in_period_negative_mirror_pct_024": round(
                    (neg_mirror_matched_024 / in_period_neg_024) * 100.0, 4
                )
                if in_period_neg_024 > 0
                else 0.0,
                "in_period_negative_unmatched_024": (
                    in_period_neg_024 - neg_mirror_matched_024
                ),
                "in_period_zero_total_024": in_period_zero_024,
            },
            "corrupt_extreme_amount_gt_2000_024": {
                "count_024": int(row_024["drop_corrupt_extreme_amount_024"]),
                "pct_of_raw_024": pct_raw_024(
                    int(row_024["drop_corrupt_extreme_amount_024"])
                ),
            },
        },
        "v2_dropped_disjoint_combinations_024": dropped_combos_024,
        "recovered_vs_silver_v1_024": {
            "total_unique_recovered_rows_024": int(
                row_024["recovered_total_vs_v1_024"]
            ),
            "by_relaxed_rule_gross_024": {
                "zero_fare_with_positive_total_024": int(
                    row_024["recovered_zero_fare_024"]
                ),
                "vendor6_flex_zero_fare_subset_024": int(
                    row_024["recovered_vendor6_flex_zero_fare_024"]
                ),
                "negative_component_with_positive_total_024": int(
                    row_024["recovered_neg_component_paid_024"]
                ),
                "extreme_duration_gt_180m_024": int(
                    row_024["recovered_extreme_duration_024"]
                ),
                "extreme_distance_gt_100mi_024": int(
                    row_024["recovered_extreme_distance_024"]
                ),
                "extreme_speed_gt_80mph_024": int(
                    row_024["recovered_extreme_speed_024"]
                ),
                "high_fare_500_to_2000_or_tip_gt_200_024": int(
                    row_024["recovered_high_fare_500_2000_024"]
                ),
            },
        },
        "valid_segment_preservation_024": {
            "flex_fare_raw_024": raw_flex_024,
            "flex_fare_silver_v2_024": v2_flex_024,
            "flex_fare_retention_pct_024": round(
                (v2_flex_024 / raw_flex_024) * 100.0, 4
            ),
            "vendor7_raw_024": raw_v7_024,
            "vendor7_silver_v2_024": v2_v7_024,
            "vendor7_retention_pct_024": round((v2_v7_024 / raw_v7_024) * 100.0, 4),
            "vendor6_raw_024": raw_v6_024,
            "vendor6_silver_v2_024": v2_v6_024,
            "vendor6_retention_pct_024": round((v2_v6_024 / raw_v6_024) * 100.0, 4),
        },
    }


def build_silver_v2_dataframe_024(
    raw_df_024: DataFrame, conds_024: dict[str, Any]
) -> DataFrame:
    """Apply Silver v2 hard filtering, join reversal-pair flags, and append `_024` features."""
    v2_keep_024 = ~(
        conds_024["rule_out_of_period_024"]
        | conds_024["rule_invalid_location_024"]
        | conds_024["rule_negative_telemetry_024"]
        | conds_024["rule_non_positive_total_024"]
        | conds_024["rule_corrupt_extreme_amount_024"]
    )
    filtered_df_024 = raw_df_024.filter(v2_keep_024)

    # Mark positive rows that have an exact negative mirror reversal in Raw
    neg_rev_keys_024 = extract_negative_reversal_keys_024(raw_df_024)
    joined_df_024 = (
        filtered_df_024.withColumn(
            "_rev_dist_024", F.round(F.col("trip_distance"), 2)
        )
        .withColumn("_rev_abs_total_024", F.round(F.abs(F.col("total_amount")), 2))
        .join(
            F.broadcast(neg_rev_keys_024),
            on=[
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "_rev_dist_024",
                "_rev_abs_total_024",
            ],
            how="left",
        )
        .withColumn(
            "has_negative_reversal_pair_024",
            F.coalesce(F.col("_has_neg_rev_024"), F.lit(False)),
        )
        .drop("_rev_dist_024", "_rev_abs_total_024", "_has_neg_rev_024")
    )

    # Recompute surcharge negative condition on joined DataFrame
    present_surcharges_024 = [
        c_024 for c_024 in SURCHARGE_COLUMNS_024 if c_024 in joined_df_024.columns
    ]
    surcharge_neg_024 = F.lit(False)
    for col_024 in present_surcharges_024:
        surcharge_neg_024 = surcharge_neg_024 | (
            F.col(f"`{col_024}`").isNotNull() & (F.col(f"`{col_024}`") < 0)
        )

    dow_iso_024 = (
        (F.dayofweek(F.col("tpep_pickup_datetime")) + F.lit(5)) % F.lit(7)
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
    pickup_hour_024 = F.hour(F.col("tpep_pickup_datetime")).cast("int")
    time_bucket_024 = (
        F.when(pickup_hour_024 < 6, F.lit("Dini Hari (00-05)"))
        .when(pickup_hour_024 < 12, F.lit("Pagi (06-11)"))
        .when(pickup_hour_024 < 18, F.lit("Siang (12-17)"))
        .otherwise(F.lit("Malam (18-23)"))
    )

    duration_min_024 = F.round(
        (
            F.unix_timestamp(F.col("tpep_dropoff_datetime"))
            - F.unix_timestamp(F.col("tpep_pickup_datetime"))
        )
        / F.lit(60.0),
        4,
    )
    raw_speed_024 = F.when(
        (duration_min_024 > 0) & (F.col("trip_distance") > 0),
        F.col("trip_distance") / (duration_min_024 / F.lit(60.0)),
    )

    is_extreme_dur_024 = duration_min_024 > F.lit(FLAG_MAX_DURATION_MIN_024)
    is_extreme_dist_024 = F.col("trip_distance") > F.lit(FLAG_MAX_DISTANCE_MI_024)
    is_extreme_spd_024 = raw_speed_024.isNotNull() & (
        raw_speed_024 > F.lit(FLAG_MAX_SPEED_MPH_024)
    )
    is_zero_fare_024 = (F.col("fare_amount") == F.lit(0.0)) & (
        F.col("total_amount") > F.lit(0.0)
    )
    is_high_fare_024 = (
        (F.col("fare_amount") > F.lit(FLAG_HIGH_FARE_USD_024))
        | (F.col("total_amount") > F.lit(FLAG_HIGH_TOTAL_USD_024))
        | (F.col("tip_amount") > F.lit(FLAG_HIGH_TIP_USD_024))
    )
    is_neg_comp_024 = (
        (F.col("fare_amount") < F.lit(0.0))
        | (F.col("tip_amount") < F.lit(0.0))
        | surcharge_neg_024
    )

    # Validated analytical metric columns (NULLed on flagged anomalies)
    dur_valid_024 = F.when((duration_min_024 > 0) & (~is_extreme_dur_024), duration_min_024)
    dist_valid_024 = F.when(
        (F.col("trip_distance") > 0) & (~is_extreme_dist_024), F.col("trip_distance")
    )
    speed_valid_024 = F.when(
        raw_speed_024.isNotNull() & (~is_extreme_spd_024) & (~is_extreme_dist_024),
        F.round(raw_speed_024, 4),
    )
    fare_valid_024 = F.when(
        (F.col("fare_amount") > 0)
        & (F.col("fare_amount") <= F.lit(FLAG_HIGH_FARE_USD_024)),
        F.col("fare_amount"),
    )
    total_valid_024 = F.when(
        (F.col("total_amount") > 0)
        & (F.col("total_amount") <= F.lit(FLAG_HIGH_TOTAL_USD_024)),
        F.col("total_amount"),
    )
    tip_ratio_024 = F.when(
        (F.col("fare_amount") > 0) & (F.col("tip_amount") >= 0),
        F.round(F.col("tip_amount") / F.col("fare_amount"), 4),
    )

    # Preserve exact original column order first, then append `_024` columns cleanly
    original_cols_024 = list(raw_df_024.columns)
    enriched_df_024 = (
        joined_df_024.withColumn(
            "pickup_date_024", F.to_date(F.col("tpep_pickup_datetime"))
        )
        .withColumn("pickup_hour_024", pickup_hour_024)
        .withColumn("day_of_week_024", dow_iso_024)
        .withColumn("day_name_024", day_name_024)
        .withColumn("is_weekend_024", dow_iso_024.isin(5, 6))
        .withColumn(
            "period_024", F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM")
        )
        .withColumn("time_of_day_bucket_024", time_bucket_024)
        .withColumn("trip_duration_min_024", duration_min_024)
        .withColumn("trip_duration_valid_min_024", dur_valid_024)
        .withColumn("trip_distance_valid_mi_024", dist_valid_024)
        .withColumn("avg_speed_mph_024", speed_valid_024)
        .withColumn("fare_amount_valid_usd_024", fare_valid_024)
        .withColumn("total_amount_valid_usd_024", total_valid_024)
        .withColumn("tip_ratio_024", tip_ratio_024)
        .withColumn("is_flex_fare_024", F.col("payment_type") == F.lit(0))
        .withColumn("is_passenger_missing_024", F.col("passenger_count").isNull())
        .withColumn(
            "is_passenger_valid_024",
            F.col("passenger_count").isNotNull()
            & F.col("passenger_count").between(1, 6),
        )
        .withColumn("is_zero_duration_024", duration_min_024 == F.lit(0.0))
        .withColumn("is_extreme_duration_024", is_extreme_dur_024)
        .withColumn("is_zero_distance_024", F.col("trip_distance") == F.lit(0.0))
        .withColumn("is_extreme_distance_024", is_extreme_dist_024)
        .withColumn("is_extreme_speed_024", is_extreme_spd_024)
        .withColumn("is_zero_fare_024", is_zero_fare_024)
        .withColumn("is_high_fare_outlier_024", is_high_fare_024)
        .withColumn("is_negative_component_024", is_neg_comp_024)
        .withColumn(
            "is_known_pickup_zone_024", ~F.col("PULocationID").isin(264, 265)
        )
        .withColumn(
            "is_known_dropoff_zone_024", ~F.col("DOLocationID").isin(264, 265)
        )
    )

    derived_order_024 = [
        "pickup_date_024",
        "pickup_hour_024",
        "day_of_week_024",
        "day_name_024",
        "is_weekend_024",
        "period_024",
        "time_of_day_bucket_024",
        "trip_duration_min_024",
        "trip_duration_valid_min_024",
        "trip_distance_valid_mi_024",
        "avg_speed_mph_024",
        "fare_amount_valid_usd_024",
        "total_amount_valid_usd_024",
        "tip_ratio_024",
        "is_flex_fare_024",
        "is_passenger_missing_024",
        "is_passenger_valid_024",
        "is_zero_duration_024",
        "is_extreme_duration_024",
        "is_zero_distance_024",
        "is_extreme_distance_024",
        "is_extreme_speed_024",
        "is_zero_fare_024",
        "is_high_fare_outlier_024",
        "is_negative_component_024",
        "has_negative_reversal_pair_024",
        "is_known_pickup_zone_024",
        "is_known_dropoff_zone_024",
    ]
    return enriched_df_024.select(
        *[F.col(f"`{c}`") for c in original_cols_024],
        *[F.col(c) for c in derived_order_024],
    )


def compare_distributions_024(
    raw_df_024: DataFrame,
    v2_df_024: DataFrame,
    v1_df_024: DataFrame | None = None,
) -> dict[str, Any]:
    """Compare hourly and zone pickup distributions across Raw Sep 2026, Silver v1, and Silver v2."""
    raw_sep_024 = raw_df_024.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(TARGET_START_TS_024))
        & (F.col("tpep_pickup_datetime") < F.lit(TARGET_END_TS_024))
    )
    raw_tot_024 = raw_sep_024.count()
    v2_tot_024 = v2_df_024.count()
    v1_tot_024 = v1_df_024.count() if v1_df_024 is not None else None

    raw_hr_024 = {
        int(r["hr"]): int(r["cnt"])
        for r in raw_sep_024.groupBy(F.hour("tpep_pickup_datetime").alias("hr"))
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    v2_hr_024 = {
        int(r["pickup_hour_024"]): int(r["cnt"])
        for r in v2_df_024.groupBy("pickup_hour_024")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    v1_hr_024 = (
        {
            int(r["pickup_hour_024"]): int(r["cnt"])
            for r in v1_df_024.groupBy("pickup_hour_024")
            .agg(F.count("*").alias("cnt"))
            .collect()
        }
        if v1_df_024 is not None
        else {}
    )

    hourly_table_024 = []
    for hr_024 in range(24):
        rc_024 = raw_hr_024.get(hr_024, 0)
        c2_024 = v2_hr_024.get(hr_024, 0)
        c1_024 = v1_hr_024.get(hr_024) if v1_df_024 is not None else None
        row_dict_024: dict[str, Any] = {
            "pickup_hour_024": hr_024,
            "raw_sep_pickups_024": rc_024,
            "silver_v2_pickups_024": c2_024,
            "v2_minus_raw_024": c2_024 - rc_024,
            "v2_drop_pct_vs_raw_024": round(((rc_024 - c2_024) / rc_024) * 100.0, 4),
            "raw_share_pct_024": round((rc_024 / raw_tot_024) * 100.0, 4),
            "v2_share_pct_024": round((c2_024 / v2_tot_024) * 100.0, 4),
        }
        if c1_024 is not None:
            row_dict_024["silver_v1_pickups_024"] = c1_024
            row_dict_024["v2_minus_v1_recovered_024"] = c2_024 - c1_024
        hourly_table_024.append(row_dict_024)

    raw_hr_sorted_024 = sorted(
        hourly_table_024, key=lambda x: x["raw_sep_pickups_024"], reverse=True
    )
    v2_hr_sorted_024 = sorted(
        hourly_table_024, key=lambda x: x["silver_v2_pickups_024"], reverse=True
    )

    # Zone distributions
    raw_z_024 = {
        int(r["PULocationID"]): int(r["cnt"])
        for r in raw_sep_024.groupBy("PULocationID")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    v2_z_024 = {
        int(r["PULocationID"]): int(r["cnt"])
        for r in v2_df_024.groupBy("PULocationID")
        .agg(F.count("*").alias("cnt"))
        .collect()
    }
    v1_z_024 = (
        {
            int(r["PULocationID"]): int(r["cnt"])
            for r in v1_df_024.groupBy("PULocationID")
            .agg(F.count("*").alias("cnt"))
            .collect()
        }
        if v1_df_024 is not None
        else {}
    )

    all_z_024 = sorted(set(raw_z_024.keys()) | set(v2_z_024.keys()))
    raw_order_024 = sorted(all_z_024, key=lambda z: raw_z_024.get(z, 0), reverse=True)
    v2_order_024 = sorted(all_z_024, key=lambda z: v2_z_024.get(z, 0), reverse=True)
    raw_rank_024 = {z: i + 1 for i, z in enumerate(raw_order_024)}
    v2_rank_024 = {z: i + 1 for i, z in enumerate(v2_order_024)}

    zone_rows_024 = []
    for z_024 in raw_order_024:
        rc_024 = raw_z_024.get(z_024, 0)
        c2_024 = v2_z_024.get(z_024, 0)
        z_entry_024: dict[str, Any] = {
            "PULocationID": z_024,
            "raw_rank_024": raw_rank_024[z_024],
            "silver_v2_rank_024": v2_rank_024[z_024],
            "rank_delta_024": raw_rank_024[z_024] - v2_rank_024[z_024],
            "raw_sep_pickups_024": rc_024,
            "silver_v2_pickups_024": c2_024,
            "v2_drop_pct_vs_raw_024": round(((rc_024 - c2_024) / rc_024) * 100.0, 4)
            if rc_024 > 0
            else 0.0,
            "share_shift_pp_024": round(
                ((c2_024 / v2_tot_024) - (rc_024 / raw_tot_024)) * 100.0, 4
            ),
        }
        if v1_df_024 is not None:
            c1_024 = v1_z_024.get(z_024, 0)
            z_entry_024["silver_v1_pickups_024"] = c1_024
            z_entry_024["v2_minus_v1_recovered_024"] = c2_024 - c1_024
        zone_rows_024.append(z_entry_024)

    top_recovered_zones_024 = (
        sorted(
            zone_rows_024,
            key=lambda z: z.get("v2_minus_v1_recovered_024", 0),
            reverse=True,
        )[:10]
        if v1_df_024 is not None
        else []
    )

    return {
        "totals_024": {
            "raw_sep_2026_rows_024": raw_tot_024,
            "silver_v1_rows_024": v1_tot_024,
            "silver_v2_rows_024": v2_tot_024,
            "v2_minus_v1_net_recovered_024": (
                v2_tot_024 - v1_tot_024 if v1_tot_024 is not None else None
            ),
            "v2_in_period_dropped_pct_024": round(
                ((raw_tot_024 - v2_tot_024) / raw_tot_024) * 100.0, 4
            ),
        },
        "hourly_024": {
            "raw_peak_hour_024": raw_hr_sorted_024[0]["pickup_hour_024"],
            "silver_v2_peak_hour_024": v2_hr_sorted_024[0]["pickup_hour_024"],
            "raw_quietest_hour_024": raw_hr_sorted_024[-1]["pickup_hour_024"],
            "silver_v2_quietest_hour_024": v2_hr_sorted_024[-1]["pickup_hour_024"],
            "top5_hours_raw_024": [r["pickup_hour_024"] for r in raw_hr_sorted_024[:5]],
            "top5_hours_v2_024": [r["pickup_hour_024"] for r in v2_hr_sorted_024[:5]],
            "pearson_raw_vs_v2_024": pearson_corr_024(
                [float(r["raw_sep_pickups_024"]) for r in hourly_table_024],
                [float(r["silver_v2_pickups_024"]) for r in hourly_table_024],
            ),
            "spearman_raw_vs_v2_024": spearman_corr_024(
                [float(r["raw_sep_pickups_024"]) for r in hourly_table_024],
                [float(r["silver_v2_pickups_024"]) for r in hourly_table_024],
            ),
            "hourly_table_024": hourly_table_024,
        },
        "zone_024": {
            "total_zones_raw_sep_024": len(raw_z_024),
            "total_zones_silver_v2_024": len(v2_z_024),
            "top10_rank_changes_024": sum(
                1 for z in zone_rows_024[:10] if z["rank_delta_024"] != 0
            ),
            "top20_rank_changes_024": sum(
                1 for z in zone_rows_024[:20] if z["rank_delta_024"] != 0
            ),
            "top50_rank_changes_024": sum(
                1 for z in zone_rows_024[:50] if z["rank_delta_024"] != 0
            ),
            "pearson_all_zones_raw_vs_v2_024": pearson_corr_024(
                [float(raw_z_024.get(z, 0)) for z in all_z_024],
                [float(v2_z_024.get(z, 0)) for z in all_z_024],
            ),
            "spearman_all_zones_raw_vs_v2_024": spearman_corr_024(
                [float(raw_z_024.get(z, 0)) for z in all_z_024],
                [float(v2_z_024.get(z, 0)) for z in all_z_024],
            ),
            "max_abs_share_shift_pp_024": max(
                abs(z["share_shift_pp_024"]) for z in zone_rows_024
            ),
            "top20_zones_024": zone_rows_024[:20],
            "top10_recovered_zones_v2_vs_v1_024": top_recovered_zones_024,
        },
    }


def verify_silver_v2_output_024(
    spark_024: SparkSession,
    silver_path_024: str,
    original_schema_024: list[tuple[str, str]],
) -> dict[str, Any]:
    """Re-read written Silver v2 Parquet from HDFS and compute full audit verification."""
    silver_df_024 = spark_024.read.parquet(silver_path_024)
    silver_rows_024 = int(silver_df_024.count())

    actual_schema_map_024 = {
        f.name: f.dataType.simpleString() for f in silver_df_024.schema.fields
    }
    orig_names_024 = [name for name, _ in original_schema_024]
    orig_preserved_exact_024 = all(
        actual_schema_map_024.get(name) == dtype
        for name, dtype in original_schema_024
    )
    derived_cols_024 = [
        f.name for f in silver_df_024.schema.fields if f.name not in orig_names_024
    ]
    all_derived_use_024_suffix_024 = all(
        c.endswith("_024") for c in derived_cols_024
    )

    # Single-pass null counts
    null_exprs_024 = [
        F.sum(F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)).alias(f"null__{c}")
        for c in silver_df_024.columns
    ]
    null_row_024 = silver_df_024.agg(*null_exprs_024).collect()[0].asDict()
    null_summary_024 = [
        {
            "column_name": f.name,
            "data_type": f.dataType.simpleString(),
            "is_derived_024": f.name not in orig_names_024,
            "null_count_024": int(null_row_024[f"null__{f.name}"] or 0),
            "null_pct_024": round(
                (int(null_row_024[f"null__{f.name}"] or 0) / silver_rows_024) * 100.0,
                4,
            ),
        }
        for f in silver_df_024.schema.fields
    ]

    all_flags_024 = [
        "is_flex_fare_024",
        "is_passenger_missing_024",
        "is_passenger_valid_024",
        "is_zero_duration_024",
        "is_extreme_duration_024",
        "is_zero_distance_024",
        "is_extreme_distance_024",
        "is_extreme_speed_024",
        "is_zero_fare_024",
        "is_high_fare_outlier_024",
        "is_negative_component_024",
        "has_negative_reversal_pair_024",
        "is_known_pickup_zone_024",
        "is_known_dropoff_zone_024",
        "is_weekend_024",
    ]
    anomaly_flags_024 = [
        "is_zero_duration_024",
        "is_extreme_duration_024",
        "is_zero_distance_024",
        "is_extreme_distance_024",
        "is_extreme_speed_024",
        "is_zero_fare_024",
        "is_high_fare_outlier_024",
        "is_negative_component_024",
        "has_negative_reversal_pair_024",
    ]
    key_metrics_024 = [
        "trip_distance",
        "trip_distance_valid_mi_024",
        "trip_duration_min_024",
        "trip_duration_valid_min_024",
        "avg_speed_mph_024",
        "fare_amount",
        "fare_amount_valid_usd_024",
        "total_amount",
        "total_amount_valid_usd_024",
        "tip_amount",
        "tip_ratio_024",
    ]

    agg_exprs_024: list[Any] = [
        F.min("tpep_pickup_datetime").alias("pickup_min_024"),
        F.max("tpep_pickup_datetime").alias("pickup_max_024"),
        F.min("tpep_dropoff_datetime").alias("dropoff_min_024"),
        F.max("tpep_dropoff_datetime").alias("dropoff_max_024"),
    ]
    for flg_024 in all_flags_024:
        agg_exprs_024.append(
            F.sum(F.when(F.col(flg_024), 1).otherwise(0)).alias(f"flag__{flg_024}")
        )
    for col_024 in key_metrics_024:
        c_ref_024 = F.col(col_024).cast("double")
        agg_exprs_024.extend(
            [
                F.count(c_ref_024).alias(f"{col_024}__count"),
                F.mean(c_ref_024).alias(f"{col_024}__mean"),
                F.stddev(c_ref_024).alias(f"{col_024}__stddev"),
                F.min(c_ref_024).alias(f"{col_024}__min"),
                F.percentile_approx(
                    c_ref_024, [0.25, 0.5, 0.75, 0.99], 10000
                ).alias(f"{col_024}__q"),
                F.max(c_ref_024).alias(f"{col_024}__max"),
            ]
        )

    stats_row_024 = silver_df_024.agg(*agg_exprs_024).collect()[0].asDict()
    flag_summary_024 = {
        flg_024: {
            "true_count_024": int(stats_row_024[f"flag__{flg_024}"] or 0),
            "true_pct_024": round(
                (int(stats_row_024[f"flag__{flg_024}"] or 0) / silver_rows_024) * 100.0,
                4,
            ),
        }
        for flg_024 in all_flags_024
    }

    # Exact overlap breakdown across anomaly flags
    anom_cnt_expr_024 = sum(F.col(c).cast("int") for c in anomaly_flags_024)
    anom_combo_024 = F.concat_ws(
        " + ", *[F.when(F.col(c), F.lit(c)) for c in anomaly_flags_024]
    )
    anom_overlap_summary_024 = (
        silver_df_024.agg(
            F.sum(F.when(anom_cnt_expr_024 == 0, 1).otherwise(0)).alias(
                "clean_no_anomaly_flag_024"
            ),
            F.sum(F.when(anom_cnt_expr_024 == 1, 1).otherwise(0)).alias(
                "single_anomaly_flag_024"
            ),
            F.sum(F.when(anom_cnt_expr_024 >= 2, 1).otherwise(0)).alias(
                "multi_anomaly_flag_overlap_024"
            ),
        )
        .collect()[0]
        .asDict()
    )
    top_flag_combos_024 = [
        {
            "flag_combination_024": r["combo"],
            "rows_024": int(r["cnt"]),
            "pct_of_silver_024": round((int(r["cnt"]) / silver_rows_024) * 100.0, 4),
        }
        for r in silver_df_024.filter(anom_cnt_expr_024 > 0)
        .groupBy(anom_combo_024.alias("combo"))
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(20)
        .collect()
    ]

    key_statistics_024 = []
    for col_024 in key_metrics_024:
        q_024 = stats_row_024.get(f"{col_024}__q") or [None, None, None, None]
        key_statistics_024.append(
            {
                "column_name": col_024,
                "non_null_count_024": int(stats_row_024[f"{col_024}__count"] or 0),
                "mean_024": format_value_024(stats_row_024[f"{col_024}__mean"]),
                "stddev_024": format_value_024(stats_row_024[f"{col_024}__stddev"]),
                "min_024": format_value_024(stats_row_024[f"{col_024}__min"]),
                "p25_024": format_value_024(q_024[0]),
                "p50_median_024": format_value_024(q_024[1]),
                "p75_024": format_value_024(q_024[2]),
                "p99_024": format_value_024(q_024[3]),
                "max_024": format_value_024(stats_row_024[f"{col_024}__max"]),
            }
        )

    return {
        "readback_verified_024": True,
        "hdfs_verified_path_024": silver_path_024,
        "silver_v2_rows_verified_024": silver_rows_024,
        "silver_v2_column_count_024": len(silver_df_024.columns),
        "original_21_columns_preserved_exact_024": orig_preserved_exact_024,
        "all_derived_columns_have_024_suffix_024": all_derived_use_024_suffix_024,
        "derived_columns_added_024": derived_cols_024,
        "temporal_range_silver_v2_024": {
            "tpep_pickup_datetime_min_024": format_value_024(
                stats_row_024["pickup_min_024"]
            ),
            "tpep_pickup_datetime_max_024": format_value_024(
                stats_row_024["pickup_max_024"]
            ),
            "tpep_dropoff_datetime_min_024": format_value_024(
                stats_row_024["dropoff_min_024"]
            ),
            "tpep_dropoff_datetime_max_024": format_value_024(
                stats_row_024["dropoff_max_024"]
            ),
        },
        "flag_summary_024": flag_summary_024,
        "anomaly_flag_overlap_024": {
            "clean_no_anomaly_flag_024": int(
                anom_overlap_summary_024["clean_no_anomaly_flag_024"]
            ),
            "single_anomaly_flag_024": int(
                anom_overlap_summary_024["single_anomaly_flag_024"]
            ),
            "multi_anomaly_flag_overlap_024": int(
                anom_overlap_summary_024["multi_anomaly_flag_overlap_024"]
            ),
            "top_flag_combinations_024": top_flag_combos_024,
        },
        "null_summary_silver_v2_024": null_summary_024,
        "key_statistics_silver_v2_024": key_statistics_024,
    }


def run_silver_cleaning_v2_024(
    input_path_024: str = HDFS_RAW_INPUT_024,
    output_path_024: str = HDFS_SILVER_OUTPUT_024,
    compare_v1_path_024: str | None = None,
    output_json_024: Path = DEFAULT_OUTPUT_JSON_024,
) -> dict[str, Any]:
    """Run Silver Layer v2 cleaning, write to HDFS (staging or target), and verify read-back."""
    if input_path_024.rstrip("/") == output_path_024.rstrip("/"):
        raise ValueError("Refusing to write Silver output to the raw input path.")

    start_time_024 = time.time()
    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    try:
        raw_df_024 = spark_024.read.parquet(input_path_024)
        original_schema_024 = [
            (f.name, f.dataType.simpleString()) for f in raw_df_024.schema.fields
        ]
        conds_024 = build_v2_conditions_024(raw_df_024)

        cleaning_impact_024 = evaluate_v2_cleaning_and_recovery_024(
            raw_df_024, conds_024
        )
        silver_v2_df_024 = build_silver_v2_dataframe_024(raw_df_024, conds_024)

        (
            silver_v2_df_024.repartition(SILVER_OUTPUT_PARTITIONS_024)
            .write.mode("overwrite")
            .parquet(output_path_024)
        )

        verification_024 = verify_silver_v2_output_024(
            spark_024, output_path_024, original_schema_024
        )

        # Compare hourly & zone distributions vs Raw Sep 2026 (and vs v1 if provided)
        verified_v2_df_024 = spark_024.read.parquet(output_path_024)
        v1_df_024 = (
            spark_024.read.parquet(compare_v1_path_024)
            if compare_v1_path_024
            and compare_v1_path_024.rstrip("/") != output_path_024.rstrip("/")
            else None
        )
        dist_comparison_024 = compare_distributions_024(
            raw_df_024, verified_v2_df_024, v1_df_024
        )

        # Safety assertions before declaring success
        assert verification_024["original_21_columns_preserved_exact_024"], (
            "Original 21 source columns were not preserved exactly!"
        )
        assert verification_024["all_derived_columns_have_024_suffix_024"], (
            "Not all derived columns end with _024!"
        )
        assert (
            verification_024["silver_v2_rows_verified_024"]
            == cleaning_impact_024["silver_v2_retained_rows_024"]
        ), "Verified read-back row count does not match expected retained rows!"

        elapsed_sec_024 = round(time.time() - start_time_024, 2)
        summary_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "silver_version_024": "v2_audit_refined_024",
                "script_024": "spark/processing/clean_yellow_taxi_024.py",
                "hdfs_raw_input_024": input_path_024,
                "hdfs_written_path_024": output_path_024,
                "hdfs_live_target_path_024": HDFS_SILVER_OUTPUT_024,
                "spark_version_024": spark_024.version,
                "spark_master_024": spark_024.sparkContext.master,
                "output_partitions_024": SILVER_OUTPUT_PARTITIONS_024,
                "executed_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": elapsed_sec_024,
            },
            "cleaning_policy_v2_024": {
                "hard_removal_rules_024": {
                    "target_pickup_start_024": TARGET_START_TS_024,
                    "target_pickup_end_exclusive_024": TARGET_END_TS_024,
                    "location_id_range_024": [1, 265],
                    "negative_duration_or_distance_removed_024": True,
                    "non_positive_total_amount_removed_024": True,
                    "corrupt_extreme_amount_gt_usd_024": CORRUPT_MAX_AMOUNT_USD_024,
                },
                "preserved_and_flagged_in_silver_024": {
                    "passenger_count_null_024": "is_passenger_missing_024 (Flex Fare preserved)",
                    "zero_fare_with_positive_total_024": "is_zero_fare_024 (VendorID=6 Flex Fare preserved)",
                    "negative_component_with_positive_total_024": "is_negative_component_024",
                    "positive_counterpart_of_raw_negative_reversal_024": "has_negative_reversal_pair_024",
                    "zero_duration_024": "is_zero_duration_024 (VendorID=7 preserved; trip_duration_valid_min_024=NULL)",
                    "extreme_duration_gt_180m_024": "is_extreme_duration_024 (trip_duration_valid_min_024=NULL)",
                    "zero_distance_024": "is_zero_distance_024 (Flex Fare preserved; trip_distance_valid_mi_024=NULL)",
                    "extreme_distance_gt_100mi_024": "is_extreme_distance_024 (trip_distance_valid_mi_024=NULL)",
                    "extreme_speed_gt_80mph_024": "is_extreme_speed_024 (avg_speed_mph_024=NULL)",
                    "high_fare_500_to_2000_or_tip_gt_200_024": "is_high_fare_outlier_024 (fare/total_valid_usd_024=NULL)",
                    "unknown_zones_264_265_024": "is_known_pickup_zone_024 / is_known_dropoff_zone_024",
                },
            },
            "cleaning_impact_v2_024": cleaning_impact_024,
            "silver_v2_verification_024": verification_024,
            "distribution_comparison_024": dist_comparison_024,
        }

        output_json_024.parent.mkdir(parents=True, exist_ok=True)
        with output_json_024.open("w", encoding="utf-8") as fh_024:
            json.dump(summary_024, fh_024, indent=2, ensure_ascii=False)

        print("=== URBANPULSE SILVER LAYER v2 SUMMARY (_024) ===")
        print(f"Raw Input         : {input_path_024}")
        print(f"Written Path      : {output_path_024}")
        print(f"Raw Rows          : {cleaning_impact_024['raw_total_rows_024']:,}")
        print(
            f"Silver v2 Rows    : {verification_024['silver_v2_rows_verified_024']:,} "
            f"({cleaning_impact_024['retention_pct_024']}% retained)"
        )
        print(
            f"Silver v2 Dropped : {cleaning_impact_024['silver_v2_dropped_rows_024']:,} "
            f"({cleaning_impact_024['dropped_pct_024']}%)"
        )
        print(
            "Recovered vs v1   : "
            f"+{cleaning_impact_024['recovered_vs_silver_v1_024']['total_unique_recovered_rows_024']:,} rows"
        )
        print(
            "Reversal Flagged  : "
            f"{verification_024['flag_summary_024']['has_negative_reversal_pair_024']['true_count_024']:,} "
            "retained positive rows paired with raw negative reversal"
        )
        print(
            f"Columns           : {verification_024['silver_v2_column_count_024']} "
            f"(21 original intact + {len(verification_024['derived_columns_added_024'])} derived _024)"
        )
        print(f"Elapsed Time      : {elapsed_sec_024} s")
        print(f"Saved JSON        : {output_json_024}")
        return summary_024
    finally:
        spark_024.stop()


if __name__ == "__main__":
    run_silver_cleaning_v2_024(
        input_path_024=os.environ.get("URBANPULSE_RAW_INPUT_024", HDFS_RAW_INPUT_024),
        output_path_024=os.environ.get(
            "URBANPULSE_SILVER_OUTPUT_024", HDFS_SILVER_OUTPUT_024
        ),
        compare_v1_path_024=os.environ.get("URBANPULSE_COMPARE_V1_PATH_024"),
        output_json_024=Path(
            os.environ.get("URBANPULSE_SILVER_JSON_024", str(DEFAULT_OUTPUT_JSON_024))
        ),
    )
