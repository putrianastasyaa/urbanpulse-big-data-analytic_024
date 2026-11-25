"""UrbanPulse raw Yellow Taxi dataset exploration and validation using PySpark.

Reads the NYC TLC Yellow Taxi Parquet file directly from HDFS in Spark local mode,
inspects the original schema without modifying source data, computes descriptive
statistics and data quality checks, and writes structured validation results to
`reports/raw_validation_summary_024.json`.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
)

HDFS_INPUT_PATH_024 = (
    "hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/"
    "yellow_tripdata_2026-09_024.parquet"
)
DEFAULT_OUTPUT_JSON_024 = Path("reports/raw_validation_summary_024.json")
NUMERIC_TYPES_024 = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)
FLOAT_TYPES_024 = (FloatType, DoubleType)


def build_spark_session_024() -> SparkSession:
    """Create a SparkSession in local[*] mode without writing to shared event logs."""
    return (
        SparkSession.builder.appName("UrbanPulse_Raw_Validation_024")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def format_value_024(value_024: Any) -> Any:
    """Convert Python/Spark scalar values to JSON-serializable primitives."""
    if value_024 is None:
        return None
    if isinstance(value_024, datetime):
        return value_024.isoformat(sep=" ")
    if isinstance(value_024, float):
        return round(value_024, 6)
    return value_024


def collect_schema_info_024(raw_df_024: DataFrame) -> dict[str, Any]:
    """Capture exact source schema details without modifying column names."""
    columns_024 = [
        {
            "index_024": idx_024 + 1,
            "column_name": field_024.name,
            "data_type": field_024.dataType.simpleString(),
            "nullable": field_024.nullable,
        }
        for idx_024, field_024 in enumerate(raw_df_024.schema.fields)
    ]
    return {
        "column_count_024": len(columns_024),
        "columns_024": columns_024,
        "schema_tree_024": raw_df_024._jdf.schema().treeString(),
    }


def collect_null_and_nan_counts_024(
    raw_df_024: DataFrame, total_rows_024: int
) -> list[dict[str, Any]]:
    """Compute null (and NaN for floating columns) counts for every column in one pass."""
    agg_exprs_024 = []
    float_cols_024 = set()

    for field_024 in raw_df_024.schema.fields:
        col_name_024 = field_024.name
        col_ref_024 = F.col(f"`{col_name_024}`")
        agg_exprs_024.append(
            F.sum(F.when(col_ref_024.isNull(), 1).otherwise(0)).alias(
                f"null__{col_name_024}"
            )
        )
        if isinstance(field_024.dataType, FLOAT_TYPES_024):
            float_cols_024.add(col_name_024)
            agg_exprs_024.append(
                F.sum(F.when(F.isnan(col_ref_024), 1).otherwise(0)).alias(
                    f"nan__{col_name_024}"
                )
            )

    row_024 = raw_df_024.agg(*agg_exprs_024).collect()[0].asDict()
    results_024 = []
    for field_024 in raw_df_024.schema.fields:
        col_name_024 = field_024.name
        null_count_024 = int(row_024.get(f"null__{col_name_024}", 0) or 0)
        nan_count_024 = (
            int(row_024.get(f"nan__{col_name_024}", 0) or 0)
            if col_name_024 in float_cols_024
            else 0
        )
        null_pct_024 = (
            round((null_count_024 / total_rows_024) * 100.0, 4)
            if total_rows_024 > 0
            else 0.0
        )
        results_024.append(
            {
                "column_name": col_name_024,
                "data_type": field_024.dataType.simpleString(),
                "null_count_024": null_count_024,
                "null_pct_024": null_pct_024,
                "nan_count_024": nan_count_024,
            }
        )
    return results_024


def collect_numeric_stats_024(raw_df_024: DataFrame) -> list[dict[str, Any]]:
    """Compute descriptive statistics for all numeric columns in a single pass."""
    numeric_fields_024 = [
        field_024
        for field_024 in raw_df_024.schema.fields
        if isinstance(field_024.dataType, NUMERIC_TYPES_024)
    ]

    agg_exprs_024 = []
    for field_024 in numeric_fields_024:
        col_name_024 = field_024.name
        col_ref_024 = F.col(f"`{col_name_024}`").cast("double")
        agg_exprs_024.extend(
            [
                F.count(col_ref_024).alias(f"{col_name_024}__count"),
                F.mean(col_ref_024).alias(f"{col_name_024}__mean"),
                F.stddev(col_ref_024).alias(f"{col_name_024}__stddev"),
                F.min(col_ref_024).alias(f"{col_name_024}__min"),
                F.percentile_approx(
                    col_ref_024, [0.25, 0.5, 0.75, 0.99], 10000
                ).alias(f"{col_name_024}__quantiles"),
                F.max(col_ref_024).alias(f"{col_name_024}__max"),
            ]
        )

    row_024 = raw_df_024.agg(*agg_exprs_024).collect()[0].asDict()
    stats_list_024 = []
    for field_024 in numeric_fields_024:
        col_name_024 = field_024.name
        quantiles_024 = row_024.get(f"{col_name_024}__quantiles") or [
            None,
            None,
            None,
            None,
        ]
        stats_list_024.append(
            {
                "column_name": col_name_024,
                "data_type": field_024.dataType.simpleString(),
                "non_null_count_024": int(row_024.get(f"{col_name_024}__count") or 0),
                "mean_024": format_value_024(row_024.get(f"{col_name_024}__mean")),
                "stddev_024": format_value_024(row_024.get(f"{col_name_024}__stddev")),
                "min_024": format_value_024(row_024.get(f"{col_name_024}__min")),
                "p25_024": format_value_024(quantiles_024[0]),
                "p50_median_024": format_value_024(quantiles_024[1]),
                "p75_024": format_value_024(quantiles_024[2]),
                "p99_024": format_value_024(quantiles_024[3]),
                "max_024": format_value_024(row_024.get(f"{col_name_024}__max")),
            }
        )
    return stats_list_024


def collect_data_quality_024(
    raw_df_024: DataFrame, total_rows_024: int
) -> dict[str, Any]:
    """Evaluate temporal range, duration, passengers, distance, fare, and zone quality."""
    existing_cols_024 = set(raw_df_024.columns)
    duration_min_024 = (
        F.unix_timestamp(F.col("tpep_dropoff_datetime"))
        - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    ) / F.lit(60.0)
    avg_speed_mph_024 = F.when(
        (duration_min_024 > 0) & (F.col("trip_distance") > 0),
        F.col("trip_distance") / (duration_min_024 / F.lit(60.0)),
    )

    df_eval_024 = raw_df_024.withColumn(
        "trip_duration_min_024", duration_min_024
    ).withColumn("avg_speed_mph_024", avg_speed_mph_024)

    def count_cond_024(cond_024: Any, alias_024: str) -> Any:
        return F.sum(F.when(cond_024, 1).otherwise(0)).alias(alias_024)

    agg_exprs_024 = [
        F.min("tpep_pickup_datetime").alias("pickup_min_024"),
        F.max("tpep_pickup_datetime").alias("pickup_max_024"),
        F.min("tpep_dropoff_datetime").alias("dropoff_min_024"),
        F.max("tpep_dropoff_datetime").alias("dropoff_max_024"),
        # Temporal scope (expected September 2026)
        count_cond_024(
            F.col("tpep_pickup_datetime") < F.lit("2026-09-01 00:00:00"),
            "pickup_before_2026_09_024",
        ),
        count_cond_024(
            F.col("tpep_pickup_datetime") >= F.lit("2026-10-01 00:00:00"),
            "pickup_after_2026_09_024",
        ),
        # Duration checks
        F.min("trip_duration_min_024").alias("duration_min_val_024"),
        F.mean("trip_duration_min_024").alias("duration_mean_val_024"),
        F.percentile_approx(
            "trip_duration_min_024", [0.25, 0.5, 0.75, 0.99], 10000
        ).alias("duration_quantiles_024"),
        F.max("trip_duration_min_024").alias("duration_max_val_024"),
        count_cond_024(
            F.col("trip_duration_min_024") < 0, "duration_negative_024"
        ),
        count_cond_024(F.col("trip_duration_min_024") == 0, "duration_zero_024"),
        count_cond_024(
            (F.col("trip_duration_min_024") > 0)
            & (F.col("trip_duration_min_024") < 1),
            "duration_under_1min_024",
        ),
        count_cond_024(
            F.col("trip_duration_min_024") > 180, "duration_over_3h_024"
        ),
        count_cond_024(
            F.col("trip_duration_min_024") > 720, "duration_over_12h_024"
        ),
        count_cond_024(
            F.col("trip_duration_min_024") > 1440, "duration_over_24h_024"
        ),
        # Passenger checks
        count_cond_024(F.col("passenger_count").isNull(), "passenger_null_024"),
        count_cond_024(F.col("passenger_count") < 0, "passenger_negative_024"),
        count_cond_024(F.col("passenger_count") == 0, "passenger_zero_024"),
        count_cond_024(F.col("passenger_count") > 6, "passenger_over_6_024"),
        # Distance & speed checks
        count_cond_024(F.col("trip_distance") < 0, "distance_negative_024"),
        count_cond_024(F.col("trip_distance") == 0, "distance_zero_024"),
        count_cond_024(F.col("trip_distance") > 100, "distance_over_100mi_024"),
        count_cond_024(F.col("trip_distance") > 500, "distance_over_500mi_024"),
        count_cond_024(
            F.col("avg_speed_mph_024") > 80, "implied_speed_over_80mph_024"
        ),
        # Fare & monetary checks
        count_cond_024(F.col("fare_amount") < 0, "fare_negative_024"),
        count_cond_024(F.col("fare_amount") == 0, "fare_zero_024"),
        count_cond_024(F.col("fare_amount") > 500, "fare_over_500_024"),
        count_cond_024(F.col("total_amount") < 0, "total_amount_negative_024"),
        count_cond_024(F.col("total_amount") == 0, "total_amount_zero_024"),
        count_cond_024(F.col("total_amount") > 1000, "total_amount_over_1000_024"),
        count_cond_024(F.col("tip_amount") < 0, "tip_negative_024"),
        count_cond_024(F.col("tip_amount") > 200, "tip_over_200_024"),
        # Location & Ratecode checks
        count_cond_024(
            (F.col("PULocationID") <= 0) | (F.col("PULocationID") > 265),
            "pu_location_out_of_range_024",
        ),
        count_cond_024(
            F.col("PULocationID").isin(264, 265), "pu_location_unknown_264_265_024"
        ),
        count_cond_024(
            (F.col("DOLocationID") <= 0) | (F.col("DOLocationID") > 265),
            "do_location_out_of_range_024",
        ),
        count_cond_024(
            F.col("DOLocationID").isin(264, 265), "do_location_unknown_264_265_024"
        ),
        count_cond_024(
            F.col("RatecodeID").isNotNull() & (~F.col("RatecodeID").isin(1, 2, 3, 4, 5, 6)),
            "ratecode_non_standard_024",
        ),
    ]

    optional_fee_cols_024 = [
        "extra",
        "mta_tax",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
    ]
    present_fee_cols_024 = [
        col_024 for col_024 in optional_fee_cols_024 if col_024 in existing_cols_024
    ]
    for fee_col_024 in present_fee_cols_024:
        agg_exprs_024.append(
            count_cond_024(
                F.col(f"`{fee_col_024}`") < 0, f"{fee_col_024}_negative_024"
            )
        )

    dq_row_024 = df_eval_024.agg(*agg_exprs_024).collect()[0].asDict()

    def build_metric_024(key_024: str) -> dict[str, Any]:
        cnt_024 = int(dq_row_024.get(key_024) or 0)
        pct_024 = (
            round((cnt_024 / total_rows_024) * 100.0, 4)
            if total_rows_024 > 0
            else 0.0
        )
        return {"count_024": cnt_024, "pct_024": pct_024}

    dur_q_024 = dq_row_024.get("duration_quantiles_024") or [None, None, None, None]

    # Small categorical breakdowns (useful for explaining anomalies in report)
    payment_dist_024 = [
        {
            "payment_type": row_024["payment_type"],
            "row_count_024": int(row_024["count"]),
            "pct_024": round((int(row_024["count"]) / total_rows_024) * 100.0, 4),
        }
        for row_024 in raw_df_024.groupBy("payment_type")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    ]
    vendor_dist_024 = [
        {
            "VendorID": row_024["VendorID"],
            "row_count_024": int(row_024["count"]),
            "pct_024": round((int(row_024["count"]) / total_rows_024) * 100.0, 4),
        }
        for row_024 in raw_df_024.groupBy("VendorID")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    ]
    passenger_dist_024 = [
        {
            "passenger_count": row_024["passenger_count"],
            "row_count_024": int(row_024["count"]),
            "pct_024": round((int(row_024["count"]) / total_rows_024) * 100.0, 4),
        }
        for row_024 in raw_df_024.groupBy("passenger_count")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    ]

    fee_negatives_024 = {
        fee_col_024: build_metric_024(f"{fee_col_024}_negative_024")
        for fee_col_024 in present_fee_cols_024
    }

    return {
        "temporal_range_024": {
            "tpep_pickup_datetime_min_024": format_value_024(
                dq_row_024["pickup_min_024"]
            ),
            "tpep_pickup_datetime_max_024": format_value_024(
                dq_row_024["pickup_max_024"]
            ),
            "tpep_dropoff_datetime_min_024": format_value_024(
                dq_row_024["dropoff_min_024"]
            ),
            "tpep_dropoff_datetime_max_024": format_value_024(
                dq_row_024["dropoff_max_024"]
            ),
            "pickup_before_2026_09_024": build_metric_024(
                "pickup_before_2026_09_024"
            ),
            "pickup_after_2026_09_024": build_metric_024(
                "pickup_after_2026_09_024"
            ),
        },
        "trip_duration_quality_024": {
            "min_min_024": format_value_024(dq_row_024["duration_min_val_024"]),
            "mean_min_024": format_value_024(dq_row_024["duration_mean_val_024"]),
            "p25_min_024": format_value_024(dur_q_024[0]),
            "p50_median_min_024": format_value_024(dur_q_024[1]),
            "p75_min_024": format_value_024(dur_q_024[2]),
            "p99_min_024": format_value_024(dur_q_024[3]),
            "max_min_024": format_value_024(dq_row_024["duration_max_val_024"]),
            "negative_duration_024": build_metric_024("duration_negative_024"),
            "zero_duration_024": build_metric_024("duration_zero_024"),
            "under_1min_duration_024": build_metric_024("duration_under_1min_024"),
            "over_3h_duration_024": build_metric_024("duration_over_3h_024"),
            "over_12h_duration_024": build_metric_024("duration_over_12h_024"),
            "over_24h_duration_024": build_metric_024("duration_over_24h_024"),
        },
        "passenger_quality_024": {
            "null_passenger_024": build_metric_024("passenger_null_024"),
            "negative_passenger_024": build_metric_024("passenger_negative_024"),
            "zero_passenger_024": build_metric_024("passenger_zero_024"),
            "over_6_passenger_024": build_metric_024("passenger_over_6_024"),
            "distribution_024": passenger_dist_024,
        },
        "distance_quality_024": {
            "negative_distance_024": build_metric_024("distance_negative_024"),
            "zero_distance_024": build_metric_024("distance_zero_024"),
            "over_100mi_distance_024": build_metric_024("distance_over_100mi_024"),
            "over_500mi_distance_024": build_metric_024("distance_over_500mi_024"),
            "implied_speed_over_80mph_024": build_metric_024(
                "implied_speed_over_80mph_024"
            ),
        },
        "fare_quality_024": {
            "negative_fare_024": build_metric_024("fare_negative_024"),
            "zero_fare_024": build_metric_024("fare_zero_024"),
            "over_500_fare_024": build_metric_024("fare_over_500_024"),
            "negative_total_amount_024": build_metric_024(
                "total_amount_negative_024"
            ),
            "zero_total_amount_024": build_metric_024("total_amount_zero_024"),
            "over_1000_total_amount_024": build_metric_024(
                "total_amount_over_1000_024"
            ),
            "negative_tip_024": build_metric_024("tip_negative_024"),
            "over_200_tip_024": build_metric_024("tip_over_200_024"),
            "negative_surcharges_024": fee_negatives_024,
        },
        "location_and_code_quality_024": {
            "pu_location_out_of_range_024": build_metric_024(
                "pu_location_out_of_range_024"
            ),
            "pu_location_unknown_264_265_024": build_metric_024(
                "pu_location_unknown_264_265_024"
            ),
            "do_location_out_of_range_024": build_metric_024(
                "do_location_out_of_range_024"
            ),
            "do_location_unknown_264_265_024": build_metric_024(
                "do_location_unknown_264_265_024"
            ),
            "ratecode_non_standard_024": build_metric_024(
                "ratecode_non_standard_024"
            ),
            "payment_type_distribution_024": payment_dist_024,
            "vendor_distribution_024": vendor_dist_024,
        },
    }


def run_raw_validation_024(
    input_path_024: str = HDFS_INPUT_PATH_024,
    output_json_024: Path = DEFAULT_OUTPUT_JSON_024,
) -> dict[str, Any]:
    """Execute end-to-end read-only validation of the raw HDFS Parquet dataset."""
    start_time_024 = time.time()
    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    try:
        raw_df_024 = spark_024.read.parquet(input_path_024)
        schema_info_024 = collect_schema_info_024(raw_df_024)

        total_rows_024 = int(raw_df_024.count())
        distinct_rows_024 = int(raw_df_024.distinct().count())
        exact_duplicate_rows_024 = total_rows_024 - distinct_rows_024
        duplicate_pct_024 = (
            round((exact_duplicate_rows_024 / total_rows_024) * 100.0, 4)
            if total_rows_024 > 0
            else 0.0
        )

        null_summary_024 = collect_null_and_nan_counts_024(raw_df_024, total_rows_024)
        numeric_stats_024 = collect_numeric_stats_024(raw_df_024)
        data_quality_024 = collect_data_quality_024(raw_df_024, total_rows_024)

        elapsed_sec_024 = round(time.time() - start_time_024, 2)
        summary_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "script_024": "spark/processing/validate_raw_yellow_taxi_024.py",
                "hdfs_input_path_024": input_path_024,
                "spark_version_024": spark_024.version,
                "spark_master_024": spark_024.sparkContext.master,
                "executed_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": elapsed_sec_024,
            },
            "dataset_overview_024": {
                "total_rows_024": total_rows_024,
                "total_columns_024": schema_info_024["column_count_024"],
                "distinct_rows_024": distinct_rows_024,
                "exact_duplicate_rows_024": exact_duplicate_rows_024,
                "exact_duplicate_pct_024": duplicate_pct_024,
            },
            "schema_024": schema_info_024,
            "null_summary_024": null_summary_024,
            "numeric_statistics_024": numeric_stats_024,
            "data_quality_024": data_quality_024,
        }

        output_json_024.parent.mkdir(parents=True, exist_ok=True)
        with output_json_024.open("w", encoding="utf-8") as file_handle_024:
            json.dump(summary_024, file_handle_024, indent=2, ensure_ascii=False)

        print("=== URBANPULSE RAW VALIDATION SUMMARY (_024) ===")
        print(f"HDFS Path     : {input_path_024}")
        print(f"Spark Version : {spark_024.version} ({spark_024.sparkContext.master})")
        print(f"Total Rows    : {total_rows_024:,}")
        print(f"Total Columns : {schema_info_024['column_count_024']}")
        print(
            f"Duplicates    : {exact_duplicate_rows_024:,} "
            f"({duplicate_pct_024}% exact full-row)"
        )
        print(
            "Pickup Range  : "
            f"{data_quality_024['temporal_range_024']['tpep_pickup_datetime_min_024']} "
            f"-> {data_quality_024['temporal_range_024']['tpep_pickup_datetime_max_024']}"
        )
        print(f"Elapsed Time  : {elapsed_sec_024} s")
        print(f"Saved JSON    : {output_json_024}")
        return summary_024
    finally:
        spark_024.stop()


if __name__ == "__main__":
    input_override_024 = os.environ.get("URBANPULSE_INPUT_024", HDFS_INPUT_PATH_024)
    output_override_024 = Path(
        os.environ.get("URBANPULSE_OUTPUT_JSON_024", str(DEFAULT_OUTPUT_JSON_024))
    )
    run_raw_validation_024(input_override_024, output_override_024)
