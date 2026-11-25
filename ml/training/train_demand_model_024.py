"""UrbanPulse Spark MLlib demand forecasting training & model selection pipeline.

Reads the verified Gold Layer feature panel (`gold_demand_features_024`) directly
from HDFS in Spark local mode, enforces strict pre-training integrity and
zero-leakage assertions, compares Naive `lag_168h` against Spark MLlib
Ridge Regression, Random Forest Regressor, and Gradient-Boosted Tree Regressor
using a strict chronological split:
  - Train (Selection):  2026-09-08 .. 2026-09-20 (13 days, 81,432 rows)
  - Validation:         2026-09-21 .. 2026-09-23 ( 3 days, 18,792 rows)
  - Final Refit Train:  2026-09-08 .. 2026-09-23 (16 days, 100,224 rows)
  - Test Holdout:       2026-09-24 .. 2026-09-30 ( 7 days, 43,848 rows)

Never touches the Test Holdout during model selection or hyperparameter tuning.
Saves the final refit best PipelineModel to `ml/models/`, writes enriched Test
predictions to `data/analytics/demand_predictions_024.parquet`, and stores full
training/validation metadata to `ml/models/training_run_metadata_024.json`.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

HDFS_GOLD_FEATURES_024 = (
    "hdfs://localhost:9000/urbanpulse_024/analytics/gold_demand_features_024"
)
HDFS_GOLD_ZONE_DEMAND_024 = (
    "hdfs://localhost:9000/urbanpulse_024/analytics/gold_zone_demand_024"
)
ZONE_LOOKUP_CSV_024 = Path("data/reference/taxi_zone_lookup_024.csv")
MODELS_DIR_024 = Path("ml/models")
TRAINING_METADATA_JSON_024 = MODELS_DIR_024 / "training_run_metadata_024.json"
PREDICTIONS_PARQUET_024 = Path("data/analytics/demand_predictions_024.parquet")

# Strict chronological boundaries (September 2026 ML-ready period)
TRAIN_START_DATE_024 = "2026-09-08"
TRAIN_SELECT_END_DATE_024 = "2026-09-20"
VAL_START_DATE_024 = "2026-09-21"
VAL_END_DATE_024 = "2026-09-23"
TEST_START_DATE_024 = "2026-09-24"
TEST_END_DATE_024 = "2026-09-30"

TARGET_COL_024 = "pickup_count_024"
FORBIDDEN_FEATURE_COLS_024 = {
    "pickup_count_024",
    "non_reversed_pickup_count_024",
    "is_observed_in_silver_024",
    "pickup_ts_hour_024",
    "pickup_date_024",
    "period_024",
    "day_name_024",
    "day_type_024",
    "time_of_day_bucket_024",
    "is_ml_ready_full_history_024",
    "PULocationID",  # Must NOT enter as raw continuous integer
}

NUMERIC_FEATURES_024 = [
    "lag_1h_pickup_count_024",
    "lag_24h_pickup_count_024",
    "lag_168h_pickup_count_024",
    "rolling_avg_24h_pickup_count_024",
    "rolling_avg_7d_pickup_count_024",
    "rolling_std_7d_pickup_count_024",
    "pickup_hour_024",
    "day_of_week_024",
    "is_weekend_num_024",
    "is_rush_hour_num_024",
    "hour_sin_024",
    "hour_cos_024",
    "dow_sin_024",
    "dow_cos_024",
    "is_known_pickup_zone_num_024",
]
CATEGORICAL_SOURCE_COL_024 = "pu_location_str_024"
RANDOM_SEED_024 = 24


def build_spark_session_024() -> SparkSession:
    """Create a SparkSession tuned for stable local[*] MLlib execution in 16 GB WSL."""
    return (
        SparkSession.builder.appName("UrbanPulse_MLlib_Training_024")
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.warehouse.dir", "file:///tmp/urbanpulse_spark_warehouse_024")
        .getOrCreate()
    )


def save_pipeline_model_local_024(model_024: PipelineModel, dest_dir_024: Path) -> None:
    """Save Spark PipelineModel to a local workspace path safely even if path has spaces."""
    dest_abs_024 = dest_dir_024.resolve()
    dest_abs_024.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="urbanpulse_model_save_024_") as tmp_024:
        stage_dir_024 = Path(tmp_024) / "model"
        model_024.save(f"file://{stage_dir_024}")
        if dest_abs_024.exists():
            shutil.rmtree(dest_abs_024)
        shutil.copytree(stage_dir_024, dest_abs_024)


def load_pipeline_model_local_024(src_dir_024: Path) -> PipelineModel:
    """Reload Spark PipelineModel from a local workspace path safely even if path has spaces."""
    src_abs_024 = src_dir_024.resolve()
    with tempfile.TemporaryDirectory(prefix="urbanpulse_model_load_024_") as tmp_024:
        stage_dir_024 = Path(tmp_024) / "model"
        shutil.copytree(src_abs_024, stage_dir_024)
        return PipelineModel.load(f"file://{stage_dir_024}")


def write_parquet_local_024(df_024: DataFrame, dest_dir_024: Path) -> None:
    """Write Spark DataFrame to a local Parquet directory safely even if path has spaces."""
    dest_abs_024 = dest_dir_024.resolve()
    dest_abs_024.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="urbanpulse_pq_save_024_") as tmp_024:
        stage_dir_024 = Path(tmp_024) / "parquet_out"
        df_024.coalesce(1).write.mode("overwrite").parquet(f"file://{stage_dir_024}")
        if dest_abs_024.exists():
            shutil.rmtree(dest_abs_024)
        shutil.copytree(stage_dir_024, dest_abs_024)


def read_parquet_local_024(spark_024: SparkSession, src_dir_024: Path) -> DataFrame:
    """Read a local Parquet directory into Spark safely even if path has spaces."""
    src_abs_024 = src_dir_024.resolve()
    stage_root_024 = Path(tempfile.mkdtemp(prefix="urbanpulse_pq_read_024_"))
    stage_dir_024 = stage_root_024 / "parquet_in"
    shutil.copytree(src_abs_024, stage_dir_024)
    return spark_024.read.parquet(f"file://{stage_dir_024}")


def prepare_ml_features_df_024(raw_gold_df_024: DataFrame) -> DataFrame:
    """Filter ML-ready rows and cast boolean/zone identifiers cleanly without leakage."""
    return (
        raw_gold_df_024.filter(F.col("is_ml_ready_full_history_024"))
        .withColumn("label_024", F.col(TARGET_COL_024).cast("double"))
        .withColumn("is_weekend_num_024", F.col("is_weekend_024").cast("double"))
        .withColumn("is_rush_hour_num_024", F.col("is_rush_hour_024").cast("double"))
        .withColumn(
            "is_known_pickup_zone_num_024",
            F.col("is_known_pickup_zone_024").cast("double"),
        )
        .withColumn(CATEGORICAL_SOURCE_COL_024, F.col("PULocationID").cast("string"))
    )


def run_pre_training_assertions_024(
    ml_df_024: DataFrame,
    train_df_024: DataFrame,
    val_df_024: DataFrame,
    test_df_024: DataFrame,
) -> dict[str, Any]:
    """Verify row counts, schema, zero nulls, zero leakage, and strict chronological split."""
    assert not (set(NUMERIC_FEATURES_024) & FORBIDDEN_FEATURE_COLS_024), (
        "Forbidden target/leakage column found in NUMERIC_FEATURES_024!"
    )
    assert CATEGORICAL_SOURCE_COL_024 not in FORBIDDEN_FEATURE_COLS_024

    total_ml_rows_024 = int(ml_df_024.count())
    assert total_ml_rows_024 == 144072, f"Expected 144072 ML-ready rows, got {total_ml_rows_024}"

    # Verify zero nulls on target + all features
    check_cols_024 = [TARGET_COL_024, "label_024", CATEGORICAL_SOURCE_COL_024] + NUMERIC_FEATURES_024
    null_row_024 = (
        ml_df_024.agg(
            *[
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
                for c in check_cols_024
            ]
        )
        .collect()[0]
        .asDict()
    )
    non_zero_nulls_024 = {k: int(v) for k, v in null_row_024.items() if int(v or 0) > 0}
    assert not non_zero_nulls_024, f"Unexpected nulls in ML features: {non_zero_nulls_024}"

    # Verify strict chronological ordering and non-overlap
    def bounds_024(df_s: DataFrame) -> dict[str, Any]:
        r = df_s.agg(
            F.count("*").alias("rows"),
            F.min("pickup_ts_hour_024").alias("min_ts"),
            F.max("pickup_ts_hour_024").alias("max_ts"),
            F.min("pickup_date_024").alias("min_d"),
            F.max("pickup_date_024").alias("max_d"),
        ).collect()[0]
        return {
            "rows_024": int(r["rows"]),
            "min_ts_024": r["min_ts"].isoformat(sep=" "),
            "max_ts_024": r["max_ts"].isoformat(sep=" "),
            "min_date_024": r["min_d"].isoformat(),
            "max_date_024": r["max_d"].isoformat(),
        }

    b_train_024 = bounds_024(train_df_024)
    b_val_024 = bounds_024(val_df_024)
    b_test_024 = bounds_024(test_df_024)

    assert b_train_024["max_ts_024"] < b_val_024["min_ts_024"], "Train/Val temporal overlap!"
    assert b_val_024["max_ts_024"] < b_test_024["min_ts_024"], "Val/Test temporal overlap!"
    assert (
        b_train_024["rows_024"] + b_val_024["rows_024"] + b_test_024["rows_024"]
        == total_ml_rows_024
    ), "Split row counts do not sum to total ML-ready rows!"

    return {
        "total_ml_ready_rows_024": total_ml_rows_024,
        "feature_null_count_total_024": 0,
        "target_excluded_from_features_024": True,
        "chronological_non_overlap_verified_024": True,
        "train_bounds_024": b_train_024,
        "validation_bounds_024": b_val_024,
        "test_bounds_024": b_test_024,
    }


def compute_regression_metrics_024(
    pred_df_024: DataFrame,
    actual_col_024: str = "label_024",
    pred_col_024: str = "predicted_demand_024",
) -> dict[str, float]:
    """Compute MAE, RMSE, R^2, and WAPE (%) in a single aggregation pass."""
    stats_024 = pred_df_024.agg(
        F.count("*").alias("n"),
        F.avg(F.col(actual_col_024)).alias("y_mean"),
        F.sum(F.abs(F.col(actual_col_024) - F.col(pred_col_024))).alias("sum_abs_err"),
        F.sum(F.pow(F.col(actual_col_024) - F.col(pred_col_024), 2)).alias("sse"),
        F.sum(F.col(actual_col_024)).alias("sum_y"),
        F.sum(F.pow(F.col(actual_col_024), 2)).alias("sum_y2"),
    ).collect()[0]

    n_024 = float(stats_024["n"])
    y_mean_024 = float(stats_024["y_mean"])
    sum_abs_024 = float(stats_024["sum_abs_err"])
    sse_024 = float(stats_024["sse"])
    sum_y_024 = float(stats_024["sum_y"])
    sum_y2_024 = float(stats_024["sum_y2"])

    sst_024 = sum_y2_024 - n_024 * (y_mean_024 ** 2)
    mae_024 = sum_abs_024 / n_024
    rmse_024 = (sse_024 / n_024) ** 0.5
    r2_024 = 1.0 - (sse_024 / sst_024) if sst_024 > 0 else 0.0
    wape_pct_024 = (sum_abs_024 / sum_y_024) * 100.0 if sum_y_024 > 0 else 0.0

    return {
        "row_count_024": int(n_024),
        "mae_024": round(mae_024, 4),
        "rmse_024": round(rmse_024, 4),
        "r2_024": round(r2_024, 6),
        "wape_pct_024": round(wape_pct_024, 4),
    }


def evaluate_predictions_all_and_top50_024(
    pred_df_024: DataFrame,
    top50_ids_024: set[int],
    pred_col_024: str = "predicted_demand_024",
) -> dict[str, Any]:
    """Compute metrics on All 261 Zones and Top 50 Operational Zones."""
    all_m_024 = compute_regression_metrics_024(pred_df_024, "label_024", pred_col_024)
    top50_df_024 = pred_df_024.filter(F.col("PULocationID").isin(list(top50_ids_024)))
    top50_m_024 = compute_regression_metrics_024(top50_df_024, "label_024", pred_col_024)
    return {
        "all_261_zones_024": all_m_024,
        "top_50_zones_024": top50_m_024,
    }


def build_feature_stages_024(use_scaler_024: bool) -> tuple[list[Any], str]:
    """Construct Spark MLlib feature preprocessing stages (fit strictly on train split)."""
    indexer_024 = StringIndexer(
        inputCol=CATEGORICAL_SOURCE_COL_024,
        outputCol="pu_zone_idx_024",
        handleInvalid="keep",
    )
    ohe_024 = OneHotEncoder(
        inputCols=["pu_zone_idx_024"],
        outputCols=["pu_zone_ohe_024"],
        dropLast=True,
    )
    assembler_024 = VectorAssembler(
        inputCols=NUMERIC_FEATURES_024 + ["pu_zone_ohe_024"],
        outputCol="raw_features_024",
        handleInvalid="error",
    )
    stages_024: list[Any] = [indexer_024, ohe_024, assembler_024]
    feat_col_024 = "raw_features_024"

    if use_scaler_024:
        scaler_024 = StandardScaler(
            inputCol="raw_features_024",
            outputCol="scaled_features_024",
            withMean=False,  # Preserves sparse OHE vector representation
            withStd=True,
        )
        stages_024.append(scaler_024)
        feat_col_024 = "scaled_features_024"

    return stages_024, feat_col_024


def build_candidate_specs_024() -> list[dict[str, Any]]:
    """Define realistic candidate Spark MLlib models for 16 GB RAM / WSL local mode."""
    return [
        {
            "model_id_024": "ridge_reg_0p1_024",
            "model_family_024": "Ridge Regression (LinearRegression)",
            "use_scaler_024": True,
            "params_024": {"regParam": 0.1, "elasticNetParam": 0.0, "maxIter": 100},
            "builder_024": lambda feat_col: LinearRegression(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                regParam=0.1,
                elasticNetParam=0.0,
                maxIter=100,
            ),
        },
        {
            "model_id_024": "ridge_reg_1p0_024",
            "model_family_024": "Ridge Regression (LinearRegression)",
            "use_scaler_024": True,
            "params_024": {"regParam": 1.0, "elasticNetParam": 0.0, "maxIter": 100},
            "builder_024": lambda feat_col: LinearRegression(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                regParam=1.0,
                elasticNetParam=0.0,
                maxIter=100,
            ),
        },
        {
            "model_id_024": "random_forest_d8_t40_024",
            "model_family_024": "Random Forest Regressor",
            "use_scaler_024": False,
            "params_024": {
                "numTrees": 40,
                "maxDepth": 8,
                "minInstancesPerNode": 5,
                "featureSubsetStrategy": "onethird",
                "seed": RANDOM_SEED_024,
            },
            "builder_024": lambda feat_col: RandomForestRegressor(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                numTrees=40,
                maxDepth=8,
                minInstancesPerNode=5,
                featureSubsetStrategy="onethird",
                seed=RANDOM_SEED_024,
            ),
        },
        {
            "model_id_024": "random_forest_d10_t60_024",
            "model_family_024": "Random Forest Regressor",
            "use_scaler_024": False,
            "params_024": {
                "numTrees": 60,
                "maxDepth": 10,
                "minInstancesPerNode": 5,
                "featureSubsetStrategy": "onethird",
                "seed": RANDOM_SEED_024,
            },
            "builder_024": lambda feat_col: RandomForestRegressor(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                numTrees=60,
                maxDepth=10,
                minInstancesPerNode=5,
                featureSubsetStrategy="onethird",
                seed=RANDOM_SEED_024,
            ),
        },
        {
            "model_id_024": "gbt_d5_i40_024",
            "model_family_024": "Gradient-Boosted Tree Regressor",
            "use_scaler_024": False,
            "params_024": {
                "maxIter": 40,
                "maxDepth": 5,
                "stepSize": 0.1,
                "subsamplingRate": 0.8,
                "minInstancesPerNode": 5,
                "seed": RANDOM_SEED_024,
            },
            "builder_024": lambda feat_col: GBTRegressor(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                maxIter=40,
                maxDepth=5,
                stepSize=0.1,
                subsamplingRate=0.8,
                minInstancesPerNode=5,
                seed=RANDOM_SEED_024,
            ),
        },
        {
            "model_id_024": "gbt_d6_i60_024",
            "model_family_024": "Gradient-Boosted Tree Regressor",
            "use_scaler_024": False,
            "params_024": {
                "maxIter": 60,
                "maxDepth": 6,
                "stepSize": 0.08,
                "subsamplingRate": 0.8,
                "minInstancesPerNode": 5,
                "seed": RANDOM_SEED_024,
            },
            "builder_024": lambda feat_col: GBTRegressor(
                featuresCol=feat_col,
                labelCol="label_024",
                predictionCol="raw_prediction_024",
                maxIter=60,
                maxDepth=6,
                stepSize=0.08,
                subsamplingRate=0.8,
                minInstancesPerNode=5,
                seed=RANDOM_SEED_024,
            ),
        },
    ]


def load_zone_lookup_spark_024(spark_024: SparkSession) -> DataFrame:
    """Load official taxi_zone_lookup_024.csv preserving literal 'N/A' strings."""
    with ZONE_LOOKUP_CSV_024.open("r", encoding="utf-8-sig", newline="") as fh_024:
        rows_024 = [
            (
                int(r["LocationID"]),
                str(r["Borough"]),
                str(r["Zone"]),
                str(r["service_zone"]),
            )
            for r in csv.DictReader(fh_024)
        ]
    schema_024 = StructType(
        [
            StructField("LocationID", IntegerType(), False),
            StructField("Borough", StringType(), False),
            StructField("Zone", StringType(), False),
            StructField("service_zone", StringType(), False),
        ]
    )
    return spark_024.createDataFrame(rows_024, schema=schema_024)


def run_training_pipeline_024() -> dict[str, Any]:
    """Execute full chronological training, validation selection, refit, and model export."""
    overall_start_024 = time.time()
    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    try:
        gold_df_024 = spark_024.read.parquet(HDFS_GOLD_FEATURES_024)
        zone_rank_df_024 = spark_024.read.parquet(HDFS_GOLD_ZONE_DEMAND_024).select(
            "PULocationID", "zone_demand_rank_024", "demand_tier_024"
        )
        top50_ids_024 = {
            int(r["PULocationID"])
            for r in zone_rank_df_024.filter(F.col("zone_demand_rank_024") <= 50).collect()
        }

        ml_df_024 = prepare_ml_features_df_024(gold_df_024)
        train_df_024 = ml_df_024.filter(
            F.col("pickup_date_024") <= F.lit(TRAIN_SELECT_END_DATE_024)
        ).cache()
        val_df_024 = ml_df_024.filter(
            (F.col("pickup_date_024") >= F.lit(VAL_START_DATE_024))
            & (F.col("pickup_date_024") <= F.lit(VAL_END_DATE_024))
        ).cache()
        test_df_024 = ml_df_024.filter(
            F.col("pickup_date_024") >= F.lit(TEST_START_DATE_024)
        )

        pre_checks_024 = run_pre_training_assertions_024(
            ml_df_024, train_df_024, val_df_024, test_df_024
        )
        print("=== PRE-TRAINING INTEGRITY ASSERTIONS PASSED ===")
        print(
            f"Train={pre_checks_024['train_bounds_024']['rows_024']:,} | "
            f"Val={pre_checks_024['validation_bounds_024']['rows_024']:,} | "
            f"Test={pre_checks_024['test_bounds_024']['rows_024']:,}"
        )

        # 1. Evaluate Naive baseline (lag_168h) on Validation
        naive_val_metrics_024 = evaluate_predictions_all_and_top50_024(
            val_df_024, top50_ids_024, pred_col_024="lag_168h_pickup_count_024"
        )
        print(
            "[VAL] naive_lag_168h_024      : "
            f"All MAE={naive_val_metrics_024['all_261_zones_024']['mae_024']:.4f}, "
            f"RMSE={naive_val_metrics_024['all_261_zones_024']['rmse_024']:.4f}, "
            f"R2={naive_val_metrics_024['all_261_zones_024']['r2_024']:.4f}, "
            f"WAPE={naive_val_metrics_024['all_261_zones_024']['wape_pct_024']:.2f}% | "
            f"Top50 RMSE={naive_val_metrics_024['top_50_zones_024']['rmse_024']:.4f}"
        )

        # 2. Train & evaluate each candidate sequentially on Train -> Validation
        candidates_024 = build_candidate_specs_024()
        val_results_024: list[dict[str, Any]] = [
            {
                "model_id_024": "naive_lag_168h_024",
                "model_family_024": "Seasonal Naive Persistence Baseline",
                "params_024": {"lag_hours": 168},
                "status_024": "succeeded",
                "train_seconds_024": 0.0,
                "validation_metrics_024": naive_val_metrics_024,
            }
        ]

        successful_specs_024: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for spec_024 in candidates_024:
            t_m_024 = time.time()
            try:
                stages_024, feat_col_024 = build_feature_stages_024(
                    spec_024["use_scaler_024"]
                )
                estimator_024 = spec_024["builder_024"](feat_col_024)
                pipeline_024 = Pipeline(stages=stages_024 + [estimator_024])
                model_024 = pipeline_024.fit(train_df_024)

                val_pred_024 = model_024.transform(val_df_024).withColumn(
                    "predicted_demand_024",
                    F.greatest(F.lit(0.0), F.col("raw_prediction_024")),
                )
                val_metrics_024 = evaluate_predictions_all_and_top50_024(
                    val_pred_024, top50_ids_024, pred_col_024="predicted_demand_024"
                )
                sec_024 = round(time.time() - t_m_024, 2)
                rec_024 = {
                    "model_id_024": spec_024["model_id_024"],
                    "model_family_024": spec_024["model_family_024"],
                    "params_024": spec_024["params_024"],
                    "status_024": "succeeded",
                    "train_seconds_024": sec_024,
                    "validation_metrics_024": val_metrics_024,
                }
                val_results_024.append(rec_024)
                successful_specs_024.append((spec_024, rec_024))
                print(
                    f"[VAL] {spec_024['model_id_024']:<24}: "
                    f"All MAE={val_metrics_024['all_261_zones_024']['mae_024']:.4f}, "
                    f"RMSE={val_metrics_024['all_261_zones_024']['rmse_024']:.4f}, "
                    f"R2={val_metrics_024['all_261_zones_024']['r2_024']:.4f}, "
                    f"WAPE={val_metrics_024['all_261_zones_024']['wape_pct_024']:.2f}% | "
                    f"Top50 RMSE={val_metrics_024['top_50_zones_024']['rmse_024']:.4f} "
                    f"({sec_024}s)"
                )
            except Exception as exc_024:
                sec_024 = round(time.time() - t_m_024, 2)
                val_results_024.append(
                    {
                        "model_id_024": spec_024["model_id_024"],
                        "model_family_024": spec_024["model_family_024"],
                        "params_024": spec_024["params_024"],
                        "status_024": "failed",
                        "error_024": str(exc_024),
                        "train_seconds_024": sec_024,
                    }
                )
                print(f"[VAL] {spec_024['model_id_024']} FAILED: {exc_024}")

        assert successful_specs_024, "No Spark MLlib candidate model trained successfully!"

        # 3. Select best model strictly from Validation (lowest All-261 RMSE, tie-break MAE)
        best_spec_024, best_val_rec_024 = min(
            successful_specs_024,
            key=lambda pair: (
                pair[1]["validation_metrics_024"]["all_261_zones_024"]["rmse_024"],
                pair[1]["validation_metrics_024"]["all_261_zones_024"]["mae_024"],
            ),
        )
        best_model_id_024 = best_spec_024["model_id_024"]
        val_rmse_for_band_024 = best_val_rec_024["validation_metrics_024"][
            "all_261_zones_024"
        ]["rmse_024"]
        print(
            f"=== SELECTED BEST MODEL (ON VALIDATION): {best_model_id_024} "
            f"(Val RMSE={val_rmse_for_band_024:.4f}) ==="
        )

        # Free selection caches before final refit
        train_df_024.unpersist()
        val_df_024.unpersist()

        # 4. Final Refit on Train+Validation (Sep 8-23: 100,224 rows)
        refit_start_024 = time.time()
        train_val_df_024 = ml_df_024.filter(
            F.col("pickup_date_024") <= F.lit(VAL_END_DATE_024)
        ).cache()
        refit_rows_024 = int(train_val_df_024.count())
        assert refit_rows_024 == 100224, f"Expected 100224 refit rows, got {refit_rows_024}"

        final_stages_024, final_feat_col_024 = build_feature_stages_024(
            best_spec_024["use_scaler_024"]
        )
        final_estimator_024 = best_spec_024["builder_024"](final_feat_col_024)
        final_pipeline_024 = Pipeline(stages=final_stages_024 + [final_estimator_024])
        final_model_024 = final_pipeline_024.fit(train_val_df_024)
        refit_sec_024 = round(time.time() - refit_start_024, 2)
        train_val_df_024.unpersist()

        # 5. Save best PipelineModel locally to ml/models/ & verify reload
        MODELS_DIR_024.mkdir(parents=True, exist_ok=True)
        model_save_path_024 = (MODELS_DIR_024 / best_model_id_024).resolve()
        save_pipeline_model_local_024(final_model_024, model_save_path_024)

        reloaded_model_024 = load_pipeline_model_local_024(model_save_path_024)
        print(f"MODEL_RELOAD_VERIFIED: {model_save_path_024}")

        # Extract compact interpretability summary for the final refit model
        final_stage_024 = reloaded_model_024.stages[-1]
        model_interpretability_024: dict[str, Any] = {}
        if hasattr(final_stage_024, "featureImportances"):
            imp_vec_024 = final_stage_024.featureImportances
            num_count_024 = len(NUMERIC_FEATURES_024)
            num_imps_024 = [
                {
                    "feature_024": NUMERIC_FEATURES_024[i],
                    "importance_024": round(float(imp_vec_024[i]), 6),
                }
                for i in range(num_count_024)
            ]
            zone_ohe_total_imp_024 = round(
                float(sum(float(imp_vec_024[i]) for i in range(num_count_024, imp_vec_024.size))),
                6,
            )
            num_imps_024.append(
                {
                    "feature_024": "pu_zone_ohe_024 (all zone dummies combined)",
                    "importance_024": zone_ohe_total_imp_024,
                }
            )
            num_imps_024.sort(key=lambda x: x["importance_024"], reverse=True)
            model_interpretability_024 = {
                "type_024": "tree_feature_importances",
                "total_feature_dim_024": int(imp_vec_024.size),
                "top_features_024": num_imps_024,
            }
        elif hasattr(final_stage_024, "coefficients"):
            coef_vec_024 = final_stage_024.coefficients
            num_count_024 = len(NUMERIC_FEATURES_024)
            num_coefs_024 = [
                {
                    "feature_024": NUMERIC_FEATURES_024[i],
                    "standardized_coef_024": round(float(coef_vec_024[i]), 6),
                }
                for i in range(num_count_024)
            ]
            num_coefs_024.sort(key=lambda x: abs(x["standardized_coef_024"]), reverse=True)
            model_interpretability_024 = {
                "type_024": "linear_standardized_coefficients",
                "intercept_024": round(float(final_stage_024.intercept), 6),
                "total_feature_dim_024": int(coef_vec_024.size),
                "numeric_coefficients_024": num_coefs_024,
            }

        # 6. Single final inference on locked Test Holdout (Sep 24-30: 43,848 rows)
        test_scored_024 = reloaded_model_024.transform(test_df_024).withColumn(
            "predicted_demand_024",
            F.round(F.greatest(F.lit(0.0), F.col("raw_prediction_024")), 4),
        )
        final_test_metrics_024 = evaluate_predictions_all_and_top50_024(
            test_scored_024, top50_ids_024, pred_col_024="predicted_demand_024"
        )
        naive_test_metrics_024 = evaluate_predictions_all_and_top50_024(
            test_df_024, top50_ids_024, pred_col_024="lag_168h_pickup_count_024"
        )
        print(
            f"[TEST FINAL] {best_model_id_024}: "
            f"All MAE={final_test_metrics_024['all_261_zones_024']['mae_024']:.4f}, "
            f"RMSE={final_test_metrics_024['all_261_zones_024']['rmse_024']:.4f}, "
            f"R2={final_test_metrics_024['all_261_zones_024']['r2_024']:.4f}, "
            f"WAPE={final_test_metrics_024['all_261_zones_024']['wape_pct_024']:.2f}% | "
            f"Top50 MAE={final_test_metrics_024['top_50_zones_024']['mae_024']:.4f}, "
            f"RMSE={final_test_metrics_024['top_50_zones_024']['rmse_024']:.4f}"
        )

        # 7. Enrich Test predictions with official Taxi Zone Lookup (PULocationID == LocationID)
        #    and export Parquet to data/analytics/demand_predictions_024.parquet
        lookup_df_024 = load_zone_lookup_spark_024(spark_024)
        band_half_width_024 = round(1.28 * val_rmse_for_band_024, 4)  # From Validation only (zero test leakage)

        export_pred_df_024 = (
            test_scored_024.join(
                F.broadcast(lookup_df_024),
                test_scored_024["PULocationID"] == lookup_df_024["LocationID"],
                how="left",
            )
            .select(
                test_scored_024["PULocationID"],
                lookup_df_024["Zone"],
                lookup_df_024["Borough"],
                lookup_df_024["service_zone"],
                F.date_format(F.col("pickup_date_024"), "yyyy-MM-dd").alias(
                    "prediction_date_024"
                ),
                F.col("pickup_hour_024").cast("int"),
                F.col("pickup_ts_hour_024"),
                F.col("day_of_week_024").cast("int"),
                F.col("day_name_024"),
                F.col("is_weekend_024"),
                F.col("predicted_demand_024"),
                F.col("label_024").alias("actual_demand_024"),
                F.col("lag_168h_pickup_count_024").alias("naive_lag_168h_pred_024"),
                F.round(
                    F.abs(F.col("label_024") - F.col("predicted_demand_024")), 4
                ).alias("abs_error_024"),
                F.round(
                    F.greatest(
                        F.lit(0.0),
                        F.col("predicted_demand_024") - F.lit(band_half_width_024),
                    ),
                    4,
                ).alias("lower_bound_024"),
                F.round(
                    F.col("predicted_demand_024") + F.lit(band_half_width_024), 4
                ).alias("upper_bound_024"),
                F.lit(best_model_id_024).alias("model_name_024"),
            )
            .orderBy("prediction_date_024", "pickup_hour_024", "PULocationID")
        )

        pred_out_abs_024 = PREDICTIONS_PARQUET_024.resolve()
        write_parquet_local_024(export_pred_df_024, pred_out_abs_024)

        # Verify exported predictions read-back from real workspace path
        with tempfile.TemporaryDirectory(prefix="urbanpulse_pq_verify_024_") as tmp_v_024:
            stage_v_024 = Path(tmp_v_024) / "pred_verify"
            shutil.copytree(pred_out_abs_024, stage_v_024)
            rb_pred_024 = spark_024.read.parquet(f"file://{stage_v_024}")
            rb_cnt_024 = int(rb_pred_024.count())
        assert rb_cnt_024 == 43848, f"Expected 43848 prediction rows, got {rb_cnt_024}"

        elapsed_total_024 = round(time.time() - overall_start_024, 2)
        run_meta_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "script_024": "ml/training/train_demand_model_024.py",
                "hdfs_gold_input_024": HDFS_GOLD_FEATURES_024,
                "zone_lookup_input_024": str(ZONE_LOOKUP_CSV_024),
                "spark_version_024": spark_024.version,
                "spark_master_024": spark_024.sparkContext.master,
                "executed_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": elapsed_total_024,
            },
            "feature_specification_024": {
                "target_column_024": TARGET_COL_024,
                "numeric_features_024": NUMERIC_FEATURES_024,
                "categorical_zone_encoding_024": (
                    "PULocationID cast to string -> StringIndexer(handleInvalid='keep') "
                    "-> OneHotEncoder(dropLast=True) inside Spark MLlib Pipeline "
                    "(fit strictly on training split)"
                ),
                "excluded_columns_024": sorted(FORBIDDEN_FEATURE_COLS_024),
                "post_processing_024": "predicted_demand_024 = max(0.0, raw_prediction_024)",
                "uncertainty_band_note_024": (
                    f"+/- 1.28 * Validation RMSE ({val_rmse_for_band_024:.4f}) "
                    f"= +/- {band_half_width_024:.4f} (derived strictly from Validation, zero test leakage)"
                ),
            },
            "pre_training_checks_024": pre_checks_024,
            "chronological_split_policy_024": {
                "warmup_excluded_024": "2026-09-01 .. 2026-09-07 (7 days, 43,848 rows)",
                "train_selection_024": f"{TRAIN_START_DATE_024} .. {TRAIN_SELECT_END_DATE_024} (13 days, 81,432 rows)",
                "validation_024": f"{VAL_START_DATE_024} .. {VAL_END_DATE_024} (3 days, 18,792 rows)",
                "final_refit_train_024": f"{TRAIN_START_DATE_024} .. {VAL_END_DATE_024} (16 days, 100,224 rows)",
                "test_holdout_024": f"{TEST_START_DATE_024} .. {TEST_END_DATE_024} (7 days, 43,848 rows)",
            },
            "validation_comparison_024": val_results_024,
            "selected_model_024": {
                "model_id_024": best_model_id_024,
                "model_family_024": best_spec_024["model_family_024"],
                "params_024": best_spec_024["params_024"],
                "selection_basis_024": (
                    "Selected strictly on Validation (Sep 21-23) based on lowest "
                    "All-261 RMSE with MAE tie-break; refit once on Sep 8-23 (100,224 rows)."
                ),
                "refit_seconds_024": refit_sec_024,
                "saved_model_path_024": f"ml/models/{best_model_id_024}",
                "model_reload_verified_024": True,
                "predictions_parquet_path_024": str(PREDICTIONS_PARQUET_024),
                "predictions_rows_verified_024": rb_cnt_024,
                "interpretability_024": model_interpretability_024,
            },
            "final_test_holdout_summary_024": {
                "selected_model_test_metrics_024": final_test_metrics_024,
                "naive_lag_168h_test_metrics_024": naive_test_metrics_024,
            },
        }

        with TRAINING_METADATA_JSON_024.open("w", encoding="utf-8") as fh_024:
            json.dump(run_meta_024, fh_024, indent=2, ensure_ascii=False)

        print(f"SAVED_TRAINING_METADATA: {TRAINING_METADATA_JSON_024}")
        print(f"SAVED_PREDICTIONS_PARQUET: {PREDICTIONS_PARQUET_024} ({rb_cnt_024:,} rows)")
        return run_meta_024
    finally:
        spark_024.stop()


if __name__ == "__main__":
    run_training_pipeline_024()
