"""UrbanPulse Spark MLlib demand forecasting independent evaluation script.

Reloads the saved PipelineModel from `ml/models/`, re-scores the locked Test
Holdout (`2026-09-24` .. `2026-09-30`, 43,848 rows) directly from HDFS
`gold_demand_features_024`, cross-verifies reproducibility against
`data/analytics/demand_predictions_024.parquet`, enriches per-zone evaluations
with `data/reference/taxi_zone_lookup_024.csv` (`PULocationID == LocationID`,
preserving literal `N/A`), and outputs:
  - `reports/ml_evaluation_summary_024.json`
  - `data/analytics/model_metrics_024.json`
"""

from __future__ import annotations

import csv
import json
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
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
TRAINING_METADATA_JSON_024 = Path("ml/models/training_run_metadata_024.json")
PREDICTIONS_PARQUET_024 = Path("data/analytics/demand_predictions_024.parquet")
EVAL_SUMMARY_JSON_024 = Path("reports/ml_evaluation_summary_024.json")
DASHBOARD_METRICS_JSON_024 = Path("data/analytics/model_metrics_024.json")
TEST_START_DATE_024 = "2026-09-24"


def build_spark_session_024() -> SparkSession:
    """Create a SparkSession in local[*] mode for reproducible evaluation."""
    return (
        SparkSession.builder.appName("UrbanPulse_MLlib_Evaluation_024")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.eventLog.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", "file:///tmp/urbanpulse_spark_warehouse_024")
        .getOrCreate()
    )


def load_pipeline_model_local_024(src_dir_024: Path) -> PipelineModel:
    """Reload Spark PipelineModel safely from a local path that may contain spaces."""
    src_abs_024 = src_dir_024.resolve()
    with tempfile.TemporaryDirectory(prefix="urbanpulse_eval_model_024_") as tmp_024:
        stage_dir_024 = Path(tmp_024) / "model"
        shutil.copytree(src_abs_024, stage_dir_024)
        return PipelineModel.load(f"file://{stage_dir_024}")


def load_zone_lookup_spark_024(spark_024: SparkSession) -> DataFrame:
    """Load taxi_zone_lookup_024.csv preserving literal 'N/A' values."""
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


def compute_metrics_dict_024(
    df_024: DataFrame, actual_col_024: str, pred_col_024: str
) -> dict[str, Any]:
    """Compute MAE, RMSE, R^2, WAPE (%), and mean bias."""
    s_024 = df_024.agg(
        F.count("*").alias("n"),
        F.avg(actual_col_024).alias("y_mean"),
        F.avg(F.col(pred_col_024) - F.col(actual_col_024)).alias("bias"),
        F.sum(F.abs(F.col(actual_col_024) - F.col(pred_col_024))).alias("sum_ae"),
        F.sum(F.pow(F.col(actual_col_024) - F.col(pred_col_024), 2)).alias("sse"),
        F.sum(actual_col_024).alias("sum_y"),
        F.sum(F.pow(F.col(actual_col_024), 2)).alias("sum_y2"),
    ).collect()[0]

    n_024 = float(s_024["n"])
    if n_024 == 0:
        return {"row_count_024": 0, "mae_024": None, "rmse_024": None, "r2_024": None, "wape_pct_024": None}

    y_mean_024 = float(s_024["y_mean"])
    sum_ae_024 = float(s_024["sum_ae"])
    sse_024 = float(s_024["sse"])
    sum_y_024 = float(s_024["sum_y"])
    sum_y2_024 = float(s_024["sum_y2"])
    sst_024 = sum_y2_024 - n_024 * (y_mean_024 ** 2)

    return {
        "row_count_024": int(n_024),
        "sum_actual_pickups_024": int(round(sum_y_024)),
        "mae_024": round(sum_ae_024 / n_024, 4),
        "rmse_024": round((sse_024 / n_024) ** 0.5, 4),
        "r2_024": round(1.0 - (sse_024 / sst_024), 6) if sst_024 > 0 else 0.0,
        "wape_pct_024": round((sum_ae_024 / sum_y_024) * 100.0, 4) if sum_y_024 > 0 else 0.0,
        "mean_bias_pred_minus_actual_024": round(float(s_024["bias"]), 4),
    }


def group_metrics_024(df_024: DataFrame, group_col_024: str) -> list[dict[str, Any]]:
    """Compute model vs naive metrics grouped by a categorical dimension."""
    rows_024 = (
        df_024.groupBy(group_col_024)
        .agg(
            F.count("*").alias("n"),
            F.sum("actual_demand_024").alias("sum_y"),
            F.avg("actual_demand_024").alias("y_mean"),
            F.sum(F.pow("actual_demand_024", 2)).alias("sum_y2"),
            F.sum(F.abs(F.col("actual_demand_024") - F.col("predicted_demand_024"))).alias("sum_ae_m"),
            F.sum(F.pow(F.col("actual_demand_024") - F.col("predicted_demand_024"), 2)).alias("sse_m"),
            F.sum(F.abs(F.col("actual_demand_024") - F.col("naive_lag_168h_pred_024"))).alias("sum_ae_n"),
            F.sum(F.pow(F.col("actual_demand_024") - F.col("naive_lag_168h_pred_024"), 2)).alias("sse_n"),
        )
        .orderBy(F.desc("sum_y"))
        .collect()
    )
    out_024 = []
    for r in rows_024:
        n_024 = float(r["n"])
        sum_y_024 = float(r["sum_y"])
        y_mean_024 = float(r["y_mean"])
        sst_024 = float(r["sum_y2"]) - n_024 * (y_mean_024 ** 2)
        mae_m_024 = float(r["sum_ae_m"]) / n_024
        rmse_m_024 = (float(r["sse_m"]) / n_024) ** 0.5
        mae_n_024 = float(r["sum_ae_n"]) / n_024
        rmse_n_024 = (float(r["sse_n"]) / n_024) ** 0.5
        out_024.append(
            {
                group_col_024: r[group_col_024],
                "row_count_024": int(n_024),
                "sum_actual_pickups_024": int(round(sum_y_024)),
                "model_mae_024": round(mae_m_024, 4),
                "model_rmse_024": round(rmse_m_024, 4),
                "model_r2_024": round(1.0 - float(r["sse_m"]) / sst_024, 6) if sst_024 > 0 else 0.0,
                "model_wape_pct_024": round((float(r["sum_ae_m"]) / sum_y_024) * 100.0, 4) if sum_y_024 > 0 else 0.0,
                "naive_mae_024": round(mae_n_024, 4),
                "naive_rmse_024": round(rmse_n_024, 4),
                "rmse_improvement_pct_024": round(((rmse_n_024 - rmse_m_024) / rmse_n_024) * 100.0, 2)
                if rmse_n_024 > 0
                else 0.0,
            }
        )
    return out_024


def run_evaluation_024() -> dict[str, Any]:
    """Reload model, verify reproducibility, compute segmented & per-zone evaluations."""
    t0_024 = time.time()
    train_meta_024 = json.loads(TRAINING_METADATA_JSON_024.read_text(encoding="utf-8"))
    best_model_id_024 = train_meta_024["selected_model_024"]["model_id_024"]
    model_path_024 = Path(train_meta_024["selected_model_024"]["saved_model_path_024"]).resolve()

    spark_024 = build_spark_session_024()
    spark_024.sparkContext.setLogLevel("WARN")

    tmp_pq_root_024 = Path(tempfile.mkdtemp(prefix="urbanpulse_eval_pq_024_"))
    try:
        # 1. Reload saved PipelineModel and re-score HDFS Test Holdout independently
        reloaded_model_024 = load_pipeline_model_local_024(model_path_024)
        gold_test_024 = (
            spark_024.read.parquet(HDFS_GOLD_FEATURES_024)
            .filter(
                F.col("is_ml_ready_full_history_024")
                & (F.col("pickup_date_024") >= F.lit(TEST_START_DATE_024))
            )
            .withColumn("label_024", F.col("pickup_count_024").cast("double"))
            .withColumn("is_weekend_num_024", F.col("is_weekend_024").cast("double"))
            .withColumn("is_rush_hour_num_024", F.col("is_rush_hour_024").cast("double"))
            .withColumn(
                "is_known_pickup_zone_num_024",
                F.col("is_known_pickup_zone_024").cast("double"),
            )
            .withColumn("pu_location_str_024", F.col("PULocationID").cast("string"))
        )

        rescored_024 = reloaded_model_024.transform(gold_test_024).withColumn(
            "predicted_demand_024",
            F.round(F.greatest(F.lit(0.0), F.col("raw_prediction_024")), 4),
        )
        rescored_metrics_024 = compute_metrics_dict_024(
            rescored_024, "label_024", "predicted_demand_024"
        )

        # 2. Load exported Parquet predictions and verify exact reproducibility
        pred_abs_024 = PREDICTIONS_PARQUET_024.resolve()
        stage_pq_024 = tmp_pq_root_024 / "demand_predictions_024.parquet"
        shutil.copytree(pred_abs_024, stage_pq_024)
        pred_df_024 = spark_024.read.parquet(f"file://{stage_pq_024}").cache()
        exported_metrics_024 = compute_metrics_dict_024(
            pred_df_024, "actual_demand_024", "predicted_demand_024"
        )
        assert exported_metrics_024["row_count_024"] == 43848
        assert abs(exported_metrics_024["mae_024"] - rescored_metrics_024["mae_024"]) < 1e-4
        assert abs(exported_metrics_024["rmse_024"] - rescored_metrics_024["rmse_024"]) < 1e-4

        # Cross-verify official Taxi Zone Lookup enrichment integrity
        lookup_ref_024 = load_zone_lookup_spark_024(spark_024)
        lookup_mismatch_024 = (
            pred_df_024.select("PULocationID", "Zone", "Borough", "service_zone")
            .distinct()
            .join(
                lookup_ref_024.withColumnRenamed("Zone", "ref_Zone")
                .withColumnRenamed("Borough", "ref_Borough")
                .withColumnRenamed("service_zone", "ref_service_zone"),
                F.col("PULocationID") == F.col("LocationID"),
                how="left",
            )
            .filter(
                F.col("LocationID").isNull()
                | (F.col("Zone") != F.col("ref_Zone"))
                | (F.col("Borough") != F.col("ref_Borough"))
                | (F.col("service_zone") != F.col("ref_service_zone"))
            )
            .count()
        )
        assert lookup_mismatch_024 == 0, f"Zone lookup mismatch on {lookup_mismatch_024} zones!"

        # 3. Join Gold zone rank/tier + verify lookup enrichment
        zone_meta_024 = spark_024.read.parquet(HDFS_GOLD_ZONE_DEMAND_024).select(
            "PULocationID", "zone_demand_rank_024", "demand_tier_024"
        )
        eval_df_024 = (
            pred_df_024.join(F.broadcast(zone_meta_024), on="PULocationID", how="left")
            .withColumn(
                "hour_period_024",
                F.when(F.col("pickup_hour_024").isin(17, 18, 19, 20, 21), F.lit("Peak (17-21)"))
                .otherwise(F.lit("Off-Peak (00-16, 22-23)")),
            )
            .withColumn(
                "day_type_024",
                F.when(F.col("is_weekend_024"), F.lit("Weekend")).otherwise(F.lit("Weekday")),
            )
            .withColumn(
                "residual_024",
                F.col("predicted_demand_024") - F.col("actual_demand_024"),
            )
            .cache()
        )

        # Verify 100% lookup match (zero NULL Zone/Borough/service_zone)
        lookup_nulls_024 = eval_df_024.filter(
            F.col("Zone").isNull() | F.col("Borough").isNull() | F.col("service_zone").isNull()
        ).count()
        assert lookup_nulls_024 == 0, f"Found {lookup_nulls_024} rows missing zone lookup!"

        # 4. Overall & Top-50 Metrics (Model vs Naive lag_168h)
        all_model_024 = compute_metrics_dict_024(eval_df_024, "actual_demand_024", "predicted_demand_024")
        all_naive_024 = compute_metrics_dict_024(eval_df_024, "actual_demand_024", "naive_lag_168h_pred_024")

        top50_df_024 = eval_df_024.filter(F.col("zone_demand_rank_024") <= 50)
        top50_model_024 = compute_metrics_dict_024(top50_df_024, "actual_demand_024", "predicted_demand_024")
        top50_naive_024 = compute_metrics_dict_024(top50_df_024, "actual_demand_024", "naive_lag_168h_pred_024")

        low_df_024 = eval_df_024.filter(F.col("zone_demand_rank_024") > 50)
        low_model_024 = compute_metrics_dict_024(low_df_024, "actual_demand_024", "predicted_demand_024")
        low_naive_024 = compute_metrics_dict_024(low_df_024, "actual_demand_024", "naive_lag_168h_pred_024")

        # 5. Error distribution percentiles & tolerance bands
        err_row_024 = eval_df_024.agg(
            F.percentile_approx("abs_error_024", [0.5, 0.75, 0.9, 0.95, 0.99], 10000).alias("ae_q"),
            F.max("abs_error_024").alias("max_ae"),
            F.percentile_approx("residual_024", [0.05, 0.25, 0.5, 0.75, 0.95], 10000).alias("res_q"),
            F.avg(F.when(F.col("abs_error_024") <= 5.0, 1.0).otherwise(0.0)).alias("within_5"),
            F.avg(F.when(F.col("abs_error_024") <= 10.0, 1.0).otherwise(0.0)).alias("within_10"),
            F.avg(F.when(F.col("abs_error_024") <= 20.0, 1.0).otherwise(0.0)).alias("within_20"),
        ).collect()[0]

        error_dist_024 = {
            "abs_error_p50_median_024": round(float(err_row_024["ae_q"][0]), 4),
            "abs_error_p75_024": round(float(err_row_024["ae_q"][1]), 4),
            "abs_error_p90_024": round(float(err_row_024["ae_q"][2]), 4),
            "abs_error_p95_024": round(float(err_row_024["ae_q"][3]), 4),
            "abs_error_p99_024": round(float(err_row_024["ae_q"][4]), 4),
            "abs_error_max_024": round(float(err_row_024["max_ae"]), 4),
            "residual_p05_024": round(float(err_row_024["res_q"][0]), 4),
            "residual_p25_024": round(float(err_row_024["res_q"][1]), 4),
            "residual_p50_median_024": round(float(err_row_024["res_q"][2]), 4),
            "residual_p75_024": round(float(err_row_024["res_q"][3]), 4),
            "residual_p95_024": round(float(err_row_024["res_q"][4]), 4),
            "pct_within_5_trips_024": round(float(err_row_024["within_5"]) * 100.0, 2),
            "pct_within_10_trips_024": round(float(err_row_024["within_10"]) * 100.0, 2),
            "pct_within_20_trips_024": round(float(err_row_024["within_20"]) * 100.0, 2),
        }

        # 6. Segmented evaluations
        by_tier_024 = group_metrics_024(eval_df_024, "demand_tier_024")
        by_borough_024 = group_metrics_024(eval_df_024, "Borough")
        by_service_zone_024 = group_metrics_024(eval_df_024, "service_zone")
        by_hour_period_024 = group_metrics_024(eval_df_024, "hour_period_024")
        by_day_type_024 = group_metrics_024(eval_df_024, "day_type_024")

        # 7. Per-zone evaluation enriched with Zone, Borough, service_zone (keyed by PULocationID)
        zone_eval_rows_024 = (
            eval_df_024.groupBy(
                "PULocationID", "Zone", "Borough", "service_zone",
                "zone_demand_rank_024", "demand_tier_024",
            )
            .agg(
                F.sum("actual_demand_024").alias("test_actual_pickups_024"),
                F.sum("predicted_demand_024").alias("test_pred_pickups_024"),
                F.avg("abs_error_024").alias("mae_024"),
                F.sqrt(F.avg(F.pow("residual_024", 2))).alias("rmse_024"),
                F.avg(F.abs(F.col("actual_demand_024") - F.col("naive_lag_168h_pred_024"))).alias("naive_mae_024"),
                F.sqrt(F.avg(F.pow(F.col("actual_demand_024") - F.col("naive_lag_168h_pred_024"), 2))).alias("naive_rmse_024"),
            )
            .collect()
        )
        per_zone_list_024 = []
        for r in zone_eval_rows_024:
            act_024 = float(r["test_actual_pickups_024"])
            mae_024 = float(r["mae_024"])
            rmse_024 = float(r["rmse_024"])
            n_rmse_024 = float(r["naive_rmse_024"])
            wape_024 = round((mae_024 * 168.0 / act_024) * 100.0, 2) if act_024 > 0 else None
            per_zone_list_024.append(
                {
                    "PULocationID": int(r["PULocationID"]),
                    "Zone": r["Zone"],
                    "Borough": r["Borough"],
                    "service_zone": r["service_zone"],
                    "zone_demand_rank_024": int(r["zone_demand_rank_024"]),
                    "demand_tier_024": r["demand_tier_024"],
                    "test_actual_pickups_024": int(round(act_024)),
                    "test_pred_pickups_024": round(float(r["test_pred_pickups_024"]), 1),
                    "mae_024": round(mae_024, 4),
                    "rmse_024": round(rmse_024, 4),
                    "wape_pct_024": wape_024,
                    "naive_mae_024": round(float(r["naive_mae_024"]), 4),
                    "naive_rmse_024": round(n_rmse_024, 4),
                    "beats_naive_rmse_024": rmse_024 < n_rmse_024,
                }
            )

        highest_err_zones_024 = sorted(
            per_zone_list_024, key=lambda x: x["rmse_024"], reverse=True
        )[:10]
        lowest_err_top50_zones_024 = sorted(
            [z for z in per_zone_list_024 if z["zone_demand_rank_024"] <= 50],
            key=lambda x: (x["wape_pct_024"] if x["wape_pct_024"] is not None else 9999.0),
        )[:10]
        lowest_err_active_zones_024 = sorted(
            [z for z in per_zone_list_024 if z["test_actual_pickups_024"] >= 100],
            key=lambda x: x["mae_024"],
        )[:10]

        zones_beating_naive_024 = sum(1 for z in per_zone_list_024 if z["beats_naive_rmse_024"])
        top50_beating_naive_024 = sum(
            1 for z in per_zone_list_024
            if z["zone_demand_rank_024"] <= 50 and z["beats_naive_rmse_024"]
        )

        # Improvements vs Naive
        def pct_impr_024(naive_v: float, model_v: float) -> float:
            return round(((naive_v - model_v) / naive_v) * 100.0, 2) if naive_v > 0 else 0.0

        eval_summary_024 = {
            "metadata_024": {
                "project_024": "UrbanPulse — Intelligent Urban Taxi Analytics",
                "script_024": "ml/evaluation/evaluate_demand_model_024.py",
                "selected_model_id_024": best_model_id_024,
                "selected_model_family_024": train_meta_024["selected_model_024"]["model_family_024"],
                "selected_model_params_024": train_meta_024["selected_model_024"]["params_024"],
                "saved_model_path_024": train_meta_024["selected_model_024"]["saved_model_path_024"],
                "predictions_parquet_path_024": str(PREDICTIONS_PARQUET_024),
                "reproducibility_verified_024": True,
                "lookup_match_rate_pct_024": 100.0,
                "evaluated_at_utc_024": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds_024": round(time.time() - t0_024, 2),
            },
            "feature_specification_024": train_meta_024["feature_specification_024"],
            "chronological_split_policy_024": train_meta_024["chronological_split_policy_024"],
            "selected_model_interpretability_024": train_meta_024["selected_model_024"].get(
                "interpretability_024", {}
            ),
            "validation_model_comparison_024": train_meta_024["validation_comparison_024"],
            "final_test_evaluation_sep24_30_024": {
                "all_261_zones_model_024": all_model_024,
                "all_261_zones_naive_lag168h_024": all_naive_024,
                "all_261_improvement_vs_naive_024": {
                    "mae_improvement_pct_024": pct_impr_024(all_naive_024["mae_024"], all_model_024["mae_024"]),
                    "rmse_improvement_pct_024": pct_impr_024(all_naive_024["rmse_024"], all_model_024["rmse_024"]),
                    "wape_improvement_pp_024": round(all_naive_024["wape_pct_024"] - all_model_024["wape_pct_024"], 4),
                    "beats_naive_baseline_024": (
                        all_model_024["mae_024"] < all_naive_024["mae_024"]
                        and all_model_024["rmse_024"] < all_naive_024["rmse_024"]
                    ),
                },
                "top_50_zones_model_024": top50_model_024,
                "top_50_zones_naive_lag168h_024": top50_naive_024,
                "top_50_improvement_vs_naive_024": {
                    "mae_improvement_pct_024": pct_impr_024(top50_naive_024["mae_024"], top50_model_024["mae_024"]),
                    "rmse_improvement_pct_024": pct_impr_024(top50_naive_024["rmse_024"], top50_model_024["rmse_024"]),
                    "wape_improvement_pp_024": round(top50_naive_024["wape_pct_024"] - top50_model_024["wape_pct_024"], 4),
                },
                "remaining_211_zones_model_024": low_model_024,
                "remaining_211_zones_naive_lag168h_024": low_naive_024,
                "zones_beating_naive_rmse_count_024": zones_beating_naive_024,
                "top50_beating_naive_rmse_count_024": top50_beating_naive_024,
            },
            "error_distribution_024": error_dist_024,
            "segmented_evaluation_024": {
                "by_demand_tier_024": by_tier_024,
                "by_borough_024": by_borough_024,
                "by_service_zone_024": by_service_zone_024,
                "by_hour_period_024": by_hour_period_024,
                "by_day_type_024": by_day_type_024,
            },
            "per_zone_highlights_024": {
                "top10_highest_rmse_zones_024": highest_err_zones_024,
                "top10_lowest_wape_within_top50_zones_024": lowest_err_top50_zones_024,
                "top10_lowest_mae_active_ge100_zones_024": lowest_err_active_zones_024,
            },
        }

        EVAL_SUMMARY_JSON_024.parent.mkdir(parents=True, exist_ok=True)
        with EVAL_SUMMARY_JSON_024.open("w", encoding="utf-8") as fh_024:
            json.dump(eval_summary_024, fh_024, indent=2, ensure_ascii=False)

        # Contract-aligned compact metrics JSON for data/analytics/model_metrics_024.json
        rmse_impr_all_024 = pct_impr_024(all_naive_024["rmse_024"], all_model_024["rmse_024"])
        rmse_delta_label_024 = (
            f"mengungguli Naive lag_168h RMSE -{rmse_impr_all_024}%"
            if rmse_impr_all_024 >= 0
            else f"vs Naive lag_168h RMSE +{abs(rmse_impr_all_024)}%"
        )
        dashboard_metrics_024 = {
            "model_name_024": best_model_id_024,
            "model_family_024": train_meta_024["selected_model_024"]["model_family_024"],
            "mae_024": all_model_024["mae_024"],
            "rmse_024": all_model_024["rmse_024"],
            "r2_024": all_model_024["r2_024"],
            "wape_pct_024": all_model_024["wape_pct_024"],
            "top50_mae_024": top50_model_024["mae_024"],
            "top50_rmse_024": top50_model_024["rmse_024"],
            "top50_r2_024": top50_model_024["r2_024"],
            "top50_wape_pct_024": top50_model_024["wape_pct_024"],
            "naive_lag168h_mae_024": all_naive_024["mae_024"],
            "naive_lag168h_rmse_024": all_naive_024["rmse_024"],
            "test_period_024": "2026-09-24 .. 2026-09-30 (7 days, 43,848 rows)",
            "status_024": (
                f"Terlatih (Refit Sep 8-23, Test Sep 24-30; {rmse_delta_label_024})"
            ),
        }
        DASHBOARD_METRICS_JSON_024.parent.mkdir(parents=True, exist_ok=True)
        with DASHBOARD_METRICS_JSON_024.open("w", encoding="utf-8") as fh_024:
            json.dump(dashboard_metrics_024, fh_024, indent=2, ensure_ascii=False)

        print("=== URBANPULSE INDEPENDENT EVALUATION VERIFIED (_024) ===")
        print(
            f"All 261 Zones : Model MAE={all_model_024['mae_024']} vs Naive={all_naive_024['mae_024']} | "
            f"RMSE={all_model_024['rmse_024']} vs Naive={all_naive_024['rmse_024']} | "
            f"R2={all_model_024['r2_024']} | WAPE={all_model_024['wape_pct_024']}%"
        )
        print(
            f"Top 50 Zones  : Model MAE={top50_model_024['mae_024']} vs Naive={top50_naive_024['mae_024']} | "
            f"RMSE={top50_model_024['rmse_024']} vs Naive={top50_naive_024['rmse_024']} | "
            f"R2={top50_model_024['r2_024']} | WAPE={top50_model_024['wape_pct_024']}%"
        )
        print(
            f"Zones beating naive RMSE: {zones_beating_naive_024}/261 "
            f"(Top 50: {top50_beating_naive_024}/50)"
        )
        print(f"Saved Summary : {EVAL_SUMMARY_JSON_024}")
        print(f"Saved Metrics : {DASHBOARD_METRICS_JSON_024}")

        eval_df_024.unpersist()
        pred_df_024.unpersist()
        return eval_summary_024
    finally:
        spark_024.stop()
        shutil.rmtree(tmp_pq_root_024, ignore_errors=True)


if __name__ == "__main__":
    run_evaluation_024()
