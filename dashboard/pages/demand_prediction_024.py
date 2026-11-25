"""Halaman D: Demand Prediction (Evaluasi Prediksi Machine Learning) — UrbanPulse_024."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import streamlit as st

from dashboard.charts_024 import (
    build_actual_vs_predicted_scatter_024,
    build_feature_importance_chart_024,
    build_prediction_chart_024,
)
from dashboard.components_024 import (
    format_hour_id_024,
    format_number_id_024,
    render_empty_state_024,
    render_insight_card_024,
    render_kpi_card_024,
    render_mode_banner_024,
    render_notice_banner_024,
    render_page_header_024,
    render_section_header_024,
)
from dashboard.data_loader_024 import (
    load_data_024,
    load_ml_evaluation_summary_024,
    load_training_metadata_024,
)


def render_demand_prediction_page_024(use_demo_024: bool) -> None:
    """Merender halaman evaluasi prediksi pickup per zona per jam pada Locked Test Holdout."""
    pred_res_024 = load_data_024("demand_predictions", use_demo_024=use_demo_024)
    metrics_res_024 = load_data_024("model_metrics", use_demo_024=use_demo_024)
    ml_summary_024 = None if use_demo_024 else load_ml_evaluation_summary_024()
    train_meta_024 = None if use_demo_024 else load_training_metadata_024()

    pred_df_024: pd.DataFrame | None = pred_res_024["df_024"]
    metrics_df_024: pd.DataFrame | None = metrics_res_024["df_024"]
    has_pred_024 = bool(pred_res_024["available_024"])
    has_metrics_024 = bool(metrics_res_024["available_024"])

    render_page_header_024(
        eyebrow_024="Machine Learning • Demand Prediction (Test Holdout Evaluation)",
        title_024="Prediksi Permintaan Taksi per Zona & Jam",
        subtitle_024="Perbandingan nilai aktual vs prediksi Ridge Regression (ridge_reg_0p1_024) pada periode Locked Test Holdout September 24–30, 2026.",
        use_demo_024=use_demo_024,
        has_actual_data_024=has_pred_024 and not use_demo_024,
    )
    render_mode_banner_024(use_demo_024=use_demo_024, has_actual_data_024=has_pred_024)

    if pred_res_024["error_024"]:
        st.error(pred_res_024["error_024"])
    if metrics_res_024["error_024"]:
        st.error(metrics_res_024["error_024"])

    # Penjelasan eksplisit: hasil evaluasi Test Holdout Sep 24–30, 2026 (bukan prediksi masa depan)
    if not use_demo_024 and has_pred_024:
        render_notice_banner_024(
            title_024="Klarifikasi Evaluasi Test Holdout (24–30 September 2026) — Bukan Proyeksi Masa Depan",
            body_024=(
                "Seluruh kurva dan metrik di halaman ini merupakan hasil evaluasi out-of-sample pada Locked Test Holdout "
                "(24–30 September 2026, 43.848 baris = 261 zona × 168 jam, 933.894 pickup aktual) menggunakan model "
                "Ridge Regression (ridge_reg_0p1_024) yang diseleksi pada Validation (21–23 Sep) dan di-refit pada Train+Val "
                "(8–23 Sep 2026) dengan proteksi zero data leakage."
            ),
        )

    # Baris 1: Pilih Cakupan Metrik Global (All 261 Zones vs Top 50 Operational Zones) + 4 Metrik Wajib (MAE, RMSE, R², WAPE)
    metric_scope_024 = "All 261 Zones"
    if has_metrics_024 and metrics_df_024 is not None and "top50_mae_024" in metrics_df_024.columns:
        metric_scope_024 = st.radio(
            "Cakupan Metrik Evaluasi Model (Locked Test Holdout 24–30 Sep 2026)",
            options=["All 261 Zones", "Top 50 Operational Zones (91,0% Volume)"],
            horizontal=True,
            key="ml_metric_scope_024",
        )

    mae_str_024 = None
    rmse_str_024 = None
    r2_str_024 = None
    wape_str_024 = None
    mae_cap_024 = "Rata-rata kesalahan absolut (pickup/jam)."
    rmse_cap_024 = "Sensitivitas terhadap galat prediksi besar."
    r2_cap_024 = "Proporsi variasi permintaan yang dijelaskan model."
    wape_cap_024 = "Weighted Absolute Percentage Error (total |err| / total aktual)."

    if has_metrics_024 and metrics_df_024 is not None and not metrics_df_024.empty:
        m_row_024 = metrics_df_024.iloc[0]
        if metric_scope_024.startswith("Top 50") and pd.notna(m_row_024.get("top50_mae_024")):
            mae_str_024 = format_number_id_024(m_row_024.get("top50_mae_024"), 2)
            rmse_str_024 = format_number_id_024(m_row_024.get("top50_rmse_024"), 2)
            r2_str_024 = format_number_id_024(m_row_024.get("top50_r2_024"), 4)
            wape_str_024 = f"{format_number_id_024(m_row_024.get('top50_wape_pct_024'), 2)}%"
            mae_cap_024 = "Top-50 zona utama (vs Naive 19,77; membaik -11,76%)."
            rmse_cap_024 = "Top-50 zona utama (vs Naive 34,11; membaik -19,50%)."
            r2_cap_024 = "Top-50 zona utama (45/50 zona mengungguli Naive RMSE)."
            wape_cap_024 = "Top-50 zona utama (vs Naive 19,55%; turun -2,30 pp)."
        else:
            mae_str_024 = format_number_id_024(m_row_024.get("mae_024"), 2)
            rmse_str_024 = format_number_id_024(m_row_024.get("rmse_024"), 2)
            r2_str_024 = format_number_id_024(m_row_024.get("r2_024"), 4)
            wape_val_024 = m_row_024.get("wape_pct_024")
            wape_str_024 = f"{format_number_id_024(wape_val_024, 2)}%" if pd.notna(wape_val_024) else "N/A"
            if pd.notna(m_row_024.get("naive_lag168h_mae_024")):
                mae_cap_024 = f"All 261 zona (vs Naive {format_number_id_024(m_row_024.get('naive_lag168h_mae_024'), 2)}; -6,76%)."
            if pd.notna(m_row_024.get("naive_lag168h_rmse_024")):
                rmse_cap_024 = f"All 261 zona (vs Naive {format_number_id_024(m_row_024.get('naive_lag168h_rmse_024'), 2)}; -19,28%)."
            r2_cap_024 = "All 261 zona pada Test Holdout 24–30 Sep 2026."
            wape_cap_024 = "All 261 zona (vs Naive 22,91%; turun -1,55 pp)."

    m1_024, m2_024, m3_024, m4_024 = st.columns(4, gap="medium")
    with m1_024:
        render_kpi_card_024(
            label_024="MAE (Mean Absolute Error)",
            value_024=mae_str_024,
            caption_024=mae_cap_024,
            tag_024="Evaluasi",
            is_demo_024=use_demo_024,
        )
    with m2_024:
        render_kpi_card_024(
            label_024="RMSE (Root Mean Squared Error)",
            value_024=rmse_str_024,
            caption_024=rmse_cap_024,
            tag_024="Evaluasi",
            is_demo_024=use_demo_024,
        )
    with m3_024:
        render_kpi_card_024(
            label_024="Koefisien Determinasi (R²)",
            value_024=r2_str_024,
            caption_024=r2_cap_024,
            tag_024="Akurasi",
            is_demo_024=use_demo_024,
        )
    with m4_024:
        render_kpi_card_024(
            label_024="WAPE (Weighted Abs % Error)",
            value_024=wape_str_024,
            caption_024=wape_cap_024,
            tag_024="Error Relatif",
            is_demo_024=use_demo_024,
        )

    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    if not has_pred_024 or pred_df_024 is None:
        render_empty_state_024(
            title_024="Hasil Prediksi Test Holdout Belum Tersedia",
            description_024="Pastikan data/analytics/demand_predictions_024.parquet/ tersedia.",
            expected_artifact_024="data/analytics/demand_predictions_024.parquet/",
            icon_024="◈",
        )
        return

    # Baris 2: Panel Filter Zona (berbasis PULocationID unik!), Tanggal, dan Jam
    work_pred_024 = pred_df_024.copy()
    work_pred_024["prediction_date_str_024"] = work_pred_024["prediction_date_024"].astype(str)

    with st.container(border=True):
        st.markdown('<div class="up-filter-title-024">Filter Evaluasi Prediksi (Test Holdout 24–30 September 2026)</div>', unsafe_allow_html=True)
        p_col0_024, p_col1_024, p_col2_024, p_col3_024 = st.columns([0.85, 1.35, 1.0, 1.0], gap="medium")

        boro_choices_024 = ["Semua Borough"] + sorted(work_pred_024["Borough"].dropna().astype(str).unique().tolist())
        with p_col0_024:
            selected_pred_boro_024 = st.selectbox(
                "Filter Borough",
                options=boro_choices_024,
                key="pred_borough_024",
            )

        zone_pool_024 = work_pred_024 if selected_pred_boro_024 == "Semua Borough" else work_pred_024[
            work_pred_024["Borough"] == selected_pred_boro_024
        ]
        # Urutkan zona berdasarkan total aktual Test Holdout menurun agar zona utama langsung muncul di atas
        zone_catalog_024 = (
            zone_pool_024.groupby(["PULocationID", "Zone", "Borough"], as_index=False)["predicted_demand_024"]
            .sum()
            .sort_values("predicted_demand_024", ascending=False)
        )
        label_to_pid_024: Dict[str, int] = {}
        zone_labels_024: List[str] = []
        for _, zr_024 in zone_catalog_024.iterrows():
            pid_024 = int(zr_024["PULocationID"])
            lbl_024 = f"{zr_024['Zone']} (ID {pid_024}) — {zr_024['Borough']}"
            label_to_pid_024[lbl_024] = pid_024
            zone_labels_024.append(lbl_024)

        date_values_024 = sorted(work_pred_024["prediction_date_str_024"].unique().tolist())
        date_options_024 = ["Semua Hari Test (24–30 Sep)"] + date_values_024 if len(date_values_024) > 1 else date_values_024

        with p_col1_024:
            selected_zone_label_024 = st.selectbox(
                "Pilih Zona Pickup (ID Unik)",
                options=zone_labels_024,
                index=0,
                key=f"pred_zone_{selected_pred_boro_024}_024",
            )
        with p_col2_024:
            selected_date_opt_024 = st.selectbox(
                "Pilih Tanggal Test Holdout",
                options=date_options_024,
                index=1 if len(date_options_024) > 1 else 0,
                key="pred_date_024",
            )
        with p_col3_024:
            hour_window_024 = st.slider(
                "Rentang Jam",
                min_value=0,
                max_value=23,
                value=(0, 23),
                format="%02d:00",
                key="pred_hour_window_024",
            )

        show_naive_024 = st.checkbox(
            "Tampilkan pembanding Baseline Seasonal Naive (lag_168h)",
            value=True,
            key="pred_show_naive_024",
        )

    selected_pid_024 = label_to_pid_024[selected_zone_label_024]
    is_multi_day_024 = selected_date_opt_024.startswith("Semua Hari")

    filtered_pred_024 = work_pred_024[
        (work_pred_024["PULocationID"] == selected_pid_024)
        & (work_pred_024["pickup_hour_024"] >= hour_window_024[0])
        & (work_pred_024["pickup_hour_024"] <= hour_window_024[1])
    ]
    if not is_multi_day_024:
        filtered_pred_024 = filtered_pred_024[
            filtered_pred_024["prediction_date_str_024"] == selected_date_opt_024
        ]
    filtered_pred_024 = filtered_pred_024.sort_values(["prediction_date_str_024", "pickup_hour_024"])

    if filtered_pred_024.empty:
        st.warning("Tidak ada baris evaluasi untuk kombinasi zona, tanggal, dan jam yang dipilih.")
        return

    # Baris 3: Kurva Aktual vs Prediksi + Ringkasan Slice Aktif
    chart_col_024, summary_col_024 = st.columns([1.65, 1.0], gap="medium")

    with chart_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Aktual vs Prediksi Permintaan per Jam (Test Holdout)",
                subtitle_024=f"{selected_zone_label_024} • {selected_date_opt_024} • Jam {hour_window_024[0]:02d}:00–{hour_window_024[1]:02d}:59.",
                badge_024="DATA DEMO" if use_demo_024 else "Ridge 0.1",
            )
            fig_pred_024 = build_prediction_chart_024(
                pred_df_024=filtered_pred_024,
                multi_day_024=is_multi_day_024,
                show_naive_024=show_naive_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_pred_024, width="stretch")

    with summary_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Ringkasan Evaluasi Slice Aktif",
                subtitle_024="Metrik langsung pada pilihan zona, tanggal, dan jam saat ini.",
                badge_024="DATA DEMO" if use_demo_024 else "Test Holdout",
            )
            peak_pred_row_024 = filtered_pred_024.loc[filtered_pred_024["predicted_demand_024"].idxmax()]
            total_pred_024 = float(filtered_pred_024["predicted_demand_024"].sum())
            has_actual_024 = (
                "actual_demand_024" in filtered_pred_024.columns
                and filtered_pred_024["actual_demand_024"].notna().any()
            )
            prefix_024 = "[DATA DEMO] " if use_demo_024 else ""

            if has_actual_024:
                valid_eval_024 = filtered_pred_024.dropna(subset=["actual_demand_024"])
                total_actual_024 = float(valid_eval_024["actual_demand_024"].sum())
                err_series_024 = (valid_eval_024["predicted_demand_024"] - valid_eval_024["actual_demand_024"]).abs()
                local_mae_024 = float(err_series_024.mean())
                local_rmse_024 = float(((valid_eval_024["predicted_demand_024"] - valid_eval_024["actual_demand_024"]) ** 2).mean() ** 0.5)
                local_wape_024 = (float(err_series_024.sum()) / total_actual_024 * 100.0) if total_actual_024 > 0 else None
                peak_act_row_024 = valid_eval_024.loc[valid_eval_024["actual_demand_024"].idxmax()]

                render_insight_card_024(
                    title_024=f"{prefix_024}Total Aktual vs Prediksi",
                    body_024=(
                        f"Pickup Aktual: {format_number_id_024(total_actual_024, 0)} vs "
                        f"Prediksi Ridge: {format_number_id_024(total_pred_024, 1)} "
                        f"({len(valid_eval_024)} slot jam)."
                    ),
                )
                wape_slice_txt_024 = (
                    f"{format_number_id_024(local_wape_024, 2)}%"
                    if local_wape_024 is not None
                    else "N/A (aktual = 0)"
                )
                render_insight_card_024(
                    title_024=f"{prefix_024}Akurasi Slice Aktif",
                    body_024=(
                        f"MAE lokal: {format_number_id_024(local_mae_024, 2)} pickup/jam • "
                        f"RMSE lokal: {format_number_id_024(local_rmse_024, 2)} • "
                        f"WAPE lokal: {wape_slice_txt_024}."
                    ),
                )
                render_insight_card_024(
                    title_024=f"{prefix_024}Puncak Aktual vs Prediksi",
                    body_024=(
                        f"Puncak aktual pada pukul {format_hour_id_024(int(peak_act_row_024['pickup_hour_024']))} "
                        f"({format_number_id_024(peak_act_row_024['actual_demand_024'], 0)} pickup); "
                        f"puncak prediksi pada pukul {format_hour_id_024(int(peak_pred_row_024['pickup_hour_024']))} "
                        f"({format_number_id_024(peak_pred_row_024['predicted_demand_024'], 1)} pickup)."
                    ),
                )
            else:
                render_insight_card_024(
                    title_024=f"{prefix_024}Total Prediksi",
                    body_024=f"Estimasi total {format_number_id_024(total_pred_024, 1)} pickup pada {len(filtered_pred_024)} slot jam.",
                )

    st.markdown("<div style='height: 0.4rem;'></div>", unsafe_allow_html=True)

    # Baris 4: Scatter Aktual vs Prediksi + Informasi Model Ridge Regression & Koefisien Fitur
    sc_col_024, info_col_024 = st.columns([1.05, 1.15], gap="medium")

    with sc_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Sebaran Aktual vs Prediksi (Test Holdout)",
                subtitle_024="Sampel 2.500 titik slot Zona × Jam (24–30 Sep 2026) terhadap garis ideal y = x.",
                badge_024="R² = 0,9564" if not use_demo_024 else "DATA DEMO",
            )
            fig_scatter_024 = build_actual_vs_predicted_scatter_024(
                pred_df_024=work_pred_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_scatter_024, width="stretch")

    with info_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Informasi Model Ridge Regression (ridge_reg_0p1_024)",
                subtitle_024="Arsitektur Spark MLlib Pipeline & 10 koefisien fitur numerik terstandardisasi terkuat.",
                badge_024="Spark MLlib",
            )
            if ml_summary_024 is not None:
                interp_024 = ml_summary_024.get("selected_model_interpretability_024", {})
                coefs_024 = interp_024.get("numeric_coefficients_024", [])
                if coefs_024:
                    fig_coef_024 = build_feature_importance_chart_024(
                        coef_list_024=coefs_024,
                        is_demo_024=use_demo_024,
                    )
                    st.plotly_chart(fig_coef_024, width="stretch")
                st.caption(
                    "Pipeline: StringIndexer(PULocationID) → OneHotEncoder(261 zona) → VectorAssembler(276 fitur) → "
                    "StandardScaler(withMean=False) → LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100). "
                    "Post-processing: max(0.0, raw_prediction). Artefak tersimpan di ml/models/ridge_reg_0p1_024/."
                )
            else:
                render_insight_card_024(
                    title_024="Spesifikasi Model",
                    body_024="Aktifkan mode Data Aktual untuk melihat rincian Ridge Regression dan koefisien fitur Spark MLlib.",
                )

    # Baris 5 (Opsional Expandable): Tabel Perbandingan Kandidat Model pada Validation Set
    if ml_summary_024 is not None and ml_summary_024.get("validation_model_comparison_024"):
        with st.container(border=True):
            render_section_header_024(
                title_024="Transparansi Seleksi Model & Chronological Split",
                subtitle_024="Perbandingan seluruh kandidat model pada Validation Set (21–23 Sep 2026) sebelum refit dan evaluasi Test Holdout.",
                badge_024="7 Kandidat",
            )
            split_pol_024 = (train_meta_024 or ml_summary_024).get("chronological_split_policy_024", {})
            if split_pol_024:
                s1_024, s2_024, s3_024 = st.columns(3, gap="medium")
                with s1_024:
                    render_insight_card_024(
                        title_024="Train Selection & Refit",
                        body_024=f"Selection: {split_pol_024.get('train_selection_024', '-')}. Refit Final: {split_pol_024.get('final_refit_train_024', '-')}.",
                    )
                with s2_024:
                    render_insight_card_024(
                        title_024="Validation Set (Seleksi Model)",
                        body_024=f"{split_pol_024.get('validation_024', '-')}. Ridge reg=0.1 unggul All-261 RMSE (12,01) & seluruh metrik Top-50.",
                    )
                with s3_024:
                    render_insight_card_024(
                        title_024="Locked Test Holdout",
                        body_024=f"{split_pol_024.get('test_holdout_024', '-')}. Evaluasi objektif 1 minggu penuh tanpa leakage.",
                    )

            with st.expander("Lihat Tabel Perbandingan 7 Kandidat Model (Validation Set 21–23 Sep 2026)", expanded=False):
                val_rows_024 = []
                for item_024 in ml_summary_024["validation_model_comparison_024"]:
                    all_m_024 = item_024["validation_metrics_024"]["all_261_zones_024"]
                    t50_m_024 = item_024["validation_metrics_024"]["top_50_zones_024"]
                    val_rows_024.append(
                        {
                            "Model ID": item_024["model_id_024"],
                            "Keluarga Model": item_024["model_family_024"],
                            "All MAE": all_m_024["mae_024"],
                            "All RMSE": all_m_024["rmse_024"],
                            "All R²": round(all_m_024["r2_024"], 4),
                            "All WAPE (%)": round(all_m_024["wape_pct_024"], 2),
                            "Top-50 MAE": t50_m_024["mae_024"],
                            "Top-50 RMSE": t50_m_024["rmse_024"],
                            "Top-50 WAPE (%)": round(t50_m_024["wape_pct_024"], 2),
                            "Waktu Fit (s)": item_024["train_seconds_024"],
                        }
                    )
                st.dataframe(pd.DataFrame(val_rows_024), width="stretch", hide_index=True)
