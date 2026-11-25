"""Halaman E: Recommendations (Rekomendasi Operasional Berbasis Bukti Gold & ML) — UrbanPulse_024."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.charts_024 import build_surge_zones_chart_024
from dashboard.components_024 import (
    format_number_id_024,
    render_empty_state_024,
    render_insight_card_024,
    render_kpi_card_024,
    render_mode_banner_024,
    render_page_header_024,
    render_recommendation_card_024,
    render_section_header_024,
)
from dashboard.data_loader_024 import (
    load_data_024,
    load_evidence_recommendations_024,
)


def render_recommendations_page_024(use_demo_024: bool) -> None:
    """Merender halaman rekomendasi operasional berbasis angka aktual Gold Layer dan evaluasi ML."""
    rec_res_024 = load_evidence_recommendations_024(use_demo_024=use_demo_024)
    pred_res_024 = load_data_024("demand_predictions", use_demo_024=use_demo_024)
    rec_df_024: pd.DataFrame | None = rec_res_024["df_024"]
    pred_df_024: pd.DataFrame | None = pred_res_024["df_024"]
    has_data_024 = bool(rec_res_024["available_024"])

    render_page_header_024(
        eyebrow_024="Pengambilan Keputusan • Recommendations",
        title_024="Rekomendasi Operasional & Mitigasi Armada",
        subtitle_024="Seluruh rekomendasi diturunkan langsung dari bukti numerik Gold Layer (September 2026) dan hasil evaluasi Ridge Regression (Test Holdout 24–30 Sep 2026).",
        use_demo_024=use_demo_024,
        has_actual_data_024=has_data_024 and not use_demo_024,
    )
    render_mode_banner_024(use_demo_024=use_demo_024, has_actual_data_024=has_data_024)

    if not has_data_024 or rec_df_024 is None:
        render_empty_state_024(
            title_024="Data Rekomendasi Berbasis Bukti Belum Tersedia",
            description_024="Pastikan tabel Gold Layer dan hasil evaluasi ML sudah tersedia di data/analytics/.",
            expected_artifact_024="data/analytics/gold_zone_demand_024/ & demand_predictions_024.parquet/",
            icon_024="◆",
        )
        return

    # Panel Filter Kategori, Prioritas, dan Wilayah
    with st.container(border=True):
        st.markdown('<div class="up-filter-title-024">Filter Rekomendasi Operasional</div>', unsafe_allow_html=True)
        has_cat_024 = "category_024" in rec_df_024.columns
        if has_cat_024:
            rf1_024, rf2_024, rf3_024 = st.columns([1.3, 0.9, 1.0], gap="medium")
            cat_options_024 = ["Semua Kategori"] + rec_df_024["category_024"].dropna().astype(str).unique().tolist()
            with rf1_024:
                selected_cat_024 = st.selectbox("Kategori Temuan", options=cat_options_024, key="rec_category_024")
        else:
            rf2_024, rf3_024 = st.columns(2, gap="medium")
            selected_cat_024 = "Semua Kategori"

        priority_options_024 = ["Semua Prioritas"] + sorted(
            rec_df_024["priority_level_024"].dropna().astype(str).unique().tolist()
        )
        borough_options_024 = ["Semua Wilayah"] + sorted(
            rec_df_024["Borough"].dropna().astype(str).unique().tolist()
        )
        with rf2_024:
            selected_prio_024 = st.selectbox("Tingkat Prioritas", options=priority_options_024, key="rec_priority_024")
        with rf3_024:
            selected_boro_024 = st.selectbox("Cakupan Wilayah", options=borough_options_024, key="rec_borough_024")

    filtered_rec_024 = rec_df_024.copy()
    if has_cat_024 and selected_cat_024 != "Semua Kategori":
        filtered_rec_024 = filtered_rec_024[filtered_rec_024["category_024"] == selected_cat_024]
    if selected_prio_024 != "Semua Prioritas":
        filtered_rec_024 = filtered_rec_024[filtered_rec_024["priority_level_024"] == selected_prio_024]
    if selected_boro_024 != "Semua Wilayah":
        filtered_rec_024 = filtered_rec_024[filtered_rec_024["Borough"] == selected_boro_024]

    if filtered_rec_024.empty:
        st.warning("Tidak ada rekomendasi yang sesuai dengan kombinasi filter yang dipilih.")
        return

    # Baris 1: KPI Ringkasan Rekomendasi
    high_cnt_024 = int((filtered_rec_024["priority_level_024"].str.lower() == "tinggi").sum())
    cat_cnt_024 = int(filtered_rec_024["category_024"].nunique()) if has_cat_024 else 1

    rk1_024, rk2_024, rk3_024, rk4_024 = st.columns(4, gap="medium")
    with rk1_024:
        render_kpi_card_024(
            label_024="Rekomendasi Aktif",
            value_024=format_number_id_024(len(filtered_rec_024), 0),
            caption_024=f"Mencakup {cat_cnt_024} pilar analisis berbasis bukti aktual.",
            tag_024="Cakupan",
            is_demo_024=use_demo_024,
        )
    with rk2_024:
        render_kpi_card_024(
            label_024="Aksi Prioritas Tinggi",
            value_024=format_number_id_024(high_cnt_024, 0),
            caption_024="Berdampak langsung pada puncak volume & pendapatan kota.",
            tag_024="Prioritas",
            is_demo_024=use_demo_024,
        )
    with rk3_024:
        render_kpi_card_024(
            label_024="Fokus Jendela Puncak",
            value_024="17:00 – 21:59",
            caption_024="Menyumbang 32,28% pickup Sep 2026 (siaga mulai 16:30)." if not use_demo_024 else "Simulasi jendela puncak.",
            tag_024="Operasional",
            is_demo_024=use_demo_024,
        )
    with rk4_024:
        render_kpi_card_024(
            label_024="Reliabilitas Model Top-50",
            value_024="17,25% WAPE" if not use_demo_024 else "DEMO",
            caption_024="Kontras dengan Tier Low (>150) WAPE 287,9% (butuh on-demand)." if not use_demo_024 else "Data simulasi.",
            tag_024="Risiko ML",
            is_demo_024=use_demo_024,
        )

    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    # Baris 2: Visualisasi Zona Berisiko Error Relatif Tinggi + Sintesis Eksekutif
    c_left_024, c_right_024 = st.columns([1.25, 1.0], gap="medium")
    with c_left_024:
        with st.container(border=True):
            if not use_demo_024 and pred_df_024 is not None and not pred_df_024.empty:
                render_section_header_024(
                    title_024="Zona Utama dengan Error Prediksi Relatif (WAPE) Tertinggi",
                    subtitle_024="10 zona bervolume ≥10.000 pickup pada Test Holdout (24–30 Sep) yang paling memerlukan buffer armada.",
                    badge_024="Test Holdout ML",
                )
                zone_eval_024 = (
                    pred_df_024.groupby(["PULocationID", "Zone", "Borough"], as_index=False)
                    .agg(
                        actual_sum_024=("actual_demand_024", "sum"),
                        abs_err_sum_024=("abs_error_024", "sum"),
                        mae_024=("abs_error_024", "mean"),
                    )
                )
                zone_eval_024["wape_pct_024"] = (
                    zone_eval_024["abs_err_sum_024"] / zone_eval_024["actual_sum_024"].clip(lower=1.0) * 100.0
                )
                top_risk_zones_024 = (
                    zone_eval_024[zone_eval_024["actual_sum_024"] >= 10000]
                    .sort_values("wape_pct_024", ascending=False)
                    .head(10)
                )
                fig_risk_024 = build_surge_zones_chart_024(
                    rec_df_024=top_risk_zones_024,
                    is_demo_024=False,
                )
                st.plotly_chart(fig_risk_024, width="stretch")
            else:
                render_section_header_024(
                    title_024="Zona dengan Estimasi Peningkatan Permintaan",
                    subtitle_024="Persentase estimasi lonjakan permintaan (DATA DEMO).",
                    badge_024="DATA DEMO",
                )
                fig_surge_024 = build_surge_zones_chart_024(
                    rec_df_024=filtered_rec_024,
                    is_demo_024=True,
                )
                st.plotly_chart(fig_surge_024, width="stretch")

    with c_right_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Ringkasan Strategi Pengambilan Keputusan",
                subtitle_024="Tiga prinsip alokasi armada berbasis bukti Gold Layer & Ridge Regression.",
                badge_024="DATA DEMO" if use_demo_024 else "Eksekutif",
            )
            prefix_024 = "[DATA DEMO] " if use_demo_024 else ""
            render_insight_card_024(
                title_024=f"{prefix_024}1. Prioritaskan Koridor Padat & High-Yield",
                body_024=(
                    "Top-10 zona menyumbang 33,03% permintaan NYC (Top-20 = 56,45%). Gabungkan strategi rotasi cepat "
                    "di Upper East Side & Midtown dengan antrean khusus bandara di JFK (USD 80,85/trip; pendapatan #1 kota)."
                ),
            )
            render_insight_card_024(
                title_024=f"{prefix_024}2. Relokasi Dinamis Weekday → Weekend Nightlife",
                body_024=(
                    "Kawasan bisnis Midtown (18,7% akhir pekan) melandai saat akhir pekan, sementara East Village "
                    "(42,8% akhir pekan, puncak 23:00) dan jam 00:00–03:59 akhir pekan melonjak tajam."
                ),
            )
            render_insight_card_024(
                title_024=f"{prefix_024}3. Gunakan Prediksi ML Secara Selektif",
                body_024=(
                    "Andalkan Ridge Regression untuk Top-50 zona operasional (WAPE 17,25%, unggul 90% zona vs Naive), "
                    "tambah buffer di bandara/nightlife (LGA 21,95%, East Village 18,27%), dan hindari static staging di Tier Low (>150)."
                ),
            )

    # Baris 3: Daftar Kartu Rekomendasi Berbasis Bukti
    with st.container(border=True):
        render_section_header_024(
            title_024="Daftar Rekomendasi Operasional Berbasis Bukti",
            subtitle_024="Setiap rekomendasi mencantumkan kategori, metrik bukti, tindakan operasional, dan dasar data aktual.",
            badge_024=f"{len(filtered_rec_024)} Rekomendasi",
        )
        for _, rec_row_024 in filtered_rec_024.iterrows():
            metric_badge_024 = (
                str(rec_row_024["metric_label_024"])
                if "metric_label_024" in rec_row_024 and pd.notna(rec_row_024["metric_label_024"])
                else f"Lonjakan +{format_number_id_024(rec_row_024.get('predicted_surge_pct_024'), 1)}%"
            )
            render_recommendation_card_024(
                zone_024=str(rec_row_024.get("Zone", "-")),
                borough_024=str(rec_row_024.get("Borough", "-")),
                window_024=str(rec_row_024.get("target_window_024", "-")),
                priority_024=str(rec_row_024.get("priority_level_024", "Sedang")),
                metric_badge_024=metric_badge_024,
                action_024=str(rec_row_024.get("recommended_action_024", "-")),
                basis_024=str(rec_row_024.get("supporting_basis_024", "-")),
                allocation_note_024=str(rec_row_024.get("fleet_allocation_note_024", "-")),
                category_024=str(rec_row_024.get("category_024", "")),
                is_demo_024=use_demo_024,
            )
