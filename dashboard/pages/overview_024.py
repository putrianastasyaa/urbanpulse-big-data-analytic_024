"""Halaman A: Overview (Ringkasan Utama) — UrbanPulse_024."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.charts_024 import (
    build_hourly_demand_chart_024,
    build_zone_borough_comparison_chart_024,
    build_zone_ranking_chart_024,
)
from dashboard.components_024 import (
    format_currency_usd_024,
    format_hour_id_024,
    format_number_id_024,
    render_empty_state_024,
    render_insight_card_024,
    render_kpi_card_024,
    render_mode_banner_024,
    render_page_header_024,
    render_pipeline_status_grid_024,
    render_section_header_024,
)
from dashboard.data_loader_024 import get_pipeline_status_024, load_data_024


def render_overview_page_024(use_demo_024: bool) -> None:
    """Merender halaman Overview aktivitas Yellow Taxi NYC periode September 2026."""
    hourly_res_024 = load_data_024("hourly_demand", use_demo_024=use_demo_024)
    zone_res_024 = load_data_024("zone_summary", use_demo_024=use_demo_024)
    pipeline_status_024 = get_pipeline_status_024()

    hourly_df_024: pd.DataFrame | None = hourly_res_024["df_024"]
    zone_df_024: pd.DataFrame | None = zone_res_024["df_024"]
    has_data_024 = bool(hourly_res_024["available_024"] and zone_res_024["available_024"])

    render_page_header_024(
        eyebrow_024="Ringkasan Eksekutif • Overview (September 2026)",
        title_024="Ringkasan Aktivitas Yellow Taxi NYC",
        subtitle_024="KPI utama September 2026, distribusi pickup per jam, Top 10 zona pickup, distribusi per Borough, dan status integrasi pipeline.",
        use_demo_024=use_demo_024,
        has_actual_data_024=has_data_024 and not use_demo_024,
    )
    render_mode_banner_024(use_demo_024=use_demo_024, has_actual_data_024=has_data_024)

    if hourly_res_024["error_024"]:
        st.error(hourly_res_024["error_024"])
    if zone_res_024["error_024"]:
        st.error(zone_res_024["error_024"])

    # Hitung seluruh KPI utama langsung dari Gold Layer
    total_pickups_024 = None
    avg_daily_024 = None
    avg_hourly_024 = None
    total_revenue_024 = None
    non_rev_pickups_024 = None
    peak_hour_024 = None
    peak_hour_total_024 = None
    quiet_hour_024 = None
    quiet_hour_total_024 = None
    top_zone_name_024 = None
    top_zone_caption_024 = "Menunggu data zona."

    if hourly_df_024 is not None and not hourly_df_024.empty:
        total_pickups_024 = int(hourly_df_024["pickup_count_024"].sum())
        num_days_024 = max(1, int(hourly_df_024["pickup_date_024"].nunique()))
        avg_daily_024 = total_pickups_024 / num_days_024
        avg_hourly_024 = float(hourly_df_024["pickup_count_024"].mean())
        if "net_settled_revenue_usd_024" in hourly_df_024.columns:
            total_revenue_024 = float(hourly_df_024["net_settled_revenue_usd_024"].sum())
        if "non_reversed_pickup_count_024" in hourly_df_024.columns:
            non_rev_pickups_024 = int(hourly_df_024["non_reversed_pickup_count_024"].sum())

        by_hour_024 = hourly_df_024.groupby("pickup_hour_024")["pickup_count_024"].sum()
        peak_hour_024 = int(by_hour_024.idxmax())
        peak_hour_total_024 = int(by_hour_024.max())
        quiet_hour_024 = int(by_hour_024.idxmin())
        quiet_hour_total_024 = int(by_hour_024.min())

    if zone_df_024 is not None and not zone_df_024.empty:
        zone_grouped_024 = (
            zone_df_024.groupby(["PULocationID", "Zone", "Borough"], as_index=False)["pickup_count_024"]
            .sum()
            .sort_values("pickup_count_024", ascending=False)
        )
        top_row_024 = zone_grouped_024.iloc[0]
        top_zone_name_024 = str(top_row_024["Zone"])
        top_share_024 = float(top_row_024["pickup_count_024"]) / max(1.0, float(zone_grouped_024["pickup_count_024"].sum())) * 100.0
        top_zone_caption_024 = (
            f"ID {int(top_row_024['PULocationID'])} • {top_row_024['Borough']} • "
            f"{format_number_id_024(top_row_024['pickup_count_024'], 0)} pickup ({format_number_id_024(top_share_024, 2)}%)"
        )

    # Baris 1: 4 KPI Utama
    col1_024, col2_024, col3_024, col4_024 = st.columns(4, gap="medium")
    with col1_024:
        pickup_cap_024 = (
            f"Rata-rata {format_number_id_024(avg_daily_024, 0)}/hari • {format_number_id_024(avg_hourly_024, 1)}/jam."
            if avg_daily_024 is not None
            else "Agregasi Gold Layer September 2026."
        )
        render_kpi_card_024(
            label_024="Total Pickup (Sep 2026)",
            value_024=format_number_id_024(total_pickups_024, 0) if total_pickups_024 is not None else None,
            caption_024=pickup_cap_024,
            tag_024="Volume",
            is_demo_024=use_demo_024,
        )
    with col2_024:
        rev_cap_024 = (
            f"Exact: {format_currency_usd_024(total_revenue_024, 2)} (non-reversal: {format_number_id_024(non_rev_pickups_024, 0)} trip)."
            if total_revenue_024 is not None and non_rev_pickups_024 is not None
            else "Pendapatan bersih tervalidasi (tidak tersedia di mode demo)."
        )
        render_kpi_card_024(
            label_024="Total Pendapatan Bersih",
            value_024=format_currency_usd_024(total_revenue_024, compact_024=True) if total_revenue_024 is not None else "N/A (Demo)",
            caption_024=rev_cap_024,
            tag_024="Finansial",
            is_demo_024=use_demo_024,
        )
    with col3_024:
        if peak_hour_024 is not None and quiet_hour_024 is not None:
            ratio_pq_024 = peak_hour_total_024 / max(1, quiet_hour_total_024)
            pq_val_024 = f"{peak_hour_024:02d}:00 vs {quiet_hour_024:02d}:00"
            pq_cap_024 = (
                f"Tersibuk {peak_hour_024:02d}:00 ({format_number_id_024(peak_hour_total_024, 0)}) vs "
                f"Tersunyi {quiet_hour_024:02d}:00 ({format_number_id_024(quiet_hour_total_024, 0)}) — {format_number_id_024(ratio_pq_024, 1)}×."
            )
        else:
            pq_val_024 = None
            pq_cap_024 = "Perbandingan jam puncak dan jam paling sepi."
        render_kpi_card_024(
            label_024="Jam Tersibuk & Tersunyi",
            value_024=pq_val_024,
            caption_024=pq_cap_024,
            tag_024="Temporal",
            is_demo_024=use_demo_024,
        )
    with col4_024:
        render_kpi_card_024(
            label_024="Zona Pickup Tertinggi",
            value_024=top_zone_name_024,
            caption_024=top_zone_caption_024,
            tag_024="Spasial",
            is_demo_024=use_demo_024,
        )

    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    # Baris 2: Distribusi Pickup per Jam + Ringkasan Insight Utama
    left_col_024, right_col_024 = st.columns([1.65, 1.0], gap="medium")

    with left_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Distribusi Pickup per Jam (00:00 – 23:00)",
                subtitle_024="Total volume perjalanan Yellow Taxi NYC berdasarkan jam pickup (Navy = puncak, Abu = tersepi).",
                badge_024="DATA DEMO" if use_demo_024 else "Gold Hourly",
            )
            if hourly_df_024 is not None and not hourly_df_024.empty:
                fig_hourly_024 = build_hourly_demand_chart_024(
                    hourly_df_024=hourly_df_024,
                    agg_mode_024="sum",
                    is_demo_024=use_demo_024,
                )
                st.plotly_chart(fig_hourly_024, width="stretch")
            else:
                render_empty_state_024(
                    title_024="Data Distribusi Jam Belum Tersedia",
                    description_024="Menunggu tabel gold_hourly_demand_024 di data/analytics/.",
                    expected_artifact_024="data/analytics/gold_hourly_demand_024/",
                    icon_024="◷",
                )

    with right_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Ringkasan Insight Utama",
                subtitle_024="Temuan numerik langsung dari agregasi Gold Layer September 2026.",
                badge_024="DATA DEMO" if use_demo_024 else "Berbasis Bukti",
            )
            if hourly_df_024 is not None and not hourly_df_024.empty and zone_df_024 is not None and not zone_df_024.empty:
                prefix_024 = "[DATA DEMO] " if use_demo_024 else ""

                # Insight 1: Peak vs Quiet
                render_insight_card_024(
                    title_024=f"{prefix_024}Konsentrasi Jam Sibuk & Sepi",
                    body_024=(
                        f"Aktivitas memuncak pada pukul {format_hour_id_024(peak_hour_024)} "
                        f"({format_number_id_024(peak_hour_total_024, 0)} pickup) dan mencapai titik terendah pada "
                        f"pukul {format_hour_id_024(quiet_hour_024)} ({format_number_id_024(quiet_hour_total_024, 0)} pickup)."
                    ),
                )

                # Insight 2: Top 10 Concentration & Airport Revenue
                z_rank_024 = zone_df_024.sort_values("pickup_count_024", ascending=False)
                top10_share_024 = (
                    float(z_rank_024.head(10)["pickup_count_024"].sum())
                    / max(1.0, float(z_rank_024["pickup_count_024"].sum()))
                    * 100.0
                )
                rev_note_024 = ""
                if "net_settled_revenue_usd_024" in z_rank_024.columns:
                    top_rev_z_024 = z_rank_024.sort_values("net_settled_revenue_usd_024", ascending=False).iloc[0]
                    rev_note_024 = (
                        f" Sementara pendapatan bersih tertinggi berasal dari {top_rev_z_024['Zone']} "
                        f"({format_currency_usd_024(top_rev_z_024['net_settled_revenue_usd_024'], compact_024=True)})."
                    )
                render_insight_card_024(
                    title_024=f"{prefix_024}Konsentrasi Spasial Tinggi",
                    body_024=(
                        f"Top 10 zona teratas menyumbang {format_number_id_024(top10_share_024, 2)}% dari seluruh "
                        f"pickup NYC (dipimpin {top_zone_name_024}).{rev_note_024}"
                    ),
                )

                # Insight 3: Borough Dominance
                boro_sum_024 = zone_df_024.groupby("Borough")["pickup_count_024"].sum().sort_values(ascending=False)
                top_boro_024 = str(boro_sum_024.index[0])
                top_boro_pct_024 = float(boro_sum_024.iloc[0]) / max(1.0, float(boro_sum_024.sum())) * 100.0
                sec_boro_text_024 = ""
                if len(boro_sum_024) > 1:
                    sec_boro_pct_024 = float(boro_sum_024.iloc[1]) / max(1.0, float(boro_sum_024.sum())) * 100.0
                    sec_boro_text_024 = f", disusul {boro_sum_024.index[1]} ({format_number_id_024(sec_boro_pct_024, 2)}%)"
                render_insight_card_024(
                    title_024=f"{prefix_024}Dominasi Wilayah (Borough)",
                    body_024=(
                        f"{top_boro_024} mendominasi {format_number_id_024(top_boro_pct_024, 2)}% permintaan Yellow Taxi "
                        f"({format_number_id_024(boro_sum_024.iloc[0], 0)} pickup){sec_boro_text_024}."
                    ),
                )
            else:
                render_insight_card_024(
                    title_024="Menunggu Data Serving Layer",
                    body_024="Insight akan terisi otomatis saat tabel Gold Layer tersedia.",
                )

    st.markdown("<div style='height: 0.4rem;'></div>", unsafe_allow_html=True)

    # Baris 3: Top 10 Zona Pickup + Distribusi Demand per Borough
    z_col_024, b_col_024 = st.columns([1.15, 1.0], gap="medium")

    with z_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Top 10 Zona Pickup Tertinggi",
                subtitle_024="Hasil join PULocationID == LocationID dengan taxi_zone_lookup_024.csv.",
                badge_024="DATA DEMO" if use_demo_024 else "Top 10 Zona",
            )
            if zone_df_024 is not None and not zone_df_024.empty:
                fig_top10_024 = build_zone_ranking_chart_024(
                    zone_df_024=zone_df_024,
                    top_n_024=10,
                    metric_col_024="pickup_count_024",
                    is_demo_024=use_demo_024,
                )
                st.plotly_chart(fig_top10_024, width="stretch")
            else:
                render_empty_state_024(
                    title_024="Data Zona Belum Tersedia",
                    description_024="Menunggu tabel gold_zone_demand_024 di data/analytics/.",
                    expected_artifact_024="data/analytics/gold_zone_demand_024/",
                    icon_024="◎",
                )

    with b_col_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Distribusi Demand per Borough",
                subtitle_024="Proporsi total pickup Yellow Taxi menurut wilayah administratif NYC.",
                badge_024="DATA DEMO" if use_demo_024 else "Per Borough",
            )
            if zone_df_024 is not None and not zone_df_024.empty:
                fig_boro_024 = build_zone_borough_comparison_chart_024(
                    zone_df_024=zone_df_024,
                    is_demo_024=use_demo_024,
                )
                st.plotly_chart(fig_boro_024, width="stretch")
            else:
                render_empty_state_024(
                    title_024="Data Borough Belum Tersedia",
                    description_024="Menunggu tabel gold_zone_demand_024 dan taxi_zone_lookup_024.csv.",
                    expected_artifact_024="data/analytics/gold_zone_demand_024/",
                    icon_024="⌖",
                )

    # Baris 4: Transparansi Kesiapan Pipeline & Inventaris Serving Layer
    with st.container(border=True):
        render_section_header_024(
            title_024="Status Integrasi Pipeline Big Data & Serving Layer",
            subtitle_024="Pemantauan langsung ketersediaan artefak dari Raw Layer, Reference, Silver v2, hingga Gold & Spark MLlib.",
            badge_024="Terintegrasi",
        )
        render_pipeline_status_grid_024(pipeline_status_024)

        with st.expander("Lihat Detail Inventaris Artefak Serving Layer (data/analytics/)", expanded=False):
            inv_df_024 = pd.DataFrame(pipeline_status_024["analytics_inventory_024"]).rename(
                columns={
                    "label_024": "Dataset Analitik",
                    "expected_file_024": "Nama Artefak",
                    "producer_024": "Sumber Pipeline",
                    "resolved_name_024": "Status Saat Ini",
                }
            )[["Dataset Analitik", "Nama Artefak", "Sumber Pipeline", "Status Saat Ini"]]
            st.dataframe(inv_df_024, width="stretch", hide_index=True)
