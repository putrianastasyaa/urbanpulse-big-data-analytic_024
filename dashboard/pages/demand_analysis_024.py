"""Halaman B: Demand Analysis (Analisis Permintaan Temporal) — UrbanPulse_024."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from dashboard.charts_024 import (
    build_day_hour_heatmap_024,
    build_day_of_week_chart_024,
    build_demand_trend_chart_024,
    build_hourly_demand_chart_024,
    build_time_period_comparison_chart_024,
    build_weekday_weekend_chart_024,
)
from dashboard.components_024 import (
    format_currency_usd_024,
    format_hour_id_024,
    format_number_id_024,
    render_empty_state_024,
    render_kpi_card_024,
    render_mode_banner_024,
    render_page_header_024,
    render_section_header_024,
)
from dashboard.config_024 import TIME_BLOCK_MAP_024
from dashboard.data_loader_024 import load_data_024


def render_demand_analysis_page_024(use_demo_024: bool) -> None:
    """Merender halaman analisis pola permintaan berbasis jam, hari kerja/akhir pekan, harian, dan periode waktu."""
    hourly_res_024 = load_data_024("hourly_demand", use_demo_024=use_demo_024)
    hourly_df_024: pd.DataFrame | None = hourly_res_024["df_024"]
    has_data_024 = bool(hourly_res_024["available_024"])

    render_page_header_024(
        eyebrow_024="Analisis Temporal • Demand Analysis",
        title_024="Analisis Pola Permintaan Taksi (Temporal)",
        subtitle_024="Pola demand per jam, komparasi hari kerja vs akhir pekan, tren harian, perbandingan periode waktu, dan matriks 7×24.",
        use_demo_024=use_demo_024,
        has_actual_data_024=has_data_024 and not use_demo_024,
    )
    render_mode_banner_024(use_demo_024=use_demo_024, has_actual_data_024=has_data_024)

    if hourly_res_024["error_024"]:
        st.error(hourly_res_024["error_024"])

    # Panel Filter Interaktif
    with st.container(border=True):
        st.markdown('<div class="up-filter-title-024">Filter Analisis Temporal</div>', unsafe_allow_html=True)
        f_col1_024, f_col2_024, f_col3_024 = st.columns([1.25, 1.0, 1.0], gap="medium")

        if has_data_024 and hourly_df_024 is not None:
            work_df_024 = hourly_df_024.copy()
            work_df_024["pickup_date_dt_024"] = pd.to_datetime(work_df_024["pickup_date_024"]).dt.date
            min_date_024 = work_df_024["pickup_date_dt_024"].min()
            max_date_024 = work_df_024["pickup_date_dt_024"].max()
        else:
            work_df_024 = None
            min_date_024 = date(2026, 9, 1)
            max_date_024 = date(2026, 9, 30)

        with f_col1_024:
            date_range_024 = st.date_input(
                "Rentang Tanggal",
                value=(min_date_024, max_date_024),
                min_value=min_date_024,
                max_value=max_date_024,
                disabled=not has_data_024,
                key="demand_date_range_024",
            )
        with f_col2_024:
            day_filter_024 = st.selectbox(
                "Filter Jenis Hari",
                options=["Semua Hari", "Hari Kerja (Senin–Jumat)", "Akhir Pekan (Sabtu–Minggu)"],
                disabled=not has_data_024,
                key="demand_day_type_024",
            )
        with f_col3_024:
            time_block_024 = st.selectbox(
                "Filter Periode Waktu",
                options=list(TIME_BLOCK_MAP_024.keys()),
                disabled=not has_data_024,
                key="demand_time_block_024",
            )

    if not has_data_024 or work_df_024 is None:
        render_empty_state_024(
            title_024="Data Permintaan Temporal Belum Tersedia",
            description_024="Menunggu tabel gold_hourly_demand_024 di data/analytics/.",
            expected_artifact_024="data/analytics/gold_hourly_demand_024/",
            icon_024="◷",
        )
        return

    # Filter rentang tanggal terlebih dahulu (dipakai juga untuk komparasi weekday vs weekend yang konsisten)
    date_filtered_024 = work_df_024.copy()
    if isinstance(date_range_024, (tuple, list)) and len(date_range_024) == 2:
        start_dt_024, end_dt_024 = date_range_024[0], date_range_024[1]
        date_filtered_024 = date_filtered_024[
            (date_filtered_024["pickup_date_dt_024"] >= start_dt_024)
            & (date_filtered_024["pickup_date_dt_024"] <= end_dt_024)
        ]

    # Filter lengkap (tanggal + jenis hari + periode waktu)
    filtered_df_024 = date_filtered_024.copy()
    if day_filter_024 == "Hari Kerja (Senin–Jumat)":
        filtered_df_024 = filtered_df_024[~filtered_df_024["is_weekend_024"]]
    elif day_filter_024 == "Akhir Pekan (Sabtu–Minggu)":
        filtered_df_024 = filtered_df_024[filtered_df_024["is_weekend_024"]]

    selected_hours_024 = TIME_BLOCK_MAP_024[time_block_024]
    filtered_df_024 = filtered_df_024[filtered_df_024["pickup_hour_024"].isin(selected_hours_024)]

    if filtered_df_024.empty:
        st.warning("Tidak ada data yang sesuai dengan kombinasi filter yang dipilih. Silakan perluas rentang filter.")
        return

    # KPI Ringkas yang akurat mengikuti filter aktif
    total_filtered_024 = int(filtered_df_024["pickup_count_024"].sum())
    has_rev_024 = "net_settled_revenue_usd_024" in filtered_df_024.columns
    rev_filtered_024 = float(filtered_df_024["net_settled_revenue_usd_024"].sum()) if has_rev_024 else None

    hourly_means_024 = filtered_df_024.groupby("pickup_hour_024")["pickup_count_024"].mean()
    peak_hour_024 = int(hourly_means_024.idxmax())
    low_hour_024 = int(hourly_means_024.idxmin())

    # Weekday vs Weekend ternormalisasi per hari kalender pada rentang tanggal + blok jam aktif
    comp_base_024 = date_filtered_024[date_filtered_024["pickup_hour_024"].isin(selected_hours_024)]
    wkday_sub_024 = comp_base_024[~comp_base_024["is_weekend_024"]]
    wkend_sub_024 = comp_base_024[comp_base_024["is_weekend_024"]]
    wkday_avg_hr_024 = float(wkday_sub_024["pickup_count_024"].mean()) if not wkday_sub_024.empty else None
    wkend_avg_hr_024 = float(wkend_sub_024["pickup_count_024"].mean()) if not wkend_sub_024.empty else None
    wkday_days_024 = int(wkday_sub_024["pickup_date_dt_024"].nunique())
    wkend_days_024 = int(wkend_sub_024["pickup_date_dt_024"].nunique())
    wkday_avg_day_024 = float(wkday_sub_024["pickup_count_024"].sum()) / max(1, wkday_days_024) if wkday_days_024 > 0 else None
    wkend_avg_day_024 = float(wkend_sub_024["pickup_count_024"].sum()) / max(1, wkend_days_024) if wkend_days_024 > 0 else None

    k1_024, k2_024, k3_024, k4_024 = st.columns(4, gap="medium")
    with k1_024:
        cap1_024 = (
            f"Pendapatan bersih: {format_currency_usd_024(rev_filtered_024, compact_024=True)} ({len(filtered_df_024)} slot jam)."
            if rev_filtered_024 is not None
            else f"Total {len(filtered_df_024)} slot jam pada filter aktif."
        )
        render_kpi_card_024(
            label_024="Total Pickup (Filter Aktif)",
            value_024=format_number_id_024(total_filtered_024, 0),
            caption_024=cap1_024,
            tag_024="Volume",
            is_demo_024=use_demo_024,
        )
    with k2_024:
        render_kpi_card_024(
            label_024="Jam Tersibuk & Tersepi",
            value_024=f"{peak_hour_024:02d}:00 vs {low_hour_024:02d}:00",
            caption_024=(
                f"Puncak {format_number_id_024(hourly_means_024.max(), 1)}/jam • "
                f"Tersepi {format_number_id_024(hourly_means_024.min(), 1)}/jam."
            ),
            tag_024="Puncak",
            is_demo_024=use_demo_024,
        )
    with k3_024:
        render_kpi_card_024(
            label_024="Hari Kerja (Rata-rata/Jam)",
            value_024=format_number_id_024(wkday_avg_hr_024, 1) if wkday_avg_hr_024 is not None else "—",
            caption_024=(
                f"{format_number_id_024(wkday_avg_day_024, 0)}/hari ({wkday_days_024} hari kalender)."
                if wkday_avg_day_024 is not None
                else "Tidak ada hari kerja di rentang ini."
            ),
            tag_024="Weekday",
            is_demo_024=use_demo_024,
        )
    with k4_024:
        render_kpi_card_024(
            label_024="Akhir Pekan (Rata-rata/Jam)",
            value_024=format_number_id_024(wkend_avg_hr_024, 1) if wkend_avg_hr_024 is not None else "—",
            caption_024=(
                f"{format_number_id_024(wkend_avg_day_024, 0)}/hari ({wkend_days_024} hari kalender)."
                if wkend_avg_day_024 is not None
                else "Tidak ada akhir pekan di rentang ini."
            ),
            tag_024="Weekend",
            is_demo_024=use_demo_024,
        )

    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    # Baris Grafik 1: Pola Demand per Jam + Perbandingan Weekday vs Weekend
    g1_024, g2_024 = st.columns(2, gap="medium")
    with g1_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Pola Demand per Jam",
                subtitle_024="Rata-rata pickup per jam sesuai rentang tanggal, jenis hari, dan periode waktu aktif.",
                badge_024="DATA DEMO" if use_demo_024 else "Per Jam",
            )
            fig_hourly_024 = build_hourly_demand_chart_024(
                hourly_df_024=filtered_df_024,
                agg_mode_024="mean",
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_hourly_024, width="stretch")

    with g2_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Perbandingan Weekday vs Weekend",
                subtitle_024="Kurva komparatif rata-rata pickup/jam Senin–Jumat terhadap Sabtu–Minggu.",
                badge_024="DATA DEMO" if use_demo_024 else "Komparasi",
            )
            fig_comp_024 = build_weekday_weekend_chart_024(
                hourly_df_024=comp_base_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_comp_024, width="stretch")

    # Baris Grafik 2: Pola Demand Harian (Tren Tanggal) + Rata-rata per Hari dalam Seminggu
    d1_024, d2_024 = st.columns([1.2, 1.0], gap="medium")
    with d1_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Pola Demand Harian (Tren Kalender)",
                subtitle_024="Fluktuasi total pickup harian sepanjang rentang tanggal terpilih.",
                badge_024="DATA DEMO" if use_demo_024 else "Harian",
            )
            fig_trend_024 = build_demand_trend_chart_024(
                hourly_df_024=filtered_df_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_trend_024, width="stretch")

    with d2_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Rata-rata Demand per Hari (Ternormalisasi)",
                subtitle_024="Rata-rata pickup per kemunculan hari kalender (menghindari bias 4 vs 5 hari).",
                badge_024="DATA DEMO" if use_demo_024 else "Senin–Minggu",
            )
            fig_dow_024 = build_day_of_week_chart_024(
                hourly_df_024=filtered_df_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_dow_024, width="stretch")

    # Baris Grafik 3: Perbandingan Periode Waktu + Heatmap Hari × Jam
    p1_024, p2_024 = st.columns([0.9, 1.35], gap="medium")
    with p1_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Perbandingan Periode Waktu",
                subtitle_024="Distribusi permintaan pada Dini Hari, Pagi, Siang, dan Malam (mengikuti filter tanggal & hari).",
                badge_024="DATA DEMO" if use_demo_024 else "4 Periode",
            )
            period_base_024 = date_filtered_024.copy()
            if day_filter_024 == "Hari Kerja (Senin–Jumat)":
                period_base_024 = period_base_024[~period_base_024["is_weekend_024"]]
            elif day_filter_024 == "Akhir Pekan (Sabtu–Minggu)":
                period_base_024 = period_base_024[period_base_024["is_weekend_024"]]
            fig_period_024 = build_time_period_comparison_chart_024(
                hourly_df_024=period_base_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_period_024, width="stretch")

    with p2_024:
        with st.container(border=True):
            render_section_header_024(
                title_024="Heatmap Intensitas Hari × Jam",
                subtitle_024="Warna lebih gelap menandakan rata-rata pickup/jam lebih tinggi pada matriks 7×24.",
                badge_024="DATA DEMO" if use_demo_024 else "Matriks 7×24",
            )
            fig_heat_024 = build_day_hour_heatmap_024(
                hourly_df_024=filtered_df_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_heat_024, width="stretch")
