"""Halaman C: Zone Analysis (Analisis Zona Pickup NYC) — UrbanPulse_024."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import streamlit as st

from dashboard.charts_024 import (
    build_nyc_zone_map_024,
    build_zone_borough_comparison_chart_024,
    build_zone_hourly_comparison_chart_024,
    build_zone_ranking_chart_024,
    build_zone_revenue_vs_demand_chart_024,
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
from dashboard.data_loader_024 import load_data_024, load_nyc_geojson_024


def render_zone_analysis_page_024(use_demo_024: bool) -> None:
    """Merender halaman analisis spasial zona pickup, pendapatan, dan perbandingan antarzona."""
    zone_res_024 = load_data_024("zone_summary", use_demo_024=use_demo_024)
    zone_hourly_res_024 = load_data_024("zone_hourly_demand", use_demo_024=use_demo_024)
    zone_df_024: pd.DataFrame | None = zone_res_024["df_024"]
    zone_hourly_df_024: pd.DataFrame | None = zone_hourly_res_024["df_024"]
    has_data_024 = bool(zone_res_024["available_024"])
    geojson_024, geojson_name_024 = load_nyc_geojson_024()

    render_page_header_024(
        eyebrow_024="Analisis Spasial • Zone Analysis",
        title_024="Analisis Lokasi, Demand & Pendapatan Zona NYC",
        subtitle_024="Ranking zona pickup (enrichment PULocationID == LocationID), analisis demand vs pendapatan bersih, filter Borough & service_zone, dan profil perbandingan antarzona.",
        use_demo_024=use_demo_024,
        has_actual_data_024=has_data_024 and not use_demo_024,
    )
    render_mode_banner_024(use_demo_024=use_demo_024, has_actual_data_024=has_data_024)

    if zone_res_024["error_024"]:
        st.error(zone_res_024["error_024"])

    if not has_data_024 or zone_df_024 is None:
        render_empty_state_024(
            title_024="Data Zona Belum Tersedia",
            description_024="Menunggu tabel gold_zone_demand_024 di data/analytics/ dan data/reference/taxi_zone_lookup_024.csv.",
            expected_artifact_024="data/analytics/gold_zone_demand_024/",
            icon_024="◎",
        )
        return

    # Pastikan agregasi per PULocationID unik (aman terhadap nama Zone duplikat)
    work_zone_024 = zone_df_024.copy()
    has_rev_024 = "net_settled_revenue_usd_024" in work_zone_024.columns
    if "service_zone" not in work_zone_024.columns:
        work_zone_024["service_zone"] = "N/A"

    # Panel Filter Borough, service_zone, Metrik Ranking, dan Top-N
    with st.container(border=True):
        st.markdown('<div class="up-filter-title-024">Filter Wilayah (Borough), Service Zone, dan Tampilan Ranking</div>', unsafe_allow_html=True)
        z_col1_024, z_col2_024, z_col3_024, z_col4_024 = st.columns([1.05, 1.05, 1.05, 0.85], gap="medium")

        borough_options_024 = ["Semua Borough"] + sorted(work_zone_024["Borough"].dropna().astype(str).unique().tolist())
        service_options_024 = ["Semua Service Zone"] + sorted(work_zone_024["service_zone"].dropna().astype(str).unique().tolist())

        with z_col1_024:
            selected_borough_024 = st.selectbox(
                "Filter Borough",
                options=borough_options_024,
                key="zone_borough_024",
            )
        with z_col2_024:
            selected_service_024 = st.selectbox(
                "Filter Service Zone",
                options=service_options_024,
                key="zone_service_zone_024",
            )
        with z_col3_024:
            rank_metric_options_024 = (
                ["Volume Pickup", "Pendapatan Bersih (USD)"]
                if has_rev_024
                else ["Volume Pickup"]
            )
            selected_metric_label_024 = st.selectbox(
                "Urutkan Ranking Berdasarkan",
                options=rank_metric_options_024,
                key="zone_rank_metric_024",
            )
        with z_col4_024:
            top_n_024 = st.slider(
                "Jumlah Top Zona",
                min_value=5,
                max_value=20,
                value=10,
                key="zone_top_n_024",
            )

    # Terapkan filter Borough & service_zone
    filtered_zone_024 = work_zone_024.copy()
    if selected_borough_024 != "Semua Borough":
        filtered_zone_024 = filtered_zone_024[filtered_zone_024["Borough"] == selected_borough_024]
    if selected_service_024 != "Semua Service Zone":
        filtered_zone_024 = filtered_zone_024[filtered_zone_024["service_zone"] == selected_service_024]

    if filtered_zone_024.empty:
        st.warning("Tidak ada zona untuk kombinasi filter Borough dan service_zone tersebut. Silakan ubah pilihan filter.")
        return

    # Agregasi per PULocationID agar 100% bebas bentrokan nama Zone
    agg_dict_024: Dict[str, object] = {
        "pickup_count_024": "sum",
    }
    if has_rev_024:
        agg_dict_024["net_settled_revenue_usd_024"] = "sum"
    if "avg_settled_total_usd_024" in filtered_zone_024.columns:
        agg_dict_024["avg_settled_total_usd_024"] = "mean"
    if "peak_hour_024" in filtered_zone_024.columns:
        agg_dict_024["peak_hour_024"] = "first"
    if "weekend_share_pct_024" in filtered_zone_024.columns:
        agg_dict_024["weekend_share_pct_024"] = "mean"
    if "demand_tier_024" in filtered_zone_024.columns:
        agg_dict_024["demand_tier_024"] = "first"
    if "latitude_024" in filtered_zone_024.columns:
        agg_dict_024["latitude_024"] = "first"
    if "longitude_024" in filtered_zone_024.columns:
        agg_dict_024["longitude_024"] = "first"

    grouped_zones_024 = (
        filtered_zone_024.groupby(["PULocationID", "Zone", "Borough", "service_zone"], as_index=False)
        .agg(agg_dict_024)
        .sort_values("pickup_count_024", ascending=False)
        .reset_index(drop=True)
    )

    total_filtered_pickups_024 = float(grouped_zones_024["pickup_count_024"].sum())
    total_filtered_rev_024 = float(grouped_zones_024["net_settled_revenue_usd_024"].sum()) if has_rev_024 else None
    highest_vol_row_024 = grouped_zones_024.iloc[0]
    highest_rev_row_024 = (
        grouped_zones_024.sort_values("net_settled_revenue_usd_024", ascending=False).iloc[0]
        if has_rev_024
        else highest_vol_row_024
    )
    top_k_conc_024 = min(10, len(grouped_zones_024))
    top_k_share_024 = (
        float(grouped_zones_024.head(top_k_conc_024)["pickup_count_024"].sum())
        / max(1.0, total_filtered_pickups_024)
        * 100.0
    )

    # Baris 1: 4 KPI Spasial
    k1_024, k2_024, k3_024, k4_024 = st.columns(4, gap="medium")
    with k1_024:
        render_kpi_card_024(
            label_024="Zona Volume Tertinggi",
            value_024=str(highest_vol_row_024["Zone"]),
            caption_024=(
                f"ID {int(highest_vol_row_024['PULocationID'])} • {highest_vol_row_024['Borough']} • "
                f"{format_number_id_024(highest_vol_row_024['pickup_count_024'], 0)} pickup"
            ),
            tag_024="Volume #1",
            is_demo_024=use_demo_024,
        )
    with k2_024:
        if has_rev_024:
            rev_val_str_024 = str(highest_rev_row_024["Zone"])
            rev_cap_str_024 = (
                f"{format_currency_usd_024(highest_rev_row_024['net_settled_revenue_usd_024'], compact_024=True)} • "
                f"Avg {format_currency_usd_024(highest_rev_row_024.get('avg_settled_total_usd_024'), 2)}/trip"
            )
        else:
            rev_val_str_024 = "N/A (Demo)"
            rev_cap_str_024 = "Data pendapatan hanya tersedia pada Gold Layer aktual."
        render_kpi_card_024(
            label_024="Zona Pendapatan Tertinggi",
            value_024=rev_val_str_024,
            caption_024=rev_cap_str_024,
            tag_024="Revenue #1",
            is_demo_024=use_demo_024,
        )
    with k3_024:
        total_cap_024 = (
            f"Pendapatan bersih: {format_currency_usd_024(total_filtered_rev_024, compact_024=True)}."
            if total_filtered_rev_024 is not None
            else f"Akumulasi dari {len(grouped_zones_024)} zona aktif."
        )
        render_kpi_card_024(
            label_024="Total Pickup (Filter Aktif)",
            value_024=format_number_id_024(total_filtered_pickups_024, 0),
            caption_024=total_cap_024,
            tag_024=f"{len(grouped_zones_024)} Zona",
            is_demo_024=use_demo_024,
        )
    with k4_024:
        render_kpi_card_024(
            label_024=f"Konsentrasi Top-{top_k_conc_024} Zona",
            value_024=f"{format_number_id_024(top_k_share_024, 2)}%",
            caption_024=f"Pangsa {top_k_conc_024} zona teratas dari total pickup filter aktif.",
            tag_024="Konsentrasi",
            is_demo_024=use_demo_024,
        )

    st.markdown("<div style='height: 0.9rem;'></div>", unsafe_allow_html=True)

    # Baris 2: Ranking Zona + Demand vs Pendapatan (atau Peta GeoJSON jika tersedia)
    metric_col_024 = (
        "net_settled_revenue_usd_024"
        if selected_metric_label_024 == "Pendapatan Bersih (USD)" and has_rev_024
        else "pickup_count_024"
    )
    col_rank_024, col_scatter_024 = st.columns([1.05, 1.15], gap="medium")

    with col_rank_024:
        with st.container(border=True):
            render_section_header_024(
                title_024=f"Top {min(top_n_024, len(grouped_zones_024))} Zona ({selected_metric_label_024})",
                subtitle_024="Peringkat berbasis PULocationID unik (enrichment taxi_zone_lookup_024.csv).",
                badge_024="DATA DEMO" if use_demo_024 else "Ranking",
            )
            fig_rank_024 = build_zone_ranking_chart_024(
                zone_df_024=grouped_zones_024,
                top_n_024=top_n_024,
                metric_col_024=metric_col_024,
                is_demo_024=use_demo_024,
            )
            st.plotly_chart(fig_rank_024, width="stretch")

    with col_scatter_024:
        with st.container(border=True):
            if has_rev_024:
                render_section_header_024(
                    title_024="Demand vs Pendapatan Bersih per Zona",
                    subtitle_024="Ukuran titik mencerminkan rata-rata total tarif/perjalanan (menyoroti divergensi JFK vs Manhattan).",
                    badge_024="DATA DEMO" if use_demo_024 else "Volume × Revenue",
                )
                fig_rev_024 = build_zone_revenue_vs_demand_chart_024(
                    zone_df_024=grouped_zones_024,
                    is_demo_024=use_demo_024,
                )
                st.plotly_chart(fig_rev_024, width="stretch")
            else:
                fig_map_024 = build_nyc_zone_map_024(
                    zone_df_024=grouped_zones_024,
                    geojson_024=geojson_024,
                    is_demo_024=use_demo_024,
                )
                render_section_header_024(
                    title_024="Persebaran Zona Pickup di NYC",
                    subtitle_024=f"Menggunakan {geojson_name_024}" if geojson_name_024 else "Peta koordinat zona.",
                    badge_024="DATA DEMO" if use_demo_024 else "Peta NYC",
                )
                if fig_map_024 is not None:
                    st.plotly_chart(fig_map_024, width="stretch")

    # Baris 3: Perbandingan Antar-Zona (Profil 24 Jam dari gold_zone_hourly_demand_024)
    if zone_hourly_df_024 is not None and not zone_hourly_df_024.empty:
        with st.container(border=True):
            render_section_header_024(
                title_024="Perbandingan Profil Permintaan 24 Jam Antar-Zona",
                subtitle_024="Bandingkan pola jam puncak antar zona terpilih (mis. Upper East Side vs JFK Airport vs East Village).",
                badge_024="Gold Zone × Hour",
            )
            # Pemetaan label unik -> PULocationID
            label_to_id_024: Dict[str, int] = {}
            all_labels_024: List[str] = []
            for _, r_024 in grouped_zones_024.iterrows():
                pid_024 = int(r_024["PULocationID"])
                lbl_024 = f"{r_024['Zone']} (ID {pid_024}) — {r_024['Borough']}"
                label_to_id_024[lbl_024] = pid_024
                all_labels_024.append(lbl_024)

            # Default zona yang kontras jika tersedia dalam filter aktif
            preferred_ids_024 = [237, 132, 161, 79]
            default_labels_024 = [
                lbl_024
                for lbl_024 in all_labels_024
                if label_to_id_024[lbl_024] in preferred_ids_024
            ]
            if not default_labels_024:
                default_labels_024 = all_labels_024[: min(4, len(all_labels_024))]

            selected_compare_labels_024 = st.multiselect(
                "Pilih 1 hingga 6 Zona untuk Dibandingkan (berdasarkan PULocationID unik)",
                options=all_labels_024,
                default=default_labels_024[:4],
                max_selections=6,
                key=f"zone_compare_ms_{selected_borough_024}_{selected_service_024}_024",
            )
            if selected_compare_labels_024:
                compare_ids_024 = [label_to_id_024[lbl_024] for lbl_024 in selected_compare_labels_024]
                fig_z_hourly_024 = build_zone_hourly_comparison_chart_024(
                    zone_hourly_df_024=zone_hourly_df_024,
                    selected_ids_024=compare_ids_024,
                    is_demo_024=use_demo_024,
                )
                st.plotly_chart(fig_z_hourly_024, width="stretch")
            else:
                st.info("Pilih minimal satu zona di atas untuk menampilkan profil permintaan 24 jam.")

    # Baris 4: Perbandingan Antar-Borough + Tabel Lengkap Zona
    with st.container(border=True):
        render_section_header_024(
            title_024="Perbandingan Permintaan Antar-Borough",
            subtitle_024="Distribusi total pickup dan pendapatan bersih menurut Borough (sesuai filter aktif).",
            badge_024="DATA DEMO" if use_demo_024 else "Borough",
        )
        fig_boro_024 = build_zone_borough_comparison_chart_024(
            zone_df_024=grouped_zones_024,
            is_demo_024=use_demo_024,
        )
        st.plotly_chart(fig_boro_024, width="stretch")

    with st.container(border=True):
        render_section_header_024(
            title_024="Tabel Rincian Demand & Pendapatan per Zona",
            subtitle_024="Seluruh zona aktif sesuai filter Borough dan service_zone (join PULocationID == LocationID).",
            badge_024=f"{len(grouped_zones_024)} Zona",
        )
        display_tbl_024 = grouped_zones_024.copy()
        display_tbl_024["Peringkat"] = range(1, len(display_tbl_024) + 1)
        display_tbl_024["Pangsa (%)"] = (
            display_tbl_024["pickup_count_024"] / max(1.0, total_filtered_pickups_024) * 100.0
        ).round(2)

        rename_map_024 = {
            "PULocationID": "PULocationID",
            "Zone": "Nama Zona",
            "Borough": "Borough",
            "service_zone": "Service Zone",
            "pickup_count_024": "Total Pickup",
            "net_settled_revenue_usd_024": "Pendapatan Bersih (USD)",
            "avg_settled_total_usd_024": "Avg Total/Trip (USD)",
            "peak_hour_024": "Jam Puncak",
            "weekend_share_pct_024": "Pangsa Weekend (%)",
        }
        ordered_cols_024 = ["Peringkat", "PULocationID", "Nama Zona", "Borough", "Service Zone", "Total Pickup", "Pangsa (%)"]
        if has_rev_024:
            display_tbl_024["net_settled_revenue_usd_024"] = display_tbl_024["net_settled_revenue_usd_024"].round(2)
            ordered_cols_024.append("Pendapatan Bersih (USD)")
        if "avg_settled_total_usd_024" in display_tbl_024.columns:
            display_tbl_024["avg_settled_total_usd_024"] = display_tbl_024["avg_settled_total_usd_024"].round(2)
            ordered_cols_024.append("Avg Total/Trip (USD)")
        if "peak_hour_024" in display_tbl_024.columns:
            display_tbl_024["peak_hour_024"] = display_tbl_024["peak_hour_024"].apply(
                lambda h_024: format_hour_id_024(int(h_024)) if pd.notna(h_024) else "-"
            )
            ordered_cols_024.append("Jam Puncak")
        if "weekend_share_pct_024" in display_tbl_024.columns:
            display_tbl_024["weekend_share_pct_024"] = display_tbl_024["weekend_share_pct_024"].round(2)
            ordered_cols_024.append("Pangsa Weekend (%)")

        display_tbl_024 = display_tbl_024.rename(columns=rename_map_024)[ordered_cols_024]
        st.dataframe(display_tbl_024, width="stretch", hide_index=True, height=380)
