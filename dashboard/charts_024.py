"""Builder grafik Plotly interaktif dengan tema konsisten untuk UrbanPulse_024."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.config_024 import (
    BOROUGH_COLOR_MAP_024,
    CHART_COLORSCALE_024,
    DAY_ORDER_ID_024,
    PALETTE_024,
    TIME_BUCKET_ORDER_024,
)


def apply_urbanpulse_layout_024(
    fig_024: go.Figure,
    height_024: int = 380,
    is_demo_024: bool = False,
    xaxis_title_024: str = "",
    yaxis_title_024: str = "",
) -> go.Figure:
    """Menerapkan tata letak, tipografi, gridline, dan label konsisten ke figur Plotly."""
    fig_024.update_layout(
        height=height_024,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="Segoe UI Variable, Segoe UI, -apple-system, sans-serif",
            color=PALETTE_024["text_primary"],
            size=12,
        ),
        margin=dict(l=16, r=28, t=36, b=46 if is_demo_024 else 24),
        hoverlabel=dict(
            bgcolor=PALETTE_024["navy_deep"],
            bordercolor=PALETTE_024["blue_primary"],
            font=dict(color="#FFFFFF", size=12),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.03,
            xanchor="right",
            x=1.0,
            bgcolor="rgba(255,255,255,0)",
            font=dict(size=11, color=PALETTE_024["text_secondary"]),
        ),
    )

    if xaxis_title_024:
        fig_024.update_xaxes(title_text=xaxis_title_024)
    if yaxis_title_024:
        fig_024.update_yaxes(title_text=yaxis_title_024)

    fig_024.update_xaxes(
        showgrid=False,
        linecolor=PALETTE_024["border_subtle"],
        tickfont=dict(color=PALETTE_024["text_secondary"]),
        title_font=dict(color=PALETTE_024["text_secondary"], size=12),
    )
    fig_024.update_yaxes(
        showgrid=True,
        gridcolor="#EDF2F7",
        zeroline=False,
        tickfont=dict(color=PALETTE_024["text_secondary"]),
        title_font=dict(color=PALETTE_024["text_secondary"], size=12),
    )

    if is_demo_024:
        fig_024.add_annotation(
            text="Sumber visualisasi: DATA DEMO (simulasi pengujian UI)",
            xref="paper",
            yref="paper",
            x=0.0,
            y=-0.22,
            yanchor="top",
            showarrow=False,
            font=dict(size=10, color=PALETTE_024["amber_demo"]),
        )

    return fig_024


def build_demand_trend_chart_024(
    hourly_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik area tren total permintaan harian (dan pendapatan di tooltip jika tersedia)."""
    agg_dict_024: Dict[str, Any] = {"pickup_count_024": "sum"}
    has_rev_024 = "net_settled_revenue_usd_024" in hourly_df_024.columns
    if has_rev_024:
        agg_dict_024["net_settled_revenue_usd_024"] = "sum"
    if "day_name_024" in hourly_df_024.columns:
        agg_dict_024["day_name_024"] = "first"

    daily_df_024 = (
        hourly_df_024.groupby("pickup_date_024", as_index=False)
        .agg(agg_dict_024)
        .sort_values("pickup_date_024")
    )
    daily_df_024["date_str_024"] = daily_df_024["pickup_date_024"].astype(str)

    hover_tmpl_024 = "<b>Tanggal:</b> %{x}<br><b>Total Pickup:</b> %{y:,.0f}<extra></extra>"
    customdata_024 = None
    if has_rev_024 and "day_name_024" in daily_df_024.columns:
        customdata_024 = daily_df_024[["day_name_024", "net_settled_revenue_usd_024"]].values
        hover_tmpl_024 = (
            "<b>%{customdata[0]}, %{x}</b><br>"
            "Total Pickup: %{y:,.0f}<br>"
            "Pendapatan Bersih: USD %{customdata[1]:,.2f}<extra></extra>"
        )

    fig_024 = go.Figure()
    fig_024.add_trace(
        go.Scatter(
            x=daily_df_024["date_str_024"],
            y=daily_df_024["pickup_count_024"],
            mode="lines+markers",
            name="Total Pickup Harian",
            line=dict(color=PALETTE_024["blue_primary"], width=2.8, shape="spline"),
            marker=dict(size=6.5, color=PALETTE_024["navy_deep"], line=dict(color="#FFFFFF", width=1.2)),
            fill="tozeroy",
            fillcolor="rgba(37, 99, 235, 0.10)",
            customdata=customdata_024,
            hovertemplate=hover_tmpl_024,
        )
    )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=350,
        is_demo_024=is_demo_024,
        xaxis_title_024="Tanggal",
        yaxis_title_024="Total Pickup / Hari",
    )


def build_hourly_demand_chart_024(
    hourly_df_024: pd.DataFrame,
    agg_mode_024: str = "mean",
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik batang distribusi permintaan per jam (00:00–23:00)."""
    agg_spec_024: Dict[str, Any] = {
        "mean_pickups_024": ("pickup_count_024", "mean"),
        "sum_pickups_024": ("pickup_count_024", "sum"),
    }
    has_rev_024 = "net_settled_revenue_usd_024" in hourly_df_024.columns
    if has_rev_024:
        agg_spec_024["sum_rev_024"] = ("net_settled_revenue_usd_024", "sum")

    agg_df_024 = (
        hourly_df_024.groupby("pickup_hour_024", as_index=False)
        .agg(**agg_spec_024)
        .sort_values("pickup_hour_024")
    )

    y_col_024 = "sum_pickups_024" if agg_mode_024 == "sum" else "mean_pickups_024"
    y_title_024 = "Total Pickup (September 2026)" if agg_mode_024 == "sum" else "Rata-rata Pickup / Jam"
    peak_val_024 = float(agg_df_024[y_col_024].max()) if not agg_df_024.empty else 0.0
    min_val_024 = float(agg_df_024[y_col_024].min()) if not agg_df_024.empty else 0.0

    bar_colors_024: List[str] = []
    for v_024 in agg_df_024[y_col_024]:
        fv_024 = float(v_024)
        if fv_024 >= peak_val_024 * 0.94:
            bar_colors_024.append(PALETTE_024["navy_deep"])
        elif fv_024 <= min_val_024 * 1.15:
            bar_colors_024.append(PALETTE_024["slate_chart"])
        else:
            bar_colors_024.append(PALETTE_024["blue_accent"])

    hour_labels_024 = [f"{int(h_024):02d}:00" for h_024 in agg_df_024["pickup_hour_024"]]

    if has_rev_024:
        custom_024 = agg_df_024[["sum_pickups_024", "mean_pickups_024", "sum_rev_024"]].values
        hover_tmpl_024 = (
            "<b>Jam %{x}</b><br>"
            "Total Pickup: %{customdata[0]:,.0f}<br>"
            "Rata-rata/Hari: %{customdata[1]:,.1f}<br>"
            "Pendapatan Bersih: USD %{customdata[2]:,.2f}<extra></extra>"
        )
    else:
        custom_024 = agg_df_024[["sum_pickups_024", "mean_pickups_024"]].values
        hover_tmpl_024 = (
            "<b>Jam %{x}</b><br>"
            "Total Pickup: %{customdata[0]:,.0f}<br>"
            "Rata-rata/Slot: %{customdata[1]:,.1f}<extra></extra>"
        )

    fig_024 = go.Figure()
    fig_024.add_trace(
        go.Bar(
            x=hour_labels_024,
            y=agg_df_024[y_col_024],
            name=y_title_024,
            marker=dict(color=bar_colors_024, cornerradius=5),
            customdata=custom_024,
            hovertemplate=hover_tmpl_024,
        )
    )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=360,
        is_demo_024=is_demo_024,
        xaxis_title_024="Jam dalam Sehari (NYC Local Time)",
        yaxis_title_024=y_title_024,
    )


def build_weekday_weekend_chart_024(
    hourly_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membandingkan pola permintaan per jam antara Hari Kerja dan Akhir Pekan."""
    comp_df_024 = (
        hourly_df_024.groupby(["is_weekend_024", "pickup_hour_024"], as_index=False)["pickup_count_024"]
        .mean()
        .sort_values("pickup_hour_024")
    )

    fig_024 = go.Figure()
    series_cfg_024 = [
        (False, "Hari Kerja (Sen–Jum)", PALETTE_024["navy_deep"], "solid"),
        (True, "Akhir Pekan (Sab–Min)", PALETTE_024["blue_accent"], "dash"),
    ]

    for flag_024, label_024, color_024, dash_024 in series_cfg_024:
        subset_024 = comp_df_024[comp_df_024["is_weekend_024"] == flag_024]
        if subset_024.empty:
            continue
        fig_024.add_trace(
            go.Scatter(
                x=[f"{int(h_024):02d}:00" for h_024 in subset_024["pickup_hour_024"]],
                y=subset_024["pickup_count_024"],
                mode="lines+markers",
                name=label_024,
                line=dict(color=color_024, width=3, dash=dash_024, shape="spline"),
                marker=dict(size=6),
                hovertemplate=f"<b>{label_024}</b><br>Jam: %{{x}}<br>Rata-rata Pickup/Jam: %{{y:,.1f}}<extra></extra>",
            )
        )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=360,
        is_demo_024=is_demo_024,
        xaxis_title_024="Jam dalam Sehari",
        yaxis_title_024="Rata-rata Jumlah Pickup / Jam",
    )


def build_day_of_week_chart_024(
    hourly_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik rata-rata pickup per hari kalender (Senin–Minggu) ternormalisasi."""
    daily_024 = (
        hourly_df_024.groupby(["pickup_date_024", "day_name_024", "is_weekend_024"], as_index=False)[
            "pickup_count_024"
        ]
        .sum()
    )
    dow_agg_024 = (
        daily_024.groupby(["day_name_024", "is_weekend_024"], as_index=False)
        .agg(
            avg_daily_pickups_024=("pickup_count_024", "mean"),
            calendar_days_024=("pickup_date_024", "nunique"),
            total_pickups_024=("pickup_count_024", "sum"),
        )
    )
    dow_agg_024["day_order_024"] = dow_agg_024["day_name_024"].apply(
        lambda d_024: DAY_ORDER_ID_024.index(d_024) if d_024 in DAY_ORDER_ID_024 else 99
    )
    dow_agg_024 = dow_agg_024.sort_values("day_order_024")
    colors_024 = [
        PALETTE_024["blue_accent"] if bool(w_024) else PALETTE_024["navy_deep"]
        for w_024 in dow_agg_024["is_weekend_024"]
    ]
    max_val_024 = float(dow_agg_024["avg_daily_pickups_024"].max()) if not dow_agg_024.empty else 100.0

    fig_024 = go.Figure(
        go.Bar(
            x=dow_agg_024["day_name_024"],
            y=dow_agg_024["avg_daily_pickups_024"],
            text=[f"{v_024:,.0f}".replace(",", ".") for v_024 in dow_agg_024["avg_daily_pickups_024"]],
            textposition="outside",
            cliponaxis=False,
            marker=dict(color=colors_024, cornerradius=6),
            customdata=dow_agg_024[["calendar_days_024", "total_pickups_024"]].values,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Rata-rata/Hari: %{y:,.1f} pickup<br>"
                "Jumlah Hari Kalender: %{customdata[0]}<br>"
                "Total Pickup: %{customdata[1]:,.0f}<extra></extra>"
            ),
        )
    )
    fig_024 = apply_urbanpulse_layout_024(
        fig_024,
        height_024=350,
        is_demo_024=is_demo_024,
        xaxis_title_024="Hari dalam Seminggu (Navy = Hari Kerja, Biru = Akhir Pekan)",
        yaxis_title_024="Rata-rata Pickup / Hari",
    )
    fig_024.update_yaxes(range=[0, max_val_024 * 1.18])
    return fig_024


def build_time_period_comparison_chart_024(
    hourly_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membandingkan total pickup dan rata-rata pickup/jam antar 4 periode waktu dalam sehari."""
    work_024 = hourly_df_024.copy()
    if "time_of_day_bucket_024" not in work_024.columns:
        def _bucket_024(h_024: int) -> str:
            if h_024 < 6:
                return "Dini Hari (00-05)"
            if h_024 < 12:
                return "Pagi (06-11)"
            if h_024 < 18:
                return "Siang (12-17)"
            return "Malam (18-23)"
        work_024["time_of_day_bucket_024"] = work_024["pickup_hour_024"].astype(int).apply(_bucket_024)

    agg_spec_024: Dict[str, Any] = {
        "total_pickups_024": ("pickup_count_024", "sum"),
        "avg_hourly_024": ("pickup_count_024", "mean"),
    }
    has_rev_024 = "net_settled_revenue_usd_024" in work_024.columns
    if has_rev_024:
        agg_spec_024["total_rev_024"] = ("net_settled_revenue_usd_024", "sum")

    bucket_df_024 = work_024.groupby("time_of_day_bucket_024", as_index=False).agg(**agg_spec_024)
    bucket_df_024["bucket_order_024"] = bucket_df_024["time_of_day_bucket_024"].apply(
        lambda b_024: TIME_BUCKET_ORDER_024.index(b_024) if b_024 in TIME_BUCKET_ORDER_024 else 99
    )
    bucket_df_024 = bucket_df_024.sort_values("bucket_order_024")
    total_all_024 = float(bucket_df_024["total_pickups_024"].sum()) or 1.0
    bucket_df_024["share_pct_024"] = bucket_df_024["total_pickups_024"] / total_all_024 * 100.0
    max_y_024 = float(bucket_df_024["total_pickups_024"].max()) if not bucket_df_024.empty else 100.0

    bucket_colors_024 = [
        PALETTE_024["slate_chart"],
        PALETTE_024["blue_sky"],
        PALETTE_024["blue_primary"],
        PALETTE_024["navy_deep"],
    ][: len(bucket_df_024)]

    if has_rev_024:
        custom_024 = bucket_df_024[["share_pct_024", "avg_hourly_024", "total_rev_024"]].values
        hover_tmpl_024 = (
            "<b>%{x}</b><br>"
            "Total Pickup: %{y:,.0f} (%{customdata[0]:.1f}%)<br>"
            "Rata-rata/Jam: %{customdata[1]:,.1f}<br>"
            "Pendapatan Bersih: USD %{customdata[2]:,.2f}<extra></extra>"
        )
    else:
        custom_024 = bucket_df_024[["share_pct_024", "avg_hourly_024"]].values
        hover_tmpl_024 = (
            "<b>%{x}</b><br>"
            "Total Pickup: %{y:,.0f} (%{customdata[0]:.1f}%)<br>"
            "Rata-rata/Jam: %{customdata[1]:,.1f}<extra></extra>"
        )

    fig_024 = go.Figure(
        go.Bar(
            x=bucket_df_024["time_of_day_bucket_024"],
            y=bucket_df_024["total_pickups_024"],
            text=[f"{p_024:.1f}%" for p_024 in bucket_df_024["share_pct_024"]],
            textposition="outside",
            cliponaxis=False,
            marker=dict(color=bucket_colors_024, cornerradius=6),
            customdata=custom_024,
            hovertemplate=hover_tmpl_024,
        )
    )
    fig_024 = apply_urbanpulse_layout_024(
        fig_024,
        height_024=350,
        is_demo_024=is_demo_024,
        xaxis_title_024="Periode Waktu dalam Sehari",
        yaxis_title_024="Total Pickup",
    )
    fig_024.update_yaxes(range=[0, max_y_024 * 1.18])
    return fig_024


def build_day_hour_heatmap_024(
    hourly_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat heatmap intensitas permintaan untuk kombinasi Hari (Senin–Minggu) × Jam (00–23)."""
    pivot_024 = hourly_df_024.pivot_table(
        index="day_name_024",
        columns="pickup_hour_024",
        values="pickup_count_024",
        aggfunc="mean",
    )
    available_days_024 = [d_024 for d_024 in DAY_ORDER_ID_024 if d_024 in pivot_024.index]
    pivot_024 = pivot_024.reindex(available_days_024)
    col_labels_024 = [f"{int(h_024):02d}:00" for h_024 in pivot_024.columns]

    fig_024 = go.Figure(
        data=go.Heatmap(
            z=pivot_024.values,
            x=col_labels_024,
            y=list(pivot_024.index),
            colorscale=CHART_COLORSCALE_024,
            hovertemplate="<b>Hari:</b> %{y}<br><b>Jam:</b> %{x}<br><b>Rata-rata Pickup:</b> %{z:,.1f}<extra></extra>",
            colorbar=dict(
                title="Pickup/Jam",
                thickness=12,
                len=0.85,
                outlinewidth=0,
            ),
        )
    )
    fig_024.update_yaxes(autorange="reversed", showgrid=False)

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=370,
        is_demo_024=is_demo_024,
        xaxis_title_024="Jam dalam Sehari",
        yaxis_title_024="Hari",
    )


def build_zone_ranking_chart_024(
    zone_df_024: pd.DataFrame,
    top_n_024: int = 10,
    metric_col_024: str = "pickup_count_024",
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik batang horizontal peringkat zona pickup (berdasarkan PULocationID unik)."""
    agg_cols_024: Dict[str, Any] = {"pickup_count_024": "sum"}
    has_rev_024 = "net_settled_revenue_usd_024" in zone_df_024.columns
    if has_rev_024:
        agg_cols_024["net_settled_revenue_usd_024"] = "sum"
    if "service_zone" in zone_df_024.columns:
        agg_cols_024["service_zone"] = "first"

    ranked_024 = (
        zone_df_024.groupby(["PULocationID", "Zone", "Borough"], as_index=False)
        .agg(agg_cols_024)
        .sort_values(metric_col_024, ascending=False)
        .head(top_n_024)
        .sort_values(metric_col_024, ascending=True)
    )

    # Gunakan label unik jika ada nama zona ganda
    dup_names_024 = ranked_024["Zone"].duplicated(keep=False)
    ranked_024["zone_label_024"] = [
        f"{z_024} (ID {int(pid_024)})" if is_dup_024 else str(z_024)
        for z_024, pid_024, is_dup_024 in zip(
            ranked_024["Zone"], ranked_024["PULocationID"], dup_names_024
        )
    ]

    is_currency_024 = metric_col_024 == "net_settled_revenue_usd_024"
    x_title_024 = "Pendapatan Bersih (USD)" if is_currency_024 else "Total Perjalanan (Pickup)"

    if has_rev_024:
        custom_024 = ranked_024[["PULocationID", "Borough", "pickup_count_024", "net_settled_revenue_usd_024"]].values
        hover_tmpl_024 = (
            "<b>%{y}</b> (ID %{customdata[0]} • %{customdata[1]})<br>"
            "Total Pickup: %{customdata[2]:,.0f}<br>"
            "Pendapatan Bersih: USD %{customdata[3]:,.2f}<extra></extra>"
        )
    else:
        custom_024 = ranked_024[["PULocationID", "Borough"]].values
        hover_tmpl_024 = (
            "<b>%{y}</b> (ID %{customdata[0]} • %{customdata[1]})<br>"
            "Total Pickup: %{x:,.0f}<extra></extra>"
        )

    fig_024 = go.Figure(
        go.Bar(
            x=ranked_024[metric_col_024],
            y=ranked_024["zone_label_024"],
            orientation="h",
            marker=dict(
                color=ranked_024[metric_col_024],
                colorscale=CHART_COLORSCALE_024,
                cornerradius=5,
            ),
            customdata=custom_024,
            hovertemplate=hover_tmpl_024,
        )
    )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=max(360, top_n_024 * 34),
        is_demo_024=is_demo_024,
        xaxis_title_024=x_title_024,
        yaxis_title_024="",
    )


def build_zone_borough_comparison_chart_024(
    zone_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik perbandingan permintaan dan pendapatan antar-Borough NYC."""
    agg_spec_024: Dict[str, Any] = {
        "pickup_count_024": ("pickup_count_024", "sum"),
        "zone_count_024": ("PULocationID", "nunique"),
    }
    has_rev_024 = "net_settled_revenue_usd_024" in zone_df_024.columns
    if has_rev_024:
        agg_spec_024["net_settled_revenue_usd_024"] = ("net_settled_revenue_usd_024", "sum")

    borough_df_024 = (
        zone_df_024.groupby("Borough", as_index=False)
        .agg(**agg_spec_024)
        .sort_values("pickup_count_024", ascending=False)
    )
    total_024 = float(borough_df_024["pickup_count_024"].sum()) or 1.0
    borough_df_024["share_pct_024"] = (borough_df_024["pickup_count_024"] / total_024) * 100.0
    max_borough_024 = float(borough_df_024["pickup_count_024"].max()) if not borough_df_024.empty else 100.0

    colors_024 = [
        BOROUGH_COLOR_MAP_024.get(str(b_024), PALETTE_024["navy_mid"])
        for b_024 in borough_df_024["Borough"]
    ]

    if has_rev_024:
        custom_024 = borough_df_024[["share_pct_024", "zone_count_024", "net_settled_revenue_usd_024"]].values
        hover_tmpl_024 = (
            "<b>%{x}</b><br>"
            "Total Pickup: %{y:,.0f} (%{customdata[0]:.2f}%)<br>"
            "Jumlah Zona Aktif: %{customdata[1]}<br>"
            "Pendapatan Bersih: USD %{customdata[2]:,.2f}<extra></extra>"
        )
    else:
        custom_024 = borough_df_024[["share_pct_024", "zone_count_024"]].values
        hover_tmpl_024 = (
            "<b>%{x}</b><br>"
            "Total Pickup: %{y:,.0f} (%{customdata[0]:.2f}%)<br>"
            "Jumlah Zona Aktif: %{customdata[1]}<extra></extra>"
        )

    fig_024 = go.Figure(
        go.Bar(
            x=borough_df_024["Borough"],
            y=borough_df_024["pickup_count_024"],
            text=[
                f"{pct_024:.1f}%" if pct_024 >= 0.1 else "<0.1%"
                for pct_024 in borough_df_024["share_pct_024"]
            ],
            textposition="outside",
            cliponaxis=False,
            marker=dict(color=colors_024, cornerradius=6),
            customdata=custom_024,
            hovertemplate=hover_tmpl_024,
        )
    )

    fig_024 = apply_urbanpulse_layout_024(
        fig_024,
        height_024=360,
        is_demo_024=is_demo_024,
        xaxis_title_024="Wilayah (Borough)",
        yaxis_title_024="Total Pickup",
    )
    fig_024.update_yaxes(range=[0, max_borough_024 * 1.18])
    return fig_024


def build_zone_revenue_vs_demand_chart_024(
    zone_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat scatter chart Volume Pickup vs Pendapatan Bersih per Zona (menyoroti JFK vs Manhattan)."""
    plot_df_024 = zone_df_024.copy()
    if "avg_settled_total_usd_024" not in plot_df_024.columns:
        plot_df_024["avg_settled_total_usd_024"] = (
            plot_df_024["net_settled_revenue_usd_024"] / plot_df_024["pickup_count_024"].clip(lower=1)
        )
    plot_df_024["marker_size_024"] = plot_df_024["avg_settled_total_usd_024"].clip(lower=15, upper=90)

    fig_024 = px.scatter(
        plot_df_024,
        x="pickup_count_024",
        y="net_settled_revenue_usd_024",
        color="Borough",
        size="marker_size_024",
        size_max=22,
        color_discrete_map=BOROUGH_COLOR_MAP_024,
        hover_name="Zone",
        custom_data=["PULocationID", "Borough", "service_zone", "avg_settled_total_usd_024"],
        labels={
            "pickup_count_024": "Total Pickup",
            "net_settled_revenue_usd_024": "Pendapatan Bersih (USD)",
        },
    )
    fig_024.update_traces(
        marker=dict(opacity=0.82, line=dict(width=1, color="#FFFFFF")),
        hovertemplate=(
            "<b>%{hovertext}</b> (ID %{customdata[0]})<br>"
            "Borough: %{customdata[1]} • %{customdata[2]}<br>"
            "Total Pickup: %{x:,.0f}<br>"
            "Pendapatan Bersih: USD %{y:,.2f}<br>"
            "Rata-rata Total/Trip: USD %{customdata[3]:,.2f}<extra></extra>"
        ),
    )

    # Anotasi zona teratas & JFK Airport agar insight langsung terbaca
    top_vol_024 = plot_df_024.sort_values("pickup_count_024", ascending=False).head(1)
    top_rev_024 = plot_df_024.sort_values("net_settled_revenue_usd_024", ascending=False).head(1)
    for _, row_024 in pd.concat([top_vol_024, top_rev_024]).drop_duplicates("PULocationID").iterrows():
        fig_024.add_annotation(
            x=row_024["pickup_count_024"],
            y=row_024["net_settled_revenue_usd_024"],
            text=str(row_024["Zone"]),
            showarrow=True,
            arrowhead=2,
            ax=-25,
            ay=-24,
            font=dict(size=10, color=PALETTE_024["navy_deep"]),
            bgcolor="rgba(255,255,255,0.88)",
            bordercolor=PALETTE_024["border_subtle"],
        )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=390,
        is_demo_024=is_demo_024,
        xaxis_title_024="Total Pickup (Volume)",
        yaxis_title_024="Pendapatan Bersih (USD)",
    )


def build_zone_hourly_comparison_chart_024(
    zone_hourly_df_024: pd.DataFrame,
    selected_ids_024: List[int],
    is_demo_024: bool = False,
) -> go.Figure:
    """Membandingkan kurva profil permintaan 24 jam antar beberapa zona terpilih (berbasis PULocationID)."""
    subset_024 = zone_hourly_df_024[zone_hourly_df_024["PULocationID"].isin(selected_ids_024)].copy()
    palette_cycle_024 = [
        PALETTE_024["navy_deep"],
        PALETTE_024["blue_primary"],
        PALETTE_024["teal_positive"],
        PALETTE_024["amber_demo"],
        PALETTE_024["rose_alert"],
        PALETTE_024["blue_sky"],
    ]

    fig_024 = go.Figure()
    for idx_024, pid_024 in enumerate(selected_ids_024):
        z_sub_024 = subset_024[subset_024["PULocationID"] == pid_024].sort_values("pickup_hour_024")
        if z_sub_024.empty:
            continue
        z_name_024 = str(z_sub_024["Zone"].iloc[0])
        z_boro_024 = str(z_sub_024["Borough"].iloc[0])
        y_col_024 = (
            "avg_daily_pickups_at_hour_024"
            if "avg_daily_pickups_at_hour_024" in z_sub_024.columns
            else "pickup_count_024"
        )
        fig_024.add_trace(
            go.Scatter(
                x=[f"{int(h_024):02d}:00" for h_024 in z_sub_024["pickup_hour_024"]],
                y=z_sub_024[y_col_024],
                mode="lines+markers",
                name=f"{z_name_024} (ID {pid_024})",
                line=dict(color=palette_cycle_024[idx_024 % len(palette_cycle_024)], width=2.6, shape="spline"),
                marker=dict(size=5.5),
                hovertemplate=(
                    f"<b>{z_name_024}</b> (ID {pid_024} • {z_boro_024})<br>"
                    "Jam: %{x}<br>Rata-rata Pickup/Hari: %{y:,.1f}<extra></extra>"
                ),
            )
        )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=360,
        is_demo_024=is_demo_024,
        xaxis_title_024="Jam dalam Sehari",
        yaxis_title_024="Rata-rata Pickup / Hari pada Jam Tersebut",
    )


def build_nyc_zone_map_024(
    zone_df_024: pd.DataFrame,
    geojson_024: Optional[Dict[str, Any]] = None,
    is_demo_024: bool = False,
) -> Optional[go.Figure]:
    """Membuat peta interaktif persebaran zona NYC apabila GeoJSON atau koordinat tersedia."""
    agg_zone_024 = (
        zone_df_024.groupby(["PULocationID", "Zone", "Borough"], as_index=False)
        .agg(
            pickup_count_024=("pickup_count_024", "sum"),
            latitude_024=("latitude_024", "first") if "latitude_024" in zone_df_024.columns else ("PULocationID", lambda _: None),
            longitude_024=("longitude_024", "first") if "longitude_024" in zone_df_024.columns else ("PULocationID", lambda _: None),
        )
    )

    if geojson_024 is not None:
        choropleth_fn_024 = getattr(px, "choropleth_map", None) or px.choropleth_mapbox
        map_kwargs_024: Dict[str, Any] = {
            "data_frame": agg_zone_024,
            "geojson": geojson_024,
            "locations": "PULocationID",
            "featureidkey": "properties.LocationID",
            "color": "pickup_count_024",
            "hover_name": "Zone",
            "hover_data": {"Borough": True, "pickup_count_024": ":,.0f", "PULocationID": True},
            "color_continuous_scale": CHART_COLORSCALE_024,
            "center": {"lat": 40.7306, "lon": -73.9352},
            "zoom": 9.8,
            "opacity": 0.78,
            "labels": {"pickup_count_024": "Total Pickup"},
        }
        if choropleth_fn_024 is px.choropleth_mapbox:
            map_kwargs_024["mapbox_style"] = "carto-positron"
        else:
            map_kwargs_024["map_style"] = "carto-positron"
        fig_024 = choropleth_fn_024(**map_kwargs_024)
        fig_024.update_layout(margin=dict(l=0, r=0, t=28 if is_demo_024 else 8, b=0), height=420)
        return fig_024

    valid_geo_024 = agg_zone_024.dropna(subset=["latitude_024", "longitude_024"])
    if valid_geo_024.empty:
        return None

    scatter_fn_024 = getattr(px, "scatter_map", None) or px.scatter_mapbox
    scatter_kwargs_024: Dict[str, Any] = {
        "data_frame": valid_geo_024,
        "lat": "latitude_024",
        "lon": "longitude_024",
        "size": "pickup_count_024",
        "color": "pickup_count_024",
        "hover_name": "Zone",
        "hover_data": {
            "Borough": True,
            "pickup_count_024": ":,.0f",
            "latitude_024": False,
            "longitude_024": False,
        },
        "color_continuous_scale": CHART_COLORSCALE_024,
        "size_max": 28,
        "zoom": 10.2,
        "center": {"lat": 40.7350, "lon": -73.9350},
        "labels": {"pickup_count_024": "Total Pickup"},
    }
    if scatter_fn_024 is px.scatter_mapbox:
        scatter_kwargs_024["mapbox_style"] = "carto-positron"
    else:
        scatter_kwargs_024["map_style"] = "carto-positron"

    fig_024 = scatter_fn_024(**scatter_kwargs_024)
    fig_024.update_layout(
        height=420,
        margin=dict(l=0, r=0, t=8, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Segoe UI Variable, Segoe UI, sans-serif", size=12),
    )
    return fig_024


def build_prediction_chart_024(
    pred_df_024: pd.DataFrame,
    multi_day_024: bool = False,
    show_naive_024: bool = True,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik Aktual vs Prediksi Ridge Regression (+ pita validasi & baseline Naive opsional)."""
    if multi_day_024 and "pickup_ts_hour_024" in pred_df_024.columns:
        ordered_024 = pred_df_024.sort_values("pickup_ts_hour_024")
        x_vals_024 = ordered_024["pickup_ts_hour_024"].dt.strftime("%d Sep %H:00").tolist()
        x_title_024 = "Waktu Test Holdout (24–30 Sep 2026)"
    else:
        ordered_024 = pred_df_024.sort_values(["prediction_date_024", "pickup_hour_024"])
        x_vals_024 = [f"{int(h_024):02d}:00" for h_024 in ordered_024["pickup_hour_024"]]
        x_title_024 = "Jam (Test Holdout)"

    fig_024 = go.Figure()

    # Pita ketidakpastian berbasis Validation RMSE (bebas leakage Test)
    if "upper_bound_024" in ordered_024.columns and "lower_bound_024" in ordered_024.columns:
        fig_024.add_trace(
            go.Scatter(
                x=x_vals_024 + x_vals_024[::-1],
                y=list(ordered_024["upper_bound_024"]) + list(ordered_024["lower_bound_024"])[::-1],
                fill="toself",
                fillcolor="rgba(59, 130, 246, 0.13)",
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=True,
                name="Interval Validasi (±1,28 × Val RMSE)",
            )
        )

    # Garis Nilai Aktual
    if "actual_demand_024" in ordered_024.columns and ordered_024["actual_demand_024"].notna().any():
        fig_024.add_trace(
            go.Scatter(
                x=x_vals_024,
                y=ordered_024["actual_demand_024"],
                mode="lines+markers" if not multi_day_024 else "lines",
                name="Pickup Aktual (Test Holdout)",
                line=dict(color=PALETTE_024["navy_deep"], width=2.8),
                marker=dict(size=6, symbol="diamond", color=PALETTE_024["navy_deep"]),
                hovertemplate="<b>%{x}</b><br>Aktual Pickup: %{y:,.0f}<extra></extra>",
            )
        )

    # Garis Prediksi Ridge Regression
    fig_024.add_trace(
        go.Scatter(
            x=x_vals_024,
            y=ordered_024["predicted_demand_024"],
            mode="lines+markers" if not multi_day_024 else "lines",
            name="Prediksi Ridge (ridge_reg_0p1_024)",
            line=dict(color=PALETTE_024["blue_primary"], width=2.6, dash="solid"),
            marker=dict(size=6, color=PALETTE_024["blue_primary"]),
            hovertemplate="<b>%{x}</b><br>Prediksi Ridge: %{y:,.1f}<extra></extra>",
        )
    )

    # Garis Baseline Naive lag_168h (opsional)
    if show_naive_024 and "naive_lag_168h_pred_024" in ordered_024.columns:
        fig_024.add_trace(
            go.Scatter(
                x=x_vals_024,
                y=ordered_024["naive_lag_168h_pred_024"],
                mode="lines",
                name="Baseline Naive (lag_168h)",
                line=dict(color=PALETTE_024["slate_chart"], width=1.8, dash="dot"),
                hovertemplate="<b>%{x}</b><br>Baseline Naive: %{y:,.0f}<extra></extra>",
            )
        )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=395,
        is_demo_024=is_demo_024,
        xaxis_title_024=x_title_024,
        yaxis_title_024="Jumlah Pickup / Jam",
    )


def build_actual_vs_predicted_scatter_024(
    pred_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat scatter chart Aktual vs Prediksi per hari/slot pada Test Holdout."""
    sample_df_024 = pred_df_024.dropna(subset=["actual_demand_024", "predicted_demand_024"])
    if len(sample_df_024) > 2500:
        sample_df_024 = sample_df_024.sample(n=2500, random_state=24)

    max_val_024 = float(
        max(
            sample_df_024["actual_demand_024"].max(),
            sample_df_024["predicted_demand_024"].max(),
        )
    ) if not sample_df_024.empty else 100.0

    fig_024 = go.Figure()
    fig_024.add_trace(
        go.Scattergl(
            x=sample_df_024["actual_demand_024"],
            y=sample_df_024["predicted_demand_024"],
            mode="markers",
            name="Slot Zona × Jam",
            marker=dict(color="rgba(37, 99, 235, 0.45)", size=5.5),
            customdata=sample_df_024[["Zone", "prediction_date_024", "pickup_hour_024"]].values,
            hovertemplate=(
                "<b>%{customdata[0]}</b> (%{customdata[1]} %{customdata[2]:02d}:00)<br>"
                "Aktual: %{x:,.0f} • Prediksi: %{y:,.1f}<extra></extra>"
            ),
        )
    )
    fig_024.add_trace(
        go.Scatter(
            x=[0, max_val_024],
            y=[0, max_val_024],
            mode="lines",
            name="Garis Ideal (y = x)",
            line=dict(color=PALETTE_024["navy_deep"], width=1.8, dash="dash"),
            hoverinfo="skip",
        )
    )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=350,
        is_demo_024=is_demo_024,
        xaxis_title_024="Pickup Aktual (Test Holdout)",
        yaxis_title_024="Pickup Prediksi (Ridge Regression)",
    )


def build_feature_importance_chart_024(
    coef_list_024: List[Dict[str, Any]],
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik koefisien terstandardisasi fitur numerik model Ridge Regression."""
    df_coef_024 = pd.DataFrame(coef_list_024).head(10).copy()
    df_coef_024["abs_coef_024"] = df_coef_024["standardized_coef_024"].abs()
    df_coef_024 = df_coef_024.sort_values("abs_coef_024", ascending=True)
    colors_024 = [
        PALETTE_024["blue_primary"] if float(v_024) >= 0 else PALETTE_024["rose_alert"]
        for v_024 in df_coef_024["standardized_coef_024"]
    ]

    fig_024 = go.Figure(
        go.Bar(
            x=df_coef_024["standardized_coef_024"],
            y=df_coef_024["feature_024"],
            orientation="h",
            marker=dict(color=colors_024, cornerradius=4),
            hovertemplate="<b>%{y}</b><br>Koefisien Terstandardisasi: %{x:+.3f}<extra></extra>",
        )
    )

    return apply_urbanpulse_layout_024(
        fig_024,
        height_024=340,
        is_demo_024=is_demo_024,
        xaxis_title_024="Koefisien Terstandardisasi (Biru = Positif, Merah = Koreksi Negatif)",
        yaxis_title_024="",
    )


def build_surge_zones_chart_024(
    rec_df_024: pd.DataFrame,
    is_demo_024: bool = False,
) -> go.Figure:
    """Membuat grafik ringkasan zona berisiko error tinggi atau lonjakan permintaan (DEMO/Aktual)."""
    if "predicted_surge_pct_024" in rec_df_024.columns:
        ordered_024 = rec_df_024.sort_values("predicted_surge_pct_024", ascending=True)
        max_val_024 = float(ordered_024["predicted_surge_pct_024"].max()) if not ordered_024.empty else 40.0
        fig_024 = go.Figure(
            go.Bar(
                x=ordered_024["predicted_surge_pct_024"],
                y=ordered_024["Zone"],
                orientation="h",
                text=[f"+{v_024:.1f}%" for v_024 in ordered_024["predicted_surge_pct_024"]],
                textposition="outside",
                cliponaxis=False,
                marker=dict(color=PALETTE_024["blue_primary"], cornerradius=6),
                hovertemplate="<b>%{y}</b><br>Estimasi Peningkatan: +%{x:.1f}%<extra></extra>",
            )
        )
        fig_024 = apply_urbanpulse_layout_024(
            fig_024,
            height_024=320,
            is_demo_024=is_demo_024,
            xaxis_title_024="Estimasi Peningkatan Permintaan (%)",
            yaxis_title_024="",
        )
        fig_024.update_xaxes(range=[0, max_val_024 * 1.24])
        return fig_024

    # Mode aktual: grafik WAPE tertinggi pada zona operasional utama (≥10rb pickup Test)
    ordered_024 = rec_df_024.sort_values("wape_pct_024", ascending=True)
    max_val_024 = float(ordered_024["wape_pct_024"].max()) if not ordered_024.empty else 30.0
    colors_024 = [
        PALETTE_024["rose_alert"] if float(v_024) >= 19.0 else PALETTE_024["blue_primary"]
        for v_024 in ordered_024["wape_pct_024"]
    ]
    fig_024 = go.Figure(
        go.Bar(
            x=ordered_024["wape_pct_024"],
            y=ordered_024["Zone"],
            orientation="h",
            text=[f"{v_024:.1f}%" for v_024 in ordered_024["wape_pct_024"]],
            textposition="outside",
            cliponaxis=False,
            marker=dict(color=colors_024, cornerradius=5),
            customdata=ordered_024[["Borough", "actual_sum_024", "mae_024"]].values,
            hovertemplate=(
                "<b>%{y}</b> (%{customdata[0]})<br>"
                "WAPE Test Holdout: %{x:.2f}%<br>"
                "Pickup Aktual Test: %{customdata[1]:,.0f}<br>"
                "MAE: %{customdata[2]:.2f} pickup/jam<extra></extra>"
            ),
        )
    )
    fig_024 = apply_urbanpulse_layout_024(
        fig_024,
        height_024=340,
        is_demo_024=is_demo_024,
        xaxis_title_024="WAPE Test Holdout (%) — Zona Utama (≥10.000 Pickup/Minggu)",
        yaxis_title_024="",
    )
    fig_024.update_xaxes(range=[0, max_val_024 * 1.22])
    return fig_024
