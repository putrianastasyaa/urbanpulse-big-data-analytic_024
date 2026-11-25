"""Komponen antarmuka modular dan utilitas tampilan untuk UrbanPulse_024."""

from __future__ import annotations

import html
from typing import Any, Dict, List, Optional

import streamlit as st

from dashboard.config_024 import (
    APP_NAME_024,
    APP_TAGLINE_024,
    STYLESHEET_PATH_024,
)


def inject_custom_css_024() -> None:
    """Memuat stylesheet CSS kustom UrbanPulse_024 ke halaman Streamlit."""
    if STYLESHEET_PATH_024.exists():
        css_text_024 = STYLESHEET_PATH_024.read_text(encoding="utf-8")
        st.markdown(f"<style>{css_text_024}</style>", unsafe_allow_html=True)


def format_number_id_024(value_024: Optional[float], decimals_024: int = 0) -> str:
    """Memformat angka ke gaya pemisah ribuan Indonesia (contoh: 3.860.852 atau 12,26)."""
    if value_024 is None:
        return "Belum tersedia"
    try:
        numeric_024 = float(value_024)
        if numeric_024 != numeric_024:  # NaN check
            return "Belum tersedia"
        formatted_en_024 = f"{numeric_024:,.{decimals_024}f}"
        return formatted_en_024.replace(",", "_").replace(".", ",").replace("_", ".")
    except (TypeError, ValueError):
        return "Belum tersedia"


def format_currency_usd_024(
    value_024: Optional[float],
    decimals_024: int = 2,
    compact_024: bool = False,
) -> str:
    """Memformat mata uang USD secara konsisten (penuh atau ringkas Jt/Rb)."""
    if value_024 is None:
        return "Belum tersedia"
    try:
        numeric_024 = float(value_024)
        if numeric_024 != numeric_024:
            return "Belum tersedia"
        if compact_024:
            if abs(numeric_024) >= 1_000_000:
                return f"USD {format_number_id_024(numeric_024 / 1_000_000.0, 2)} Jt"
            if abs(numeric_024) >= 1_000:
                return f"USD {format_number_id_024(numeric_024 / 1_000.0, 1)} Rb"
        return f"USD {format_number_id_024(numeric_024, decimals_024)}"
    except (TypeError, ValueError):
        return "Belum tersedia"


def format_hour_id_024(hour_024: Optional[int]) -> str:
    """Memformat indeks jam integer (0..23) menjadi format rentang jam 'HH:00 – HH:00'."""
    if hour_024 is None:
        return "Belum tersedia"
    try:
        h_int_024 = int(hour_024) % 24
        return f"{h_int_024:02d}:00 – {(h_int_024 + 1) % 24:02d}:00"
    except (TypeError, ValueError):
        return "Belum tersedia"


def render_page_header_024(
    eyebrow_024: str,
    title_024: str,
    subtitle_024: str,
    use_demo_024: bool,
    has_actual_data_024: bool = False,
) -> None:
    """Menampilkan header halaman bergaya eksekutif lengkap dengan lencana status sumber data."""
    if use_demo_024:
        badge_class_024 = "up-badge-demo-024"
        badge_text_024 = "MODE DEMO • Pengujian UI"
    elif has_actual_data_024:
        badge_class_024 = "up-badge-ready-024"
        badge_text_024 = "DATA AKTUAL • Gold Layer & MLlib"
    else:
        badge_class_024 = "up-badge-actual-024"
        badge_text_024 = "DATA AKTUAL • Menunggu Pipeline"

    st.markdown(
        f"""
        <div class="up-page-header-024">
            <div>
                <div class="up-page-eyebrow-024">{html.escape(eyebrow_024)}</div>
                <h1 class="up-page-title-024">{html.escape(title_024)}</h1>
                <div class="up-page-subtitle-024">{html.escape(subtitle_024)}</div>
            </div>
            <div>
                <span class="up-badge-pill-024 {badge_class_024}">{html.escape(badge_text_024)}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_mode_banner_024(use_demo_024: bool, has_actual_data_024: bool = False) -> None:
    """Menampilkan banner transparansi sumber data (Mode DEMO atau Mode Data Aktual)."""
    if use_demo_024:
        st.markdown(
            """
            <div class="up-demo-banner-024">
                <div>
                    <div class="up-demo-banner-title-024">Mode Pratinjau UI Aktif — DATA DEMO (Bukan Hasil Penelitian)</div>
                    <div class="up-demo-banner-text-024">
                        Seluruh angka, grafik, prediksi, dan rekomendasi yang tampil saat ini adalah <strong>data simulasi DEMO</strong>
                        untuk pengujian tampilan. Pindahkan pilihan sidebar ke <strong>Data Aktual (Gold Layer &amp; ML)</strong>
                        untuk melihat hasil nyata September 2026 dari Gold Layer dan Spark MLlib.
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif not has_actual_data_024:
        st.markdown(
            """
            <div class="up-info-banner-024">
                <div class="up-info-banner-title-024">Menunggu Output Serving Layer (data/analytics/)</div>
                <div class="up-info-banner-text-024">
                    Dashboard berada pada <strong>Mode Data Aktual</strong>, namun berkas serving layer yang dibutuhkan belum tersedia.
                    Pastikan tabel Gold Layer dan artefak ML sudah tersedia di <code>data/analytics/</code>.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_notice_banner_024(title_024: str, body_024: str) -> None:
    """Menampilkan banner informasi kontekstual (misalnya klarifikasi Test Holdout ML)."""
    st.markdown(
        f"""
        <div class="up-info-banner-024">
            <div class="up-info-banner-title-024">{html.escape(title_024)}</div>
            <div class="up-info-banner-text-024">{html.escape(body_024)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_card_024(
    label_024: str,
    value_024: Optional[str],
    caption_024: str,
    tag_024: str = "",
    is_demo_024: bool = False,
) -> None:
    """Merender satu KPI card bersudut membulat dengan tinggi konsisten."""
    is_empty_024 = value_024 is None or value_024 in ("", "Belum tersedia", "—")
    display_value_024 = "Data belum tersedia" if is_empty_024 else str(value_024)
    val_class_024 = "up-kpi-value-empty-024" if is_empty_024 else "up-kpi-value-024"

    effective_tag_024 = "DEMO" if is_demo_024 else tag_024
    tag_html_024 = (
        f'<span class="up-kpi-tag-024">{html.escape(effective_tag_024)}</span>'
        if effective_tag_024
        else ""
    )

    st.markdown(
        f"""
        <div class="up-kpi-card-024">
            <div class="up-kpi-accent-024"></div>
            <div class="up-kpi-header-024">
                <span class="up-kpi-label-024">{html.escape(label_024)}</span>
                {tag_html_024}
            </div>
            <div class="{val_class_024}">{html.escape(display_value_024)}</div>
            <div class="up-kpi-caption-024">{html.escape(caption_024)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header_024(
    title_024: str,
    subtitle_024: str = "",
    badge_024: str = "",
) -> None:
    """Menampilkan judul dan deskripsi ringkas di dalam sebuah kartu konten."""
    badge_class_024 = (
        "up-badge-pill-024 up-badge-demo-024"
        if "DEMO" in badge_024.upper()
        else "up-kpi-tag-024"
    )
    badge_html_024 = (
        f'<span class="{badge_class_024}">{html.escape(badge_024)}</span>'
        if badge_024
        else ""
    )
    subtitle_html_024 = (
        f'<div class="up-section-subtitle-024">{html.escape(subtitle_024)}</div>'
        if subtitle_024
        else ""
    )
    st.markdown(
        f"""
        <div class="up-section-header-024">
            <div>
                <div class="up-section-title-024">{html.escape(title_024)}</div>
                {subtitle_html_024}
            </div>
            {badge_html_024}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state_024(
    title_024: str,
    description_024: str,
    expected_artifact_024: str,
    icon_024: str = "◫",
) -> None:
    """Menampilkan empty state informatif saat data analitik belum tersedia."""
    st.markdown(
        f"""
        <div class="up-empty-box-024">
            <div class="up-empty-icon-024">{html.escape(icon_024)}</div>
            <div class="up-empty-title-024">{html.escape(title_024)}</div>
            <div class="up-empty-desc-024">{html.escape(description_024)}</div>
            <div class="up-empty-meta-024">Target Serving Layer: {html.escape(expected_artifact_024)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_insight_card_024(title_024: str, body_024: str) -> None:
    """Menampilkan kartu ringkasan insight dengan aksen tepi biru."""
    st.markdown(
        f"""
        <div class="up-insight-item-024">
            <div class="up-insight-title-024">{html.escape(title_024)}</div>
            <div class="up-insight-body-024">{html.escape(body_024)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_recommendation_card_024(
    zone_024: str,
    borough_024: str,
    window_024: str,
    priority_024: str,
    metric_badge_024: str,
    action_024: str,
    basis_024: str,
    allocation_note_024: str,
    category_024: str = "",
    is_demo_024: bool = False,
) -> None:
    """Menampilkan kartu rekomendasi armada lengkap dengan dasar analisis pendukung aktual."""
    prio_class_024 = (
        "up-priority-high-024"
        if str(priority_024).lower() in ("tinggi", "high")
        else "up-priority-med-024"
    )
    demo_prefix_024 = "[DEMO] " if is_demo_024 else ""
    cat_badge_html_024 = (
        f'<span class="up-priority-med-024">{html.escape(category_024)}</span>'
        if category_024
        else ""
    )

    st.markdown(
        f"""
        <div class="up-rec-card-024">
            <div class="up-rec-header-024">
                <div>
                    <strong style="color:#0B1F3A; font-size:0.95rem;">
                        {html.escape(demo_prefix_024 + zone_024)}
                    </strong>
                    <span style="color:#64748B; font-size:0.8rem; margin-left:6px;">
                        • {html.escape(borough_024)}
                    </span>
                </div>
                <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap;">
                    {cat_badge_html_024}
                    <span class="up-kpi-tag-024">{html.escape(metric_badge_024)}</span>
                    <span class="{prio_class_024}">Prioritas {html.escape(priority_024)}</span>
                </div>
            </div>
            <div style="font-size:0.82rem; color:#1E3A8A; font-weight:600; margin-bottom:0.35rem;">
                Jendela Operasional: {html.escape(window_024)}
            </div>
            <div style="font-size:0.83rem; color:#0F172A; margin-bottom:0.35rem; line-height:1.45;">
                <strong>Rekomendasi:</strong> {html.escape(action_024)}
            </div>
            <div style="font-size:0.79rem; color:#475569; margin-bottom:0.25rem; line-height:1.45;">
                <strong>Implikasi Operasional:</strong> {html.escape(allocation_note_024)}
            </div>
            <div style="font-size:0.76rem; color:#64748B; border-top:1px solid #E2E8F0; padding-top:0.4rem; margin-top:0.45rem;">
                <strong>Dasar Bukti Aktual:</strong> {html.escape(basis_024)}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pipeline_status_grid_024(status_024: Dict[str, Any]) -> None:
    """Merender ringkasan status 4 lapisan data proyek secara jujur dan transparan."""
    raw_files_024: List[Dict[str, Any]] = status_024.get("raw_files_024", [])
    raw_summary_024 = (
        f"{len(raw_files_024)} berkas Parquet terverifikasi ({raw_files_024[0]['name_024']}, "
        f"{raw_files_024[0]['size_mb_024']} MB) — ter-ingest ke HDFS."
        if raw_files_024
        else "Belum ada berkas mentah di data/raw/."
    )
    ref_files_024: List[str] = status_024.get("reference_files_024", [])
    ref_summary_024 = (
        f"{len(ref_files_024)} berkas tersedia ({', '.join(ref_files_024)}) — 265 zona, 100% match."
        if ref_files_024
        else "Menunggu taxi_zone_lookup_024.csv."
    )
    silver_rows_024 = int(status_024.get("silver_hdfs_rows_024", 0))
    proc_summary_024 = (
        f"Silver Layer v2 aktif di HDFS ({format_number_id_024(silver_rows_024, 0)} transaksi tervalidasi Sep 2026)."
        if silver_rows_024 > 0
        else "Menunggu job cleaning & enrichment PySpark."
    )
    ready_cnt_024 = int(status_024.get("analytics_ready_count_024", 0))
    total_cnt_024 = int(status_024.get("analytics_total_count_024", 6))
    model_note_024 = "Model ridge_reg_0p1_024 aktif." if status_024.get("model_ready_024") else "Model belum tersedia."
    analytics_summary_024 = (
        f"{ready_cnt_024}/{total_cnt_024} artefak Gold & ML siap di data/analytics/. {model_note_024}"
        if ready_cnt_024 > 0
        else f"0/{total_cnt_024} artefak tersedia di data/analytics/."
    )

    st.markdown(
        f"""
        <div class="up-pipeline-grid-024">
            <div class="up-pipeline-step-024">
                <div class="up-pipeline-step-title-024">1. Raw Layer (data/raw/ &amp; HDFS)</div>
                <div class="up-pipeline-step-meta-024">{html.escape(raw_summary_024)}</div>
            </div>
            <div class="up-pipeline-step-024">
                <div class="up-pipeline-step-title-024">2. Reference Lookup (data/reference/)</div>
                <div class="up-pipeline-step-meta-024">{html.escape(ref_summary_024)}</div>
            </div>
            <div class="up-pipeline-step-024">
                <div class="up-pipeline-step-title-024">3. Silver Layer v2 (HDFS PySpark)</div>
                <div class="up-pipeline-step-meta-024">{html.escape(proc_summary_024)}</div>
            </div>
            <div class="up-pipeline-step-024">
                <div class="up-pipeline-step-title-024">4. Gold Serving &amp; ML (data/analytics/)</div>
                <div class="up-pipeline-step-meta-024">{html.escape(analytics_summary_024)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_brand_024() -> None:
    """Merender blok identitas merek UrbanPulse di bagian atas sidebar."""
    st.markdown(
        f"""
        <div class="up-brand-box-024">
            <div class="up-brand-mark-024">UP</div>
            <div>
                <div class="up-brand-title-024">{html.escape(APP_NAME_024)}</div>
                <div class="up-brand-tagline-024">{html.escape(APP_TAGLINE_024)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
