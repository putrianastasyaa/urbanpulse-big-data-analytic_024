"""Titik masuk utama aplikasi Streamlit UrbanPulse_024.

Menjalankan:
    streamlit run dashboard/app_024.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# Pastikan root proyek berada di sys.path agar import modul dashboard.* selalu stabil
PROJECT_ROOT_PATH_024 = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT_PATH_024) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_PATH_024))

from dashboard.components_024 import (
    inject_custom_css_024,
    render_sidebar_brand_024,
)
from dashboard.config_024 import (
    APP_NAME_024,
    APP_TAGLINE_024,
    APP_VERSION_024,
    NAV_PAGES_024,
)
from dashboard.data_loader_024 import get_pipeline_status_024
from dashboard.pages.demand_analysis_024 import render_demand_analysis_page_024
from dashboard.pages.demand_prediction_024 import render_demand_prediction_page_024
from dashboard.pages.overview_024 import render_overview_page_024
from dashboard.pages.recommendations_024 import render_recommendations_page_024
from dashboard.pages.zone_analysis_024 import render_zone_analysis_page_024


class UrbanPulseDashboard_024:
    """Orkestrator utama antarmuka multi-halaman UrbanPulse — Intelligent Urban Taxi Analytics."""

    def __init__(self) -> None:
        self.page_handlers_024 = {
            "overview": render_overview_page_024,
            "demand_analysis": render_demand_analysis_page_024,
            "zone_analysis": render_zone_analysis_page_024,
            "demand_prediction": render_demand_prediction_page_024,
            "recommendations": render_recommendations_page_024,
        }

    def configure_app_024(self) -> None:
        """Mengatur konfigurasi halaman Streamlit dan memuat stylesheet kustom."""
        st.set_page_config(
            page_title=f"{APP_NAME_024} — {APP_TAGLINE_024}",
            page_icon="🚕",
            layout="wide",
            initial_sidebar_state="expanded",
        )
        inject_custom_css_024()

        # Mendukung query param ?page=<key> untuk navigasi langsung & pengujian
        qp_page_024 = st.query_params.get("page")
        if "active_page_024" not in st.session_state:
            if qp_page_024 in self.page_handlers_024:
                st.session_state["active_page_024"] = str(qp_page_024)
            else:
                st.session_state["active_page_024"] = "overview"

    def render_sidebar_024(self) -> tuple[str, bool]:
        """Merender sidebar navigasi, pemilihan sumber data, dan ringkasan kesiapan pipeline."""
        pipeline_status_024 = get_pipeline_status_024()

        with st.sidebar:
            render_sidebar_brand_024()

            st.markdown('<div class="up-nav-section-label-024">Navigasi Utama</div>', unsafe_allow_html=True)
            active_key_024 = str(st.session_state["active_page_024"])

            for page_meta_024 in NAV_PAGES_024:
                key_024 = page_meta_024["key_024"]
                btn_label_024 = f"{page_meta_024['icon_024']}  {page_meta_024['label_024']}"
                btn_type_024 = "primary" if key_024 == active_key_024 else "secondary"
                if st.button(
                    btn_label_024,
                    key=f"nav_btn_{key_024}_024",
                    type=btn_type_024,
                    width="stretch",
                ):
                    st.session_state["active_page_024"] = key_024
                    st.query_params["page"] = key_024
                    st.rerun()

            st.markdown('<div class="up-nav-section-label-024">Sumber Data Tampilan</div>', unsafe_allow_html=True)
            mode_label_024 = st.radio(
                "Mode Sumber Data",
                options=[
                    "Data Aktual (Gold Layer & ML)",
                    "Pratinjau UI (DATA DEMO)",
                ],
                index=0,
                label_visibility="collapsed",
                key="data_source_mode_024",
                help=(
                    "Data Aktual membaca langsung tabel Gold Layer & artefak evaluasi ML di data/analytics/. "
                    "Mode DEMO hanya untuk pratinjau simulasi."
                ),
            )
            use_demo_024 = mode_label_024 == "Pratinjau UI (DATA DEMO)"

            # Ringkasan status integrasi proyek
            raw_cnt_024 = int(pipeline_status_024["raw_count_024"])
            ref_cnt_024 = int(pipeline_status_024["reference_count_024"])
            ready_cnt_024 = int(pipeline_status_024["analytics_ready_count_024"])
            total_cnt_024 = int(pipeline_status_024["analytics_total_count_024"])
            model_ok_024 = bool(pipeline_status_024.get("model_ready_024", False))

            raw_dot_024 = "up-dot-ok-024" if raw_cnt_024 > 0 else "up-dot-wait-024"
            ref_dot_024 = "up-dot-ok-024" if ref_cnt_024 > 0 else "up-dot-wait-024"
            gold_dot_024 = "up-dot-ok-024" if ready_cnt_024 == total_cnt_024 else "up-dot-wait-024"
            ml_dot_024 = "up-dot-ok-024" if model_ok_024 else "up-dot-wait-024"

            st.markdown('<div class="up-nav-section-label-024">Status Integrasi</div>', unsafe_allow_html=True)
            st.markdown(
                f"""
                <div class="up-sidebar-status-024">
                    <div class="up-sidebar-status-row-024">
                        <span><span class="{raw_dot_024}"></span>Raw Parquet</span>
                        <strong>{raw_cnt_024} berkas</strong>
                    </div>
                    <div class="up-sidebar-status-row-024">
                        <span><span class="{ref_dot_024}"></span>Zone Lookup</span>
                        <strong>{"265 zona" if ref_cnt_024 > 0 else "Belum ada"}</strong>
                    </div>
                    <div class="up-sidebar-status-row-024">
                        <span><span class="{gold_dot_024}"></span>Gold &amp; ML Serving</span>
                        <strong>{ready_cnt_024}/{total_cnt_024} artefak</strong>
                    </div>
                    <div class="up-sidebar-status-row-024">
                        <span><span class="{ml_dot_024}"></span>Model MLlib</span>
                        <strong>{"Ridge 0.1" if model_ok_024 else "Menunggu"}</strong>
                    </div>
                    <div class="up-sidebar-status-row-024" style="border-top:1px solid rgba(255,255,255,0.1); margin-top:6px; padding-top:6px;">
                        <span>Versi UI</span>
                        <span style="color:#93C5FD;">{APP_VERSION_024}</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        return str(st.session_state["active_page_024"]), use_demo_024

    def run_024(self) -> None:
        """Menjalankan siklus hidup aplikasi dashboard UrbanPulse_024."""
        self.configure_app_024()
        active_page_024, use_demo_024 = self.render_sidebar_024()
        handler_024 = self.page_handlers_024.get(active_page_024, render_overview_page_024)
        handler_024(use_demo_024=use_demo_024)


def main_024() -> None:
    """Fungsi utama untuk memulai instance UrbanPulseDashboard_024."""
    app_024 = UrbanPulseDashboard_024()
    app_024.run_024()


if __name__ == "__main__":
    main_024()
