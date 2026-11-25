"""Konfigurasi terpusat dan kontrak serving layer untuk UrbanPulse_024."""

from pathlib import Path
from typing import Dict, List


APP_NAME_024: str = "UrbanPulse"
APP_TAGLINE_024: str = "Intelligent Urban Taxi Analytics"
APP_VERSION_024: str = "1.0.0-gold-ml"
PROJECT_SUFFIX_024: str = "_024"

# Direktori root dan subdirektori utama proyek
DASHBOARD_DIR_024: Path = Path(__file__).resolve().parent
PROJECT_ROOT_DIR_024: Path = DASHBOARD_DIR_024.parent
ASSETS_DIR_024: Path = DASHBOARD_DIR_024 / "assets"
STYLESHEET_PATH_024: Path = ASSETS_DIR_024 / "styles_024.css"

DATA_DIR_024: Path = PROJECT_ROOT_DIR_024 / "data"
DATA_RAW_DIR_024: Path = DATA_DIR_024 / "raw"
DATA_REFERENCE_DIR_024: Path = DATA_DIR_024 / "reference"
DATA_PROCESSED_DIR_024: Path = DATA_DIR_024 / "processed"
DATA_ANALYTICS_DIR_024: Path = DATA_DIR_024 / "analytics"
REPORTS_DIR_024: Path = PROJECT_ROOT_DIR_024 / "reports"
ML_MODELS_DIR_024: Path = PROJECT_ROOT_DIR_024 / "ml" / "models"

# File referensi & evaluasi resmi (read-only)
ZONE_LOOKUP_PATH_024: Path = DATA_REFERENCE_DIR_024 / "taxi_zone_lookup_024.csv"
ML_EVAL_SUMMARY_PATH_024: Path = REPORTS_DIR_024 / "ml_evaluation_summary_024.json"
GOLD_SUMMARY_PATH_024: Path = REPORTS_DIR_024 / "gold_layer_summary_024.json"
TRAINING_METADATA_PATH_024: Path = ML_MODELS_DIR_024 / "training_run_metadata_024.json"

# Palet warna resmi UrbanPulse (Navy, Putih, Aksen Biru)
PALETTE_024: Dict[str, str] = {
    "navy_deep": "#0B1F3A",
    "navy_mid": "#16325C",
    "navy_soft": "#1E3A6D",
    "blue_primary": "#2563EB",
    "blue_accent": "#3B82F6",
    "blue_sky": "#60A5FA",
    "blue_tint": "#DBEAFE",
    "blue_ice": "#EFF6FF",
    "bg_canvas": "#F8FAFC",
    "surface_white": "#FFFFFF",
    "border_subtle": "#E2E8F0",
    "border_strong": "#CBD5E1",
    "text_primary": "#0F172A",
    "text_secondary": "#475569",
    "text_muted": "#64748B",
    "teal_positive": "#0F766E",
    "teal_soft": "#CCFBF1",
    "amber_demo": "#B45309",
    "amber_soft": "#FEF3C7",
    "rose_alert": "#BE123C",
    "rose_soft": "#FFE4E6",
    "slate_chart": "#94A3B8",
}

# Skala warna sequential Plotly bernuansa navy-biru
CHART_COLORSCALE_024: List[List[object]] = [
    [0.0, "#EFF6FF"],
    [0.25, "#BFDBFE"],
    [0.5, "#60A5FA"],
    [0.75, "#2563EB"],
    [1.0, "#0B1F3A"],
]

# Warna konsisten per Borough NYC
BOROUGH_COLOR_MAP_024: Dict[str, str] = {
    "Manhattan": "#0B1F3A",
    "Queens": "#2563EB",
    "Brooklyn": "#3B82F6",
    "Bronx": "#60A5FA",
    "Staten Island": "#0F766E",
    "EWR": "#0284C7",
    "Unknown": "#94A3B8",
    "N/A": "#CBD5E1",
}

# Kontrak nama berkas serving layer di data/analytics/
# Mendukung direktori Parquet Gold Layer (ekspor read-only HDFS) dan artefak MLlib
ANALYTICS_DATASETS_024: Dict[str, Dict[str, object]] = {
    "hourly_demand": {
        "stem_024": "gold_hourly_demand_024",
        "fallback_stems_024": ["hourly_demand_024"],
        "label_024": "Permintaan per Tanggal & Jam (Gold)",
        "producer_024": "PySpark Gold Layer (HDFS)",
        "formats_024": ["", ".parquet", ".csv"],
    },
    "zone_summary": {
        "stem_024": "gold_zone_demand_024",
        "fallback_stems_024": ["zone_summary_024"],
        "label_024": "Ringkasan Permintaan & Pendapatan Zona (Gold)",
        "producer_024": "PySpark Gold Layer + Zone Lookup",
        "formats_024": ["", ".parquet", ".csv"],
    },
    "temporal_patterns": {
        "stem_024": "gold_temporal_patterns_024",
        "fallback_stems_024": [],
        "label_024": "Pola Temporal 7 Hari × 24 Jam (Gold)",
        "producer_024": "PySpark Gold Layer (HDFS)",
        "formats_024": ["", ".parquet", ".csv"],
    },
    "zone_hourly_demand": {
        "stem_024": "gold_zone_hourly_demand_024",
        "fallback_stems_024": [],
        "label_024": "Profil Permintaan Zona × 24 Jam (Gold)",
        "producer_024": "PySpark Gold Layer + Zone Lookup",
        "formats_024": ["", ".parquet", ".csv"],
    },
    "demand_predictions": {
        "stem_024": "demand_predictions_024",
        "fallback_stems_024": [],
        "label_024": "Prediksi Test Holdout (Zona × Jam)",
        "producer_024": "Spark MLlib (ridge_reg_0p1_024)",
        "formats_024": [".parquet", "", ".csv"],
    },
    "model_metrics": {
        "stem_024": "model_metrics_024",
        "fallback_stems_024": [],
        "label_024": "Metrik Evaluasi Model ML",
        "producer_024": "Spark MLlib Evaluation",
        "formats_024": [".json", ".parquet", ".csv"],
    },
}

# File referensi geospasial opsional di data/reference/
REFERENCE_GEOJSON_CANDIDATES_024: List[str] = [
    "taxi_zones_024.geojson",
    "taxi_zones.geojson",
]

# Daftar halaman navigasi utama (Bahasa Indonesia)
NAV_PAGES_024: List[Dict[str, str]] = [
    {
        "key_024": "overview",
        "label_024": "Ringkasan Utama",
        "en_subtitle_024": "Overview",
        "icon_024": "◉",
        "description_024": "Ringkasan KPI September 2026, distribusi jam, Top 10 zona, dan Borough.",
    },
    {
        "key_024": "demand_analysis",
        "label_024": "Analisis Permintaan",
        "en_subtitle_024": "Demand Analysis",
        "icon_024": "◷",
        "description_024": "Pola demand per jam, weekday vs weekend, tren harian, periode waktu, dan heatmap.",
    },
    {
        "key_024": "zone_analysis",
        "label_024": "Analisis Zona",
        "en_subtitle_024": "Zone Analysis",
        "icon_024": "◎",
        "description_024": "Ranking zona, pendapatan vs demand, perbandingan antarzona, filter Borough & service_zone.",
    },
    {
        "key_024": "demand_prediction",
        "label_024": "Prediksi Permintaan",
        "en_subtitle_024": "Demand Prediction",
        "icon_024": "◈",
        "description_024": "Evaluasi prediksi Ridge Regression pada Test Holdout (24–30 Sep 2026) & metrik ML.",
    },
    {
        "key_024": "recommendations",
        "label_024": "Rekomendasi Armada",
        "en_subtitle_024": "Recommendations",
        "icon_024": "◆",
        "description_024": "Rekomendasi operasional berbasis bukti aktual Gold Layer & evaluasi model ML.",
    },
]

# Pemetaan nama hari Bahasa Indonesia (0=Senin .. 6=Minggu)
DAY_ORDER_ID_024: List[str] = [
    "Senin",
    "Selasa",
    "Rabu",
    "Kamis",
    "Jumat",
    "Sabtu",
    "Minggu",
]

# Urutan baku periode waktu (cocok dengan kolom Gold time_of_day_bucket_024)
TIME_BUCKET_ORDER_024: List[str] = [
    "Dini Hari (00-05)",
    "Pagi (06-11)",
    "Siang (12-17)",
    "Malam (18-23)",
]

# Kategori rentang jam untuk filter analisis permintaan
TIME_BLOCK_MAP_024: Dict[str, List[int]] = {
    "Semua Waktu (00–23)": list(range(0, 24)),
    "Dini Hari (00–05)": list(range(0, 6)),
    "Pagi (06–11)": list(range(6, 12)),
    "Siang (12–17)": list(range(12, 18)),
    "Malam (18–23)": list(range(18, 24)),
}
