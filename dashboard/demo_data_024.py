"""Penyedia DATA DEMO berlabel eksplisit untuk pengujian tampilan & interaktivitas UI UrbanPulse_024.

PENTING:
Seluruh angka pada modul ini HANYA data simulasi untuk menguji grafik Plotly,
filter interaktif, peta, dan tata letak antarmuka sebelum pipeline Big Data
(Apache NiFi -> Hadoop HDFS -> PySpark -> Spark MLlib) dijalankan.
Modul ini TIDAK BOLEH digunakan sebagai hasil analisis maupun klaim penelitian.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from dashboard.config_024 import DAY_ORDER_ID_024


# Daftar zona NYC ilustratif beserta koordinat centroid perkiraan untuk pengujian peta UI
DEMO_ZONES_CATALOG_024: List[Dict[str, object]] = [
    {"PULocationID": 161, "Zone": "Midtown Center", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7580, "longitude_024": -73.9776, "weight_024": 1.00},
    {"PULocationID": 237, "Zone": "Upper East Side South", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7686, "longitude_024": -73.9656, "weight_024": 0.94},
    {"PULocationID": 132, "Zone": "JFK Airport", "Borough": "Queens", "service_zone": "Airports", "latitude_024": 40.6413, "longitude_024": -73.7781, "weight_024": 0.91},
    {"PULocationID": 236, "Zone": "Upper East Side North", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7804, "longitude_024": -73.9570, "weight_024": 0.87},
    {"PULocationID": 162, "Zone": "Midtown East", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7566, "longitude_024": -73.9724, "weight_024": 0.84},
    {"PULocationID": 186, "Zone": "Penn Station/Madison Sq West", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7484, "longitude_024": -73.9925, "weight_024": 0.80},
    {"PULocationID": 138, "Zone": "LaGuardia Airport", "Borough": "Queens", "service_zone": "Airports", "latitude_024": 40.7769, "longitude_024": -73.8740, "weight_024": 0.76},
    {"PULocationID": 230, "Zone": "Times Sq/Theatre District", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7590, "longitude_024": -73.9845, "weight_024": 0.74},
    {"PULocationID": 170, "Zone": "Murray Hill", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7478, "longitude_024": -73.9786, "weight_024": 0.68},
    {"PULocationID": 239, "Zone": "Upper West Side South", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7834, "longitude_024": -73.9788, "weight_024": 0.64},
    {"PULocationID": 79, "Zone": "East Village", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7276, "longitude_024": -73.9859, "weight_024": 0.59},
    {"PULocationID": 231, "Zone": "TriBeCa/Civic Center", "Borough": "Manhattan", "service_zone": "Yellow Zone", "latitude_024": 40.7186, "longitude_024": -74.0078, "weight_024": 0.52},
    {"PULocationID": 255, "Zone": "Williamsburg (North Side)", "Borough": "Brooklyn", "service_zone": "Boro Zone", "latitude_024": 40.7181, "longitude_024": -73.9571, "weight_024": 0.31},
    {"PULocationID": 181, "Zone": "Park Slope", "Borough": "Brooklyn", "service_zone": "Boro Zone", "latitude_024": 40.6710, "longitude_024": -73.9814, "weight_024": 0.22},
    {"PULocationID": 7, "Zone": "Astoria", "Borough": "Queens", "service_zone": "Boro Zone", "latitude_024": 40.7615, "longitude_024": -73.9209, "weight_024": 0.19},
    {"PULocationID": 168, "Zone": "Mott Haven/Port Morris", "Borough": "Bronx", "service_zone": "Boro Zone", "latitude_024": 40.8089, "longitude_024": -73.9229, "weight_024": 0.11},
    {"PULocationID": 221, "Zone": "Stapleton", "Borough": "Staten Island", "service_zone": "Boro Zone", "latitude_024": 40.6267, "longitude_024": -74.0772, "weight_024": 0.05},
]


class DemoDataProvider_024:
    """Generator deterministik DATA DEMO untuk validasi tampilan seluruh halaman dashboard."""

    def __init__(self, seed_024: int = 2026024) -> None:
        self.rng_024 = np.random.default_rng(seed_024)

    def get_dataset_024(self, dataset_key_024: str) -> Optional[pd.DataFrame]:
        """Mengembalikan DataFrame DEMO sesuai kontrak serving layer data/analytics/."""
        builders_024 = {
            "hourly_demand": self.build_hourly_demand_demo_024,
            "zone_summary": self.build_zone_summary_demo_024,
            "demand_predictions": self.build_predictions_demo_024,
            "model_metrics": self.build_model_metrics_demo_024,
            "recommendations": self.build_recommendations_demo_024,
            "overview_kpi": self.build_overview_kpi_demo_024,
        }
        builder_fn_024 = builders_024.get(dataset_key_024)
        if builder_fn_024 is None:
            return None
        return builder_fn_024()

    def build_hourly_demand_demo_024(self) -> pd.DataFrame:
        """Menghasilkan data simulasi permintaan per jam selama 14 hari untuk pratinjau UI."""
        dates_024 = pd.date_range("2026-09-01", periods=14, freq="D")
        records_024: List[Dict[str, object]] = []

        for dt_024 in dates_024:
            dow_024 = int(dt_024.dayofweek)
            is_weekend_024 = dow_024 >= 5
            day_name_024 = DAY_ORDER_ID_024[dow_024]

            for hour_024 in range(24):
                # Pola simulasi umum aktivitas perkotaan hanya untuk pengujian visualisasi
                if is_weekend_024:
                    curve_024 = (
                        1100
                        + 950 * math.exp(-((hour_024 - 15) ** 2) / 22.0)
                        + 820 * math.exp(-((hour_024 - 22) ** 2) / 14.0)
                        - 600 * math.exp(-((hour_024 - 5) ** 2) / 8.0)
                    )
                else:
                    curve_024 = (
                        950
                        + 980 * math.exp(-((hour_024 - 9) ** 2) / 10.0)
                        + 1450 * math.exp(-((hour_024 - 18) ** 2) / 12.0)
                        - 650 * math.exp(-((hour_024 - 4) ** 2) / 7.0)
                    )

                noise_024 = float(self.rng_024.normal(0, 85))
                pickup_count_024 = max(180, int(round(curve_024 + noise_024)))
                records_024.append(
                    {
                        "pickup_date_024": dt_024.strftime("%Y-%m-%d"),
                        "pickup_hour_024": hour_024,
                        "day_of_week_024": dow_024,
                        "day_name_024": day_name_024,
                        "is_weekend_024": is_weekend_024,
                        "pickup_count_024": pickup_count_024,
                        "is_demo_024": True,
                    }
                )

        return pd.DataFrame(records_024)

    def build_zone_summary_demo_024(self) -> pd.DataFrame:
        """Menghasilkan ringkasan zona simulasi lintas periode untuk pengujian halaman Zone Analysis."""
        periods_024 = [
            ("Minggu 1 (DEMO)", 1.00),
            ("Minggu 2 (DEMO)", 1.06),
        ]
        rows_024: List[Dict[str, object]] = []
        base_scale_024 = 24500

        for period_label_024, multiplier_024 in periods_024:
            for item_024 in DEMO_ZONES_CATALOG_024:
                weight_024 = float(item_024["weight_024"])
                jitter_024 = float(self.rng_024.uniform(0.95, 1.05))
                pickups_024 = int(round(base_scale_024 * weight_024 * multiplier_024 * jitter_024))
                peak_hour_024 = 18 if item_024["Borough"] == "Manhattan" else (16 if item_024["service_zone"] == "Airports" else 19)
                rows_024.append(
                    {
                        "PULocationID": int(item_024["PULocationID"]),
                        "Zone": str(item_024["Zone"]),
                        "Borough": str(item_024["Borough"]),
                        "service_zone": str(item_024["service_zone"]),
                        "period_024": period_label_024,
                        "pickup_count_024": pickups_024,
                        "avg_hourly_demand_024": round(pickups_024 / (7 * 24), 1),
                        "peak_hour_024": peak_hour_024,
                        "latitude_024": float(item_024["latitude_024"]),
                        "longitude_024": float(item_024["longitude_024"]),
                        "is_demo_024": True,
                    }
                )

        return pd.DataFrame(rows_024)

    def build_predictions_demo_024(self) -> pd.DataFrame:
        """Menghasilkan kurva simulasi prediksi permintaan per zona per jam (24 jam × 3 hari)."""
        sample_zones_024 = DEMO_ZONES_CATALOG_024[:8]
        forecast_dates_024 = pd.date_range("2026-09-15", periods=3, freq="D")
        rows_024: List[Dict[str, object]] = []

        for day_idx_024, dt_024 in enumerate(forecast_dates_024):
            has_actual_024 = day_idx_024 == 0
            for item_024 in sample_zones_024:
                weight_024 = float(item_024["weight_024"])
                for hour_024 in range(24):
                    base_024 = (
                        75
                        + 95 * math.exp(-((hour_024 - 9) ** 2) / 11.0)
                        + 145 * math.exp(-((hour_024 - 18) ** 2) / 13.0)
                        - 45 * math.exp(-((hour_024 - 4) ** 2) / 7.0)
                    ) * weight_024
                    pred_024 = max(12.0, round(base_024 + float(self.rng_024.normal(0, 6)), 1))
                    band_024 = max(6.0, round(pred_024 * 0.11, 1))
                    actual_024 = (
                        max(8.0, round(pred_024 + float(self.rng_024.normal(0, 9)), 1))
                        if has_actual_024
                        else None
                    )
                    rows_024.append(
                        {
                            "PULocationID": int(item_024["PULocationID"]),
                            "Zone": str(item_024["Zone"]),
                            "Borough": str(item_024["Borough"]),
                            "prediction_date_024": dt_024.strftime("%Y-%m-%d"),
                            "pickup_hour_024": hour_024,
                            "predicted_demand_024": pred_024,
                            "actual_demand_024": actual_024,
                            "lower_bound_024": max(0.0, round(pred_024 - band_024, 1)),
                            "upper_bound_024": round(pred_024 + band_024, 1),
                            "is_demo_024": True,
                        }
                    )

        return pd.DataFrame(rows_024)

    def build_model_metrics_demo_024(self) -> pd.DataFrame:
        """Menghasilkan contoh struktur metrik evaluasi model (DATA DEMO — bukan model terlatih)."""
        return pd.DataFrame(
            [
                {
                    "model_name_024": "GBTRegressor_024 (SIMULASI DEMO)",
                    "mae_024": 11.42,
                    "rmse_024": 16.85,
                    "r2_024": 0.89,
                    "target_col_024": "pickup_count_024",
                    "granularity_024": "PULocationID × pickup_hour_024",
                    "status_024": "DEMO — Model Spark MLlib belum dilatih",
                    "is_demo_024": True,
                }
            ]
        )

    def build_recommendations_demo_024(self) -> pd.DataFrame:
        """Menghasilkan contoh rekomendasi terstruktur dari data DEMO untuk menguji tata letak halaman."""
        return pd.DataFrame(
            [
                {
                    "PULocationID": 161,
                    "Zone": "Midtown Center",
                    "Borough": "Manhattan",
                    "target_window_024": "17:00 – 20:00 (Siaga mulai 16:30)",
                    "predicted_surge_pct_024": 34.5,
                    "priority_level_024": "Tinggi",
                    "recommended_action_024": "Tambah alokasi armada menjelang jam pulang kerja di koridor perkantoran Midtown.",
                    "supporting_basis_024": "DATA DEMO: Puncak permintaan simulasi tertinggi terjadi pada pukul 18:00.",
                    "fleet_allocation_note_024": "Prioritaskan reposisi armada dari zona berpermintaan rendah sekitar 30 menit sebelum puncak.",
                    "is_demo_024": True,
                },
                {
                    "PULocationID": 132,
                    "Zone": "JFK Airport",
                    "Borough": "Queens",
                    "target_window_024": "15:00 – 19:00 (Siaga mulai 14:30)",
                    "predicted_surge_pct_024": 28.2,
                    "priority_level_024": "Tinggi",
                    "recommended_action_024": "Pertahankan antrean armada stabil pada terminal kedatangan sore hari.",
                    "supporting_basis_024": "DATA DEMO: Konsentrasi pickup simulasi konsisten tinggi pada kategori bandara.",
                    "fleet_allocation_note_024": "Koordinasikan rotasi armada bandara dengan durasi perjalanan kembali ke pusat kota.",
                    "is_demo_024": True,
                },
                {
                    "PULocationID": 237,
                    "Zone": "Upper East Side South",
                    "Borough": "Manhattan",
                    "target_window_024": "08:00 – 10:00 (Siaga mulai 07:30)",
                    "predicted_surge_pct_024": 22.8,
                    "priority_level_024": "Sedang",
                    "recommended_action_024": "Siapkan kapasitas tambahan untuk gelombang mobilitas pagi hari kerja.",
                    "supporting_basis_024": "DATA DEMO: Lonjakan simulasi pagi terlihat jelas pada perbandingan hari kerja.",
                    "fleet_allocation_note_024": "Arahkan sebagian armada shift pagi untuk mengisi koridor residensial–bisnis.",
                    "is_demo_024": True,
                },
                {
                    "PULocationID": 230,
                    "Zone": "Times Sq/Theatre District",
                    "Borough": "Manhattan",
                    "target_window_024": "20:00 – 23:00 (Siaga mulai 19:30)",
                    "predicted_surge_pct_024": 19.4,
                    "priority_level_024": "Sedang",
                    "recommended_action_024": "Tingkatkan ketersediaan armada malam hari khususnya pada akhir pekan.",
                    "supporting_basis_024": "DATA DEMO: Aktivitas simulasi akhir pekan bertahan tinggi hingga pukul 22:00.",
                    "fleet_allocation_note_024": "Pertimbangkan pergeseran armada dari area perkantoran yang mulai melandai malam hari.",
                    "is_demo_024": True,
                },
            ]
        )

    def build_overview_kpi_demo_024(self) -> pd.DataFrame:
        """Menghitung ringkasan KPI konsisten dari tabel DEMO hourly_demand dan zone_summary."""
        hourly_024 = self.build_hourly_demand_demo_024()
        zones_024 = self.build_zone_summary_demo_024()
        zone_totals_024 = zones_024.groupby("Zone", as_index=False)["pickup_count_024"].sum()
        top_zone_row_024 = zone_totals_024.sort_values("pickup_count_024", ascending=False).iloc[0]
        hourly_avg_024 = hourly_024.groupby("pickup_hour_024", as_index=False)["pickup_count_024"].mean()
        peak_hour_row_024 = hourly_avg_024.sort_values("pickup_count_024", ascending=False).iloc[0]

        return pd.DataFrame(
            [
                {
                    "total_pickups_024": int(hourly_024["pickup_count_024"].sum()),
                    "avg_hourly_demand_024": round(float(hourly_024["pickup_count_024"].mean()), 1),
                    "top_zone_name_024": str(top_zone_row_024["Zone"]),
                    "peak_hour_024": int(peak_hour_row_024["pickup_hour_024"]),
                    "is_demo_024": True,
                }
            ]
        )
