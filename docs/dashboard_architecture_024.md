# Arsitektur Dashboard & Kontrak Serving Layer (`_024`) — UrbanPulse

Dokumen ini menjelaskan struktur modular antarmuka Streamlit **UrbanPulse — Intelligent Urban Taxi Analytics** serta skema berkas yang diharapkan pada direktori `data/analytics/` agar integrasi fase PySpark dan Spark MLlib berjalan tanpa perlu mengubah kode UI.

---

## 1. Struktur Modul Dashboard

```text
dashboard/
├── app_024.py                      # Entry point utama (class UrbanPulseDashboard_024)
├── config_024.py                   # Konstanta direktori, palet warna, kontrak dataset, dan menu navigasi
├── data_loader_024.py              # Pembaca serving layer data/analytics/ (AnalyticsDataLoader_024, load_data_024)
├── demo_data_024.py                # Penyedia DATA DEMO berlabel eksplisit untuk pengujian UI
├── components_024.py               # Komponen UI reusable (KPI cards, header, banner, empty state, rec cards)
├── charts_024.py                   # Builder grafik Plotly interaktif bertema navy-putih-biru
├── assets/
│   └── styles_024.css              # Stylesheet kustom UrbanPulse
└── pages/
    ├── overview_024.py             # Halaman A: Ringkasan Utama (Overview)
    ├── demand_analysis_024.py      # Halaman B: Analisis Permintaan Temporal (Demand Analysis)
    ├── zone_analysis_024.py        # Halaman C: Analisis Lokasi & Zona Pickup NYC (Zone Analysis)
    ├── demand_prediction_024.py    # Halaman D: Prediksi Permintaan ML (Demand Prediction)
    └── recommendations_024.py      # Halaman E: Rekomendasi Armada Berbasis Bukti (Recommendations)
```

---

## 2. Prinsip Integrasi Serving Layer (`data/analytics/`)

- Dashboard **tidak membaca atau memproses langsung** dataset mentah `data/raw/*.parquet`.
- `AnalyticsDataLoader_024` secara otomatis mencari berkas hasil olahan di `data/analytics/` dengan prioritas format **`.parquet`** (termasuk direktori partisi Parquet hasil `df.write.parquet(...)` dari PySpark), diikuti **`.csv`** dan **`.json`**.
- Apabila berkas belum tersedia, halaman menampilkan **empty state informatif** tanpa mengarang angka statistik atau rekomendasi.
- Untuk pengujian tampilan sebelum pipeline selesai, pengguna dapat mengaktifkan **Pratinjau UI (DATA DEMO)** di sidebar yang ditandai banner serta label `DATA DEMO` pada setiap kartu dan grafik.

---

## 3. Kontrak Skema Tabel `data/analytics/`

> Kolom asli NYC TLC / Zone Lookup (`PULocationID`, `Zone`, `Borough`, `service_zone`) dipertahankan tanpa akhiran `_024`. Seluruh kolom hasil agregasi/transformasi/prediksi menggunakan akhiran `_024`.

### A. `overview_kpi_024.parquet` *(opsional — dapat diturunkan otomatis dari `hourly_demand_024` & `zone_summary_024`)*
| Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `total_pickups_024` | integer | Total akumulasi perjalanan pickup |
| `avg_hourly_demand_024` | float | Rata-rata jumlah pickup per jam |
| `top_zone_name_024` | string | Nama zona dengan permintaan tertinggi |
| `peak_hour_024` | integer (0–23) | Jam dengan rata-rata permintaan tertinggi |

### B. `hourly_demand_024.parquet` *(PySpark BI Aggregation)*
| Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `pickup_date_024` | date / string (`YYYY-MM-DD`) | Tanggal pickup |
| `pickup_hour_024` | integer (0–23) | Jam pickup |
| `day_of_week_024` | integer (0=Senin .. 6=Minggu) | Indeks hari |
| `day_name_024` | string | Nama hari Bahasa Indonesia (`Senin`..`Minggu`) |
| `is_weekend_024` | boolean | `True` untuk Sabtu/Minggu |
| `pickup_count_024` | integer | Total pickup pada kombinasi tanggal × jam |

### C. `zone_summary_024.parquet` *(PySpark BI Aggregation)*
| Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `PULocationID` | integer | ID zona asli NYC TLC |
| `Zone` | string | Nama zona dari `taxi_zone_lookup.csv` |
| `Borough` | string | Nama wilayah NYC (`Manhattan`, `Queens`, dll.) |
| `service_zone` | string | Kategori zona layanan (opsional) |
| `period_024` | string | Label periode agregasi, mis. `2026-09` (opsional) |
| `pickup_count_024` | integer | Total perjalanan pickup di zona tersebut |
| `latitude_024` | float | Latitude centroid zona (opsional jika GeoJSON belum ada) |
| `longitude_024` | float | Longitude centroid zona (opsional jika GeoJSON belum ada) |

*(Opsional di `data/reference/`: `taxi_zones_024.geojson` dengan properti `LocationID` untuk peta choropleth NYC penuh.)*

### D. `demand_predictions_024.parquet` *(Spark MLlib Inference)*
| Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `PULocationID` | integer | ID zona pickup |
| `Zone` | string | Nama zona |
| `Borough` | string | Nama wilayah |
| `prediction_date_024` | date / string (`YYYY-MM-DD`) | Tanggal target prediksi |
| `pickup_hour_024` | integer (0–23) | Jam target prediksi |
| `predicted_demand_024` | float | Nilai prediksi jumlah pickup |
| `actual_demand_024` | float (nullable) | Nilai aktual untuk tanggal evaluasi |
| `lower_bound_024` | float (nullable) | Batas bawah interval estimasi |
| `upper_bound_024` | float (nullable) | Batas atas interval estimasi |

### E. `model_metrics_024.json` *(atau `.parquet` / `.csv` — Spark MLlib Evaluation)*
| Field / Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `model_name_024` | string | Nama model terbaik (mis. `gbt_demand_model_024`) |
| `mae_024` | float | Mean Absolute Error |
| `rmse_024` | float | Root Mean Squared Error |
| `r2_024` | float | Koefisien determinasi ($R^2$) |
| `status_024` | string | Ringkasan status pelatihan/evaluasi model |

### F. `recommendations_024.parquet` *(Analytics & ML Decision Layer)*
| Kolom | Tipe | Keterangan |
| :--- | :--- | :--- |
| `PULocationID` | integer | ID zona target |
| `Zone` | string | Nama zona |
| `Borough` | string | Wilayah |
| `target_window_024` | string | Jendela waktu puncak & saran persiapan armada |
| `predicted_surge_pct_024` | float | Persentase estimasi lonjakan permintaan |
| `priority_level_024` | string | Tingkat prioritas (`Tinggi`, `Sedang`, `Rendah`) |
| `recommended_action_024` | string | Tindakan operasional yang disarankan |
| `supporting_basis_024` | string | Bukti analitik/model yang mendasari rekomendasi |
| `fleet_allocation_note_024` | string | Catatan distribusi/reposisi armada |
