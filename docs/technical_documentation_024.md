# Technical Documentation — UrbanPulse: Intelligent Urban Taxi Analytics (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Versi Dokumen:** 2.0 (Diperbarui menyeluruh untuk Progress 1–6 per 25 November 2026)  
> **Root Workspace:** `D:\A University\Big Data dan Analitik\Project Akhir`  
> **Konvensi Penamaan:** Seluruh artefak dan identifier buatan proyek menggunakan akhiran `_024` sesuai [naming_convention_024.md](naming_convention_024.md).

---

## 1. Arsitektur Sistem End-to-End (Kondisi Aktual Progress 1–6)

Arsitektur UrbanPulse menerapkan **Medallion Architecture** (*Raw → Silver → Gold*) yang terhubung dari lapisan ingestion otomatis (**Apache NiFi**) hingga **Machine Learning (Spark MLlib)** dan **Interactive Dashboard (Streamlit + Plotly)**:

```text
[NYC TLC Yellow Taxi Parquet — Sep 2026 (67,374,896 B, SHA-256 verified)]
                                 │
                                 ▼
   [Apache NiFi 1.28.1]          ◄── STATUS: TERPASANG & TERVERIFIKASI (Progress 6)
   Flow: urbanpulse_ingestion_024    • Uji aman di /urbanpulse_024/nifi_test_024/ (SHA-256 100% cocok)
   (ListFile→FetchFile→UpdateAttr→   • File Raw produksi Sep 2026 sudah ada sejak Progress 2 & tidak ditimpa
    PutHDFS→LogAttribute)
                                 │
                                 ▼
   [Hadoop HDFS 3.5.0 — Raw Layer] ◄── STATUS: SELESAI (64.3 MiB, 3,876,044 baris, 21 kolom, Read-Only)
   /urbanpulse_024/raw/yellow_taxi/
                                 │
                                 ▼
   [PySpark 4.0.4 — Silver Layer]  ◄── STATUS: SELESAI (Silver v2 Live, 159.2 M, 3,860,852 baris, 49 kolom)
   /urbanpulse_024/processed/yellow_taxi_024/
                                 │
                                 ▼
   [PySpark 4.0.4 — Gold Layer]    ◄── STATUS: SELESAI DI HDFS (5 dataset Parquet, 0-leakage & NTZ verified)
   /urbanpulse_024/analytics/
                                 │
                                 ▼
   [Spark MLlib 4.0.4 — Training & Eval] ◄── STATUS: SELESAI (Progress 5; Ridge 0.1 terpilih & tervalidasi)
   ml/models/ridge_reg_0p1_024/              All 261 RMSE 12.2597 (-19.28% vs Naive), R² 0.9564
                                 │
                                 ▼
   [Serving Layer (data/analytics/)] ◄── STATUS: SELESAI (6/6 artefak siap + taxi_zone_lookup_024.csv)
   4 Gold Parquet + demand_predictions_024.parquet + model_metrics_024.json
                                 │
                                 ▼
   [Streamlit + Plotly Dashboard]  ◄── STATUS: AKTIF DATA AKTUAL (v1.0.0-gold-ml, 5 halaman, 5 screenshot)
   dashboard/app_024.py (http://localhost:8501)
```

---

## 2. Spesifikasi Infrastruktur & Parameter Runtime

| Komponen | Spesifikasi Aktual | Lokasi / Endpoint | Status |
| :--- | :--- | :--- | :---: |
| **Host OS** | Windows 11 Home Single Language (Build 26200, 64-bit) | Intel Core i7-1355U (10C/12T), RAM 16 GB | Aktif |
| **WSL2 Linux** | Ubuntu 24.04.4 LTS (Noble) | RAM `7.5 GiB` + Swap `2.0 GiB`, `systemd`, `ssh.socket` port 22 | Aktif |
| **Java Runtime** | OpenJDK `17.0.20` | `/usr/lib/jvm/java-17-openjdk-amd64` | Aktif |
| **Hadoop HDFS** | Apache Hadoop `3.5.0` (pseudo-distributed) | `/usr/local/hadoop`, `hdfs://localhost:9000`, `http://localhost:9870`, `dfs.replication=1` | Aktif |
| **Apache Spark** | Apache Spark `4.0.4` (build `hadoop3`, Scala `2.13.16`) | `/usr/local/spark`, `--master local[*]`, Python `3.12.3` (`numpy 1.26.4`) | Aktif (On-Demand) |
| **Apache NiFi** | Apache NiFi `1.28.1` (standalone, Java 17 native, `-Xms512m -Xmx1g`) | `/home/ubuntu/nifi-1.28.1`, `http://127.0.0.1:8090/nifi` | Terpasang & Terverifikasi (On-Demand) |
| **Dashboard Runtime** | Python `.venv` (Windows), Streamlit `>=1.50`, Plotly `>=5.22`, Pandas, PyArrow | `http://localhost:8501` (`APP_VERSION_024 = 1.0.0-gold-ml`) | Aktif (Data Aktual 6/6 Artefak) |

### Konfigurasi Runtime Standar Proyek
- **PySpark & MLlib:** `--master local[*]` (menghindari konflik port `8088` YARN dan menghemat RAM host 16 GB), `SPARK_LOCAL_IP=127.0.0.1`, `spark.driver.memory=2g`, `spark.sql.shuffle.partitions=16`, `spark.eventLog.enabled=false`, `spark.sql.parquet.compression.codec=snappy`.
- **Apache NiFi 1.28.1:** `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` (lokal di `bin/nifi-env.sh`), `-Xms512m -Xmx1g` (`conf/bootstrap.conf`), `127.0.0.1:8090` (`conf/nifi.properties`), provenance max `256 MB`, membaca `core-site.xml` & `hdfs-site.xml` secara *read-only* tanpa mengubah konfigurasi Hadoop.

---

## 3. Struktur Direktori Proyek & HDFS

### 3.1 Struktur Workspace Lokal (`D:\A University\Big Data dan Analitik\Project Akhir`)
- **Dokumen Inti:** `PROJECT_BRIEF_024.md`, `README.md`, `requirements.txt`, `.gitignore`, `.streamlit/config.toml`.
- **Data (`data/`):**
  - `data/raw/yellow_tripdata_2026-09_024.parquet` (`67,374,896` byte, SHA-256 terverifikasi, *read-only*).
  - `data/reference/taxi_zone_lookup_024.csv` (`12,331` byte, `265` zona resmi NYC TLC, `100%` match pada `261` zona aktif). *(Opsional: `taxi_zones_024.geojson` belum tersedia.)*
  - `data/analytics/` *(Serving Layer lengkap — `6/6` artefak)*:
    - `gold_hourly_demand_024/` (`60,520` B, `720` baris)
    - `gold_zone_demand_024/` (`42,424` B, `261` baris)
    - `gold_temporal_patterns_024/` (`16,989` B, `168` baris)
    - `gold_zone_hourly_demand_024/` (`278,672` B, `6,264` baris)
    - `demand_predictions_024.parquet/` (`931,496` B, `43,848` baris Test Holdout `24–30 Sep 2026`)
    - `model_metrics_024.json` (`560` B)
- **Apache NiFi (`nifi/`):**
  - `nifi/flows/urbanpulse_ingestion_024.json` (`38,355` B, ekspor Process Group resmi)
  - `nifi/configs/hdfs_connection_024.properties`, `nifi_runtime_024.properties`, `ingestion_test_result_024.json`
- **Spark (`spark/processing/`):** `validate_raw_yellow_taxi_024.py`, `audit_silver_cleaning_impact_024.py`, `clean_yellow_taxi_024.py`, `build_gold_layer_024.py`.
- **Machine Learning (`ml/`):**
  - `ml/training/train_demand_model_024.py`
  - `ml/evaluation/evaluate_demand_model_024.py`
  - `ml/models/ridge_reg_0p1_024/` (`PipelineModel` 5 tahap) & `ml/models/training_run_metadata_024.json`
- **Dashboard (`dashboard/`):** `app_024.py` (`UrbanPulseDashboard_024`), `config_024.py` (`v1.0.0-gold-ml`), `data_loader_024.py` (`AnalyticsDataLoader_024`, `build_evidence_recommendations_024`), `demo_data_024.py`, `components_024.py`, `charts_024.py`, `assets/styles_024.css`, dan 5 halaman di `dashboard/pages/` (`overview_024.py`, `demand_analysis_024.py`, `zone_analysis_024.py`, `demand_prediction_024.py`, `recommendations_024.py`).
- **Laporan & Bukti Visual (`reports/`):** `raw_validation_*`, `silver_cleaning_impact_audit_*`, `silver_cleaning_*`, `gold_layer_*`, `ml_readiness_*`, `zone_lookup_validation_*`, `ml_evaluation_*`, serta `10` screenshot asli di `reports/figures/` (`progress_02_dataset_source_024.png`, `dashboard_01..05_*_024.png`, `nifi_01..04_*_024.png`).
- **Dokumentasi (`docs/`):** `naming_convention_024.md`, `dashboard_architecture_024.md`, `nifi_setup_024.md`, `development_log_024.md`, `testing_log_024.md`, `evidence_checklist_024.md`, `technical_documentation_024.md`, `methodology_024.md`, serta backup di `docs/backup_progress_01-04_024/` dan `docs/backup_progress_01-06_024/`.

### 3.2 Struktur Direktori HDFS (`hdfs://localhost:9000`)

```text
/urbanpulse_024/
├── raw/
│   ├── reference/
│   └── yellow_taxi/
│       └── yellow_tripdata_2026-09_024.parquet   # 64.3 M  | 3,876,044 baris | 21 kolom (Read-Only, sejak P2)
├── nifi_test_024/                                # Direktori uji terisolasi NiFi (Progress 6)
│   └── yellow_tripdata_2026-09_024.parquet       # 64.25 M | 67,374,896 B  | SHA-256 100% cocok lokal
├── processed/
│   └── yellow_taxi_024/                          # 159.2 M | 3,860,852 baris | 49 kolom (8 partisi Snappy)
├── analytics/                                    # Gold Layer Live (5 dataset Snappy Parquet)
│   ├── gold_hourly_demand_024/                   # 59.1 K  |       720 baris | 21 kolom
│   ├── gold_zone_demand_024/                     # 41.4 K  |       261 baris | 27 kolom
│   ├── gold_temporal_patterns_024/               # 16.6 K  |       168 baris | 17 kolom
│   ├── gold_zone_hourly_demand_024/              # 272.1 K |     6,264 baris | 18 kolom
│   └── gold_demand_features_024/                 # 1.9 M   |   187,920 baris | 26 kolom
└── models/                                       # Direktori HDFS cadangan (model tersimpan di lokal ml/models/)
```

---

## 4. Fungsi Script PySpark, Spark MLlib, & Lapisan Data

### 4.1 Daftar Script PySpark & MLlib

| Script | Mode | Durasi Aktual | Fungsi Teknis |
| :--- | :---: | :---: | :--- |
| `spark/processing/validate_raw_yellow_taxi_024.py` | Read-Only | `79.82s` | Profiling 21 kolom asli, single-pass null/NaN, duplikasi, statistik kuantil (`P25/P50/P75/P99`), dan deteksi anomali domain. |
| `spark/processing/audit_silver_cleaning_impact_024.py` | Read-Only | `51.08s` | Dekomposisi disjoint tanpa hitung ganda atas 6 aturan Silver v1, uji *exact positive mirror-pair*, profil anomali, dan korelasi jam/zona vs Raw. |
| `spark/processing/clean_yellow_taxi_024.py` | Raw → Silver | `77.64s` | Membangun Silver Layer v2 (`3,860,852` baris, `49` kolom), menandai `13,018` reversal pairs, menulis ke staging, verifikasi baca ulang, dan *atomic swap*. |
| `spark/processing/build_gold_layer_024.py` | Silver → Gold | `64.35s` | Membangun 5 dataset Gold dengan grid `TIMESTAMP_NTZ` bebas shift `+7 jam`, memisahkan demand operasional vs net revenue, membentuk panel fitur ML bebas leakage (`187,920` baris), dan verifikasi rekonsiliasi 100%. |
| `ml/training/train_demand_model_024.py` | Gold → Model | `141.37s` *(refit `2.11s`)* | Validasi pra-training, melatih 6 kandidat Spark MLlib (`Ridge 0.1/1.0`, `RF d8/d10`, `GBT d5/d6`) + Naive `lag_168h` secara kronologis, memilih `ridge_reg_0p1_024` via Validation, refit 1× (`Sep 8–23`), menyimpan `PipelineModel`, `demand_predictions_024.parquet`, dan `model_metrics_024.json`. |
| `ml/evaluation/evaluate_demand_model_024.py` | Evaluasi Read-Only | `31.99s` | Memuat ulang `ml/models/ridge_reg_0p1_024/`, verifikasi reproduksibilitas `< 1e-4`, evaluasi tersegmentasi (All 261, Top 50, Tier, Borough, Service Zone, Peak/Off-Peak, Weekday/Weekend), verifikasi lookup `100%`, dan cek immutability HDFS Gold. |

### 4.2 Ringkasan Lapisan Raw, Silver v2, Gold, & Model ML
- **Raw Layer:** `3,876,044` baris, `21` kolom asli NYC TLC, *immutable*.
- **Silver Layer v2 (`v2_audit_refined_024`):** `3,860,852` baris (`99.6081%`), `15,192` drop (`0` overlap), **`49` kolom** (`21` asli + `28` turunan `_024` termasuk `has_negative_reversal_pair_024` pada `13,018` baris).
- **Gold Layer (5 Dataset Live):** `gold_hourly_demand_024` (`720`), `gold_zone_demand_024` (`261`), `gold_temporal_patterns_024` (`168`), `gold_zone_hourly_demand_024` (`6,264`), dan `gold_demand_features_024` (`187,920` baris; `144,072` ML-ready `Sep 8–30` dengan `0` null dan lolos uji *zero-leakage*).
- **Model ML Terpilih (`ridge_reg_0p1_024`):** Pipeline 5 tahap (`StringIndexer` → `OneHotEncoder(dropLast=True)` → `VectorAssembler` `276` dimensi → `StandardScaler(withMean=False)` → `LinearRegression(regParam=0.1, elasticNetParam=0.0, maxIter=100)`), clipping non-negatif `max(0, pred)`, dan band ketidakpastian bebas leakage (`± 1.28 × 12.0061 = ± 15.3678` dari Validation RMSE).
  - **Performa Locked Test Holdout (`24–30 Sep 2026`, `43,848` baris):** All 261 MAE **`4.5495`** (`-6.76%`), RMSE **`12.2597`** (`-19.28%`), $R^2$ **`0.9564`**, WAPE **`21.36%`**; Top-50 MAE **`17.4498`** (`-11.76%`), RMSE **`27.4563`** (`-19.50%`), $R^2$ **`0.9244`**, WAPE **`17.25%`** (`45/50` zona unggul atas Naive `lag_168h`).

---

## 5. Arsitektur Dashboard Aktual (`v1.0.0-gold-ml`) & Integrasi Apache NiFi (`1.28.1`)

### 5.1 Dashboard Streamlit (`dashboard/app_024.py`)
- Class `UrbanPulseDashboard_024` mengorkestrasi **5 halaman interaktif** dengan 2 mode sumber data di sidebar (`Data Aktual (Gold Layer & ML)` sebagai default aktif dan `Pratinjau UI (DATA DEMO)` sebagai mode uji):
  1. **Ringkasan Utama (`overview_024.py`):** KPI aktual (`3.860.852` pickup, `USD 124,58 Jt` pendapatan bersih non-reversal, `18:00 vs 04:00` rasio `8,3×`, `Upper East Side South`), distribusi pickup 24 jam, Top-10 zona, distribusi Borough, dan ringkasan insight berbasis bukti.
  2. **Analisis Permintaan (`demand_analysis_024.py`):** Filter tanggal (`2026/09/01–2026/09/30`), jenis hari, dan blok waktu; membandingkan weekday (`5.346,9`/jam, `22` hari) vs weekend (`5.404,6`/jam, `8` hari), tren harian, dan matriks `7 × 24`.
  3. **Analisis Zona (`zone_analysis_024.py`):** Filter Borough, `service_zone`, urutan ranking, dan slider Top-N; menampilkan `Upper East Side South` (#1 volume, `172.229`) vs `JFK Airport` (#1 pendapatan, `USD 11,96 Jt`, `USD 80,85`/trip), konsentrasi Top-10 `33,03%`, dan scatter Demand vs Revenue. *(Peta poligon choropleth bersifat opsional karena `taxi_zones_024.geojson` belum ditambahkan.)*
  4. **Prediksi Permintaan (`demand_prediction_024.py`):** Dilengkapi banner klarifikasi bahwa tampilan adalah **Evaluasi Out-of-Sample pada Locked Test Holdout (`24–30 Sep 2026`) — Bukan Proyeksi Masa Depan**; menyediakan toggle metrik `All 261 Zones` vs `Top 50 Operational Zones`, filter Borough/Zona/Tanggal/Jam, dan overlay Baseline Naive (`lag_168h`).
  5. **Rekomendasi Armada (`recommendations_024.py`):** Diturunkan otomatis oleh `build_evidence_recommendations_024()` menjadi **8 rekomendasi aktif** (`6` prioritas tinggi, jendela puncak `17:00–21:59`, kontras reliabilitas Top-50 WAPE `17,25%` vs Tier Low `287,9%`, serta grafik 10 zona utama ber-WAPE tertinggi).
- **Enrichment Zona:** `AnalyticsDataLoader_024.enrich_with_zone_lookup_024()` menggabungkan `data/reference/taxi_zone_lookup_024.csv` menggunakan `keep_default_na=False` dan kunci ketat `PULocationID == LocationID`.

### 5.2 Integrasi Apache NiFi (`docs/nifi_setup_024.md`)
- **Process Group:** `urbanpulse_ingestion_024` (`ListFile_Raw_Parquet_024` → `FetchFile_Raw_Parquet_024` → `UpdateAttribute_Metadata_024` → `PutHDFS_UrbanPulse_Test_024` → `Log_Ingestion_Success_024` / `Log_Ingestion_Failure_024`).
- **Jaminan Keselamatan:** `FetchFile` menggunakan `Completion Strategy = None` (file lokal tidak pernah dihapus), `ListFile` melacak state timestamp (nol duplikasi), `PutHDFS` memakai `Conflict Resolution Strategy = ignore` dan `Compression codec = NONE` (byte Parquet 100% asli).
- **Hasil Uji Terisolasi:** Transfer ke `/urbanpulse_024/nifi_test_024/yellow_tripdata_2026-09_024.parquet` selesai dalam `7.27s` provenance dengan ukuran (`67,374,896` B) dan SHA-256 (`32d8d883...`) identik 100%. File produksi `/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet` sudah ada sejak Progress 2 dan dipertahankan utuh tanpa ditimpa.
- **Independensi Layanan:** NiFi dapat dihentikan kapan saja tanpa memengaruhi HDFS, Spark, maupun Dashboard.

---

## 6. Cara Menjalankan dan Memverifikasi Sistem

### 6.1 Menjalankan Dashboard Streamlit (Windows PowerShell)
```powershell
.\.venv\Scripts\streamlit.exe run dashboard/app_024.py
```
Buka `http://localhost:8501` di browser lokal.

### 6.2 Menjalankan & Menghentikan Apache NiFi 1.28.1 (WSL Ubuntu — On-Demand)
```bash
/home/ubuntu/nifi-1.28.1/bin/nifi.sh start   # UI di http://127.0.0.1:8090/nifi
/home/ubuntu/nifi-1.28.1/bin/nifi.sh status
/home/ubuntu/nifi-1.28.1/bin/nifi.sh stop    # Disarankan stop saat menjalankan job Spark berat
```

### 6.3 Verifikasi Read-Only HDFS (WSL Ubuntu — Aman, Tidak Menjalankan Job Berat)
```bash
hdfs dfs -ls -h /urbanpulse_024/raw/yellow_taxi/
hdfs dfs -ls -h /urbanpulse_024/nifi_test_024/
hdfs dfs -ls -h /urbanpulse_024/processed/yellow_taxi_024/
hdfs dfs -ls -h /urbanpulse_024/analytics/
```
