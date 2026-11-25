# Laporan Kesiapan Machine Learning & Rencana Implementasi — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Audit:** 25 November 2026 (`2026-11-25T01:21:59Z`)  
> **Mode:** **Audit & Perencanaan Read-Only** *(tidak ada training ML, unduhan data baru, perubahan pipeline, maupun perubahan dashboard)*  
> **Sumber Data Diaudit:** `hdfs://localhost:9000/urbanpulse_024/analytics/gold_demand_features_024` & [gold_layer_report_024.md](gold_layer_report_024.md)  
> **Ringkasan Metrik JSON:** [ml_readiness_audit_024.json](ml_readiness_audit_024.json)

---

## 1. Kesimpulan Kesiapan ML

**Status: SIAP UNTUK BASELINE FORECASTING (Spark MLlib) dengan data September 2026 saja.**

Dataset Yellow Taxi September 2026 telah memenuhi persyaratan proyek (data nyata dari satu tahun terakhir) tanpa perlu mengunduh bulan tambahan. Setelah periode *warm-up* 7 hari (`1–7 September 2026`), tersedia **`144,072` baris ML-ready (`23` hari penuh × `24` jam × `261` zona)** dengan **0 nilai null**, **bebas *data leakage***, serta sinyal autoregresif yang sangat kuat (`r = 0.962` untuk `lag_1h` dan `r = 0.954` untuk `lag_168h`). Volume ini sangat memadai untuk membangun, membandingkan, dan mengevaluasi model baseline *short-term demand forecasting* per zona per jam.

---

## 2. Kondisi Dataset & Kelengkapan Fitur (`gold_demand_features_024`)

### 2.1 Ringkasan Populasi Data ML
| Segmen Data | Rentang Waktu (`timestamp_ntz`) | Jumlah Hari | Jumlah Jam | Jumlah Baris (`×261` zona) | Total Pickup | Status Null |
| :--- | :--- | :-: | :-: | ---: | ---: | :--- |
| Panel Gold Penuh | `2026-09-01 00:00` → `2026-09-30 23:00` | 30 | 720 | `187,920` | `3,860,852` | Null hanya pada warm-up lag/rolling |
| Periode *Warm-Up* *(hanya konteks historis)* | `2026-09-01 00:00` → `2026-09-07 23:00` | 7 | 168 | `43,848` | `757,118` | Dikecualikan dari training/testing |
| **Populasi Siap ML (`is_ml_ready_full_history_024 = true`)** | **`2026-09-08 00:00` → `2026-09-30 23:00`** | **23** | **552** | **`144,072`** | **`3,103,734`** | **0 null di seluruh 26 kolom** |

> *Catatan Positif Periode Warm-Up:* Hari tersunyi sepanjang bulan adalah **Senin `2026-09-07` (`95,710` pickup — Labor Day)** yang jatuh di periode *warm-up* (`Sep 1–7`), sehingga tidak secara langsung menjadi baris target di set training; namun efeknya tetap perlu dicatat pada nilai `lag_168h` untuk Senin `2026-09-14` (`121,491` pickup).

### 2.2 Kelengkapan Fitur & Kekuatan Sinyal Prediktif (Aktual pada `144,072` Baris ML-Ready)
| Kelompok Fitur | Kolom di `gold_demand_features_024` | Null (ML-Ready) | Korelasi Pearson vs `pickup_count_024` | Status Leakage |
| :--- | :--- | :-: | :---: | :--- |
| **Target** | `pickup_count_024` | 0 | `1.0000` | — |
| **Lag Historis** | `lag_1h_pickup_count_024` | 0 | **`0.9622`** | Bebas (`t - 1 jam`) |
| | `lag_168h_pickup_count_024` | 0 | **`0.9544`** | Bebas (`t - 7 hari`) |
| | `lag_24h_pickup_count_024` | 0 | **`0.9253`** | Bebas (`t - 24 jam`) |
| **Rolling Historis** | `rolling_avg_24h_pickup_count_024` | 0 | **`0.7985`** | Bebas (`[t-24 .. t-1]`, eksklusif `t`) |
| | `rolling_avg_7d_pickup_count_024` | 0 | **`0.7850`** | Bebas (`[t-168 .. t-1]`, eksklusif `t`) |
| | `rolling_std_7d_pickup_count_024` | 0 | **`0.7711`** | Bebas (`[t-168 .. t-1]`, eksklusif `t`) |
| **Kalender & Siklis** | `pickup_hour_024`, `day_of_week_024`, `is_weekend_024`, `is_rush_hour_024`, `hour_sin_024`, `hour_cos_024`, `dow_sin_024`, `dow_cos_024` | 0 | Diketahui di muka | Bebas (kalender deterministik) |
| **Identitas Zona** | `PULocationID`, `is_known_pickup_zone_024` | 0 | Fitur kategorikal/statis | Bebas |

### 2.3 Distribusi Target `pickup_count_024` (Risiko *Zero-Inflation* & Skewness)
| Segmen Zona (`Sep 8–30`) | Baris | Total Pickup | % Baris Bernilai `0` | Mean | StdDev | Median | P90 | P99 | Max |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Seluruh 261 Zona** | `144,072` | `3,103,734` | **45.70%** | 21.54 | 59.03 | 1.0 | 69 | 294 | 789 |
| **Top-50 Zona Operasional** | `27,600` (`19.2%`) | `2,823,075` (**90.96%**) | **0.59%** | 102.29 | 99.82 | 73.0 | 231 | 451 | 789 |
| **211 Zona Volume Rendah** | `116,472` (`80.8%`) | `280,659` (`9.04%`) | **56.39%** | 2.41 | 6.14 | 0.0 | 7 | 30 | 183 |

> **Temuan Kunci Evaluasi:** `50` zona teratas menyumbang **90.96% permintaan nyata** dengan hampir nol sparsity (`0.59%`), sedangkan `211` zona sisanya didominasi nilai nol (`56.39%`). Oleh karena itu, evaluasi model **wajib dilaporkan secara terpisah** untuk: (a) **Seluruh 261 Zona** dan (b) **Top-50 Zona Operasional**.

---

## 3. Strategi Chronological Train / Validation / Test Split (Anti-Leakage)

**Aturan ketat:** Dilarang menggunakan *random split* karena akan membocorkan pola masa depan melalui fitur lag/rolling. Pembagian wajib murni kronologis dengan **Test Holdout tepat 7 hari penuh (1 siklus Senin–Minggu lengkap)**:

### 3.1 Skema Split yang Direkomendasikan
- **Tahap Model Selection & Hyperparameter Tuning (3-Way Chronological Split):**
  | Split | Periode (`timestamp_ntz`) | Hari | Jam | Baris | % ML-Ready | Rata-Rata Pickup/Zona-Jam | Cakupan Siklus |
  | :--- | :--- | :-: | :-: | ---: | ---: | ---: | :--- |
  | **Train (Tuning)** | `2026-09-08 00:00` → `2026-09-20 23:00` | 13 | 312 | `81,432` | 56.52% | `22.32` | 9 hari kerja + **2 akhir pekan penuh** |
  | **Validation (Tuning)** | `2026-09-21 00:00` → `2026-09-23 23:00` | 3 | 72 | `18,792` | 13.04% | `18.75` | Senin–Rabu (seleksi hyperparameter) |
  | **Test Holdout (Terkunci)** | `2026-09-24 00:00` → `2026-09-30 23:00` | **7** | **168** | **`43,848`** | **30.43%** | **`21.30`** | **1 minggu penuh (Kamis–Rabu, semua 7 hari terwakili)** |

- **Tahap Evaluasi Akhir (Final Refit 70/30 Chronological Split):**
  Setelah hyperparameter terbaik dipilih menggunakan Validation, model dilatih ulang pada seluruh **`Train Final (Sep 08–23: 16 hari, 100,224 baris / 69.57%, mean 21.65)`** lalu dievaluasi satu kali pada **`Test Holdout (Sep 24–30: 7 hari, 43,848 baris / 30.43%, mean 21.30)`**. Distribusi target Train Final vs Test Holdout sangat seimbang (selisih mean hanya `-1.6%`).

### 3.2 Benchmark Floor Non-ML (Aritmetika Langsung pada Test Holdout `Sep 24–30`)
Sebagai acuan batas minimum (*sanity floor*, dihitung tanpa training ML), setiap model Spark MLlib **harus mengalahkan** aturan persistensi naif berikut pada `Sep 24–30`:

| Aturan Persistensi Naif | MAE (All 261 Zona) | RMSE (All 261 Zona) | MAE (Top-50 Zona) | RMSE (Top-50 Zona) |
| :--- | ---: | ---: | ---: | ---: |
| **`lag_168h_pickup_count_024` (minggu lalu, jam sama)** | **4.8796** | **15.1874** | **19.7748** | **34.1088** |
| `lag_1h_pickup_count_024` (1 jam sebelumnya) | 5.6721 | 16.4549 | 23.7404 | 37.0528 |
| `lag_24h_pickup_count_024` (kemarin, jam sama) | 6.7492 | 22.7553 | — | — |
| `rolling_avg_7d_pickup_count_024` | 12.5495 | 36.2247 | — | — |

---

## 4. Rekomendasi Algoritma & Horizon Prediksi

### 4.1 Kejelasan Horizon Prediksi
- **Horizon Utama (1-Step-Ahead / Prediksi 1 Jam ke Depan):** Menggunakan seluruh fitur historis termasuk `lag_1h_pickup_count_024` dan `rolling_avg_24h_pickup_count_024`. Sangat relevan untuk **reposisi armada taksi operasional *near real-time***.
- **Eksperimen Pembanding Opsional (24h-Ahead / Perencanaan Harian):** Menghapus `lag_1h` dan `rolling_avg_24h`, hanya memakai `lag_24h`, `lag_168h`, `rolling_avg_7d`, dan fitur kalender agar transparan terhadap performa prediksi satu hari ke depan.

### 4.2 Algoritma yang Direkomendasikan (Spark MLlib — Ringan di `local[*]`)
1. **Benchmark 0 — Seasonal Naive (`lag_168h_pickup_count_024`):** Batas bawah non-ML (MAE `4.88`, RMSE `15.19`).
2. **Baseline 1 — `LinearRegression` (Ridge / ElasticNet):**
   - Konfigurasi awal: `regParam ∈ {0.01, 0.1, 1.0}`, `elasticNetParam ∈ {0.0, 0.5}`, fitur terstandardisasi (`StandardScaler`) + `OneHotEncoder` ringan untuk fitur kategorikal (`day_of_week_024`, opsional `PULocationID`).
   - Kelebihan: Sangat cepat, transparan secara koefisien, dan kuat karena korelasi linear lag sangat tinggi (`r > 0.95`).
3. **Model Baseline Utama — `RandomForestRegressor` (Spark MLlib):**
   - Konfigurasi awal realistis untuk RAM WSL: `numTrees ∈ {50, 100}`, `maxDepth ∈ {8, 10, 12}`, `minInstancesPerNode=5`, `seed=24`.
   - Kelebihan: Menangkap interaksi non-linear antara `pickup_hour`, `is_weekend`, dan lag historis tanpa mudah *overfit* pada data 23 hari; tahan terhadap skewness target.
4. **Model Pembanding Unggulan — `GBTRegressor` (Gradient-Boosted Trees, Spark MLlib):**
   - Konfigurasi awal: `maxIter ∈ {50, 80}`, `maxDepth ∈ {5, 6, 8}`, `stepSize ∈ {0.05, 0.1}`, `subsamplingRate=0.8`, `seed=24`.
   - Kelebihan: Umumnya memberikan MAE/RMSE terbaik pada data tabular time-series; tetap diawasi ketat terhadap *overfitting* melalui Validation (`Sep 21–23`).
- **Post-Processing Inferensi:** Terapkan `greatest(0.0, predicted_demand_024)` agar prediksi tidak pernah bernilai negatif pada zona sepi.

---

## 5. Metrik Evaluasi yang Ditetapkan

1. **MAE (*Mean Absolute Error*) — Metrik Utama:** Mudah diinterpretasikan secara operasional (rata-rata selisih jumlah pickup per zona per jam).
2. **RMSE (*Root Mean Squared Error*) — Metrik Utama:** Memberi penalti lebih besar pada kesalahan besar di jam puncak/zona padat (kritis untuk mencegah kekurangan armada).
3. **$R^2$ (*Coefficient of Determination*) — Metrik Pendukung:** Mengukur proporsi varians permintaan yang dijelaskan model.
4. **WAPE (*Weighted Absolute Percentage Error* = $\sum |y - \hat{y}| / \sum y$) — Pengganti MAPE:** Jauh lebih valid daripada MAPE karena `45.70%` baris bernilai `0` (MAPE tidak terdefinisi pada nilai 0).
5. **Evaluasi Tersegmentasi Wajib:** Laporkan seluruh metrik pada (a) **All 261 Zones**, (b) **Top-50 Operational Zones** (`90.96%` demand), (c) **Peak Hours (`17–21`) vs Off-Peak**, serta (d) **Weekday vs Weekend**.

---

## 6. Risiko & Keterbatasan Model (Data 1 Bulan — September 2026)

1. **Tidak Menangkap Musiman Tahunan (*Annual Seasonality*):** Model hanya mempelajari dinamika bulan September (awal musim gugur NYC), sehingga tidak dapat diekstrapolasi langsung ke musim dingin/salju, libur Natal/Tahun Baru, atau puncak wisata musim panas.
2. **Keterbatasan Efek Hari Libur & Event Kota:** Hanya terdapat 1 hari libur federal (Labor Day `2026-09-07` pada periode warm-up) dan potensi lonjakan event lokal pertengahan bulan (`Sep 10–19`, puncak `154,599` pickup pada Kamis `Sep 17`) tanpa indikator kalender event/libur eksternal.
3. **Tidak Ada Fitur Eksogen (Cuaca & Gangguan Transit):** Hujan deras, suhu ekstrem, atau gangguan NYC Subway memicu lonjakan taksi mendadak yang belum terwakili oleh fitur lag saja.
4. **Sparsity Tinggi pada Zona Ekor (`Low >150`):** `111` zona bervolume rendah memiliki `82.68%` nilai nol, sehingga kesalahan relatif pada zona kecil cenderung tinggi dan tidak sebanding dengan nilai bisnis zona inti.
5. **Batas Memori WSL Local Mode:** Training `RandomForestRegressor` / `GBTRegressor` dengan `maxDepth` terlalu dalam atau `OneHotEncoder` 261 dimensi dapat memicu tekanan memori pada `spark.driver.memory=2g` → mitigasi: gunakan fitur lag/rolling per zona + `is_known_pickup_zone_024` (tanpa OHE 261 kolom untuk model pohon) dan batasi `maxDepth <= 12`.

---

## 7. Rencana Implementasi & Evaluasi Model (Tahap Berikutnya — Belum Dijalankan)

1. **Langkah 1 — Persiapan Referensi Zona (Opsional namun Sangat Disarankan):**
   Tambahkan `taxi_zone_lookup.csv` ke `data/reference/` dan `/urbanpulse_024/raw/reference/` agar hasil evaluasi error per zona dan output prediksi dapat menampilkan `Zone` dan `Borough` resmi.
2. **Langkah 2 — Pembangunan Pipeline Training (`ml/training/train_demand_models_024.py`):**
   - Membaca `gold_demand_features_024` dari HDFS (`is_ml_ready_full_history_024 = true`).
   - Menerapkan chronological split (`Train Sep 8–20`, `Val Sep 21–23`, `Test Sep 24–30`).
   - Melatih `LinearRegression`, `RandomForestRegressor`, dan `GBTRegressor`, memilih model terbaik berdasar Validation RMSE/MAE, lalu melakukan refit pada `Sep 8–23` dan evaluasi final pada `Test Holdout Sep 24–30`.
   - Menyimpan artefak model terbaik ke `/urbanpulse_024/models/<best_model>_024` (HDFS) dan ringkasan ke `ml/models/`.
3. **Langkah 3 — Evaluasi & Analisis Error (`ml/evaluation/evaluate_demand_models_024.py`):**
   - Membandingkan seluruh model vs benchmark naif `lag_168h` (MAE, RMSE, $R^2$, WAPE — global & Top-50).
   - Menghasilkan analisis error per jam, per hari, dan Top-10 zona serta laporan `reports/ml_evaluation_report_024.md` & `ml/evaluation/model_metrics_024.json`.
4. **Langkah 4 — Ekspor Serving Layer untuk Dashboard (Fase Terpisah):**
   Menyusun output `data/analytics/` sesuai kontrak [dashboard_architecture_024.md](../docs/dashboard_architecture_024.md) (`hourly_demand_024.parquet`, `zone_summary_024.parquet`, `demand_predictions_024.parquet`, `model_metrics_024.json`, `recommendations_024.parquet`).

---

## 8. Daftar Kebutuhan yang Masih Belum Tersedia (Terverifikasi di Workspace & HDFS)

| No | Kebutuhan | Status Aktual | Prioritas | Dampak |
| :-: | :--- | :--- | :---: | :--- |
| 1 | **`taxi_zone_lookup.csv`** (`data/reference/` & `/urbanpulse_024/raw/reference/`) | **Belum tersedia** (kosong) | **Tinggi** | Dibutuhkan untuk memetakan `PULocationID` → nama `Zone`, `Borough`, `service_zone` agar hasil evaluasi ML, laporan akhir, dan dashboard dapat dibaca manusia tanpa mengarang nama zona. |
| 2 | **Geometri Zona NYC (`taxi_zones_024.geojson` / Shapefile)** | **Belum tersedia** | **Sedang** *(Opsional)* | Dibutuhkan hanya jika ingin menampilkan peta choropleth penuh NYC di dashboard; tanpa ini dashboard tetap berjalan dengan grafik/centroid alternatif. |
| 3 | **Script Training & Evaluasi ML (`ml/training/`, `ml/evaluation/`)** | **Belum dibuat** (`.gitkeep`) | **Tinggi** | Akan dibuat pada tahap implementasi ML setelah rencana ini disetujui. |
| 4 | **Artefak Model di HDFS (`/urbanpulse_024/models/`)** | **Kosong** | **Tinggi** | Akan terisi setelah training Spark MLlib dijalankan. |
| 5 | **Output Serving Layer Lokal (`data/analytics/`)** | **Kosong** (`.gitkeep`) | **Sedang** | Akan diekspor setelah tahap ML & agregasi BI siap dihubungkan ke dashboard Streamlit. |
