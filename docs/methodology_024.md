# Methodology — UrbanPulse: Intelligent Urban Taxi Analytics (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Pembaruan:** 25 November 2026  
> **Status Metodologi:** Seluruh bagian (Bagian 1 s.d. Bagian 9) mendokumentasikan metodologi yang **telah diimplementasikan dan terverifikasi secara empiris** dari Progress 1 hingga Progress 6 pada dataset NYC Yellow Taxi September 2026 — meliputi ingestion (direct HDFS & otomasi Apache NiFi teruji), validasi Raw, Silver v2, Gold Layer, Spark MLlib forecasting, Serving Layer, serta Dashboard Analitik.

---

## 1. Sumber dan Karakteristik Dataset

- **Sumber Resmi:** NYC Taxi and Limousine Commission (TLC) — Yellow Taxi Trip Record Data (`https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`), format **Apache Parquet**.
- **Kesesuaian Persyaratan Waktu:** Persyaratan akademik proyek menetapkan penggunaan data nyata yang berasal dari **satu tahun terakhir** (bukan kewajiban 12 bulan penuh). Dataset **September 2026** (`data/raw/yellow_tripdata_2026-09_024.parquet`, `67,374,896` byte, SHA-256 `32D8D883E3FB9C0D681F4D47B205AFE0FCB2A4830739673B4DD3BAFBAC9B0086`) memenuhi ketentuan tersebut dengan **`3,876,044` transaksi** dan **`21` kolom asli**.
- **Karakteristik Kunci Raw Layer:**
  - `0` duplikat persis seluruh kolom dan `0` nilai `NaN`.
  - ***Structural Missingness* (Flex Fare / E-Hail):** Tepat `1,190,413` baris (`30.7121%`) bernilai `NULL` pada 5 kolom (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`), yang cocok 100% dengan `payment_type = 0`.
- **Data Referensi Zona:** `data/reference/taxi_zone_lookup_024.csv` (`265` baris, `4` kolom asli: `LocationID`, `Borough`, `Zone`, `service_zone`) tervalidasi 100% cocok dengan `261` zona aktif Gold. Penggabungan wajib memakai **`PULocationID == LocationID`** (bukan nama `Zone` yang memiliki duplikat lintas ID) dan `keep_default_na=False` agar string literal `"N/A"` untuk `LocationID 264/265` tetap utuh.

---

## 2. Metode Data Ingestion (Direct HDFS & Otomasi Apache NiFi)

1. **Ingestion Produksi Tahap Awal (Progress 2):** File Parquet September 2026 diunggah langsung ke HDFS Raw Layer (`hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet`) secara *immutable* (*read-only*) dengan integritas ukuran & SHA-256 diverifikasi 100%.
2. **Integrasi Otomasi Apache NiFi 1.28.1 (Progress 6):** Untuk melengkapi lapisan ingestion otomatis sesuai arsitektur Big Data tanpa mengganggu data produksi eksisting, dipasang **Apache NiFi `1.28.1`** (kompatibel native OpenJDK `17.0.20`) secara standalone di `/home/ubuntu/nifi-1.28.1`.
   - **Pola Desain Aman (`urbanpulse_ingestion_024`):** Memilih pola **`ListFile` → `FetchFile` (`Completion Strategy = None`) → `UpdateAttribute` → `PutHDFS` (`Conflict Resolution = ignore`, `Compression = NONE`) → `LogAttribute` (Success/Failure)** alih-alih `GetFile` untuk mencegah penghapusan file lokal sekaligus mencegah ingestion berulang.
   - **Verifikasi Terisolasi:** Pengujian dijalankan ke direktori terpisah `/urbanpulse_024/nifi_test_024/` dan terbukti menghasilkan transfer bit-identik (`67,374,896` B, SHA-256 100% cocok, durasi provenance `7.27` detik, 0 duplikasi). File produksi di `/urbanpulse_024/raw/yellow_taxi/` sengaja tidak ditimpa karena sudah ada dan terverifikasi identik sejak Progress 2.

---

## 3. Validasi Kualitas Data (Raw Layer Profiling)

Validasi *read-only* (`spark/processing/validate_raw_yellow_taxi_024.py`) menggunakan *single-pass aggregation* untuk memetakan skema, null/NaN, duplikasi, statistik kuantil (`percentile_approx`), dan anomali domain:
- **Temporal:** `22` baris pickup di luar September 2026 (`2009-01-01` s.d. `2026-10-07`).
- **Durasi & Jarak:** `48,202` durasi 0 menit, `852` durasi `>180` menit (maks `7,798.73` mnt), `147,158` jarak 0 mil, `125` jarak `>100` mil (maks `232,644.80` mil, `stddev = 463.43`), dan `1,168` kecepatan implied `>80` mph.
- **Moneter:** `14,688` `total_amount < 0` (min `-1,801.00` USD), `480` `total_amount = 0`, `4` `total_amount > 1,000` USD (maks `10,003.70` USD).

---

## 4. Metodologi Cleaning Berbasis Audit (Silver v1 → Silver v2)

### 4.1 Alasan Audit Dampak Sebelum Finalisasi Cleaning
Menghapus seluruh baris yang mengandung `NULL` atau outlier telemetri secara serampangan akan menimbulkan **bias pengurangan permintaan (*under-counting demand*)**:
1. Menghapus `passenger_count IS NULL` akan menghilangkan **`30.71%` (`1,190,413` baris)** permintaan Flex Fare nyata.
2. Pada Silver v1 (`3,857,325` baris, `18,719` drop), audit `audit_silver_cleaning_impact_024.py` membuktikan bahwa **`88.66%` (`13,060/14,730`) transaksi negatif** memiliki pasangan transaksi positif identik (*mirror-reversal pair*) sehingga penghapusan baris negatif mencegah *double-counting*. Sebaliknya, sekitar **`3,527` baris** yang terhapus di v1 tetap merupakan event pickup berbayar valid (termasuk `805` transaksi `VendorID = 6` Flex Fare, `839` anomali durasi meter `>180` mnt, `1,158` kecepatan GPS `>80` mph, dan `64` tarif rute luar kota `500–2,000` USD).

### 4.2 Kebijakan Silver Layer v2 (`3,860,852` Baris, `49` Kolom)
- **Hard Removal Bebas Overlap (`15,192` baris / `0.3919%`):** Hanya mengeluarkan `total_amount <= 0` (`15,168`), di luar September 2026 (`22`), outlier korup mutlak `> $2,000` (`2`), lokasi invalid (`0`), dan durasi/jarak negatif (`0`).
- **Preserved + Anomaly Flagging + Validated Metrics:** Anomali telemetri dan tarif tinggi `500–2,000` USD dipertahankan sebagai event pickup disertai flag eksplisit `_024`, sedangkan kolom `*_valid_*_024` diisi `NULL` pada baris anomali agar rata-rata statistik tetap bersih.
- **Penandaan Reversal (`has_negative_reversal_pair_024`):** Menandai `13,018` baris positif berpasangan reversal negatif untuk pemisahan akuntansi pendapatan di Gold Layer.

---

## 5. Feature Engineering & Agregasi Gold Layer (Progress 3)

- **Pemisahan Demand vs Pendapatan Bersih:** `pickup_count_024` memakai seluruh `3,860,852` transaksi Silver v2 (dengan pendamping `non_reversed_pickup_count_024 = 3,847,834`), sedangkan `net_settled_revenue_usd_024` (`USD 124,583,114.07`) dan rata-rata ekonomi mengecualikan `13,018` reversal pairs serta menggunakan kolom `*_valid_*_024`.
- **Normalisasi Hari Kalender:** Perbandingan hari kerja (`22` hari) vs akhir pekan (`8` hari) dinormalisasi terhadap frekuensi kalender September 2026 (Selasa & Rabu muncul `5×`, hari lain `4×`).
- **Panel Time-Series Bebas Leakage & `TIMESTAMP_NTZ`:** Grid `261 zona × 720 jam = 187,920` slot (`86,036` zero-filled) dibangun dengan `CAST(... AS TIMESTAMP_NTZ)` untuk menghilangkan shift `+7 jam` zona waktu WIB. Fitur lag (`lag_1h`, `lag_24h`, `lag_168h`) dan rolling (`rolling_avg_24h`, `rolling_avg_7d`, `rolling_std_7d`) dibatasi ketat pada masa lalu (`rowsBetween(-24,-1)` dan `rowsBetween(-168,-1)`). Sebanyak `43,848` baris awal (`1–7 Sep`) dijadikan *warm-up*, menyisakan **`144,072` baris ML-ready (`8–30 Sep`)** dengan `0` null dan lolos spot-check *zero-leakage* (`PULocationID = 237`, `2026-09-15 18:00:00`: `260.1012 == 260.1012`).

---

## 6. Audit Kesiapan ML & Benchmark Naif (Progress 4)

Audit *read-only* (`reports/ml_readiness_and_plan_024.md`) mengonfirmasi sinyal autoregresif sangat kuat (`lag_1h r=0.9622`, `lag_168h r=0.9544`, `lag_24h r=0.9253`) serta ketimpangan sparsity tinggi antara All 261 Zona (`45.70%` nilai `0`) dan Top-50 Zona Operasional (`90.96%` demand, hanya `0.59%` nol). Benchmark persistensi naif terbaik adalah **`lag_168h_pickup_count_024`** (Test `Sep 24–30`: All 261 MAE **`4.8796`**, RMSE **`15.1874`**; Top-50 MAE **`19.7748`**, RMSE **`34.1088`**).

---

## 7. Metodologi Pemodelan Forecasting (Terimplementasi pada Progress 5)

Seluruh pelatihan dieksekusi di `ml/training/train_demand_model_024.py` menggunakan **Apache Spark `4.0.4` MLlib** (`local[*]`) dengan proteksi **zero data leakage**:

1. **Chronological Split Ketat (Tanpa Random Split):**
   - **Warm-up (Dikeluarkan):** `2026-09-01` s.d. `2026-09-07` (`43,848` baris).
   - **Train (Seleksi):** `2026-09-08` s.d. `2026-09-20` (`13` hari, `81,432` baris).
   - **Validation (Seleksi Model/Hyperparameter):** `2026-09-21` s.d. `2026-09-23` (`3` hari, `18,792` baris).
   - **Final Refit Train (1×):** `2026-09-08` s.d. `2026-09-23` (`16` hari, `100,224` baris).
   - **Locked Test Holdout (Evaluasi Objektif):** `2026-09-24` s.d. `2026-09-30` (`7` hari penuh, `43,848` baris, `933,894` pickup aktual).
2. **Desain Fitur & Transformasi di Dalam `Pipeline` (Fit Hanya pada Split Latih):**
   - **15 fitur numerik** (`lag_1h`, `lag_24h`, `lag_168h`, `rolling_avg_24h`, `rolling_avg_7d`, `rolling_std_7d`, `pickup_hour`, `day_of_week`, `is_weekend_num`, `is_rush_hour_num`, `hour_sin/cos`, `dow_sin/cos`, `is_known_pickup_zone_num`) + **One-Hot Encoding 261 zona** (`PULocationID` string → `StringIndexer(handleInvalid="keep")` → `OneHotEncoder(dropLast=True)`) = **`276` dimensi fitur**.
   - Kolom target/proxy kontemporer (`pickup_count_024`, `non_reversed_pickup_count_024`, `is_observed_in_silver_024`) dan ID numerik mentah (`PULocationID`) dikeluarkan secara eksplisit.
   - `StandardScaler(withMean=False, withStd=True)` dipasang pada keluarga linear agar regularisasi L2 adil tanpa merusak *sparsity*.
3. **Kandidat Model & Dasar Pemilihan:**
   - Enam kandidat MLlib dilatih (`ridge_reg_0p1_024`, `ridge_reg_1p0_024`, `random_forest_d8_t40_024`, `random_forest_d10_t60_024`, `gbt_d5_i40_024`, `gbt_d6_i60_024`) dan dibandingkan terhadap `naive_lag_168h_024` pada Validation Set.
   - **`ridge_reg_0p1_024` (`LinearRegression`, `regParam=0.1`, `elasticNetParam=0.0`, `maxIter=100`) terpilih** karena menghasilkan Validation All-261 RMSE terendah (`12.0061`, `-32.80%` vs Naive), unggul di seluruh metrik Top-50 zona operasional (MAE `16.6637`, RMSE `26.9129`, WAPE `18.41%`), menangkap struktur autoregresif linear + efek tetap zona tanpa distorsi *binning* pohon (`maxBins=32`), serta 4,6× lebih cepat dilatih (`6.04s`; refit final `2.11s`).
4. **Post-Processing & Band Ketidakpastian Bebas Leakage:**
   - Prediksi dibatasi non-negatif: `predicted_demand_024 = max(0.0, raw_prediction_024)`.
   - Interval estimasi dashboard (`lower_bound_024`, `upper_bound_024`) diturunkan murni dari Validation RMSE: `± 1.28 × 12.0061 = ± 15.3678` (batas bawah di-clip ke `0.0`).

---

## 8. Metodologi & Hasil Evaluasi Empiris Model (Progress 5)

Evaluasi independen (`ml/evaluation/evaluate_demand_model_024.py`, `31.99s`) memuat ulang `ml/models/ridge_reg_0p1_024/` dan menerapkan **MAE**, **RMSE**, **$R^2$**, serta **WAPE ($\sum |y - \hat{y}| / \sum y$)** sebagai pengganti MAPE (karena `45.70%` target bernilai `0`):

| Cakupan Evaluasi (`Test Holdout Sep 24–30`) | Baris | Model MAE | Model RMSE | Impr. RMSE vs Naive | Model $R^2$ | Model WAPE |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **All 261 Zones** | `43,848` | **`4.5495`** | **`12.2597`** | **`-19.28%`** | **`0.9564`** | **`21.36%`** |
| **Top 50 Operational Zones (`91.0%` volume)** | `8,400` | **`17.4498`** | **`27.4563`** | **`-19.50%`** | **`0.9244`** | **`17.25%`** |
| **Peak Hours (`17:00–21:00`)** | `9,135` | `5.7362` | `15.4971` | **`-28.78%`** | `0.9623` | `17.31%` |
| **Weekday / Weekend** | `31,320` / `12,528` | `4.3394` / `5.0748` | `11.9834` / `12.9246` | **`-23.06%`** / **`-8.80%`** | `0.9606` / `0.9437` | `20.21%` / `24.33%` |
| **Tier `Low (>150)` (`0.53%` volume)** | `18,648` | `0.7611` | `1.2128` | `+55.13%` *(zero-inflated)* | `-2.0229` | `287.94%` |

- **Interpretabilitas Koefisien Terstandardisasi (`intercept = 3.1891`):** Prediktor terkuat adalah `lag_1h_pickup_count_024` (`+28.88`), `lag_168h_pickup_count_024` (`+20.21`), dan `lag_24h_pickup_count_024` (`+10.39`), diseimbangkan oleh `rolling_std_7d` (`-8.30`) dan `rolling_avg_7d` (`-8.14`) sebagai peredam *over-shooting* / *mean-reversion*.
- **Konsistensi Zona:** Sebanyak **`45/50` (90%) zona Top-50** mengungguli RMSE baseline Naive, dengan perbaikan terbesar di koridor bisnis `Midtown Center` (`-44.4%`) dan `Midtown East` (`-42.7%`).

---

## 9. Metodologi Serving Layer, Dashboard, & Keterbatasan Keseluruhan

### 9.1 Metodologi Serving & Dukungan Keputusan Berbasis Bukti
- **Pemisahan Komputasi & Visualisasi:** Dashboard Streamlit (`v1.0.0-gold-ml`) tidak pernah membaca `data/raw/` (64.3 MiB, 3.87 juta baris), melainkan mengonsumsi 6 berkas ringan di `data/analytics/` (`< 1.4 MB` total) yang diperkaya metadata `taxi_zone_lookup_024.csv`.
- **Rekomendasi Berbasis Bukti (`build_evidence_recommendations_024`):** Seluruh 8 kartu rekomendasi armada di halaman `Rekomendasi Armada` dihitung deterministik dari angka aktual Gold Layer dan hasil evaluasi `ridge_reg_0p1_024` (tanpa angka fiktif), disertai pemisahan eksplisit antara *Evaluasi Test Holdout* dan proyeksi masa depan.

### 9.2 Keterbatasan Metodologis & Teknis
1. **Cakupan Temporal 1 Bulan (September 2026):** Model menangkap pola harian & mingguan secara kuat, tetapi belum mencakup musiman tahunan (*annual seasonality*) maupun hari libur besar lintas bulan.
2. **Absennya Variabel Eksogen:** Belum tersedia fitur cuaca historis NYC, jadwal kedatangan penerbangan (`JFK`/`LGA`), maupun kalender acara kota/gangguan transit.
3. **Zero-Inflation pada Zona Volume Sangat Rendah (`Low >150`, Staten Island, Bronx):** Intercept linear global + clipping non-negatif membuat error relatif membesar pada zona yang hampir selalu nol (`0.53%` volume); pengembangan lanjutan disarankan memakai model *two-stage/hurdle* atau Poisson/Tweedie.
4. **Horizon 1-Step-Ahead:** Ketergantungan tinggi pada `lag_1h` berarti prediksi multi-jam ke depan (>1 jam) memerlukan pendekatan *recursive* atau model *direct multi-horizon* terpisah.
5. **Keterbatasan Visual & Operasional Lokal:** Peta choropleth poligon NYC belum aktif (GeoJSON opsional belum ditambahkan), dan NiFi Web UI dijalankan pada HTTP *localhost-only* (`127.0.0.1:8090`) untuk kemudahan demonstrasi akademik lokal (wajib HTTPS + autentikasi untuk jaringan produksi).
