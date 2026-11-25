# UrbanPulse — Laporan Training & Evaluasi Model Machine Learning (`_024`)

## 1. Ringkasan Eksekutif

Tahap training dan evaluasi Machine Learning proyek **UrbanPulse – Intelligent Urban Taxi Analytics** telah dieksekusi menggunakan **Apache Spark 4.0.4 MLlib** pada mode lokal (`local[*]`) dengan membaca langsung panel fitur Gold Layer (`gold_demand_features_024`) dari HDFS tanpa mengubah lapisan Raw, Silver, maupun Gold.

- **Model Terpilih:** `ridge_reg_0p1_024` — **Ridge Regression (`LinearRegression`, `regParam=0.1`, `elasticNetParam=0.0`, `maxIter=100`)** di dalam `Pipeline` Spark MLlib (`StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler(withMean=False)` → `LinearRegression`).
- **Dasar Seleksi:** Dipilih secara ketat berdasarkan **Validation Set (`2026-09-21` s.d. `2026-09-23`, 18.792 baris)** dengan RMSE All-261 terendah (`12.0061`, turun `-32.80%` dibanding baseline Naive `lag_168h`), sekaligus unggul di **seluruh metrik zona operasional Top 50** (MAE `16.6637`, RMSE `26.9129`, WAPE `18.41%`).
- **Performa Final pada Locked Test Holdout (`2026-09-24` s.d. `2026-09-30`, 43.848 baris, 933.894 pickup aktual):**
  - **All 261 Zones:** MAE = **`4.5495`** (vs Naive `4.8796`, perbaikan **`-6.76%`**), RMSE = **`12.2597`** (vs Naive `15.1874`, perbaikan **`-19.28%`**), $R^2$ = **`0.9564`**, WAPE = **`21.36%`** (vs Naive `22.91%`, turun **`-1.55 pp`**).
  - **Top 50 Operational Zones (91,0% volume pickup Test):** MAE = **`17.4498`** (vs Naive `19.7748`, perbaikan **`-11.76%`**), RMSE = **`27.4563`** (vs Naive `34.1088`, perbaikan **`-19.50%`**), $R^2$ = **`0.9244`**, WAPE = **`17.25%`** (vs Naive `19.55%`, turun **`-2.30 pp`**). Sebanyak **45 dari 50 zona teratas (90%)** mengungguli RMSE baseline Naive.
  - **Jam Puncak (`17:00–21:00`):** RMSE turun **`-28.78%`** dibanding baseline Naive (`15.4971` vs `21.7600`) dengan $R^2$ = `0.9623`.

### Metadata Eksekusi & Artefak Output

| Komponen | Nilai / Lokasi |
|---|---|
| Spark & Runtime | Apache Spark `4.0.4`, Java `17.0.20`, Python `3.12.3` (`numpy 1.26.4`), `local[*]` |
| Input Gold HDFS (Read-Only) | `hdfs://localhost:9000/urbanpulse_024/analytics/gold_demand_features_024` |
| Referensi Zona Resmi | `data/reference/taxi_zone_lookup_024.csv` (265 zona, 100% match pada 261 zona aktif) |
| Script Training | `ml/training/train_demand_model_024.py` (`141.37` detik total, refit final `2.11` detik) |
| Script Evaluasi Independen | `ml/evaluation/evaluate_demand_model_024.py` (`31.99` detik) |
| Model Tersimpan (`PipelineModel`) | `ml/models/ridge_reg_0p1_024/` (reload terverifikasi) |
| Metadata Run Training | `ml/models/training_run_metadata_024.json` |
| Prediksi Test Holdout (Parquet) | `data/analytics/demand_predictions_024.parquet/` (`43.848` baris, `931.496` bytes) |
| Metrik Ringkas (Serving Contract) | `data/analytics/model_metrics_024.json` |
| Ringkasan Evaluasi Lengkap (JSON) | `reports/ml_evaluation_summary_024.json` |

---

## 2. Metodologi & Proteksi Anti-Leakage

Pipeline dibangun dengan prinsip **zero data leakage** dan **chronological integrity**:

1. **Pre-Training Integrity Assertions (Terverifikasi Otomatis):**
   - Filter ketat `is_ml_ready_full_history_024 = true` menghasilkan tepat **`144.072` baris** (`261 zona × 23 hari × 24 jam`, periode `2026-09-08` s.d. `2026-09-30`).
   - Total nilai `NULL` pada target dan seluruh kolom fitur = **`0`**.
   - Verifikasi batas waktu memastikan tidak ada tumpang tindih temporal antara Train, Validation, dan Test (`max_ts(Train) < min_ts(Val)` dan `max_ts(Val) < min_ts(Test)`).
2. **Pemisahan Kolom Leakage & Target:**
   - Target prediksi: `pickup_count_024` (di-cast ke `label_024` bertipe `double`).
   - Kolom yang **dikeluarkan secara eksplisit** dari vektor fitur: `pickup_count_024`, `non_reversed_pickup_count_024` (target-proxy kontemporer), `is_observed_in_silver_024` (indikator kontemporer `pickup_count_024 > 0`), `PULocationID` mentah (mencegah asumsi ordinalitas numerik ID zona), `pickup_ts_hour_024`, `pickup_date_024`, `period_024`, `day_name_024`, `day_type_024`, `time_of_day_bucket_024`, dan `is_ml_ready_full_history_024`.
3. **Transformasi di Dalam `Pipeline` (Fit Hanya pada Training Split):**
   - `PULocationID` di-cast menjadi string (`pu_location_str_024`) → `StringIndexer(handleInvalid="keep")` → `OneHotEncoder(dropLast=True)` sehingga seluruh 261 zona direpresentasikan sebagai dummy kategorikal bebas bias ordinal.
   - Untuk keluarga linear (Ridge), `StandardScaler(withMean=False, withStd=True)` dipasang setelah `VectorAssembler` agar representasi vektor sparse tetap terjaga dan regularisasi L2 adil lintas skala fitur.
4. **Post-Processing & Interval Ketidakpastian Bebas Leakage:**
   - Prediksi non-negatif diterapkan secara konsisten: `predicted_demand_024 = max(0.0, raw_prediction_024)`.
   - Interval prediksi untuk kontrak dashboard (`lower_bound_024`, `upper_bound_024`) dihitung dari **Validation RMSE** saja (`± 1.28 × 12.0061 = ± 15.3678`, batas bawah di-clip ke `0.0`) sehingga informasi Test Holdout tidak pernah bocor ke parameter output.

---

## 3. Feature Selection & Interpretabilitas Model

Sebanyak **15 fitur numerik** digabungkan dengan **One-Hot Encoding zona (`261` kategori, `dropLast=True`)** menghasilkan vektor fitur berdimensi **`276`**.

### Fitur Numerik & Koefisien Terstandardisasi (`ridge_reg_0p1_024` Final Refit)

Intercept model final = **`3.189081`**. Berikut seluruh koefisien terstandardisasi fitur numerik (diurutkan berdasarkan magnitudo absolut):

| Peringkat | Fitur (`_024`) | Kategori | Koefisien Terstandardisasi | Interpretasi Domain |
|---:|---|---|---:|---|
| 1 | `lag_1h_pickup_count_024` | Lag jangka pendek | **`+28.8783`** | Momentum permintaan 1 jam sebelumnya (prediktor dominan) |
| 2 | `lag_168h_pickup_count_024` | Lag musiman mingguan | **`+20.2142`** | Pola jam & hari yang sama pada minggu lalu |
| 3 | `lag_24h_pickup_count_024` | Lag musiman harian | **`+10.3908`** | Pola jam yang sama pada hari sebelumnya |
| 4 | `rolling_std_7d_pickup_count_024` | Volatilitas historis | **`-8.3005`** | Meredam over-shooting pada zona/jam bervolatilitas tinggi |
| 5 | `rolling_avg_7d_pickup_count_024` | Baseline 7 hari | **`-8.1394`** | Koreksi mean-reversion terhadap kombinasi lag positif |
| 6 | `rolling_avg_24h_pickup_count_024` | Baseline 24 jam | `+1.0957` | Level tren harian terkini |
| 7 | `is_weekend_num_024` | Kalender | `+0.9620` | Penyesuaian rezim akhir pekan |
| 8 | `hour_sin_024` | Siklikal jam | `+0.9084` | Fase sinusoidal siklus 24 jam |
| 9 | `day_of_week_024` | Kalender | `-0.8594` | Pergeseran progresi hari dalam seminggu |
| 10 | `pickup_hour_024` | Kalender | `+0.8478` | Tren linear intensitas aktivitas harian |
| 11 | `is_rush_hour_num_024` | Kalender | `+0.8018` | Penguatan jam sibuk pagi/sore |
| 12 | `dow_cos_024` | Siklikal mingguan | `-0.7269` | Komponen kosinus siklus 7 hari |
| 13 | `hour_cos_024` | Siklikal jam | `-0.7103` | Komponen kosinus siklus 24 jam |
| 14 | `is_known_pickup_zone_num_024` | Metadata zona | `+0.2522` | Pembeda zona terdaftar vs Unknown (`264`/`265`) |
| 15 | `dow_sin_024` | Siklikal mingguan | `-0.1296` | Komponen sinus siklus 7 hari |

---

## 4. Strategi Chronological Split

Tidak ada random split yang digunakan karena sifat deret waktu per zona.

| Split | Periode Tanggal | Durasi | Jumlah Baris | Fungsi |
|---|---|---:|---:|---|
| Warm-up (Dikeluarkan) | `2026-09-01` s.d. `2026-09-07` | 7 hari | `43.848` | Pemanasan fitur `lag_168h` & rolling 7 hari |
| **Train (Selection)** | `2026-09-08` s.d. `2026-09-20` | 13 hari | `81.432` | Pelatihan kandidat model untuk seleksi |
| **Validation** | `2026-09-21` s.d. `2026-09-23` | 3 hari | `18.792` | Perbandingan & pemilihan model/hyperparameter |
| **Final Refit Train** | `2026-09-08` s.d. `2026-09-23` | 16 hari | `100.224` | Pelatihan ulang 1× model terpilih sebelum Test |
| **Test Holdout (Locked)** | `2026-09-24` s.d. `2026-09-30` | 7 hari | `43.848` | Evaluasi final objektif 1 minggu penuh |

---

## 5. Perbandingan Model pada Validation Set (`2026-09-21` s.d. `2026-09-23`)

Seluruh kandidat dilatih secara berurutan pada `Train (81.432 baris)` dan dievaluasi pada `Validation (18.792 baris)` tanpa menyentuh Test Holdout.

| Model ID (`_024`) | Keluarga Model | All 261 MAE | All 261 RMSE | All 261 $R^2$ | All 261 WAPE (%) | Top 50 MAE | Top 50 RMSE | Top 50 WAPE (%) | Waktu Fit (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `naive_lag_168h_024` | Seasonal Naive Baseline | `5.0031` | `17.8666` | `0.8885` | `26.68%` | `21.2731` | `40.4432` | `23.51%` | `0.00` |
| **`ridge_reg_0p1_024`** *(Terpilih)* | **Ridge Regression (`reg=0.1`)** | `4.3635` | **`12.0061`** | **`0.9496`** | `23.27%` | **`16.6637`** | **`26.9129`** | **`18.41%`** | **`6.04`** |
| `ridge_reg_1p0_024` | Ridge Regression (`reg=1.0`) | `4.4459` | `12.0681` | `0.9491` | `23.71%` | `16.8911` | `27.0509` | `18.66%` | `4.05` |
| `random_forest_d8_t40_024` | Random Forest (`d=8, t=40`) | `4.3630` | `12.9440` | `0.9415` | `23.27%` | `18.3432` | `29.2612` | `20.27%` | `14.02` |
| `random_forest_d10_t60_024` | Random Forest (`d=10, t=60`) | **`4.2620`** | `12.7024` | `0.9436` | **`22.73%`** | `18.0248` | `28.7192` | `19.92%` | `27.79` |
| `gbt_d5_i40_024` | GBT (`d=5, iter=40`) | `4.7809` | `14.4690` | `0.9268` | `25.49%` | `19.7930` | `32.6362` | `21.87%` | `28.10` |
| `gbt_d6_i60_024` | GBT (`d=6, iter=60`) | `4.5398` | `13.9893` | `0.9316` | `24.21%` | `19.0335` | `31.5815` | `21.03%` | `32.85` |

### Analisis Trade-off Multi-Metrik & Alasan Pemilihan `ridge_reg_0p1_024`

1. **Seluruh 6 model MLlib mengungguli Naive `lag_168h`** pada semua metrik validasi (MAE, RMSE, $R^2$, WAPE, serta Top-50 RMSE).
2. **Keunggulan Struktural Ridge pada Demand Taksi:** Permintaan per jam memiliki struktur autoregresif linear yang sangat kuat terhadap `lag_1h`, `lag_168h`, dan `lag_24h` disertai efek tetap (fixed effect) per zona. Model berbasis pohon (`RandomForest` dan `GBT`) mempartisi fitur kontinu ke dalam bin diskret (`maxBins=32` default) sehingga kurang presisi saat mengekstrapolasi lonjakan puncak ratusan pickup/jam di zona utama Manhattan dan bandara.
3. **Trade-off Ridge vs Random Forest (`d=10, t=60`):**
   - `random_forest_d10_t60_024` mencatat All-261 MAE sedikit lebih rendah (`4.2620` vs `4.3635`, selisih `0.10` trip/jam) semata karena pohon keputusan dapat memprediksi tepat `0.0` pada zona pinggiran yang mayoritas nol.
   - Sebaliknya, pada **Top 50 Operational Zones** (yang menyumbang **~91% total demand NYC**), `ridge_reg_0p1_024` **menang mutlak di seluruh metrik**: MAE lebih baik `-7.55%` (`16.6637` vs `18.0248`), RMSE lebih baik `-6.29%` (`26.9129` vs `28.7192`), dan WAPE lebih baik `-1.50 pp` (`18.41%` vs `19.92%`).
   - Selain itu, `ridge_reg_0p1_024` jauh lebih ringan, stabil secara memori di WSL lokal, dan **4,6× lebih cepat dilatih** (`6.04s` vs `27.79s`; refit final 100.224 baris hanya **`2.11s`**).

---

## 6. Evaluasi Final pada Locked Test Holdout (`2026-09-24` s.d. `2026-09-30`)

Model `ridge_reg_0p1_024` di-refit satu kali pada `2026-09-08 .. 2026-09-23` (`100.224` baris), disimpan ke `ml/models/ridge_reg_0p1_024`, di-reload secara independen, lalu dievaluasi pada 1 minggu penuh Test Holdout (`43.848` baris; `933.894` total pickup aktual).

### 6.1 Ringkasan Global vs Top 50 Operasional vs 211 Zona Lainnya

| Cakupan Evaluasi | Baris Test | Pickup Aktual | Model MAE | Naive MAE | Impr. MAE | Model RMSE | Naive RMSE | Impr. RMSE | Model $R^2$ | Model WAPE |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **All 261 Zones** | `43.848` | `933.894` | **`4.5495`** | `4.8796` | **`-6.76%`** | **`12.2597`** | `15.1874` | **`-19.28%`** | **`0.9564`** | **`21.36%`** |
| **Top 50 Zones (91,0% volume)** | `8.400` | `849.645` | **`17.4498`** | `19.7748` | **`-11.76%`** | **`27.4563`** | `34.1088` | **`-19.50%`** | **`0.9244`** | **`17.25%`** |
| Remaining 211 Zones (9,0% volume) | `35.448` | `84.249` | `1.4925` | `1.3499` | `+10.56%` | **`2.6979`** | `3.1027` | **`-13.05%`** | `0.7887` | `62.80%` |

- **Mean Bias (Prediksi − Aktual):** `+0.9393` trip/jam pada All 261 Zones (`+3.0866` pada Top 50, `+0.4305` pada 211 zona lainnya) — bias positif sangat kecil yang bersumber dari clipping non-negatif (`max(0, pred)`) dan tren volume minggu akhir September yang sedikit melandai dibanding minggu ke-3.
- **Jumlah Zona Mengungguli Naive RMSE:** **`105 / 261`** zona secara keseluruhan dan **`45 / 50` (90,0%)** di dalam Top 50 zona operasional.

### 6.2 Distribusi Error & Residual (All 261 Zones)

| Metrik Distribusi | Nilai |
|---|---:|
| Median Absolute Error (`p50`) | **`1.1095`** trip/jam |
| Absolute Error `p75` | `3.2299` trip/jam |
| Absolute Error `p90` | `11.1317` trip/jam |
| Absolute Error `p95` | `22.4386` trip/jam |
| Absolute Error `p99` | `54.9395` trip/jam |
| Max Absolute Error | `313.0455` trip/jam |
| Residual `p05` / `p25` / `p50` / `p75` / `p95` | `-8.57` / `-0.50` / `+0.13` / `+1.74` / `+13.89` |
| Proporsi Prediksi dalam `± 5` trip/jam | **`81.91%`** |
| Proporsi Prediksi dalam `± 10` trip/jam | **`89.09%`** |
| Proporsi Prediksi dalam `± 20` trip/jam | **`94.31%`** |

### 6.3 Evaluasi Tersegmentasi

#### A. Berdasarkan Demand Tier (`demand_tier_024`)

| Demand Tier | Baris Test | Pickup Aktual | Model MAE | Model RMSE | Model $R^2$ | Model WAPE | Naive RMSE | Impr. RMSE vs Naive |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Top 10` | `1.680` | `312.279` | `31.1913` | `45.9873` | `0.8947` | `16.78%` | `58.0891` | **`-20.83%`** |
| `High (11-50)` | `6.720` | `537.366` | `14.0145` | `20.3372` | `0.9204` | `17.53%` | `24.7119` | **`-17.70%`** |
| `Medium (51-150)` | `16.800` | `79.320` | `2.3045` | `3.7047` | `0.7776` | `48.81%` | `4.4310` | **`-16.39%`** |
| `Low (>150)` | `18.648` | `4.929` | `0.7611` | `1.2128` | `-2.0229` | `287.94%` | `0.7818` | `+55.13%` *(zero-inflated)* |

> **Temuan Jujur — Zero-Inflation pada Tier `Low (>150)`:** Kelompok `Low (>150)` mencakup `18.648` baris dengan rata-rata hanya **`0.26` pickup/jam** (mayoritas bernilai `0`). Karena Ridge menggunakan intercept global `+3.19` dengan clipping `>= 0`, prediksi baseline kecil (`~0.6–0.8` trip/jam) di zona hampir-nol sedikit lebih tinggi daripada Naive `lag_168h` yang sering menebak tepat `0`. Namun, pada tiga tier yang menyumbang **99,47% seluruh permintaan NYC (`Top 10`, `High`, `Medium`)**, model mengungguli Naive RMSE secara konsisten antara **`-16.39%` hingga `-20.83%`**.

#### B. Berdasarkan Borough (`taxi_zone_lookup_024.csv`)

| Borough | Baris Test | Pickup Aktual | Model MAE | Model RMSE | Model $R^2$ | Model WAPE | Naive RMSE | Impr. RMSE vs Naive |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `Manhattan` | `11.256` | `822.304` | `12.9071` | `22.6042` | `0.9402` | `17.67%` | `28.3062` | **`-20.14%`** |
| `Queens` | `11.592` | `75.681` | `2.2810` | `7.8769` | `0.9481` | `34.94%` | `9.0440` | **`-12.91%`** |
| `Brooklyn` | `10.248` | `29.229` | `1.7406` | `3.0300` | `0.7698` | `61.03%` | `3.4894` | **`-13.17%`** |
| `Bronx` | `7.224` | `5.213` | `1.0053` | `1.6637` | `-0.0222` | `139.31%` | `1.5961` | `+4.23%` |
| `Unknown` (`LocationID 264`) | `168` | `859` | `2.1930` | `2.8524` | `0.3900` | `42.89%` | `3.6146` | **`-21.09%`** |
| `N/A` (`LocationID 265`) | `168` | `387` | `1.8421` | `2.3799` | `-0.0022` | `79.97%` | `2.5935` | **`-8.23%`** |
| `EWR` (`LocationID 1`) | `168` | `114` | `0.8971` | `1.3600` | `-0.9344` | `132.21%` | `1.1852` | `+14.75%` |
| `Staten Island` | `3.024` | `107` | `0.6063` | `1.0240` | `-24.7340` | `1713.61%` | `0.2716` | `+277.08%` *(hanya 107 trip/minggu)* |

#### C. Berdasarkan Service Zone, Jam Puncak, dan Jenis Hari

| Dimensi / Segmen | Baris Test | Pickup Aktual | Model MAE | Model RMSE | Model $R^2$ | Model WAPE | Naive RMSE | Impr. RMSE vs Naive |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `service_zone`: `Yellow Zone` | `8.904` | `789.437` | `15.1917` | `25.1539` | `0.9337` | `17.13%` | `31.4393` | **`-19.99%`** |
| `service_zone`: `Boro Zone` | `34.104` | `86.804` | `1.5363` | `2.9475` | `0.8340` | `60.36%` | `3.5759` | **`-17.57%`** |
| `service_zone`: `Airports` (`JFK` & `LGA`) | `336` | `56.293` | `32.7289` | `44.2437` | `0.8602` | `19.54%` | `50.9804` | **`-13.21%`** |
| `service_zone`: `N/A` (`264` & `265`) | `336` | `1.246` | `2.0176` | `2.6268` | `0.3983` | `54.41%` | `3.1458` | **`-16.50%`** |
| `service_zone`: `EWR` | `168` | `114` | `0.8971` | `1.3600` | `-0.9344` | `132.21%` | `1.1852` | `+14.75%` |
| **Peak (`17:00–21:00`)** | `9.135` | `302.694` | `5.7362` | `15.4971` | `0.9623` | `17.31%` | `21.7600` | **`-28.78%`** |
| **Off-Peak (`00–16, 22–23`)** | `34.713` | `631.200` | `4.2372` | `11.2540` | `0.9519` | `23.30%` | `12.9133` | **`-12.85%`** |
| **Weekday** | `31.320` | `672.563` | `4.3394` | `11.9834` | `0.9606` | `20.21%` | `15.5752` | **`-23.06%`** |
| **Weekend** | `12.528` | `261.331` | `5.0748` | `12.9246` | `0.9437` | `24.33%` | `14.1716` | **`-8.80%`** |

### 6.4 Sorotan Evaluasi Per-Zona (Enriched dengan `taxi_zone_lookup_024.csv`)

Seluruh pemetaan zona dilakukan menggunakan kunci resmi `PULocationID == LocationID` (`0` NULL, `0` mismatch).

#### Top 10 Zona dengan RMSE Tertinggi (Zona Volume Masif)

| `PULocationID` | `Zone` | `Borough` | `service_zone` | Rank | Aktual Test | Model MAE | Model RMSE | WAPE (%) | Naive RMSE | Unggul Naive? |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|:---:|
| `161` | Midtown Center | Manhattan | Yellow Zone | 4 | `36.030` | `41.27` | `59.78` | `19.25%` | `107.55` | Ya (`-44.4%`) |
| `142` | Lincoln Square East | Manhattan | Yellow Zone | 6 | `30.193` | `33.00` | `52.43` | `18.36%` | `53.95` | Ya |
| `236` | Upper East Side North | Manhattan | Yellow Zone | 2 | `39.472` | `36.66` | `52.04` | `15.60%` | `54.88` | Ya |
| `237` | Upper East Side South | Manhattan | Yellow Zone | 1 | `42.647` | `37.04` | `51.87` | `14.59%` | `60.19` | Ya |
| `79` | East Village | Manhattan | Yellow Zone | 9 | `25.859` | `28.12` | `48.25` | `18.27%` | `42.87` | Tidak *(nightlife spikes)* |
| `132` | JFK Airport | Queens | Airports | 3 | `33.350` | `35.48` | `48.19` | `17.87%` | `51.39` | Ya |
| `138` | LaGuardia Airport | Queens | Airports | 12 | `22.943` | `29.98` | `39.91` | `21.95%` | `50.57` | Ya (`-21.1%`) |
| `162` | Midtown East | Manhattan | Yellow Zone | 7 | `25.930` | `28.98` | `39.68` | `18.78%` | `69.25` | Ya (`-42.7%`) |
| `186` | Penn Station/Madison Sq West | Manhattan | Yellow Zone | 5 | `27.591` | `28.26` | `37.60` | `17.21%` | `38.38` | Ya |
| `230` | Times Sq/Theatre District | Manhattan | Yellow Zone | 10 | `26.103` | `21.52` | `32.14` | `13.85%` | `32.53` | Ya |

> Catatan: Meskipun 10 zona di atas memiliki RMSE absolut tertinggi karena skala permintaan 150–450+ pickup/jam, **WAPE relatifnya sangat sehat (`13.85%–21.95%`)** dan **9 dari 10 zona** mengungguli Naive RMSE — terutama koridor bisnis **Midtown Center (`-44.4%`)** dan **Midtown East (`-42.7%`)** yang sangat terbantu oleh `lag_1h` saat terjadi anomali lalu lintas mingguan. Hanya **East Village (`79`)** (zona hiburan malam akhir pekan yang sangat terikat pola hari yang sama minggu lalu) yang sedikit kalah dari Naive `lag_168h`.

#### Top 10 Zona dengan WAPE Terbaik di Dalam Top 50 Operasional

| `PULocationID` | `Zone` | `Borough` | `service_zone` | Rank | Aktual Test | Model MAE | Model RMSE | WAPE (%) |
|---:|---|---|---|---:|---:|---:|---:|---:|
| `48` | Clinton East | Manhattan | Yellow Zone | 16 | `22.090` | `17.55` | `22.36` | **`13.35%`** |
| `234` | Union Sq | Manhattan | Yellow Zone | 11 | `24.358` | `19.90` | `25.85` | **`13.73%`** |
| `230` | Times Sq/Theatre District | Manhattan | Yellow Zone | 10 | `26.103` | `21.52` | `32.14` | **`13.85%`** |
| `263` | Yorkville West | Manhattan | Yellow Zone | 23 | `18.416` | `15.44` | `20.52` | **`14.08%`** |
| `68` | East Chelsea | Manhattan | Yellow Zone | 8 | `25.104` | `21.59` | `27.49` | **`14.45%`** |
| `237` | Upper East Side South | Manhattan | Yellow Zone | 1 | `42.647` | `37.04` | `51.87` | **`14.59%`** |
| `164` | Midtown South | Manhattan | Yellow Zone | 20 | `18.440` | `16.09` | `21.36` | **`14.66%`** |
| `107` | Gramercy | Manhattan | Yellow Zone | 24 | `17.454` | `15.71` | `20.75` | **`15.12%`** |
| `140` | Lenox Hill East | Manhattan | Yellow Zone | 21 | `18.937` | `17.22` | `23.37` | **`15.28%`** |
| `141` | Lenox Hill West | Manhattan | Yellow Zone | 17 | `20.832` | `18.98` | `26.56` | **`15.31%`** |

#### Top 10 Zona Aktif (`>= 100` Pickup Test) dengan MAE Absolut Terendah

Zona dengan MAE absolut `< 1.0` trip/jam didominasi zona pinggiran volume rendah (`Woodlawn/Wakefield` ID `259` MAE `0.8358`, `Newark Airport` ID `1` MAE `0.8971`, `West Farms/Bronx River` ID `248` MAE `0.9167`, `Glendale` ID `102` MAE `0.9489`, `Westchester Village/Unionport` ID `250` MAE `0.9593`, `Riverdale/North Riverdale/Fieldston` ID `200` MAE `0.9728`, `Claremont/Bathgate` ID `47` MAE `0.9743`, `Columbia Street` ID `54` MAE `0.9876`, `Bedford Park` ID `18` MAE `0.9917`, `East New York/Pennsylvania Avenue` ID `77` MAE `1.0043`). Meskipun kesalahan absolutnya di bawah `1` perjalanan/jam, WAPE-nya tinggi (`108%–169%`) karena volume aktual per jamnya kecil — mengonfirmasi bahwa interpretasi akurasi harus selalu membedakan zona operasional utama (Top 50) dari zona volume rendah.

---

## 7. Verifikasi Reproducibility & Immutability

1. **Verifikasi Reload & Reproducibility Model:** Script independen `evaluate_demand_model_024.py` memuat ulang `ml/models/ridge_reg_0p1_024/` dari disk lokal, melakukan inferensi ulang langsung terhadap Test Holdout di HDFS, dan memverifikasi kecocokan identik (`selisih MAE & RMSE < 1e-4`) dengan `data/analytics/demand_predictions_024.parquet` (`43.848` baris).
2. **Verifikasi Lookup Enrichment:** Cross-check independen terhadap `data/reference/taxi_zone_lookup_024.csv` mengonfirmasi **`0` NULL** dan **`0` mismatch** di seluruh `261` zona aktif (`lookup_match_rate_pct_024 = 100.0%`), dengan nilai literal `"N/A"` pada `LocationID 264` dan `265` tetap terjaga.
3. **Verifikasi Immutability HDFS (Raw, Silver, Gold Tidak Berubah):**
   - Snapshot HDFS `/urbanpulse_024/analytics/` sesudah training & evaluasi menunjukkan kelima direktori Gold (`gold_hourly_demand_024`, `gold_zone_demand_024`, `gold_temporal_patterns_024`, `gold_zone_hourly_demand_024`, `gold_demand_features_024`) memiliki timestamp identik (`2026-11-25 08:11–08:12`), dan keempat part file `gold_demand_features_024` (`388.410`, `483.161`, `602.385`, `527.837` bytes) **identik byte-per-byte** dengan snapshot pra-training.

---

## 8. Keterbatasan & Rekomendasi Pengembangan Lanjutan

### Keterbatasan Saat Ini
1. **Cakupan Temporal 1 Bulan (September 2026):** Model menangkap pola intraharian dan mingguan dengan sangat baik, tetapi belum mengamati musiman tahunan (libur besar, musim dingin/panas, perubahan tarif/kebijakan).
2. **Zero-Inflation pada Zona Pinggiran (`Low >150`, Staten Island, Bronx):** Regresi linear global menghasilkan prediksi baseline positif kecil pada jam-jam bernilai nol, sehingga kalah dari baseline Naive pada segmen bervolume sangat kecil (`0.53%` dari total pickup).
3. **Absennya Sinyal Eksogen:** Belum tersedia data cuaca (hujan/badai), jadwal kedatangan penerbangan (untuk `JFK` & `LaGuardia`), serta kalender event besar kota (konser, UN General Assembly di Midtown East, pertandingan olahraga).
4. **Horizon Forecasting 1-Step-Ahead:** Karena `lag_1h_pickup_count_024` menjadi fitur dominan, inferensi multi-jam ke depan (>1 jam) membutuhkan recursive forecasting atau model direct multi-horizon terpisah.

### Rekomendasi Tahap Berikutnya
1. **Integrasi ke Dashboard UrbanPulse:** Artefak `data/analytics/demand_predictions_024.parquet` dan `data/analytics/model_metrics_024.json` sudah 100% sesuai kontrak `dashboard/pages/demand_prediction_024.py` & `dashboard/data_loader_024.py` dan siap disambungkan pada tahap integrasi dashboard berikutnya.
2. **Model Khusus Segmen / Two-Stage (Hurdle atau Poisson/Tweedie):** Pisahkan model atau gunakan `GeneralizedLinearRegression(family="poisson")` / post-thresholding berbasis zona untuk kelompok `Low (>150)` agar akurasi zona sepi setara atau mengungguli Naive tanpa mengorbankan presisi Top 50.
3. **Pengayaan Fitur Eksogen & Rolling Window Lintas Bulan:** Saat data bulan berikutnya (Oktober–Desember) masuk ke pipeline Gold, latih ulang model secara berkala dengan tambahan fitur cuaca historis NYC dan indikator kalender hari libur nasional.
