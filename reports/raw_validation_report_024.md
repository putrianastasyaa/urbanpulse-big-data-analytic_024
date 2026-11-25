# Laporan Validasi & Eksplorasi Data Mentah (Raw Layer) — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Eksekusi:** 25 November 2026 (`2026-11-25T00:29:57Z`)  
> **Sumber HDFS:** `hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet`  
> **Script Validasi:** `spark/processing/validate_raw_yellow_taxi_024.py`  
> **Output Metrik Terstruktur (JSON):** `reports/raw_validation_summary_024.json`

---

## 1. Metode & Konfigurasi Validasi

Validasi tahap awal (*Raw Layer Profiling*) dilakukan secara **read-only** tanpa mengubah, menghapus, maupun memodifikasi skema kolom dataset sumber di HDFS.

- **Engine:** Apache Spark `4.0.4` (Scala `2.13.16`, OpenJDK `17.0.20`, Python `3.12.3`) pada Ubuntu WSL 2.
- **Mode Eksekusi:** Spark local mode (`--master local[*]`, tanpa YARN, NiFi, maupun Docker).
- **Konfigurasi Runtime Aman:**
  - `SPARK_LOCAL_IP=127.0.0.1`
  - `spark.driver.memory=2g`
  - `spark.sql.shuffle.partitions=16`
  - `spark.eventLog.enabled=false` *(override runtime agar tidak menulis log baru ke direktori HDFS lama `/spark-logs` tanpa mengubah `/usr/local/spark/conf/spark-defaults.conf`)*
- **Efisiensi Komputasi:** Agregasi null/NaN, statistik deskriptif numerik (`mean`, `stddev`, `min`, `max`, dan `percentile_approx` untuk Q1/Median/Q3/P99), serta pengecekan kualitas data dijalankan dengan minim pemindaian (*single-pass aggregation* per grup metrik).
- **Durasi Eksekusi Aktual:** `79.82` detik untuk `3,876,044` baris (`64.3 MiB` Parquet).

---

## 2. Ringkasan Umum Dataset

| Metrik | Nilai Aktual |
| :--- | ---: |
| Total Baris (`total_rows_024`) | **3,876,044** |
| Total Kolom (`total_columns_024`) | **21** |
| Baris Unik (`distinct_rows_024`) | **3,876,044** |
| Duplikat Persis Seluruh Kolom (`exact_duplicate_rows_024`) | **0 (0.0000%)** |
| Nilai `NaN` pada Seluruh Kolom Floating-Point | **0** |
| Minimum `tpep_pickup_datetime` | `2009-01-01 09:32:44` |
| Maksimum `tpep_pickup_datetime` | `2026-10-07 07:40:40` |
| Minimum `tpep_dropoff_datetime` | `2009-01-01 10:22:44` |
| Maksimum `tpep_dropoff_datetime` | `2026-10-07 07:59:16` |

---

## 3. Skema Lengkap & Pemeriksaan Nilai Null (21 Kolom Asli)

Seluruh nama kolom asli NYC TLC dipertahankan tanpa perubahan:

| No | Nama Kolom Sumber | Tipe Data Spark | Nullable | Jumlah Null | % Null | Jumlah NaN |
| :-: | :--- | :--- | :-: | ---: | ---: | ---: |
| 1 | `VendorID` | `int` | `true` | 0 | 0.0000% | 0 |
| 2 | `tpep_pickup_datetime` | `timestamp_ntz` | `true` | 0 | 0.0000% | 0 |
| 3 | `tpep_dropoff_datetime` | `timestamp_ntz` | `true` | 0 | 0.0000% | 0 |
| 4 | `passenger_count` | `bigint` | `true` | 1,190,413 | 30.7121% | 0 |
| 5 | `trip_distance` | `double` | `true` | 0 | 0.0000% | 0 |
| 6 | `RatecodeID` | `bigint` | `true` | 1,190,413 | 30.7121% | 0 |
| 7 | `store_and_fwd_flag` | `string` | `true` | 1,190,413 | 30.7121% | 0 |
| 8 | `PULocationID` | `int` | `true` | 0 | 0.0000% | 0 |
| 9 | `DOLocationID` | `int` | `true` | 0 | 0.0000% | 0 |
| 10 | `payment_type` | `bigint` | `true` | 0 | 0.0000% | 0 |
| 11 | `fare_amount` | `double` | `true` | 0 | 0.0000% | 0 |
| 12 | `extra` | `double` | `true` | 0 | 0.0000% | 0 |
| 13 | `mta_tax` | `double` | `true` | 0 | 0.0000% | 0 |
| 14 | `tip_amount` | `double` | `true` | 0 | 0.0000% | 0 |
| 15 | `tolls_amount` | `double` | `true` | 0 | 0.0000% | 0 |
| 16 | `improvement_surcharge` | `double` | `true` | 0 | 0.0000% | 0 |
| 17 | `total_amount` | `double` | `true` | 0 | 0.0000% | 0 |
| 18 | `congestion_surcharge` | `double` | `true` | 1,190,413 | 30.7121% | 0 |
| 19 | `Airport_fee` | `double` | `true` | 1,190,413 | 30.7121% | 0 |
| 20 | `cbd_congestion_fee` | `double` | `true` | 0 | 0.0000% | 0 |
| 21 | `request_source` | `string` | `true` | 2,685,952 | 69.2962% | 0 |

### Temuan Penting Terkait Pola Missing Value (Structural Missingness)
Lima kolom (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) memiliki jumlah null yang **persis identik yaitu `1,190,413` baris (`30.7121%`)**, yang jumlahnya sama persis dengan jumlah baris pada `payment_type = 0` (*Flex Fare / e-hail trip* pada skema NYC TLC terbaru). Sebaliknya, kolom baru `request_source` terisi pada mayoritas baris tersebut (hanya null pada `2,685,952` baris / `69.2962%`).

> **Implikasi untuk Tahap Cleaning (Silver Layer):** Menghapus baris secara membabi-buta hanya karena `passenger_count IS NULL` akan menghilangkan **30.71% permintaan taksi nyata**. Baris `payment_type = 0` tetap memiliki `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`, `trip_distance`, dan `total_amount` yang terisi penuh, sehingga **wajib dipertahankan** untuk analisis BI spasial-temporal dan pemodelan prediksi permintaan (`pickup_count_024`).

---

## 4. Statistik Deskriptif Kolom Numerik

| Kolom | Non-Null | Mean | StdDev | Min | P25 | Median (P50) | P75 | P99 | Max |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `VendorID` | 3,876,044 | 1.89 | 0.71 | 1.00 | 2.00 | 2.00 | 2.00 | 7.00 | 7.00 |
| `passenger_count` | 2,685,631 | 1.22 | 0.61 | 0.00 | 1.00 | 1.00 | 1.00 | 4.00 | 8.00 |
| `trip_distance` (mil) | 3,876,044 | 4.97 | 463.43 | 0.00 | 1.06 | 1.96 | 4.16 | 19.43 | 232,644.80 |
| `RatecodeID` | 2,685,631 | 4.63 | 18.25 | 1.00 | 1.00 | 1.00 | 1.00 | 99.00 | 99.00 |
| `PULocationID` | 3,876,044 | 161.73 | 66.91 | 1.00 | 114.00 | 161.00 | 233.00 | 263.00 | 265.00 |
| `DOLocationID` | 3,876,044 | 161.20 | 70.84 | 1.00 | 107.00 | 162.00 | 234.00 | 263.00 | 265.00 |
| `payment_type` | 3,876,044 | 0.80 | 0.64 | 0.00 | 0.00 | 1.00 | 1.00 | 2.00 | 5.00 |
| `fare_amount` (USD) | 3,876,044 | 23.10 | 20.20 | -1,800.00 | 10.70 | 17.70 | 29.20 | 85.17 | 9,999.70 |
| `extra` (USD) | 3,876,044 | 1.15 | 1.78 | -7.50 | 0.00 | 0.00 | 2.50 | 7.50 | 42.43 |
| `mta_tax` (USD) | 3,876,044 | 0.49 | 0.08 | -0.50 | 0.50 | 0.50 | 0.50 | 0.50 | 11.50 |
| `tip_amount` (USD) | 3,876,044 | 2.78 | 4.06 | -94.10 | 0.00 | 2.00 | 3.99 | 17.62 | 220.30 |
| `tolls_amount` (USD) | 3,876,044 | 0.62 | 2.33 | -80.00 | 0.00 | 0.00 | 0.00 | 7.46 | 180.00 |
| `improvement_surcharge` (USD) | 3,876,044 | 0.99 | 0.13 | -1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| `total_amount` (USD) | 3,876,044 | 32.14 | 23.94 | -1,801.00 | 18.18 | 25.26 | 37.50 | 107.06 | 10,003.70 |
| `congestion_surcharge` (USD) | 2,685,631 | 2.29 | 0.74 | -2.50 | 2.50 | 2.50 | 2.50 | 2.50 | 2.75 |
| `Airport_fee` (USD) | 2,685,631 | 0.19 | 0.61 | -2.00 | 0.00 | 0.00 | 0.00 | 2.00 | 27.00 |
| `cbd_congestion_fee` (USD) | 3,876,044 | 0.56 | 0.33 | -0.75 | 0.75 | 0.75 | 0.75 | 0.75 | 0.75 |

---

## 5. Hasil Pemeriksaan Kualitas Data (Anomali & Outlier)

### 5.1 Konsistensi Waktu & Durasi Perjalanan (`trip_duration_min_024`)
- Distribusi durasi: `min = 0.00` menit, `P25 = 9.20` menit, `median = 15.42` menit, `mean = 18.75` menit, `P75 = 24.07` menit, `P99 = 69.68` menit, `max = 7,798.73` menit (~130 jam / 5.4 hari).

| Pemeriksaan | Jumlah Baris | Persentase | Catatan |
| :--- | ---: | ---: | :--- |
| Pickup sebelum September 2026 (`< 2026-09-01`) | 3 | 0.0001% | Terdapat anomali timestamp tahun 2009 (`2009-01-01`) |
| Pickup setelah September 2026 (`>= 2026-10-01`) | 19 | 0.0005% | Melampaui akhir bulan hingga `2026-10-07` |
| Durasi negatif (`< 0` menit) | 0 | 0.0000% | Bersih — tidak ada dropoff sebelum pickup |
| Durasi nol (`= 0` menit) | 48,202 | 1.2436% | Indikasi pembatalan/error pencatatan meter |
| Durasi sangat singkat (`0 < dur < 1` menit) | 36,099 | 0.9313% | Perlu dievaluasi bersama jarak & tarif |
| Durasi > 3 jam (`> 180` menit) | 852 | 0.0220% | Outlier ekstrem (meter lupa dimatikan) |
| Durasi > 12 jam (`> 720` menit) | 477 | 0.0123% | Tidak realistis untuk perjalanan taksi urban |
| Durasi > 24 jam (`> 1440` menit) | 18 | 0.0005% | Maksimum mencapai 7,798.73 menit |

### 5.2 Jumlah Penumpang (`passenger_count`)
| Pemeriksaan | Jumlah Baris | Persentase | Catatan |
| :--- | ---: | ---: | :--- |
| `passenger_count` NULL | 1,190,413 | 30.7121% | Terkait `payment_type = 0` (Flex Fare / e-hail) |
| `passenger_count < 0` | 0 | 0.0000% | Bersih |
| `passenger_count = 0` | 10,537 | 0.2718% | Nilai input pengemudi yang tidak wajar |
| `passenger_count > 6` | 2 | 0.0001% | Tercatat `8` penumpang (melebihi kapasitas standar taksi) |

Distribusi utama: 1 penumpang (`57.72%`), NULL (`30.71%`), 2 penumpang (`8.32%`), 3 penumpang (`1.70%`), 4 penumpang (`1.08%`), 0 penumpang (`0.27%`), 5–6 penumpang (`0.19%`), 8 penumpang (`2` baris).

### 5.3 Jarak Perjalanan & Kecepatan Implied (`trip_distance`)
| Pemeriksaan | Jumlah Baris | Persentase | Catatan |
| :--- | ---: | ---: | :--- |
| `trip_distance < 0` | 0 | 0.0000% | Bersih |
| `trip_distance = 0` | 147,158 | 3.7966% | Perjalanan dibatalkan atau error GPS/odometer |
| `trip_distance > 100` mil | 125 | 0.0032% | Outlier jarak ekstrem |
| `trip_distance > 500` mil | 48 | 0.0012% | Maksimum mencapai `232,644.8` mil (anomali sensor/GPS) |
| Kecepatan rata-rata > 80 mph | 1,168 | 0.0301% | Tidak realistis di jalan perkotaan NYC |

### 5.4 Nilai Tarif, Tip, dan Biaya Tambahan (`fare_amount`, `total_amount`, `tip_amount`)
| Pemeriksaan | Jumlah Baris | Persentase | Catatan |
| :--- | ---: | ---: | :--- |
| `fare_amount < 0` | 14,190 | 0.3661% | Min `-1,800.00` USD (refund/void/dispute) |
| `fare_amount = 0` | 2,352 | 0.0607% | Tarif nol |
| `fare_amount > 500` USD | 65 | 0.0017% | Maksimum `9,999.70` USD |
| `total_amount < 0` | 14,688 | 0.3789% | Min `-1,801.00` USD |
| `total_amount = 0` | 480 | 0.0124% | Total transaksi nol |
| `total_amount > 1000` USD | 4 | 0.0001% | Maksimum `10,003.70` USD |
| `tip_amount < 0` | 192 | 0.0050% | Min `-94.10` USD |
| `tip_amount > 200` USD | 1 | 0.0000% | Maksimum `220.30` USD |
| Surcharge/tax negatif (`improvement_surcharge`, `mta_tax`, `congestion_surcharge`, `cbd_congestion_fee`, `extra`, `Airport_fee`, `tolls_amount`) | 1,247 – 14,460 | 0.0322% – 0.3731% | Konsisten dengan baris transaksi negatif/dispute |

### 5.5 Lokasi Zona & Kode Referensi
- **`PULocationID` & `DOLocationID`:** 100% berada pada rentang valid `1–265` (`0` baris di luar rentang).
- **Zona Tidak Diketahui (`264` / `265`):** `5,280` baris (`0.1362%`) pada pickup dan `23,483` baris (`0.6058%`) pada dropoff.
- **`RatecodeID` non-standar (nilai `99`):** `96,707` baris (`2.4950%`).
- **Distribusi `payment_type`:** `1` Credit Card (`60.17%`), `0` Flex Fare (`30.71%`), `2` Cash (`8.28%`), `4` Dispute (`0.56%`), `3` No Charge (`0.27%`), `5` Unknown (`2` baris).
- **Distribusi `VendorID`:** `2` (`80.70%`), `1` (`17.93%`), `7` (`1.24%`), `6` (`0.14%`).

---

## 6. Rekomendasi Aturan Cleaning untuk Tahap Berikutnya (Silver Layer)

Berdasarkan bukti aktual di atas (tanpa mengubah data mentah di HDFS), aturan pembersihan yang direkomendasikan untuk tahap `spark/processing/` berikutnya adalah:

1. **Filter Rentang Waktu Valid:** Pertahankan `tpep_pickup_datetime` dalam rentang target `2026-09-01 00:00:00` s.d. `< 2026-10-01 00:00:00` (membuang 22 baris di luar periode).
2. **Filter Durasi Realistis:** Pertahankan `trip_duration_min_024 > 0` dan `<= 180` menit (atau `>= 1` menit untuk metrik perjalanan komersial standar) untuk menghilangkan `48,202` baris berdurasi 0 dan outlier ekstrem hingga 130 jam.
3. **Filter Jarak & Kecepatan:** Pertahankan `trip_distance > 0` dan `<= 100` mil (serta pertimbangkan batas kecepatan wajar `<= 80` mph) agar outlier `232,644.8` mil tidak merusak rata-rata (`stddev` saat ini `463.43`!).
4. **Filter Tarif Valid:** Pertahankan `fare_amount > 0`, `fare_amount <= 500`, `total_amount > 0`, dan `tip_amount >= 0` agar transaksi dispute/refund negatif (~0.38%) dan outlier `10,003.70` USD tidak mendistorsi analisis pendapatan/BI.
5. **Penanganan `passenger_count` NULL secara Cermat:** **Jangan drop** baris `passenger_count IS NULL` (`30.71%` Flex Fare trips) pada tabel utama permintaan spasial-temporal; tambahkan flag kualitas (`is_passenger_missing_024`) dan terapkan filter `passenger_count BETWEEN 1 AND 6` hanya pada agregasi spesifik penumpang.
6. **Penanganan Zona `264`/`265`:** Tetap petakan melalui `taxi_zone_lookup.csv` (sebagai `Unknown` / `Outside of NYC`), dan pisahkan pada visualisasi peta choropleth NYC.
