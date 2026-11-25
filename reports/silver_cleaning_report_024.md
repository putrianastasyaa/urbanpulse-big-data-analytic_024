# Laporan Cleaning & Pembentukan Silver Layer v2 (Audit-Refined) — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Versi Silver Layer:** `v2_audit_refined_024` *(berdasarkan Opsi 2 pada [silver_cleaning_impact_audit_024.md](silver_cleaning_impact_audit_024.md))*  
> **Tanggal Eksekusi:** 25 November 2026 (`2026-11-25T00:52:58Z`)  
> **Script PySpark:** `spark/processing/clean_yellow_taxi_024.py`  
> **Input Raw (HDFS):** `hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet`  
> **Output Silver Live (HDFS):** `hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024`  
> **Metrik Terstruktur JSON:** `reports/silver_cleaning_summary_024.json`

---

## 1. Prinsip Revisi Silver Layer v2

Sesuai hasil audit [silver_cleaning_impact_audit_024.md](silver_cleaning_impact_audit_024.md), Silver Layer v2 memisahkan secara tegas antara:
1. **Validasi Kelayakan Event Pickup & Transaksi Berbayar (Hard Removal di Silver Layer):** Hanya mengeluarkan baris di luar periode target, lokasi tidak valid, telemetri negatif, transaksi non-berbayar (`total_amount <= 0`), serta outlier korup mutlak (`> 2,000` USD).
2. **Penandaan Anomali Metrik & Telemetri (Preserved + Flagged untuk Filter Khusus Analisis di Gold Layer):** Transaksi pickup yang tetap berpotensi valid (Flex Fare `fare_amount == 0`, durasi `> 180` menit, kecepatan `> 80` mph, jarak `> 100` mil, tarif tinggi `500–2,000` USD, sub-komponen negatif dengan `total_amount > 0`, `passenger_count` NULL, durasi `0` Vendor 7, jarak `0` Flex Fare, zona `264/265`, serta pasangan positif dari reversal negatif Raw) **dipertahankan** dengan flag eksplisit `*_024` dan kolom metrik tervalidasi (`*_valid_*_024` bernilai `NULL` pada baris anomali).

### Protokol Penggantian Aman di HDFS (Zero-Downtime Staging Swap)
1. Output v2 ditulis terlebih dahulu ke direktori sementara `/urbanpulse_024/processed/yellow_taxi_024_staging_v2` tanpa menyentuh output v1 maupun Raw Layer.
2. Dilakukan verifikasi pembacaan ulang Spark penuh (`count = 3,860,852`, `49` kolom, `21` kolom asli utuh, seluruh kolom turunan berakhiran `_024`) serta perbandingan distribusi v1 vs v2.
3. Setelah seluruh asersi lolos, dilakukan perpindahan aman: v1 dipindahkan ke `yellow_taxi_024_v1_backup`, staging dipromosikan menjadi `yellow_taxi_024`, diverifikasi baca ulang live (`LIVE_SILVER_V2_VERIFIED: rows=3860852, cols=49`), dan barulah backup v1 dihapus.

---

## 2. Ringkasan Sebelum vs Sesudah Revisi (Raw → Silver v1 → Silver v2)

| Metrik | Raw Layer | Silver v1 (Awal) | Silver v2 (Revisi Live) | Perubahan v1 → v2 |
| :--- | ---: | ---: | ---: | :--- |
| Total Baris | `3,876,044` | `3,857,325` (`99.5171%`) | **`3,860,852` (`99.6081%`)** | **`+3,527` pickup dipulihkan** |
| Total Baris Dikeluarkan | — | `18,719` (`0.4829%`) | **`15,192` (`0.3919%`)** | `-3,527` drop (`-18.84%` lebih sedikit) |
| Retensi Flex Fare (`payment_type = 0`) | `1,190,413` | `1,188,932` (`99.8756%`) | **`1,190,293` (`99.9899%`)** | `+1,361` transaksi Flex Fare dipulihkan |
| Retensi `VendorID = 7` | `47,893` | `47,893` (`100.0000%`) | **`47,893` (`100.0000%`)** | Tetap `100%` terjaga |
| Retensi `VendorID = 6` | `5,236` | `4,369` (`83.4416%`) | **`5,174` (`98.8159%`)** | **`+805` transaksi berbayar dipulihkan** |
| Jumlah Kolom | `21` | `40` (`21 + 19`) | **`49` (`21 + 28`)** | `21` kolom sumber 100% utuh + `28` kolom `_024` |
| Ukuran HDFS Silver Parquet | `64.3 M` *(Raw)* | `146.0 M` (`8` partisi) | **`159.2 M` (`8` partisi `19.9 M`)** | Read-back live: **Terverifikasi** |

---

## 3. Rincian Transaksi yang Dipulihkan (Silver v1 → Silver v2)

Total transaksi unik yang dipulihkan dibanding Silver v1 adalah **`3,527` baris** *(catatan: satu baris dapat memenuhi lebih dari satu kategori pelonggaran sehingga jumlah gross per aturan sedikit lebih besar dari `3,527`)*:

| Kategori Aturan yang Dilonggarkan → Flag | Baris Dipulihkan (Gross) | Flag Pengganti di Silver v2 | Penanganan Metrik Valid |
| :--- | ---: | :--- | :--- |
| `fare_amount == 0` dengan `total_amount > 0` | **1,409** *(termasuk **805** Flex Fare `VendorID = 6`)* | `is_zero_fare_024` | `fare_amount_valid_usd_024 = NULL`, `tip_ratio_024 = NULL` |
| Kecepatan implied `> 80` mph | **1,158** | `is_extreme_speed_024` | `avg_speed_mph_024 = NULL` |
| Durasi `> 180` menit | **839** | `is_extreme_duration_024` | `trip_duration_valid_min_024 = NULL` |
| Jarak `> 100` mil | **123** | `is_extreme_distance_024` | `trip_distance_valid_mi_024 = NULL`, `avg_speed_mph_024 = NULL` |
| Tarif tinggi `500–2,000` USD / tip `> 200` USD | **64** | `is_high_fare_outlier_024` | `fare_amount_valid_usd_024 = NULL` (`>500`) / `total_amount_valid_usd_024 = NULL` (`>1000`) |
| Sub-komponen negatif (`extra`/`fare`) dengan `total_amount > 0` | **42** *(19 `VendorID=1` + 23 `VendorID=2` Flex)* | `is_negative_component_024` | `fare_amount_valid_usd_024 = NULL` jika `fare < 0` |
| **Total Unik Dipulihkan (v2 − v1)** | **3,527** | — | — |

> *Catatan `VendorID = 6` Flex Fare:* Dari `867` baris `fare_amount == 0` yang ditemukan pada audit awal, sebanyak **`805` baris** memiliki `total_amount > 0` dan **100% berhasil dipulihkan** ke Silver v2. Sebanyak **`62` baris sisanya** memiliki `total_amount == 0` (transaksi non-berbayar) sehingga tetap dikeluarkan oleh aturan `total_amount <= 0`.

---

## 4. Transaksi yang Tetap Dikeluarkan & Alasan Eksplisit (`15,192` Baris, Nol Overlap)

Seluruh `15,192` baris yang dikeluarkan di Silver v2 bersifat **100% saling lepas (0 overlap antaraturan)**:

| No | Aturan Hard Removal (v2) | Baris Dikeluarkan | % dari Raw | % dari Drop | Alasan Teknis & Dampak |
| :-: | :--- | ---: | ---: | ---: | :--- |
| 1 | `non_positive_total_amount_024` (`total_amount <= 0` atau NULL) | **15,168** | 0.3913% | 99.8420% | Mengecualikan populasi non-berbayar: **`14,688` baris `total_amount < 0`** (`13,018` / `88.63%` terbukti *exact positive mirror reversal* + `1,670` negatif non-pasangan) dan **`480` baris `total_amount == 0`** (dispute/no-charge/void bernilai nol). |
| 2 | `out_of_period_024` (`< 2026-09-01` atau `>= 2026-10-01`) | **22** | 0.0006% | 0.1448% | Di luar periode analisis September 2026 (`1` baris tahun 2009, `2` baris Agt 2026, `19` baris Okt 2026). |
| 3 | `corrupt_extreme_amount_gt_2000_024` (`fare > 2000` atau `total > 2000`) | **2** | 0.0001% | 0.0132% | Terbukti korup: `total = 10,003.70` USD (durasi `0.63` mnt, zona `264→264`) dan `total = 5,000.00` USD (jarak `0.0`, durasi `0.0`, `RatecodeID = 99`, zona `264→264`). |
| 4 | `invalid_location_outside_1_265_024` | **0** | 0.0000% | 0.0000% | Guard integritas zona TLC `1..265`. |
| 5 | `negative_duration_or_distance_024` | **0** | 0.0000% | 0.0000% | Guard durasi/jarak negatif. |
| | **Total Dikeluarkan (Disjoint)** | **15,192** | **0.3919%** | **100.0000%** | — |

### Penanganan Khusus Transaksi Reversal Negatif untuk Analisis Gold Layer
- Selain mengeluarkan `14,688` baris `total_amount < 0` (agar tidak terjadi *double-counting* pickup), Silver v2 secara eksplisit menandai **`13,018` baris transaksi positif yang dipertahankan (`0.3372%`)** dengan flag **`has_negative_reversal_pair_024 = true`** karena memiliki pasangan reversal negatif identik di Raw Layer.
- Dengan demikian, pada Gold Layer:
  - Analisis **permintaan pickup operasional** dapat menggunakan seluruh populasi berbayar atau membandingkan sensitivitas dengan mengecualikan `has_negative_reversal_pair_024`.
  - Analisis **pendapatan bersih (*net revenue*)** dapat secara eksplisit memfilter `has_negative_reversal_pair_024 = false` agar transaksi yang di-dispute/refund tidak dihitung sebagai pendapatan bersih.

---

## 5. Jumlah Flag per Kategori & Overlap Antar-Flag Anomali

### 5.1 Ringkasan Seluruh Flag (`_024`) pada Silver v2 (`3,860,852` Baris)
| Flag (`_024`) | Jumlah `True` | % dari Silver v2 | Kategori |
| :--- | ---: | ---: | :--- |
| `is_flex_fare_024` | 1,190,293 | 30.8298% | Segmentasi kanal Flex Fare |
| `is_passenger_missing_024` | 1,190,293 | 30.8298% | Missing struktural Flex Fare |
| `is_passenger_valid_024` | 2,660,082 | 68.8988% | Validitas penumpang (`1..6`) |
| `is_known_pickup_zone_024` | 3,855,643 | 99.8651% | Zona pickup bukan `264`/`265` |
| `is_known_dropoff_zone_024` | 3,837,665 | 99.3994% | Zona dropoff bukan `264`/`265` |
| `is_weekend_024` | 1,037,686 | 26.8771% | Akhir pekan (Sabtu/Minggu) |
| `is_zero_distance_024` | 145,266 | 3.7625% | Anomali telemetri jarak nol |
| `is_zero_duration_024` | 48,157 | 1.2473% | Anomali telemetri durasi nol (`VendorID=7`) |
| `has_negative_reversal_pair_024` | 13,018 | 0.3372% | Pasangan positif dari reversal negatif Raw |
| `is_zero_fare_024` | 1,409 | 0.0365% | `fare_amount == 0` dgn `total_amount > 0` |
| `is_extreme_speed_024` | 1,158 | 0.0300% | Kecepatan implied `> 80` mph |
| `is_extreme_duration_024` | 839 | 0.0217% | Durasi `> 180` menit |
| `is_extreme_distance_024` | 123 | 0.0032% | Jarak `> 100` mil |
| `is_high_fare_outlier_024` | 64 | 0.0017% | Tarif `500–2,000` / total `> 1,000` / tip `> 200` |
| `is_negative_component_024` | 42 | 0.0011% | Sub-komponen negatif dgn `total_amount > 0` |

### 5.2 Overlap Antar-Flag Anomali (9 Flag Kualitas/Anomali)
- **Bersih tanpa 1 pun flag anomali:** **`3,653,858` baris (`94.6386%`)**
- **Tepat 1 flag anomali:** **`204,069` baris (`5.2856%`)**
- **Overlap `>= 2` flag anomali:** **`2,925` baris (`0.0758%`)**
- **Kombinasi overlap terbesar:**
  1. `is_zero_distance_024 + has_negative_reversal_pair_024`: `1,315` baris (`0.0341%`)
  2. `is_zero_duration_024 + is_zero_distance_024`: `898` baris (`0.0233%`)
  3. `is_zero_fare_024 + has_negative_reversal_pair_024`: `381` baris (`0.0099%`)
  4. `is_zero_distance_024 + is_zero_fare_024 + has_negative_reversal_pair_024`: `136` baris (`0.0035%`)
  5. `is_extreme_distance_024 + is_extreme_speed_024`: `52` baris (`0.0013%`)
  6. `is_zero_distance_024 + is_zero_fare_024`: `38` baris (`0.0010%`)

---

## 6. Perbandingan Distribusi Pickup (Raw Sep 2026 vs Silver v1 vs Silver v2)

| Indikator Demand | Raw (Sep 2026) | Silver v1 | Silver v2 (Live) |
| :--- | :---: | :---: | :---: |
| Total Pickup dalam Periode | `3,876,022` | `3,857,325` (`-0.4824%`) | **`3,860,852` (`-0.3914%`)** |
| Jam Puncak / Jam Sepi | `18` / `4` | `18` / `4` | **`18` / `4`** *(tidak berubah)* |
| Top-5 Jam Puncak | `18, 17, 19, 20, 21` | `18, 17, 19, 20, 21` | **`18, 17, 19, 20, 21`** *(identik)* |
| Korelasi Jam (Pearson / Spearman vs Raw) | — | `0.99999903` / `0.99913043` | **`0.99999920` / `0.99913043`** |
| Perubahan Peringkat Zona Top-10 / 20 / 50 | — | `0` / `0` / `0` | **`0` / `0` / `0`** |
| Korelasi 261 Zona (Pearson / Spearman vs Raw) | — | `0.99999503` / `0.99995697` | **`0.99999604` / `0.99998431`** |
| Pergeseran Pangsa Zona Maksimum | — | `-0.0386 pp` (`132` JFK) | **`-0.0348 pp` (`132` JFK)** |

- **Zona dengan pemulihan tertinggi (v2 vs v1):** `PULocationID = 132` (JFK Airport, Rank #3) memulihkan **`+281` pickup** (`149,399 → 149,680`, drop turun dari `1.46%` menjadi `1.28%`), diikuti `236` (`+91`), `68` (`+87`), `161` (`+82`), dan `239` (`+78`).

---

## 7. Statistik Metrik Mentah vs Metrik Tervalidasi (`*_valid_*_024`)

Pemisahan metrik mentah dari metrik tervalidasi menjaga agar transaksi pickup tetap utuh di Silver Layer sambil menghasilkan statistik bersih tanpa outlier untuk Gold Layer:

| Kolom | Non-Null | Mean | StdDev | Min | Median (P50) | P99 | Max |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trip_distance` *(sumber asli)* | 3,860,852 | 4.97 | 464.34 | 0.00 | 1.97 | 19.42 | 232,644.80 |
| **`trip_distance_valid_mi_024`** *(valid `0<d<=100`)* | 3,715,463 | **3.67** | **4.33** | 0.01 | **2.06** | 19.53 | **99.41** |
| `trip_duration_min_024` *(durasi mentah)* | 3,860,852 | 18.76 | 20.89 | 0.00 | 15.43 | 69.60 | 6,704.60 |
| **`trip_duration_valid_min_024`** *(valid `0<t<=180`)* | 3,811,856 | **18.80** | **13.70** | 0.02 | **15.60** | 69.45 | **179.72** |
| **`avg_speed_mph_024`** *(valid `<=80` mph)* | 3,667,105 | **10.74** | **6.57** | 0.001 | **9.11** | 34.16 | **80.00** |
| `fare_amount` *(sumber asli)* | 3,860,852 | 23.27 | 19.09 | -15.92 | 17.70 | 85.30 | 1,800.00 |
| **`fare_amount_valid_usd_024`** *(valid `0<f<=500`)* | 3,859,357 | **23.27** | **18.88** | 0.01 | **17.70** | 85.21 | **500.00** |
| `total_amount` *(sumber asli)* | 3,860,852 | 32.37 | 22.88 | 1.00 | 25.26 | 107.06 | 1,816.54 |
| **`total_amount_valid_usd_024`** *(valid `0<t<=1000`)* | 3,860,850 | **32.37** | **22.84** | 1.00 | **25.26** | 107.06 | **991.00** |

---

## 8. Verifikasi Akhir Integritas & Konvensi `_024`
- **Skema Sumber Asli:** Seluruh `21` kolom asli NYC TLC (`original_21_columns_preserved_exact_024: true`) utuh 100% nama dan tipe datanya.
- **Konvensi Penamaan:** Seluruh `28` kolom turunan baru menggunakan akhiran `_024` (`all_derived_columns_have_024_suffix_024: true`).
- **Pembacaan Ulang HDFS Live:** Terverifikasi melalui Spark (`LIVE_SILVER_V2_VERIFIED: rows=3860852, cols=49`) pada `/urbanpulse_024/processed/yellow_taxi_024` (`159.2 M`, 8 partisi Snappy Parquet).
- **Keamanan Data:** Raw Layer dan direktori HDFS lama tidak diubah/dihapus; siap melangkah ke tahap Gold Layer setelah persetujuan.
