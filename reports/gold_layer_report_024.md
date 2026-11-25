# Laporan Pembangunan Gold Layer & Panel Fitur ML — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Eksekusi:** 25 November 2026 (`2026-11-25T01:12:14Z`)  
> **Script PySpark:** `spark/processing/build_gold_layer_024.py`  
> **Sumber Silver Layer v2 (Read-Only):** `hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024` (`3,860,852` baris)  
> **Output Live Gold Layer (HDFS):** `hdfs://localhost:9000/urbanpulse_024/analytics/` (`5` dataset Parquet)  
> **Ringkasan Metrik JSON:** `reports/gold_layer_summary_024.json`

---

## 1. Definisi Populasi Konsisten (Pemisahan Demand vs Pendapatan)

Seluruh 5 dataset Gold Layer dibangun dari Silver Layer v2 (`3,860,852` baris, periode `2026-09-01` s.d. `2026-09-30`) tanpa mengubah atau menghapus Silver Layer, dengan pemisahan populasi yang tegas dan konsisten:

1. **Populasi Demand Pickup Operasional (`pickup_count_024`):**
   - Menggunakan seluruh **`3,860,852` transaksi Silver Layer v2** (mencakup Flex Fare `payment_type = 0`, `VendorID = 6/7`, zona `264/265`, serta transaksi ber-flag anomali telemetri agar jumlah permintaan tidak bias).
   - Sebagai transparansi terhadap pasangan reversal, disediakan pula kolom pendamping **`non_reversed_pickup_count_024`** (`3,847,834` baris, mengecualikan `13,018` baris `has_negative_reversal_pair_024 = true`).
2. **Populasi Pendapatan Bersih & Metrik Ekonomi/Perjalanan (`net_settled_revenue_usd_024`, `avg_settled_*`, `avg_valid_*`):**
   - Secara eksplisit **mengecualikan `has_negative_reversal_pair_024 = true` (`13,018` baris / `0.34%`)** agar transaksi yang di-dispute/refund tidak dihitung sebagai pendapatan bersih.
   - Menggunakan kolom metrik tervalidasi dari Silver v2 (`total_amount_valid_usd_024`, `fare_amount_valid_usd_024`, `trip_distance_valid_mi_024`, `trip_duration_valid_min_024`, `avg_speed_mph_024`) sehingga outlier ekstrem (`> 500` USD tarif / `> 1,000` USD total / `> 100` mil / `> 180` menit / `> 80` mph) tidak mendistorsi rata-rata.
3. **Catatan Referensi Zona:** `taxi_zone_lookup.csv` belum tersedia di `data/reference/` maupun `/urbanpulse_024/raw/reference/`. Oleh karena itu, kolom asli **`PULocationID`** dipertahankan tanpa mengarang nama zona, dan siap di-join saat file referensi resmi dimasukkan.

---

## 2. Daftar Output Gold Layer & Hasil Verifikasi HDFS (Read-Back Live)

Seluruh dataset ditulis terlebih dahulu ke staging `/urbanpulse_024/analytics/_staging_gold_024`, melewati seluruh uji rekonsiliasi & zero-leakage, kemudian dipromosikan secara aman ke `/urbanpulse_024/analytics/` dan diverifikasi pembacaan ulang live via Spark:

| Dataset Gold (`_024`) | Lokasi Live HDFS | Baris | Kolom | Ukuran | Rekonsiliasi Total Pickup | Rentang Waktu (`timestamp_ntz`) | Status Null |
| :--- | :--- | ---: | :-: | ---: | ---: | :--- | :--- |
| `gold_hourly_demand_024` | `/urbanpulse_024/analytics/gold_hourly_demand_024` | **720** | 21 | `59.1 K` | `3,860,852` (Match) | `2026-09-01 00:00` → `2026-09-30 23:00` | **0 null** pada seluruh kolom |
| `gold_zone_demand_024` | `/urbanpulse_024/analytics/gold_zone_demand_024` | **261** | 27 | `41.4 K` | `3,860,852` (Match) | `2026-09` (30 hari) | **0 null** pada seluruh kolom |
| `gold_temporal_patterns_024` | `/urbanpulse_024/analytics/gold_temporal_patterns_024` | **168** | 17 | `16.6 K` | `3,860,852` (Match) | `7 hari × 24 jam` | **0 null** pada seluruh kolom |
| `gold_zone_hourly_demand_024` | `/urbanpulse_024/analytics/gold_zone_hourly_demand_024` | **6,264** | 18 | `272.1 K` | `3,860,852` (Match) | `261 zona × 24 jam` | `avg_settled_total` (458), `avg_valid_distance` (501), `avg_valid_duration` (459) pada slot 0-pickup/unobserved |
| `gold_demand_features_024` | `/urbanpulse_024/analytics/gold_demand_features_024` | **187,920** | 26 | `1.9 M` | `3,860,852` (Match) | `2026-09-01 00:00` → `2026-09-30 23:00` | Hanya pada periode warm-up lag/rolling (`lag_1h`: 261, `lag_24h`/`roll_24h`: 6,264, `lag_168h`/`roll_7d`: 43,848); **0 null** pada `144,072` baris ML-ready |

- **Validasi Penamaan (`_024`):** Lulus 100% pada ke-5 dataset (`naming_convention_024_valid: true`; satu-satunya kolom tanpa akhiran `_024` adalah kolom sumber asli `PULocationID`).
- **Tipe Timestamp:** `pickup_ts_hour_024` menggunakan `timestamp_ntz` (konsisten dengan `tpep_pickup_datetime` di Silver Layer, bebas pergeseran zona waktu OS).

---

## 3. Evaluasi Panel Time Series ML (`gold_demand_features_024`) & Uji Zero Data Leakage

### 3.1 Evaluasi Missing Hours & Missing Zone-Hour Combinations
- **Tingkat Kota (`gold_hourly_demand_024`):** **`0` missing hours** dari `720` jam (`100.00%` teramati).
- **Tingkat Zona × Jam (`gold_demand_features_024`, `261 zona × 720 jam = 187,920` slot):**
  - Teramati di Silver (`is_observed_in_silver_024 = true`): **`101,884` slot (`54.2167%`)**
  - Tidak teramati / *zero-filled* (`pickup_count_024 = 0`): **`86,036` slot (`45.7833%`)**
- **Sebaran Sparsity per Tier Permintaan Zona:**

| Tier Permintaan Zona | Jumlah Zona | Total Slot (`×720`) | Slot Teramati | % Zero-Filled | Implikasi Pemodelan ML (Fase 5) |
| :--- | ---: | ---: | ---: | ---: | :--- |
| `Top 10` | 10 | 7,200 | 7,192 | **0.11%** | Hampir kontinu penuh; sangat ideal untuk lag & rolling |
| `High (11-50)` | 40 | 28,800 | 28,596 | **0.71%** | Sangat padat (`99.29%` aktif) |
| `Medium (51-150)` | 100 | 72,000 | 52,256 | **27.42%** | Kepadatan menengah; kombinasi lag + kalender efektif |
| `Low (>150)` | 111 | 79,920 | 13,840 | **82.68%** | Sangat jarang (*zero-inflated*); evaluasi metrik ML nanti sebaiknya dilaporkan keseluruhan dan khusus Top-50 |

### 3.2 Fitur Historis & Verifikasi Bebas Data Leakage
Seluruh fitur historis dihitung per `PULocationID` terurut menaik menurut `pickup_ts_hour_024` pada grid reguler 1 jam:
- `lag_1h_pickup_count_024` (`t - 1 jam`)
- `lag_24h_pickup_count_024` (`t - 24 jam`)
- `lag_168h_pickup_count_024` (`t - 168 jam` / jam yang sama 7 hari lalu)
- `rolling_avg_24h_pickup_count_024` (`rowsBetween(-24, -1)` — eksklusif `t`)
- `rolling_avg_7d_pickup_count_024` & `rolling_std_7d_pickup_count_024` (`rowsBetween(-168, -1)` — eksklusif `t`, hanya terisi setelah 168 jam penuh / `2026-09-08 00:00:00`)
- Fitur kalender & siklis tanpa informasi masa depan: `pickup_hour_024`, `day_of_week_024`, `is_weekend_024`, `is_rush_hour_024`, `hour_sin_024`, `hour_cos_024`, `dow_sin_024`, `dow_cos_024`.
- **Baris ML-Ready Penuh (`is_ml_ready_full_history_024 = true`):** **`144,072` baris** (`23 hari × 24 jam × 261 zona`, `2026-09-08 00:00:00` s.d. `2026-09-30 23:00:00`) dengan **0 nilai null** pada seluruh fitur lag/rolling.
- **Uji Spot-Check Zero-Leakage (`PULocationID = 237`, `2026-09-15 18:00:00`):**
  - Aktual `pickup_count_024(t)` = `622`, `lag_1h` = `552.0`, `lag_24h` = `511.0`, `lag_168h` = `481.0`.
  - Nilai `rolling_avg_7d_pickup_count_024` tersimpan = **`260.1012`**, cocok persis dengan rata-rata manual 168 jam lampau `[2026-09-08 18:00 .. 2026-09-15 17:00]` (mengecualikan `t`) = **`260.1012`** (`zero_leakage_assertions_passed_024: true`).

---

## 4. Ringkasan KPI & Insight Awal Business Intelligence (Data Aktual September 2026)

### 4.1 KPI Utama
| Indikator | Nilai Aktual |
| :--- | ---: |
| Total Pickup Operasional (`pickup_count_024`) | **3,860,852** (`128,695.07`/hari; `5,362.29`/jam) |
| Total Pickup Non-Reversal (`non_reversed_pickup_count_024`) | **3,847,834** (`13,018` baris reversal dikecualikan dari pendapatan) |
| Kontribusi Flex Fare (`payment_type = 0`) | **1,190,293** pickup (**30.83%**) |
| Total Pendapatan Bersih Tervalidasi (`net_settled_revenue_usd_024`) | **USD 124,583,114.07** |

### 4.2 Jam Puncak & Jam Sepi
- **Jam Puncak Harian:** **Jam `18:00`** (`267,143` pickup; rata-rata `8,904.77`/hari; pendapatan bersih `USD 8,392,771.29`), disusul Jam `17:00` (`254,542` — pendapatan tertinggi `USD 8,597,706.59`), `19:00` (`251,316`), `20:00` (`237,885`), dan `21:00` (`235,505`).
- **Jam Sepi Harian:** **Jam `04:00`** (`32,138` pickup; rata-rata `1,071.27`/hari — **8.3× lebih rendah** dibanding jam puncak `18:00`), diikuti `05:00` (`36,055`) dan `03:00` (`38,964`).
- **Slot Kalender Tersibuk & Tersepi:** Tersibuk pada **Kamis `2026-09-17 19:00`** (`11,718` pickup; `USD 369,118.08`); tersepi pada **Selasa `2026-09-15 03:00`** (`298` pickup; `USD 10,640.03`).

### 4.3 Top-10 & Bottom-10 Zona Pickup (`PULocationID`)
Konsentrasi spasial sangat tinggi: **Top-10 zona (`3.8%` dari 261 zona) menyumbang `33.03%` total pickup**, dan **Top-20 zona menyumbang `56.45%`**.

| Rank | `PULocationID` | Total Pickup | Pangsa (%) | Jam Puncak Zona | Pangsa Akhir Pekan | Pendapatan Bersih (USD) | Rata-Rata Total (USD) |
| :-: | :-: | ---: | ---: | :-: | ---: | ---: | ---: |
| 1 | `237` | 172,229 | 4.46% | 18 | 19.64% | 3,993,745.03 | 23.26 |
| 2 | `236` | 154,695 | 4.01% | 15 | 19.67% | 3,700,641.99 | 23.98 |
| 3 | `132` | 149,680 | 3.88% | 16 | 26.85% | **11,959,824.66** | **80.85** |
| 4 | `161` | 144,560 | 3.74% | 18 | 18.67% | 4,206,375.15 | 29.19 |
| 5 | `186` | 119,283 | 3.09% | 19 | 24.91% | 3,386,540.72 | 28.50 |
| 6 | `142` | 110,974 | 2.87% | 21 | 24.67% | 2,814,103.73 | 25.43 |
| 7 | `162` | 110,696 | 2.87% | 18 | 19.31% | 3,057,550.16 | 27.71 |
| 8 | `68` | 105,891 | 2.74% | 19 | 25.68% | 3,225,321.09 | 30.55 |
| 9 | `79` | 103,782 | 2.69% | **23** | **42.80%** | 2,910,060.23 | 28.11 |
| 10 | `230` | 103,465 | 2.68% | 21 | 25.51% | 3,339,536.27 | 32.45 |

- **Divergensi Volume vs Pendapatan:** Zona `132` (peringkat #3 volume) menghasilkan pendapatan bersih **USD 11.96 juta** (**3× lebih besar** dari peringkat #1 `237`, rata-rata `USD 80.85`/perjalanan) — hampir `9.6%` dari seluruh pendapatan taksi kota.
- **Karakteristik Unik per Zona:** Zona `79` memuncak larut malam (`23:00`) dengan pangsa akhir pekan **`42.80%`** (profil hiburan malam), kontras dengan `161` dan `237` yang berorientasi jam kerja (`18.7%–19.6%` akhir pekan, puncak `18:00`).
- **Bottom-10 Zona (`Rank 252–261`):** `PULocationID` `199` (1 pickup), `204` (2), `105` (2), `84` (4), `44` (5), `5` (6), `187` (7), `2` (8), `111` (9), `30` (10) — hanya aktif 1–9 jam sepanjang bulan.

### 4.4 Perbandingan Weekday vs Weekend & Pola Hari (Ternormalisasi Kalender)
September 2026 memiliki `22` hari kerja (`5` Selasa, `5` Rabu, masing-masing `4` Senin/Kamis/Jumat) dan `8` hari akhir pekan (`4` Sabtu, `4` Minggu):

| Kategori | Hari Kalender | Total Pickup | Rata-Rata Pickup/Hari | Rata-Rata Pickup/Jam | Pendapatan Bersih (USD) | Jam Puncak |
| :--- | :-: | ---: | ---: | ---: | ---: | :-: |
| **Weekday** | 22 | 2,823,166 | `128,325.73` | `5,346.91` | `92,529,082.38` (`4.21M`/hari) | `18:00` |
| **Weekend** | 8 | 1,037,686 | **`129,710.75`** | **`5,404.61`** | `32,054,031.69` (`4.01M`/hari) | `18:00` |

- **Peringkat Hari Tersibuk (rata-rata per kemunculan hari):** **Kamis (`141,258`/hari)** > **Jumat (`138,547`/hari)** > **Sabtu (`137,808`/hari)** > Rabu (`131,047`/hari) > Selasa (`122,270`/hari) > Minggu (`121,614`/hari) > **Senin (`109,340`/hari)** *(Senin paling rendah, sejalan dengan efek awal pekan & libur Labor Day awal September)*.
- **Hotspot Zona × Jam Tertinggi:** `PULocationID = 237` pukul `18:00` (`13,450` pickup; `448.3`/hari) dan `17:00` (`13,265`), diikuti `237` pukul `15:00` (`12,722`), `237` pukul `14:00` (`12,672`), `236` pukul `15:00` (`12,391`), serta `161` pukul `18:00` (`12,202`).

---

## 5. Status Kesiapan Tahap Berikutnya
- Seluruh 5 dataset Gold Layer di `/urbanpulse_024/analytics/` sudah live, terverifikasi baca ulang dari HDFS, terekonsiliasi 100% terhadap Silver v2, dan bebas *data leakage*.
- Silver Layer (`/urbanpulse_024/processed/yellow_taxi_024/`), Raw Layer, serta kode dashboard tidak diubah. Tahap berikutnya siap diarahkan ke **Spark MLlib Forecasting (Fase 5)** atau pengayaan `taxi_zone_lookup.csv` terlebih dahulu.
