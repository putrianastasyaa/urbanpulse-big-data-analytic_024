# Audit Dampak Cleaning Silver Layer terhadap Analisis Demand — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Audit:** 25 November 2026 (`2026-11-25T00:43:27Z`)  
> **Mode Audit:** **Read-Only** *(Raw Layer dan Silver Layer di HDFS tidak diubah maupun dihapus)*  
> **Script Audit:** `spark/processing/audit_silver_cleaning_impact_024.py`  
> **Output JSON Audit:** `reports/silver_cleaning_impact_audit_024.json`  
> **Input Raw:** `hdfs://localhost:9000/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet` (`3,876,044` baris)  
> **Input Silver:** `hdfs://localhost:9000/urbanpulse_024/processed/yellow_taxi_024` (`3,857,325` baris)

---

## 1. Ringkasan Eksekutif Audit

1. **Dampak Makro terhadap Analisis Demand Sangat Kecil (Non-Material):**  
   Dari `3,876,022` pickup pada periode September 2026 di Raw Layer, Silver Layer mempertahankan **`3,857,325` pickup (`99.5176%`)** dan mengeluarkan `18,697` baris dalam periode (`0.4824%`, plus `22` baris di luar September 2026 = total `18,719` baris).
   - **Jam puncak (`18:00`)**, **jam sepi (`04:00`)**, dan **urutan Top-5 jam puncak (`18, 17, 19, 20, 21`)** **tidak berubah sama sekali** (Pearson `0.999999`, Spearman `0.999130`, pergeseran pangsa maks `0.0054` poin persentase).
   - **Urutan peringkat Top-10, Top-20, bahkan Top-50 zona pickup (`PULocationID`) tidak berubah sama sekali (`0` perubahan peringkat)**, dengan korelasi seluruh `261` zona aktif sebesar **Spearman `0.999957`** dan **Pearson `0.999995`**.
2. **Validasi Penghapusan Transaksi Negatif (`14,730` baris = `78.7%` dari total drop):**  
   Uji pasangan balik (*exact positive mirror-pair check*) membuktikan bahwa **`13,060` dari `14,730` baris negatif (`88.66%`) memiliki pasangan transaksi positif yang identik** di Raw Layer (`VendorID`, `pickup`, `dropoff`, `PULocationID`, `DOLocationID`, `trip_distance`, dan `|total_amount|`). Artinya baris negatif tersebut adalah **jurnal pembalik/refund/dispute**, sehingga **menghapusnya justru mencegah *double-counting* pickup**.
3. **Temuan Celah Penyempurnaan (~`2,900`–`3,000` baris = `~0.08%` dari Raw):**  
   Audit menemukan sejumlah subkelompok kecil yang kemungkinan besar tetap merupakan **event pickup nyata**, namun terhapus karena filter telemetri/tarif:
   - **`867` baris `VendorID = 6` (`payment_type = 0` Flex Fare)** yang memiliki `fare_amount = 0.0`, tetapi `total_amount` positif (rata-rata `29.65` USD) dan jarak tempuh valid (rata-rata `7.63` mil).
   - **`852` baris durasi `> 180` menit** yang mayoritas (`720–1440` menit, `459` baris) memiliki median jarak `1.96` mil dan median tarif `15.60` USD (pickup perjalanan normal dengan anomali penutupan meter/dropoff `VendorID = 2`).
   - **`1,168` baris kecepatan `> 80` mph** yang tetap berbayar (rata-rata tarif `17.43–57.04` USD) namun memiliki glitch GPS/odometer atau durasi sangat singkat `< 1` menit.
   - **`~56` dari `66` baris tarif `> 500` USD** yang bertipe `RatecodeID = 4` (Nassau/Westchester) dan `RatecodeID = 5` (Negotiated Fare) berjarak `43–93` mil.

---

## 2. Identifikasi Aturan Penghapusan & Audit Overlap (Tanpa Hitung Ganda)

Pada `spark/processing/clean_yellow_taxi_024.py`, terdapat **6 aturan penghapusan (hard removal)** yang menghasilkan tepat **`18,719` baris dihapus** (`18,596` baris oleh satu aturan tunggal dan `123` baris mengalami overlap antaraturan).

### 2.1 Tabel Per-Aturan (Gross vs Eksklusif vs Overlap)

| No | Aturan Penghapusan | Gross (Memenuhi Aturan) | Eksklusif (Hanya Aturan Ini) | Overlap dengan Aturan Lain | % dari Raw | % dari Total Drop |
| :-: | :--- | ---: | ---: | ---: | ---: | ---: |
| 1 | `invalid_fare_money_024` *(negatif, nol, atau ekstrem)* | 16,685 | 16,632 | 53 | 0.4305% | 89.1340% |
| 2 | `speed_gt_80mph_024` *(> 80 mph)* | 1,168 | 1,091 | 77 | 0.0301% | 6.2396% |
| 3 | `duration_gt_180m_024` *(< 0 atau > 180 menit)* | 852 | 813 | 39 | 0.0220% | 4.5515% |
| 4 | `distance_gt_100mi_024` *(< 0 atau > 100 mil)* | 125 | 41 | 84 | 0.0032% | 0.6678% |
| 5 | `out_of_period_024` *(di luar Sep 2026)* | 22 | 19 | 3 | 0.0006% | 0.1175% |
| 6 | `invalid_location_024` *(di luar 1..265)* | 0 | 0 | 0 | 0.0000% | 0.0000% |
| | **Total Gabungan Unik (Disjoint)** | — | **18,596** *(1 aturan)* | **123** *(>=2 aturan)* | **0.4829%** | **100.0000% (`18,719`)** |

### 2.2 Dekomposisi Disjoint (Saling Lepas — Total Tepat `18,719` Baris)

Rincian kombinasi saling lepas (*mutually exclusive*, dengan pemisahan sub-kategori moneter menjadi negatif, nol, dan ekstrem):

| Kombinasi Aturan (Saling Lepas) | Jumlah Baris | % dari Drop | % dari Raw |
| :--- | ---: | ---: | ---: |
| `negative_money_024` *(eksklusif)* | 14,717 | 78.6207% | 0.3797% |
| `zero_fare_or_total_024` *(eksklusif, non-negatif)* | 1,868 | 9.9792% | 0.0482% |
| `speed_gt_80mph_024` *(eksklusif)* | 1,091 | 5.8283% | 0.0281% |
| `duration_gt_180m_024` *(eksklusif)* | 813 | 4.3432% | 0.0210% |
| `distance_gt_100mi_024 + speed_gt_80mph_024` | 53 | 0.2831% | 0.0014% |
| `extreme_money_024` *(eksklusif)* | 47 | 0.2511% | 0.0012% |
| `distance_gt_100mi_024` *(eksklusif)* | 41 | 0.2190% | 0.0011% |
| `out_of_period_024` *(eksklusif)* | 19 | 0.1015% | 0.0005% |
| `speed_gt_80mph_024 + zero_fare_or_total_024` | 15 | 0.0801% | 0.0004% |
| `duration_gt_180m_024 + distance_gt_100mi_024` | 14 | 0.0748% | 0.0004% |
| `duration_gt_180m_024 + distance_gt_100mi_024 + extreme_money_024` | 8 | 0.0427% | 0.0002% |
| `distance_gt_100mi_024 + extreme_money_024` | 7 | 0.0374% | 0.0002% |
| `speed_gt_80mph_024 + negative_money_024` | 6 | 0.0321% | 0.0002% |
| `duration_gt_180m_024 + zero_fare_or_total_024` | 6 | 0.0321% | 0.0002% |
| `duration_gt_180m_024 + negative_money_024` | 5 | 0.0267% | 0.0001% |
| `out_of_period_024 + duration_gt_180m_024` | 3 | 0.0160% | 0.0001% |
| Kombinasi overlap lainnya (`<= 2` baris per kombinasi: 4 kombinasi) | 6 | 0.0320% | 0.0002% |
| **Total Disjoint (Verifikasi Nol Hitung Ganda)** | **18,719** | **100.0000%** | **0.4829%** |

---

## 3. Evaluasi Khusus per Kelompok Anomali

### 3.1 Fare / Total / Tip / Surcharge Negatif (`14,730` baris)
- **Komposisi:** `99.66%` berasal dari `VendorID = 2` (`payment_type = 4` Dispute `8,671` baris; `2` Cash `3,587` baris; `3` No Charge `1,518` baris; `1` Credit Card `842` baris; `0` Flex Fare `62` baris), plus `50` baris dari `VendorID = 1`.
- **Uji Mirror-Reversal Pair:** **`13,060 / 14,730` baris (`88.6626%`)** memiliki **pasangan transaksi positif yang persis sama** pada `(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, round(trip_distance,2), round(|total_amount|,2))`.
- **Evaluasi Validitas Pickup & Dampak Demand:**
  - Bukan pickup baru; mayoritas mutlak adalah entri jurnal pembalik (*reversal/refund*). Jika dipertahankan, permintaan pickup akan **terhitung dua kali (over-counting)**.
  - Pengecualian sangat minor: `19` baris `VendorID = 1, payment_type = 0` memiliki `mean_fare = +82.40` USD dan `mean_total = +99.18` USD (terkena aturan hanya karena salah satu surcharge/tip negatif), dampaknya `0.0005%` (diabaikan secara makro).
- **Status:** **Tepat dihapus** dari Silver Layer.

### 3.2 Sub-Temuan Moneter: `fare_amount == 0` Non-Negatif (`1,889` baris)
- Meskipun bagian dari `invalid_fare_money_024`, audit memisahkan kelompok ini dari transaksi negatif dan menemukan anomali struktural vendor:
  - **`867` baris (`45.9%` dari kelompok ini)** berasal dari **`VendorID = 6, payment_type = 0` (Flex Fare)**: `100%` memiliki `trip_distance > 0` (`mean = 7.63` mil), `duration > 0`, dan **`mean_total_amount = +29.65` USD**, namun `fare_amount = 0.0`. Ini adalah **16.56% dari seluruh transaksi `VendorID = 6`** (`5,236` baris).
  - Sisanya (`1,022` baris) mayoritas adalah Cash/Dispute/No-Charge bernilai `total_amount` sangat kecil (`0.29–3.35` USD).
- **Evaluasi Validitas Pickup:** `867` baris `VendorID = 6` **adalah pickup berbayar yang valid**. Dampaknya terhadap total kota kecil (`0.022%`), namun secara prinsip kualitas data sebaiknya syarat penghapusan berfokus pada `total_amount <= 0`, sedangkan `fare_amount == 0` cukup ditandai dengan flag `is_zero_fare_024`.

### 3.3 Fare atau Total Ekstrem (`66` baris; `47` eksklusif)
- **Komposisi:**
  - `28` baris `RatecodeID = 5` (Negotiated Fare, Cash): tarif `550–1,800` USD, rata-rata jarak `53.01` mil, durasi `131.12` menit.
  - `19` baris `RatecodeID = 4` (Nassau/Westchester, Cash): tarif `503.50–800.30` USD, rata-rata jarak `92.67` mil, durasi `131.90` menit.
  - `9` baris `payment_type = 0` (Flex Fare): tarif `500.08–720.00` USD, rata-rata jarak `43.73` mil, durasi `70.36` menit.
  - **Outlier korup nyata:** `1` baris `RatecodeID = 1` bertarif `9,999.70` USD dalam `0.63` menit, dan `2` baris `RatecodeID = 99` bertarif `600–5,000` USD dengan jarak `0.0` dan durasi `0.0`.
- **Evaluasi Validitas Pickup & Dampak Demand:** Sekitar `56` dari `66` baris kemungkinan merupakan pickup perjalanan luar kota / tarif negosiasi yang sah. Dampaknya terhadap demand sangat kecil (`0.0017%`), tetapi secara metodologis lebih rapi **dipisahkan menjadi flag `is_high_fare_outlier_024`** untuk analisis tarif (hanya hard-drop outlier tak masuk akal seperti `> 2,000` USD).

### 3.4 Durasi Perjalanan > 3 Jam (`852` baris; `813` eksklusif)
| Bucket Durasi | Baris | Mean Jarak (mil) | Median Jarak (mil) | Mean Tarif (USD) | Median Tarif (USD) | Baris `VendorID = 2` |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `180–360m (3–6 jam)` | 218 | 30.18 | 11.73 | 109.40 | 70.00 | 191 (87.6%) |
| `360–720m (6–12 jam)` | 157 | 6.52 | 3.82 | 35.74 | 21.90 | 149 (94.9%) |
| `720–1440m (12–24 jam)` | 459 | 4.49 | **1.96** | 28.06 | **15.60** | 440 (95.9%) |
| `>1440m (>24 jam)` | 18 | 6.84 | 4.61 | 33.00 | 18.99 | 15 (83.3%) |

- **Evaluasi Validitas Pickup & Dampak Demand:**
  - Pada bucket `12–24 jam` (`459` baris), **median jarak (`1.96` mil) dan median tarif (`15.60` USD) hampir identik dengan perjalanan taksi NYC normal**, dan `95.9%` berasal dari `VendorID = 2` — indikasi kuat bahwa **event pickup terjadi secara sah**, sementara hanya `tpep_dropoff_datetime` yang tertahan/error hingga akhir shift.
  - Dampaknya terhadap demand agregat sangat kecil (`0.022%`), namun secara konseptual **penghapusan durasi ekstrem lebih tepat diganti menjadi flag `is_extreme_duration_024` + `trip_duration_valid_min_024 = NULL`** agar pickup tetap tercatat tanpa merusak statistik durasi.

### 3.5 Kecepatan Implied > 80 mph (`1,168` baris; `1,091` eksklusif)
| Bucket Durasi | Baris | Jarak > 100 mil | Median Jarak (mil) | Median Kecepatan (mph) | Mean Tarif (USD) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `>= 5 menit` | 756 | 52 | 31.90 | 128.45 | 17.43 |
| `< 1 menit` *(artifact durasi singkat)* | 329 | 0 | 1.40 | 514.29 | 57.04 |
| `1–5 menit` | 83 | 2 | 14.50 | 223.75 | 22.47 |

- **Evaluasi Validitas Pickup & Dampak Demand:**
  - Mayoritas tetap merupakan perjalanan berbayar (`mean_fare = 17.43–57.04` USD) yang mengalami error telemetri GPS/odometer atau pembulatan timestamp dropoff (`< 1` menit).
  - Dampaknya terhadap demand agregat sangat kecil (`0.0301%`), tetapi untuk analisis jumlah pickup, **lebih tepat di-flag (`is_extreme_speed_024`, `avg_speed_mph_024 = NULL`, `trip_distance_valid_mi_024 = NULL` bila `> 100` mil)** daripada menghapus pickup-nya.

### 3.6 Timestamp di Luar September 2026 (`22` baris; `19` eksklusif)
- `2009-01`: `1` baris (`2009-01-01 09:32:44`) — error jam sistem.
- `2026-08`: `2` baris (`2026-08-31 23:46:38` s.d. `23:59:32`) — bulan sebelumnya.
- `2026-10`: `19` baris (`2026-10-01 00:28:25` s.d. `2026-10-07 07:40:40`) — bulan berikutnya.
- **Evaluasi:** **Wajib dihapus** untuk menjaga batas periode analitis September 2026 yang konsisten (`0.0006%`).

---

## 4. Perbandingan Demand Pickup: Raw (September 2026) vs Silver Layer

### 4.1 Perbandingan Pickup per Jam (`00:00` – `23:00`)

| Indikator Temporal | Raw (Sep 2026) | Silver Layer | Perubahan |
| :--- | :---: | :---: | :--- |
| Total Pickup dalam Periode | `3,876,022` | `3,857,325` | `-18,697` (`-0.4824%`) |
| **Jam Puncak (*Peak Hour*)** | **Jam `18`** (`268,207` / `6.9196%`) | **Jam `18`** (`266,942` / `6.9204%`) | **Tidak berubah** (`+0.0008 pp`) |
| **Jam Sepi (*Quietest Hour*)** | **Jam `4`** (`32,315` / `0.8337%`) | **Jam `4`** (`32,084` / `0.8318%`) | **Tidak berubah** (`-0.0019 pp`) |
| **Urutan Top-5 Jam Puncak** | `18, 17, 19, 20, 21` | `18, 17, 19, 20, 21` | **100% Identik** |
| Korelasi Pearson / Spearman | — | **`0.99999903` / `0.99913043`** | Hampir sempurna (`~1.0`) |
| Rentang % Drop per Jam | — | `0.3431%` (Jam 8) s.d. `0.7148%` (Jam 4) | Merata di seluruh jam |
| Pergeseran Pangsa Jam Maksimum | — | **`0.0054` poin persentase** (Jam 8) | Tidak material |

<details>
<summary>Tabel Lengkap 24 Jam (Raw Sep 2026 vs Silver Layer)</summary>

| Jam | Raw Sep 2026 | Silver Layer | Selisih | % Drop | Pangsa Raw | Pangsa Silver | Shift (pp) |
| :-: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 130,688 | 130,035 | -653 | 0.50% | 3.3717% | 3.3711% | -0.0006 |
| 1 | 82,835 | 82,388 | -447 | 0.54% | 2.1371% | 2.1359% | -0.0012 |
| 2 | 56,173 | 55,852 | -321 | 0.57% | 1.4492% | 1.4479% | -0.0013 |
| 3 | 39,158 | 38,941 | -217 | 0.55% | 1.0103% | 1.0095% | -0.0008 |
| 4 | 32,315 | 32,084 | -231 | 0.71% | 0.8337% | 0.8318% | -0.0019 |
| 5 | 36,199 | 35,987 | -212 | 0.59% | 0.9339% | 0.9330% | -0.0009 |
| 6 | 68,244 | 67,928 | -316 | 0.46% | 1.7607% | 1.7610% | +0.0003 |
| 7 | 120,617 | 120,080 | -537 | 0.45% | 3.1119% | 3.1130% | +0.0011 |
| 8 | 151,567 | 151,047 | -520 | 0.34% | 3.9104% | 3.9158% | +0.0054 |
| 9 | 156,858 | 156,114 | -744 | 0.47% | 4.0469% | 4.0472% | +0.0003 |
| 10 | 159,239 | 158,402 | -837 | 0.53% | 4.1083% | 4.1065% | -0.0018 |
| 11 | 169,752 | 168,903 | -849 | 0.50% | 4.3795% | 4.3788% | -0.0007 |
| 12 | 178,130 | 177,195 | -935 | 0.52% | 4.5957% | 4.5937% | -0.0020 |
| 13 | 184,244 | 183,196 | -1,048 | 0.57% | 4.7534% | 4.7493% | -0.0041 |
| 14 | 207,095 | 205,921 | -1,174 | 0.57% | 5.3430% | 5.3384% | -0.0046 |
| 15 | 220,498 | 219,278 | -1,220 | 0.55% | 5.6888% | 5.6847% | -0.0041 |
| 16 | 226,593 | 225,374 | -1,219 | 0.54% | 5.8460% | 5.8428% | -0.0032 |
| 17 | 255,476 | 254,330 | -1,146 | 0.45% | 6.5912% | 6.5934% | +0.0022 |
| 18 | 268,207 | 266,942 | -1,265 | 0.47% | 6.9196% | 6.9204% | +0.0008 |
| 19 | 252,280 | 251,181 | -1,099 | 0.44% | 6.5087% | 6.5118% | +0.0031 |
| 20 | 238,797 | 237,731 | -1,066 | 0.45% | 6.1609% | 6.1631% | +0.0022 |
| 21 | 236,316 | 235,374 | -942 | 0.40% | 6.0969% | 6.1020% | +0.0051 |
| 22 | 220,583 | 219,659 | -924 | 0.42% | 5.6910% | 5.6946% | +0.0036 |
| 23 | 184,158 | 183,383 | -775 | 0.42% | 4.7512% | 4.7541% | +0.0029 |

</details>

### 4.2 Perbandingan Distribusi & Peringkat Zona (`PULocationID`)

| Indikator Spasial | Hasil Aktual | Interpretasi |
| :--- | :---: | :--- |
| Jumlah Zona Aktif (Raw Sep 2026 vs Silver) | `261` vs `261` (`0` zona hilang) | Cakupan spasial 100% utuh |
| **Perubahan Peringkat Top-10 / Top-20 / Top-50** | **`0` / `0` / `0`** | Urutan Top-50 zona **100% identik** |
| Korelasi Pearson / Spearman (seluruh 261 zona) | **`0.99999503` / `0.99995697`** | Distribusi spasial nyaris identik |
| Pergeseran Pangsa Zona Maksimum | **`-0.0386` pp** (`PULocationID = 132`) | Sangat kecil secara makro |

#### Perbandingan Top-10 Zona Pickup (`PULocationID`)
| Rank Raw | Rank Silver | Delta | `PULocationID` | Raw Sep 2026 | Silver Layer | Drop | % Drop | Shift Pangsa (pp) |
| :-: | :-: | :-: | :-: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0 | `237` | 172,832 | 172,160 | -672 | 0.39% | +0.0042 |
| 2 | 2 | 0 | `236` | 155,118 | 154,604 | -514 | 0.33% | +0.0061 |
| 3 | 3 | 0 | `132` *(JFK)* | 151,618 | 149,399 | -2,219 | **1.46%** | **-0.0386** |
| 4 | 4 | 0 | `161` | 145,110 | 144,478 | -632 | 0.44% | +0.0017 |
| 5 | 5 | 0 | `186` | 119,790 | 119,219 | -571 | 0.48% | +0.0002 |
| 6 | 6 | 0 | `142` | 111,323 | 110,921 | -402 | 0.36% | +0.0035 |
| 7 | 7 | 0 | `162` | 111,132 | 110,642 | -490 | 0.44% | +0.0012 |
| 8 | 8 | 0 | `68` | 106,250 | 105,804 | -446 | 0.42% | +0.0017 |
| 9 | 9 | 0 | `79` | 104,097 | 103,729 | -368 | 0.35% | +0.0034 |
| 10 | 10 | 0 | `230` | 104,083 | 103,394 | -689 | 0.66% | -0.0048 |

#### Catatan Sensitivitas Lokal (Zona Non-Inti / Bandara)
- Pada zona bervolume tinggi, `PULocationID = 132` (JFK Airport, Rank #3) mengalami `% drop` tertinggi (`1.46%` vs rata-rata kota `0.48%`) akibat proporsi dispute, tarif negosiasi `> 500` USD, dan durasi/kecepatan ekstrem yang lebih tinggi di rute bandara.
- Pada zona bervolume kecil (`>= 100` pickup, di luar Top-80), beberapa zona pinggiran mengalami drop `3.7%–7.7%` (tertinggi `PULocationID = 193` sebesar `7.68%` atau `257` dari `3,345` pickup, turun 1 peringkat `85 → 86`, dan `PULocationID = 265` Outside NYC sebesar `3.76%`, peringkat `100 → 103`), namun tidak mengubah struktur zona utama maupun hasil makro.

---

## 5. Rekomendasi Tindak Lanjut (Sebelum Memodifikasi Pipeline)

Karena **tidak ada perubahan material pada jam puncak, jam sepi, maupun peringkat Top-50 zona**, Silver Layer saat ini **sudah aman secara statistik untuk lanjut ke Gold Layer**. Namun, untuk memisahkan secara tegas antara **validasi kualitas event pickup (Silver Layer)** dan **filter khusus analisis metrik turunan (Gold/BI Layer)**, berikut rekomendasi terperinci:

| Aturan | Rekomendasi | Alasan Berbasis Bukti Audit |
| :--- | :--- | :--- |
| **1. Timestamp di luar September 2026 (`22` baris)** | **PERTAHANKAN (Hard Removal di Silver)** | Memastikan cakupan periode September 2026 murni (membuang anomali tahun 2009 & bulan lain). |
| **2. Transaksi moneter negatif (`14,730` baris)** | **PERTAHANKAN (Hard Removal di Silver)** | `88.66%` (`13,060` baris) terbukti pasangan *mirror reversal* dari transaksi positif; menghapusnya mencegah *double-counting* demand. |
| **3. Lokasi di luar rentang TLC `1..265` (`0` baris)** | **PERTAHANKAN (Hard Removal di Silver)** | Guard integritas referensial dasar (tidak membuang baris valid). |
| **4. `fare_amount == 0` saat `total_amount > 0` (`867` baris `VendorID = 6`)** | **LONGGARKAN → Flag (`is_zero_fare_024`)** | `867` baris Flex Fare `VendorID = 6` valid (`mean_dist 7.63` mil, `mean_total 29.65` USD). Cukup hard-drop `total_amount <= 0` di Silver; filter `fare_amount > 0` dipisah untuk BI tarif. |
| **5. Durasi `> 180` menit (`852` baris)** | **PISAHKAN → Flag (`is_extreme_duration_024`) & `trip_duration_valid_min_024 = NULL`** | Mayoritas (`459` baris di `12–24 jam`) adalah pickup nyata berjarak median `1.96` mil dengan bug dropoff `VendorID = 2`. Pertahankan pickup-nya; filter durasi hanya saat menghitung metrik durasi. |
| **6. Kecepatan `> 80` mph (`1,168` baris) & jarak `> 100` mil (`125` baris)** | **PISAHKAN → Flag (`is_extreme_speed_024`, `is_extreme_distance_024`) & NULL-kan kolom metrik valid** | Mayoritas tetap pickup berbayar (`mean_fare 17–57` USD) dengan glitch GPS/odometer. Cukup hard-drop jika `trip_distance < 0`; anomali atas di-NULL-kan pada `trip_distance_valid_mi_024` & `avg_speed_mph_024`. |
| **7. Tarif tinggi `500–2,000` USD (`~60` baris)** | **LONGGARKAN → Hard-cap `> 2,000` USD + Flag (`is_high_fare_outlier_024`)** | `~56` baris adalah `RatecodeID = 4/5` (luar kota/negotiated fare) berjarak `43–93` mil. Hanya hard-drop error jelas (`> 2,000` USD seperti `9,999.70` USD); filter tarif `<= 500` USD dilakukan pada analisis ekonomi. |

### Opsi Keputusan untuk Tahap Berikutnya
- **Opsi 1 (Langsung lanjut ke Gold Layer):** Gunakan Silver Layer saat ini (`3,857,325` baris) dengan melampirkan temuan audit ini sebagai bukti transparansi bahwa penghapusan `0.48%` baris **tidak mengubah jam puncak, jam sepi, maupun peringkat Top-50 zona**.
- **Opsi 2 (Dipilih & Diimplementasikan — Silver Layer v2):** Perbarui `clean_yellow_taxi_024.py` agar hanya melakukan hard-drop pada timestamp di luar periode, transaksi non-berbayar (`total_amount <= 0`), dan outlier korup mutlak (`> 2,000` USD), sambil memulihkan seluruh pickup berpotensi valid dan memisahkan filter khusus analisis ke flag `_024` serta kolom `*_valid_*_024`.

---

## 6. Pembaruan Pasca-Revisi: Verifikasi Implementasi Opsi 2 (Silver Layer v2)

Revisi **Opsi 2** telah diimplementasikan pada `spark/processing/clean_yellow_taxi_024.py`, ditulis ke staging HDFS, diverifikasi penuh, dan dipromosikan secara aman (*atomic swap*) ke `/urbanpulse_024/processed/yellow_taxi_024` pada `2026-11-25T00:52:58Z` (metrik lengkap tersimpan di `reports/silver_cleaning_summary_024.json` dan `reports/silver_cleaning_report_024.md`).

| Indikator Verifikasi Pasca-Revisi | Silver v1 (Sebelum) | Silver v2 (Sesudah) | Catatan Verifikasi |
| :--- | ---: | ---: | :--- |
| Total Baris Silver (Read-Back Live HDFS) | `3,857,325` (`99.5171%`) | **`3,860,852` (`99.6081%`)** | **`+3,527` transaksi pickup dipulihkan** |
| Total Baris Dikeluarkan (Disjoint) | `18,719` (`0.4829%`) | **`15,192` (`0.3919%`)** | `15,168` `total_amount <= 0` + `22` luar Sep 2026 + `2` korup `> $2,000` (**0 overlap**) |
| Retensi Flex Fare (`payment_type = 0`) | `1,188,932` (`99.8756%`) | **`1,190,293` (`99.9899%`)** | `+1,361` transaksi Flex Fare dipulihkan |
| Retensi `VendorID = 6` | `4,369` (`83.4416%`) | **`5,174` (`98.8159%`)** | **`+805` transaksi berbayar `fare=0, total>0` dipulihkan** (`62` sisanya `total=0`) |
| Penandaan Pasangan Reversal Negatif | Belum ada | **`13,018` baris** (`has_negative_reversal_pair_024`) | Dapat dikecualikan secara eksplisit di Gold Layer |
| Jumlah Kolom (Asli + Turunan `_024`) | `40` (`21 + 19`) | **`49` (`21 + 28`)** | `21` kolom asli utuh 100%, seluruh kolom baru berakhiran `_024` |
| Jam Puncak / Jam Sepi / Top-50 Rank Change | `18` / `4` / `0` | **`18` / `4` / `0`** | Stabil 100% |
| Korelasi Zona (Spearman / Pearson vs Raw) | `0.999957` / `0.999995` | **`0.999984` / `0.999996`** | Semakin mendekati distribusi Raw (`+281` pickup pulih di JFK `132`) |

