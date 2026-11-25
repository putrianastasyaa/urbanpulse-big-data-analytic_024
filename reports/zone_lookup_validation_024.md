# Laporan Validasi Referensi Taxi Zone Lookup & Rencana Integrasi — UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Validasi:** 25 November 2026  
> **Sumber Resmi:** `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`  
> **File Lokal:** `data/reference/taxi_zone_lookup_024.csv`  
> **Output JSON Validasi:** `reports/zone_lookup_validation_024.json`  
> **Mode:** Read-Only terhadap Gold Layer HDFS *(Raw, Silver, Gold, Dashboard, dan ML tidak diubah)*

---

## 1. Hasil Validasi File & Integritas Skema

| Pemeriksaan | Hasil Aktual | Status |
| :--- | :--- | :---: |
| Keterbacaan file CSV (UTF-8) | Berhasil dibaca tanpa error | **Lulus** |
| Ukuran file / SHA-256 | `12,331` byte / `1A99E105092230F8620F301EDCCA7F80D3080642FF404D28ED957D3FA222C8ED` | **Tercatat di `naming_convention_024.md`** |
| Jumlah baris / kolom | **`265` baris** / **`4` kolom** | **Lulus** |
| Nama kolom asli (tanpa `_024`) | `LocationID`, `Borough`, `Zone`, `service_zone` | **100% Utuh** |
| Rentang `LocationID` | `1` s.d. `265` | **Lulus** |
| Duplikasi `LocationID` | **`0` duplikat** (`265` ID unik) | **Lulus** |
| Nilai kosong (*blank/null*) | **`0`** di seluruh 4 kolom | **Lulus** |
| Kecocokan dengan `gold_zone_demand_024` | **`261 / 261` zona aktif Gold cocok (`100.0%`)**, `0` ID tidak cocok | **Lulus 100%** |

### Catatan Kualitas Khusus pada Nilai `Zone`, `Borough`, & `service_zone`
1. **Distribusi `Borough` (265 zona):** `Queens` (`69`), `Manhattan` (`69`), `Brooklyn` (`61`), `Bronx` (`43`), `Staten Island` (`20`), `EWR` (`1`), `Unknown` (`1`), `N/A` (`1`).
2. **Distribusi `service_zone` (265 zona):** `Boro Zone` (`205`), `Yellow Zone` (`55`), `Airports` (`2` — JFK & LaGuardia), `N/A` (`2`), `EWR` (`1`).
3. **Baris Khusus (`264` & `265`):**
   - `LocationID = 264`: `Borough = "Unknown"`, `Zone = "N/A"`, `service_zone = "N/A"`
   - `LocationID = 265`: `Borough = "N/A"`, `Zone = "Outside of NYC"`, `service_zone = "N/A"`
   - *Catatan teknis parser:* Saat membaca dengan Pandas (`pd.read_csv`), gunakan `keep_default_na=False` agar string literal `"N/A"` tidak otomatis berubah menjadi `NaN` tanpa sengaja.
4. **Duplikasi Nama `Zone` (Non-Unik):**
   - `"Corona"` muncul pada `2` `LocationID` berbeda (`56` dan `57`).
   - `"Governor's Island/Ellis Island/Liberty Island"` muncul pada `3` `LocationID` berbeda (`103`, `104`, `105`).
   - **Aturan Wajib Integrasi:** Join dan agregasi **wajib selalu menggunakan `PULocationID == LocationID`** sebagai *primary key*, tidak pernah hanya mengandalkan string `Zone`.
5. **4 Zona Lookup yang Tidak Aktif di Gold September 2026 (`0` pickup):** `99` (*Freshkills Park*, Staten Island), `103` & `104` (*Governor's Island/Ellis Island/Liberty Island*, Manhattan), dan `110` (*Great Kills Park*, Staten Island).

---

## 2. Pratinjau Interpretasi Gold Layer dengan Nama Zona Resmi (Read-Only)

### 2.1 Sebaran Permintaan per Borough (September 2026)
| Borough | Total Pickup | Pangsa Pickup (%) | Pendapatan Bersih (USD) |
| :--- | ---: | ---: | ---: |
| **Manhattan** | 3,370,898 | **87.31%** | 94,554,551.89 |
| **Queens** | 342,961 | **8.88%** | **23,704,666.23** *(19.03% pendapatan kota)* |
| **Brooklyn** | 121,009 | 3.13% | 5,068,138.10 |
| **Bronx** | 19,927 | 0.52% | 854,727.82 |
| **Unknown (`264`)** | 3,437 | 0.09% | 128,365.81 |
| **N/A (`265` Outside NYC)** | 1,772 | 0.05% | 196,798.30 |
| **EWR (`1` Newark Airport)** | 427 | 0.01% | 50,399.53 |
| **Staten Island** | 421 | 0.01% | 25,466.39 |

### 2.2 Top-10 Zona Pickup Gold Layer dengan Nama Resmi TLC
| Rank | `PULocationID` | `Zone` | `Borough` | `service_zone` | Pickup | Pangsa | Jam Puncak | Akhir Pekan | Pendapatan Bersih (USD) |
| :-: | :-: | :--- | :--- | :--- | ---: | ---: | :-: | ---: | ---: |
| 1 | `237` | Upper East Side South | Manhattan | Yellow Zone | 172,229 | 4.46% | 18 | 19.64% | 3,993,745.03 |
| 2 | `236` | Upper East Side North | Manhattan | Yellow Zone | 154,695 | 4.01% | 15 | 19.67% | 3,700,641.99 |
| 3 | `132` | **JFK Airport** | Queens | Airports | 149,680 | 3.88% | 16 | 26.85% | **11,959,824.66** |
| 4 | `161` | Midtown Center | Manhattan | Yellow Zone | 144,560 | 3.74% | 18 | 18.67% | 4,206,375.15 |
| 5 | `186` | Penn Station/Madison Sq West | Manhattan | Yellow Zone | 119,283 | 3.09% | 19 | 24.91% | 3,386,540.72 |
| 6 | `142` | Lincoln Square East | Manhattan | Yellow Zone | 110,974 | 2.87% | 21 | 24.67% | 2,814,103.73 |
| 7 | `162` | Midtown East | Manhattan | Yellow Zone | 110,696 | 2.87% | 18 | 19.31% | 3,057,550.16 |
| 8 | `68` | East Chelsea | Manhattan | Yellow Zone | 105,891 | 2.74% | 19 | 25.68% | 3,225,321.09 |
| 9 | `79` | **East Village** | Manhattan | Yellow Zone | 103,782 | 2.69% | **23** | **42.80%** | 2,910,060.23 |
| 10 | `230` | Times Sq/Theatre District | Manhattan | Yellow Zone | 103,465 | 2.68% | 21 | 25.51% | 3,339,536.27 |

- **Bottom-10 Zona (`Rank 252–261`):** didominasi area terpencil/pulau/taman seperti `199` (*Rikers Island*, 1 pickup), `204` (*Rossville/Woodrow*, 2), `105` (*Governor's/Ellis/Liberty Island*, 2), `84` (*Eltingville/Annadale/Prince's Bay*, 4), hingga `30` (*Broad Channel*, 10).

---

## 3. Rencana Integrasi ke Evaluasi ML & Dashboard (Tahap Berikutnya)

Sesuai batasan tahap ini, integrasi **belum diterapkan** pada Gold Layer, ML, maupun Dashboard; berikut rencana implementasi bersih untuk fase berikutnya:

### 3.1 Integrasi ke Evaluasi ML (`ml/evaluation/`)
1. **Enrichment Pasca-Inferensi (Read-Only Join pada `PULocationID == LocationID`):**
   - Training tetap menggunakan fitur numerik/historis bebas leakage dari `gold_demand_features_024` (tidak perlu mengubah Gold Layer!).
   - Pada tahap evaluasi (`ml/evaluation/evaluate_demand_models_024.py`) dan keluaran inferensi (`demand_predictions_024.parquet`), lakukan `left join` dengan `taxi_zone_lookup_024.csv` (dapat juga disalin ke `/urbanpulse_024/raw/reference/taxi_zone_lookup_024.csv` saat fase ML dimulai) untuk melampirkan kolom asli `Zone`, `Borough`, dan `service_zone`.
2. **Analisis Error Tersegmentasi yang Bermakna:**
   - Laporkan MAE/RMSE/WAPE per **`Borough`** (Manhattan vs Queens/Airports vs Brooklyn/Bronx) dan per **`service_zone`** (`Yellow Zone` vs `Airports` vs `Boro Zone`).
   - Tampilkan Top-10 zona dengan akurasi terbaik dan error tertinggi lengkap dengan nama `Zone` (mis. *JFK Airport*, *Upper East Side South*, *East Village*) untuk kebutuhan laporan akhir PDF.

### 3.2 Integrasi ke Serving Layer & Dashboard (`data/analytics/` & `dashboard/`)
Sesuai kontrak pada [dashboard_architecture_024.md](../docs/dashboard_architecture_024.md) dan [dashboard/config_024.py](../dashboard/config_024.py), dashboard sudah mengharapkan kolom `PULocationID`, `Zone`, `Borough`, dan `service_zone` pada file serving layer di `data/analytics/`:
1. **Export Serving Layer Script (tahap pasca-ML, mis. `spark/processing/export_serving_layer_024.py`):**
   - `gold_hourly_demand_024` → diekspor ke `data/analytics/hourly_demand_024.parquet` (kolom kontrak: `pickup_date_024`, `pickup_hour_024`, `day_of_week_024`, `day_name_024`, `is_weekend_024`, `pickup_count_024`, dll.).
   - `gold_zone_demand_024` **LEFT JOIN** `taxi_zone_lookup_024.csv` (`PULocationID == LocationID`) → diekspor ke `data/analytics/zone_summary_024.parquet` (melengkapi `Zone`, `Borough`, `service_zone` secara otomatis).
   - Output inferensi ML **LEFT JOIN** `taxi_zone_lookup_024.csv` → `data/analytics/demand_predictions_024.parquet` dan `data/analytics/recommendations_024.parquet`.
2. **Keamanan & Konsistensi:**
   - Gunakan `keep_default_na=False` saat membaca CSV di Python/Pandas, serta pertahankan tampilan label `"Unknown"` / `"Outside of NYC"` untuk zona `264`/`265` tanpa menghapus barisnya.
   - Tidak diperlukan perubahan struktur kode UI di `dashboard/pages/*_024.py` selama kontrak kolom `Zone`, `Borough`, dan `service_zone` dipenuhi saat ekspor ke `data/analytics/`.
