# Konvensi Penamaan Proyek (`_024`) — UrbanPulse

> **Berlaku untuk:** Seluruh pengembangan proyek **UrbanPulse — Intelligent Urban Taxi Analytics** hingga selesai.  
> **Tanggal Penetapan:** 25 November 2026

---

## 1. Aturan Utama

Sesuai ketentuan dosen, setiap nama atau identifier yang **dibuat sendiri** untuk kebutuhan proyek **wajib menggunakan akhiran `_024`** selama memungkinkan dan tidak mengganggu kompatibilitas teknis.

### Panduan Penerapan per Kategori

| Kategori Artefak / Identifier | Konvensi Wajib | Contoh |
| :--- | :--- | :--- |
| File kode Python (Spark, ML, utilitas) | `<nama_modul>_024.py` | `data_processing_024.py`, `train_demand_model_024.py` |
| File halaman dashboard Streamlit | `<nama_halaman>_024.py` | `app_024.py`, `overview_024.py`, `prediction_024.py` |
| Script shell / PowerShell buatan proyek | `<nama_script>_024.sh` / `.ps1` | `run_pipeline_024.sh`, `setup_env_024.sh` |
| Class buatan proyek | `<PascalCase>_024` | `DemandPredictor_024`, `TaxiCleaner_024` |
| Function / method buatan proyek | `<snake_case>_024()` | `train_model_024()`, `aggregate_hourly_024()` |
| Variabel & konstanta buatan proyek | `<snake_case>_024` / `<UPPER_CASE>_024` | `total_pickups_024`, `RAW_DATA_DIR_024` |
| Kolom baru / hasil olahan (*engineered/aggregated*) | `<nama_kolom>_024` | `pickup_count_024`, `trip_duration_min_024`, `predicted_demand_024` |
| Nama tabel / temp view hasil olahan | `<nama_tabel>_024` | `hourly_demand_024`, `zone_summary_024` |
| File hasil pemrosesan & analisis | `<nama_output>_024.<ext>` | `hourly_demand_024.parquet`, `model_metrics_024.json` |
| Artefak model ML terlatih | `<nama_model>_024` | `gbt_demand_model_024`, `rf_demand_model_024` |
| Flow & konfigurasi Apache NiFi | `<nama_flow>_024.<ext>` | `nyc_taxi_ingestion_024.json`, `hdfs_config_024.properties` |
| Direktori kerja HDFS buatan proyek | `/urbanpulse_024/...` | `/urbanpulse_024/raw/`, `/urbanpulse_024/processed/` |
| Dokumen & laporan buatan proyek | `<nama_dokumen>_024.<ext>` | `PROJECT_BRIEF_024.md`, `naming_convention_024.md`, `urbanpulse_report_024.pdf` |
| Dataset mentah lokal (opsional nama file) | `<nama_asli>_024.<ext>` | `yellow_tripdata_2026-09_024.parquet` *(isi Parquet tidak diubah)* |

---

## 2. Aturan Pengecualian (Wajib Dipertahankan)

Konvensi `_024` **TIDAK diterapkan** pada item berikut demi menjaga validitas data dan kompatibilitas teknis:

1. **Nama kolom asli dataset sumber (NYC TLC & Zone Lookup):**  
   Kolom bawaan seperti `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `tip_amount`, `total_amount`, `LocationID`, `Borough`, `Zone`, `service_zone` **tidak boleh diubah** di sumber maupun saat pembacaan skema asli. Akhiran `_024` hanya digunakan pada kolom baru hasil transformasi, feature engineering, agregasi, atau prediksi.
2. **API, library, package, dan identifier bawaan framework:**  
   Seluruh nama modul/fungsi/parameter bawaan dari Python, PySpark, Spark MLlib, Streamlit, Plotly, Pandas, PyArrow, Apache NiFi, dan Hadoop HDFS tetap menggunakan nama resminya (mis. `SparkSession`, `DataFrame`, `st.set_page_config`, `px.choropleth`, `featuresCol`, `labelCol`, `predictionCol` saat parameter API mewajibkannya — untuk nilai nama kolom buatan sendiri tetap boleh memakai `_024`, misalnya `predictionCol="predicted_demand_024"`).
3. **File standar yang dibutuhkan tooling / Git / Python:**  
   `README.md`, `.gitignore`, `.gitkeep`, `requirements.txt`, dan file konfigurasi standar framework (seperti `.streamlit/config.toml` bila diperlukan nanti) tetap memakai nama standarnya.
4. **Isi file dataset mentah (Parquet):**  
   Nama file dataset mentah di `data/raw/` menggunakan akhiran `_024` **tanpa memodifikasi isi maupun skema internal file Parquet**. Nama asli dari sumber eksternal wajib dicatat di bagian Provenance di bawah ini.
5. **Nama folder utama proyek yang sudah ditetapkan:**  
   Struktur folder utama (`references/`, `data/`, `data/raw/`, `data/reference/`, `data/processed/`, `data/analytics/`, `nifi/`, `spark/`, `ml/`, `dashboard/`, `reports/`, `docs/`, `scripts/`) dipertahankan agar arsitektur repositori tetap standar dan bersih.

---

## 3. Log Provenance Dataset Mentah

Untuk menjaga ketertelusuran (*data provenance*), setiap file mentah yang diberi akhiran `_024` dicatat pemetaan nama aslinya di tabel berikut:

| Nama File Proyek (`data/raw/` / `data/reference/`) | Nama Asli Sumber Eksternal | Sumber | Ukuran (Byte) | SHA-256 (Integritas File) | Catatan |
| :--- | :--- | :--- | :---: | :--- | :--- |
| `data/raw/yellow_tripdata_2026-09_024.parquet` | `yellow_tripdata_2026-09.parquet` | NYC TLC Trip Record Data | `67,374,896` | `32D8D883E3FB9C0D681F4D47B205AFE0FCB2A4830739673B4DD3BAFBAC9B0086` | Hanya rename nama file; isi & skema Parquet 100% asli (identik) |
| `data/reference/taxi_zone_lookup_024.csv` | `taxi_zone_lookup.csv` | NYC TLC Official Lookup (`d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`) | `12,331` | `1A99E105092230F8620F301EDCCA7F80D3080642FF404D28ED957D3FA222C8ED` | Hanya rename nama file; 4 kolom asli (`LocationID`, `Borough`, `Zone`, `service_zone`) & 265 baris 100% utuh |

> *Tambahkan baris baru pada tabel ini setiap kali file dataset mentah atau file referensi tambahan disalin ke `data/raw/` atau `data/reference/`.*
