# Rancangan Laporan Akhir Akademik dan Audit Sumber — Proyek UrbanPulse (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics (`_024`)  
> **Tahap:** Tahap Pertama — Audit Sumber dan Rancangan Laporan  
> **Tanggal Penyusunan:** 25 November 2026  
> **Root Workspace:** `D:\A University\Big Data dan Analitik\Project Akhir`  
> **Bahasa Laporan:** Bahasa Indonesia formal dengan mempertahankan istilah teknis baku (*Data Ingestion*, *Data Lake*, *HDFS*, *PySpark*, *Machine Learning*, *Dashboard*, dsb.)  
> **Status:** Rancangan dan pemetaan sumber siap ditinjau sebelum penulisan naskah lengkap dan ekspor PDF.

---

## 1. Ringkasan Hasil Audit Sumber

Audit dilakukan secara menyeluruh terhadap **12 dokumen sumber utama** yang diminta, ditambah **5 dokumen pendukung** serta pemeriksaan langsung direktori `reports/figures/` dan artefak JSON proyek:

- **12 Sumber Utama Diaudit:** `PROJECT_BRIEF_024.md`, `README.md`, `docs/methodology_024.md`, `docs/technical_documentation_024.md`, `docs/development_log_024.md`, `docs/testing_log_024.md`, `docs/evidence_checklist_024.md`, `docs/nifi_setup_024.md`, `reports/raw_validation_report_024.md`, `reports/silver_cleaning_report_024.md`, `reports/gold_layer_report_024.md`, dan `reports/ml_evaluation_report_024.md`.
- **Dokumen Pendukung Terverifikasi:** `docs/naming_convention_024.md`, `docs/dashboard_architecture_024.md`, `reports/silver_cleaning_impact_audit_024.md`, `reports/ml_readiness_and_plan_024.md`, `reports/zone_lookup_validation_024.md`, `nifi/configs/ingestion_test_result_024.json`, dan `data/analytics/model_metrics_024.json`.
- **Bukti Visual Autentik (`reports/figures/` — Pasca-Audit 25 Nov 2026):** Sebanyak **17 berkas PNG sintetis** (9 mockup terminal/editor HTML/CSS + 8 kartu infografis HTML/CSS Progress 1–4) telah **dihapus seluruhnya** dari workspace, DOCX/PDF, dan PPTX. Hanya **10 berkas PNG autentik tangkapan layar browser terhadap layanan/situs nyata** yang dipertahankan: `progress_02_dataset_source_024.png` (1), `dashboard_01..05_*_024.png` (5), dan `nifi_01..04_*_024.png` (4), ditambah **1 diagram arsitektur ilustratif** (bukan screenshot). Seluruh metrik Progress 1–4 disajikan dalam bentuk tabel native di DOCX/PPTX.

### Catatan Sinkronisasi Antarsumber (Prinsip Akurasi Laporan)

Selama audit ditemukan beberapa perbedaan kronologis antara dokumen tahap awal dan dokumen tahap akhir yang wajib disinkronkan saat penulisan naskah:

1. **Cakupan Dataset Aktual (`September 2026`, 1 bulan):** `PROJECT_BRIEF_024.md` (tahap inisiasi) mencantumkan rencana awal 12 bulan (`Oktober 2025 – September 2026`). Realisasi aktual yang terverifikasi pada seluruh pipeline adalah dataset **September 2026** (`yellow_tripdata_2026-09_024.parquet`, `3.876.044` transaksi, `67.374.896` byte) yang memenuhi ketentuan akademik data nyata dalam satu tahun terakhir.
2. **Peran dan Ruang Lingkup Pengujian Apache NiFi:** File Raw produksi `/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet` telah diunggah langsung ke HDFS pada Progress 2 (`2026-11-25 07:24`) dan sengaja **tidak ditimpa**. Apache NiFi `1.28.1` dipasang pada Progress 6 dan berhasil diuji secara terisolasi pada direktori `/urbanpulse_024/nifi_test_024/` dengan ukuran dan SHA-256 100% identik (`7,27` detik provenance, `0` duplikasi). Laporan **tidak boleh** mengklaim NiFi mengunggah file Raw produksi September 2026.
3. **Pemisahan Evaluasi *Test Holdout* vs Prediksi Masa Depan:** Hasil pada `demand_predictions_024.parquet` (`43.848` baris) dan halaman Prediksi Permintaan di Dashboard merupakan **evaluasi *out-of-sample* pada *Locked Test Holdout* 24–30 September 2026**, bukan proyeksi permintaan masa depan (*future forecasting*).
4. **Status Repositori GitHub:** Meskipun `PROJECT_BRIEF_024.md` dan `README.md` mencantumkan rencana repositori GitHub, hingga saat audit `.git/` belum diinisialisasi dan belum ada instruksi wajib dari dosen; laporan tidak menganggap GitHub sebagai prasyarat wajib.
5. **Sinkronisasi Status Screenshot dan Artefak Serving Layer:** `docs/development_log_024.md` dan `docs/testing_log_024.md` masih menyisakan penanda `[PERLU DIAMBIL]` pada 17 screenshot Progress 1–4, padahal pemeriksaan langsung `reports/figures/` dan `docs/evidence_checklist_024.md` memastikan **10/10 PNG autentik tersedia** (17 PNG sintetis Progress 1–4 telah dihapus dan digantikan tabel native). Demikian pula, `reports/gold_layer_report_024.md` (§1.3) ditulis sebelum `taxi_zone_lookup_024.csv` divalidasi (`100%` cocok pada `261` zona aktif) dan sebelum `6/6` artefak `data/analytics/` terhubung ke Dashboard `v1.0.0-gold-ml`.

---

## 2. Outline Laporan Akhir Terperinci

Struktur laporan mengikuti 10 komponen akademik baku dengan rincian subbab dan muatan faktual terverifikasi:

### 1. Halaman Judul dan Identitas
- **Judul Laporan:** *UrbanPulse: Intelligent Urban Taxi Analytics — Implementasi Pipeline Big Data End-to-End, Analisis Business Intelligence, dan Machine Learning untuk Demand Forecasting Yellow Taxi New York City* *(judul final dapat diselaraskan dengan preferensi mahasiswa)*.
- **Identitas Akademik:** Logo institusi, jenis dokumen (Laporan Proyek Akhir), nama mata kuliah (*Big Data dan Analitik*), kelas/seksi, nama lengkap dan NIM/NPM penyusun (dengan kode proyek `_024`), nama dosen pengampu, program studi, fakultas, universitas, serta kota dan tahun/tanggal pengesahan. *(Catatan: sebagian besar data identitas belum tercantum di sumber dan ditandai `[PERLU KONFIRMASI]` pada Bagian 4.)*
- **Kelengkapan Awal Opsional (Menyesuaikan Pedoman Kampus):** Halaman Pengesahan, Kata Pengantar, Daftar Isi, Daftar Tabel, Daftar Gambar, Daftar Lampiran.

### 2. Abstrak
- Ringkasan padat 1 halaman (200–300 kata, Bahasa Indonesia; versi Bahasa Inggris opsional sesuai ketentuan kampus) yang memuat:
  - **Latar Belakang & Tujuan:** Tantangan pemrosesan data mobilitas perkotaan berskala jutaan transaksi dan kebutuhan prediksi permintaan berbasis waktu dan zona untuk dukungan keputusan armada Yellow Taxi NYC.
  - **Metodologi:** Arsitektur *Medallion Data Lake* (*Raw–Silver–Gold*) menggunakan Apache NiFi `1.28.1` (uji terisolasi ke `/urbanpulse_024/nifi_test_024/`), Hadoop HDFS `3.5.0`, Apache Spark/PySpark `4.0.4`, Spark MLlib (`chronological split`, `zero data leakage`), serta Dashboard interaktif Streamlit + Plotly (`v1.0.0-gold-ml`).
  - **Hasil Utama Terverifikasi:** Dataset NYC TLC September 2026 (`3.876.044` baris Raw → `3.860.852` baris Silver v2 / retensi `99,61%`, `49` kolom → `5` dataset Gold terekonsiliasi 100%, total pendapatan bersih `USD 124.583.114,07`, jam puncak `18:00` vs jam sepi `04:00` rasio `8,3×`, Top-10 zona menyumbang `33,03%`). Model terbaik `ridge_reg_0p1_024` mencapai performa *Locked Test Holdout* 24–30 September 2026: All 261 Zones MAE `4,5495`, RMSE `12,2597` (`-19,28%` vs Naive `lag_168h`), $R^2$ `0,9564`, WAPE `21,36%`; Top 50 Operational Zones (`91,0%` volume) MAE `17,4498`, RMSE `27,4563` (`-19,50%`), $R^2$ `0,9244`, WAPE `17,25%` (`45/50` zona unggul).
  - **Keterbatasan Ringkas:** Cakupan 1 bulan, tidak adanya fitur eksogen (cuaca/penerbangan/event), dan performa lemah pada segmen ekor *zero-inflated* (`Low >150`, `0,53%` volume).
  - **Kata Kunci:** *Big Data*, *Data Lake*, *HDFS*, *Apache NiFi*, *PySpark*, *Spark MLlib*, *Demand Forecasting*, *Ridge Regression*, *Dashboard*.

### 3. BAB I PENDAHULUAN
- **1.1 Latar Belakang**
  - Kompleksitas dinamika transportasi perkotaan di New York City dan nilai strategis analisis permintaan Yellow Taxi berdasarkan dimensi spasial-temporal (zona dan jam).
  - Skala dan kerumitan data nyata NYC TLC (jutaan transaksi per bulan, *structural missingness* pada kanal Flex Fare/e-hail sebesar `30,71%`, transaksi reversal/dispute, dan outlier telemetri) yang menuntut ekosistem Big Data terstruktur alih-alih pengolahan spreadsheet/konvensional.
  - Urgensi pembangunan sistem *end-to-end* **UrbanPulse** yang mengintegrasikan *Data Ingestion*, penyimpanan terdistribusi (*Data Lake*), pemrosesan paralel, *Machine Learning* untuk *demand forecasting*, dan visualisasi *Dashboard* berbasis bukti.
- **1.2 Rumusan Masalah**
  1. Bagaimana merancang dan mengimplementasikan arsitektur *Data Lake* berbasis *Medallion* (*Raw–Silver–Gold*) yang aman, tervalidasi, dan dapat diaudit menggunakan Apache NiFi, Hadoop HDFS, dan PySpark pada lingkungan sumber daya terbatas?
  2. Bagaimana menangani karakteristik kualitas data nyata NYC TLC (seperti *structural missingness* Flex Fare `30,71%`, pasangan reversal negatif, dan outlier telemetri) tanpa menimbulkan bias *under-counting* permintaan?
  3. Pola spasial-temporal dan insight *Business Intelligence* apa yang dihasilkan dari dataset Yellow Taxi NYC September 2026?
  4. Algoritma *Machine Learning* Spark MLlib manakah yang memberikan akurasi *short-term demand forecasting* terbaik terhadap benchmark persistensi naif pada *Locked Test Holdout* secara bebas *data leakage*, serta bagaimana profil performanya lintas kelompok zona?
  5. Bagaimana menyajikan hasil analitik Gold Layer dan evaluasi model ML ke dalam *Dashboard* interaktif yang informatif dan berbasis bukti operasional?
- **1.3 Tujuan**
  - Selaras 1-ke-1 dengan rumusan masalah (membangun pipeline Big Data *end-to-end*, menerapkan cleaning berbasis audit, menghasilkan insight BI aktual, melatih & mengevaluasi model prediksi permintaan Spark MLlib, serta membangun *Dashboard* interaktif 5 halaman berbasis Serving Layer).
- **1.4 Manfaat**
  - **Manfaat Akademis:** Referensi implementasi praktis integrasi Apache NiFi, HDFS, PySpark, Spark MLlib, dan Streamlit dengan disiplin *data quality audit*, *zero data leakage*, dan reproduksibilitas.
  - **Manfaat Operasional/Praktis:** Memberikan kerangka pemantauan hotspot permintaan, pembedaan zona bervolume tinggi vs berpendapatan tinggi, serta estimasi kebutuhan armada jangka pendek (horizon 1 jam) pada zona operasional utama.
- **1.5 Batasan Masalah**
  - Cakupan data: NYC TLC Yellow Taxi **September 2026** (`2026-09-01` s.d. `2026-09-30`, `3.876.044` transaksi mentah) dan `taxi_zone_lookup_024.csv` (`265` zona); belum mencakup musiman tahunan lintas bulan.
  - Lingkungan eksekusi: Infrastruktur lokal Windows 11 + WSL2 Ubuntu 24.04.4 LTS (mode Hadoop *pseudo-distributed* `dfs.replication=1`, Spark `--master local[*]`, NiFi `1.28.1` standalone HTTP *localhost-only*).
  - Ruang lingkup Apache NiFi: Diuji secara terisolasi ke `/urbanpulse_024/nifi_test_024/`; file Raw produksi September 2026 sudah ada sejak Progress 2 dan sengaja dipertahankan tanpa ditimpa.
  - Ruang lingkup ML: Horizon *1-step-ahead* (1 jam ke depan) tanpa fitur eksogen (cuaca, jadwal penerbangan, kalender event kota); hasil yang ditampilkan pada Dashboard adalah **evaluasi *out-of-sample* *Locked Test Holdout* 24–30 September 2026**, bukan proyeksi masa depan.
  - Visualisasi spasial: Berkas opsional `taxi_zones_024.geojson` belum digunakan sehingga peta poligon choropleth digantikan oleh ranking zona, agregasi Borough/`service_zone`, dan scatter *Demand vs Revenue*.
  - Konvensi proyek: Seluruh identifier buatan sendiri menggunakan akhiran `_024` sesuai `docs/naming_convention_024.md`.

### 4. BAB II LANDASAN TEORI
- **2.1 Big Data dan Analytics**
  - Definisi Big Data (karakteristik 3V: *Volume*, *Velocity*, *Variety* beserta relevansi *Veracity* dan *Value*), tahapan analitik (deskriptif, diagnostik, prediktif, preskriptif), dan relevansinya terhadap data mobilitas urban.
- **2.2 Apache Hadoop dan HDFS**
  - Arsitektur Hadoop Distributed File System (HDFS): peran `NameNode`, `DataNode`, `SecondaryNameNode`, konsep pemecahan blok, replikasi (`dfs.replication`), mode *pseudo-distributed*, dan prinsip *write-once, read-many* untuk *Raw Layer immutable*.
- **2.3 Apache NiFi**
  - Konsep *Flow-Based Programming* (FBP), komponen inti (*Processor*, *FlowFile*, *Connection*, *Process Group*, *Data Provenance*), serta pola *ingestion* aman `ListFile` → `FetchFile` → `UpdateAttribute` → `PutHDFS` → `LogAttribute` dibanding `GetFile`.
- **2.4 Apache Spark dan PySpark**
  - Arsitektur pemrosesan terdistribusi dalam memori (*in-memory computing*), `SparkSession`, *DataFrame API*, *Catalyst Optimizer*, eksekusi *lazy evaluation*, format kolumnar Apache Parquet dengan kompresi Snappy, serta penanganan tipe `TIMESTAMP_NTZ` pada data deret waktu.
- **2.5 Data Lake Architecture (Medallion Architecture)**
  - Konsep *Data Lake* dan pemisahan lapisan **Raw (Bronze)**, **Silver (Cleaned/Enriched)**, **Gold (Curated/Analytics)**, serta **Serving Layer** untuk memisahkan beban komputasi analitik dari aplikasi antarmuka pengguna.
- **2.6 Machine Learning untuk Demand Forecasting**
  - Formulasi *short-term spatial-temporal demand forecasting* sebagai regresi deret waktu panel, pentingnya *chronological split* untuk mencegah *data leakage*, fitur *lag* dan *rolling window* eksklusif masa lalu, pengkodean siklis (sinus/kosinus) dan *One-Hot Encoding*, serta konsep algoritma yang dibandingkan di Spark MLlib:
    - *Seasonal Naive Baseline* (`lag_168h`),
    - *Ridge Regression* (Regresi Linear dengan regularisasi L2),
    - *Random Forest Regressor*,
    - *Gradient-Boosted Trees (GBT) Regressor*.
  - Metrik evaluasi regresi: **MAE**, **RMSE**, **$R^2$**, dan alasan matematis penggunaan **WAPE** ($\sum |y - \hat{y}| / \sum y$) sebagai pengganti MAPE pada data berdistribusi *zero-inflated*.
- **2.7 Streamlit dan Plotly**
  - Konsep kerangka kerja *web application* berbasis Python (Streamlit) untuk prototipe dan penyajian *Dashboard* analitik, integrasi visualisasi interaktif berbasis Plotly, serta prinsip pemisahan *Serving Layer* ringan agar latensi antarmuka rendah.

### 5. BAB III METODOLOGI DAN PERANCANGAN SISTEM
- **3.1 Sumber dan Karakteristik Dataset**
  - Sumber resmi NYC TLC (`yellow_tripdata_2026-09.parquet` → `yellow_tripdata_2026-09_024.parquet`, `67.374.896` B, SHA-256 terverifikasi, `3.876.044` baris, `21` kolom asli) dan referensi zona (`taxi_zone_lookup_024.csv`, `265` baris, `4` kolom asli, SHA-256 terverifikasi).
  - Aturan *data provenance* dan konvensi penamaan wajib `_024` beserta pengecualian kolom asli NYC TLC.
  - Karakteristik kritis data: *structural missingness* Flex Fare (`30,7121%`), kunci join wajib `PULocationID == LocationID` (karena duplikasi nama `Zone` seperti `"Corona"` dan `"Governor's Island..."`), serta `keep_default_na=False` untuk menjaga string literal `"N/A"` pada `LocationID 264/265`.
- **3.2 Arsitektur Sistem**
  - Diagram arsitektur *end-to-end* UrbanPulse (Local Source → Apache NiFi `1.28.1` [uji terisolasi] / Direct HDFS [produksi P2] → HDFS Raw Layer → PySpark Silver v2 → PySpark Gold Layer → Spark MLlib Training & Evaluation → Local Serving Layer `data/analytics/` → Streamlit + Plotly Dashboard).
  - Strategi eksekusi *stage-by-stage* dan pembatasan memori untuk menyesuaikan keterbatasan RAM host 16 GB dan disk `C:\`.
- **3.3 Alur Data Ingestion**
  - Pembedaan jujur dua jalur: (1) jalur produksi awal Progress 2 langsung ke `/urbanpulse_024/raw/yellow_taxi/` (diverifikasi SHA-256 100%, bersifat *immutable*), dan (2) rancangan jalur otomatis Apache NiFi `1.28.1` (`urbanpulse_ingestion_024`) dengan 6 prosesor (`ListFile` → `FetchFile [Completion=None]` → `UpdateAttribute` → `PutHDFS [Conflict=ignore, Compression=NONE]` → `LogAttribute` Success/Failure) yang diuji terisolasi ke `/urbanpulse_024/nifi_test_024/` serta prosedur adaptasi untuk dataset bulan baru di masa depan.
- **3.4 Perancangan Raw, Silver, dan Gold Layer**
  - **Raw Layer:** Penyimpanan *read-only*, validasi *single-pass profiling* (`validate_raw_yellow_taxi_024.py`).
  - **Silver Layer (Evolusi Berbasis Audit v1 → v2):** Perancangan audit dampak (`audit_silver_cleaning_impact_024.py`) termasuk uji *exact positive mirror-reversal pair* (`88,66%`), keputusan Opsi 2 yang memisahkan *hard removal* bebas overlap (`total_amount <= 0`, luar Sep 2026, korup `> $2.000`, lokasi/telemetri negatif) dari *anomaly flagging* (`14` flag `_024`) + kolom metrik tervalidasi (`*_valid_*_024`), serta protokol *staging & atomic swap* HDFS.
  - **Gold Layer (5 Dataset Analitik):** Rancangan grain dan pemisahan populasi **Demand Pickup Operasional** (`3.860.852`) vs **Non-Reversal** (`3.847.834`) vs **Pendapatan Bersih Tervalidasi** (mengecualikan `13.018` reversal pairs dan outlier), normalisasi kalender hari September 2026 (`22` weekday, `8` weekend), serta penetapan `TIMESTAMP_NTZ` untuk mengatasi *timezone shift* `+7 jam` WIB.
- **3.5 Perancangan Feature Engineering**
  - Pembentukan grid reguler `261 zona × 720 jam = 187.920` slot dengan *zero-filling* eksplisit (`86.036` slot nol / `45,78%`).
  - Pemisahan periode *warm-up* (`1–7 Sep`, `43.848` baris) dan populasi ML-ready (`8–30 Sep`, `144.072` baris, `0` null).
  - Rancangan 15 fitur numerik (lag `1h/24h/168h`, rolling `24h/7d` eksklusif masa lalu `rowsBetween(-24,-1)` & `(-168,-1)`, fitur kalender & siklis sin/cos, `is_known_pickup_zone_num`) + *One-Hot Encoding* `261` zona = `276` dimensi, serta daftar eksplisit kolom target/proxy yang dikeluarkan untuk menjamin *zero data leakage*.
- **3.6 Metodologi Pelatihan dan Evaluasi Machine Learning**
  - Tahap pra-pelatihan: Audit kesiapan ML (Progress 4), analisis korelasi fitur, profil *zero-inflation* All 261 vs Top 50 zona, dan penetapan *sanity floor* Naive `lag_168h`.
  - Skema *Chronological Split*: Train (`8–20 Sep`, `81.432`), Validation (`21–23 Sep`, `18.792`), Final Refit (`8–23 Sep`, `100.224`), dan *Locked Test Holdout* (`24–30 Sep`, `43.848`).
  - Rancangan `Pipeline` Spark MLlib (`StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler(withMean=False)` → Estimator) untuk 6 kandidat (`Ridge 0.1/1.0`, `RF d8/t40 & d10/t60`, `GBT d5/i40 & d6/i60`), *post-processing* `max(0.0, pred)`, serta interval ketidakpastian bebas *leakage* dari Validation RMSE (`± 1,28 × 12,0061 = ± 15,3678`).
  - Protokol evaluasi independen (`evaluate_demand_model_024.py`): verifikasi *reload reproducibility* (`< 1e-4`), metrik MAE/RMSE/$R^2$/WAPE tersegmentasi (All 261, Top 50, 211 lainnya, Tier, Borough, `service_zone`, Peak/Off-Peak, Weekday/Weekend), serta verifikasi *immutability* HDFS.
- **3.7 Perancangan Dashboard**
  - Arsitektur modular (`UrbanPulseDashboard_024`, `AnalyticsDataLoader_024`, `components_024.py`, `charts_024.py`), pemisahan Serving Layer (`data/analytics/`, `6/6` artefak `< 1,4 MB`), dua mode sumber data (Data Aktual default vs DATA DEMO berlabel eksplisit), rancangan 5 halaman fungsional, aturan kejujuran tampilan *Locked Test Holdout*, serta logika rekomendasi armada deterministik (`build_evidence_recommendations_024`).

### 6. BAB IV IMPLEMENTASI SISTEM
- **4.1 Environment dan Infrastruktur**
  - Spesifikasi aktual Host Windows 11 (Build 26200, Intel Core i7-1355U, RAM 16 GB, kapasitas disk `C:\`/`D:\`/`E:\`), WSL2 Ubuntu 24.04.4 LTS (`7,5 GiB` RAM + `2,0 GiB` Swap), OpenJDK `17.0.20`, Apache Hadoop `3.5.0`, Apache Spark `4.0.4` (`Python 3.12.3`), Apache NiFi `1.28.1`, dan `.venv` Dashboard Windows.
  - Kendala infrastruktur yang ditemukan beserta mitigasi aktual (disk `C:\` 93% penuh, konflik port `8088` YARN → mode `local[*]`, `SPARK_LOCAL_IP=127.0.0.1`, `spark.eventLog.enabled=false`, pembatasan heap NiFi `-Xms512m -Xmx1g` & port `8090`).
  - Struktur direktori workspace lokal dan hierarki HDFS `/urbanpulse_024/`.
- **4.2 Implementasi Apache NiFi (`1.28.1`)**
  - Instalasi standalone terisolasi di `/home/ubuntu/nifi-1.28.1` (verifikasi SHA-256 paket resmi, kompatibilitas Java 17 native tanpa mengubah Hadoop/Spark).
  - Konfigurasi detail Process Group `urbanpulse_ingestion_024` (6 prosesor, 6 atribut metadata `urbanpulse.*_024`, pengaturan `PutHDFS` *read-only* terhadap `core-site.xml`/`hdfs-site.xml`, tujuan uji `/urbanpulse_024/nifi_test_024`), ekspor flow JSON (`nifi/flows/urbanpulse_ingestion_024.json`), dan prosedur *start/stop* independen.
- **4.3 Implementasi Raw Layer**
  - Penyimpanan file mentah di HDFS `/urbanpulse_024/raw/yellow_taxi/yellow_tripdata_2026-09_024.parquet` (`64,3 MiB`, *read-only*) dan eksekusi script `spark/processing/validate_raw_yellow_taxi_024.py` (`79,82` detik) beserta mekanisme *single-pass aggregation*.
- **4.4 Implementasi Silver Layer**
  - Implementasi bertahap: Silver v1 (`3.857.325` baris, `40` kolom) → eksekusi audit `audit_silver_cleaning_impact_024.py` (`51,08` detik) → revisi Silver v2 (`clean_yellow_taxi_024.py`, `77,64` detik) menghasilkan `3.860.852` baris, `49` kolom (`21` asli + `28` turunan `_024`), `159,2 M` (8 partisi Snappy), penandaan `13.018` reversal pairs, dan eksekusi *zero-downtime staging & atomic swap* HDFS.
- **4.5 Implementasi Gold Layer**
  - Eksekusi `spark/processing/build_gold_layer_024.py` (`64,35` detik) membentuk 5 dataset Parquet di `/urbanpulse_024/analytics/` (`total ~2,3 M`).
  - Dokumentasi teknis penyelesaian kendala **Timezone Shift `+7 jam`** dengan `CAST(... AS TIMESTAMP_NTZ)`, *rebuild*, dan verifikasi live.
  - Validasi *read-only* referensi `data/reference/taxi_zone_lookup_024.csv` (`265` zona, `100%` match pada `261` zona aktif).
- **4.6 Implementasi Machine Learning (Spark MLlib)**
  - Implementasi `ml/training/train_demand_model_024.py` (`141,37` detik total; refit final `2,11` detik): asersi pra-training, konstruksi 5 tahap `Pipeline`, pelatihan berurutan 6 kandidat + Naive, penyimpanan `ml/models/ridge_reg_0p1_024/`, `training_run_metadata_024.json`, `demand_predictions_024.parquet`, dan `model_metrics_024.json`.
  - Implementasi `ml/evaluation/evaluate_demand_model_024.py` (`31,99` detik) untuk audit independen dan ekspor ringkasan evaluasi.
- **4.7 Implementasi Dashboard (Streamlit + Plotly)**
  - Ekspor *read-only* 4 tabel Gold HDFS ke `data/analytics/` (melengkapi `6/6` artefak Serving Layer).
  - Implementasi modul `dashboard/app_024.py` (`v1.0.0-gold-ml`), enrichment `taxi_zone_lookup_024.csv` (`keep_default_na=False`, join `PULocationID == LocationID`), serta implementasi 5 halaman di `dashboard/pages/*_024.py` dan fungsi rekomendasi deterministik `build_evidence_recommendations_024()`.

### 7. BAB V HASIL DAN PEMBAHASAN
- **5.1 Hasil Validasi Data (Raw Layer Profiling)**
  - Profil `3.876.044` baris, `21` kolom, `0` duplikat persis, `0` NaN.
  - Analisis temuan utama: *structural missingness* Flex Fare (`1.190.413` baris / `30,7121%`), anomali timestamp (`22` baris luar Sep 2026 termasuk tahun `2009`), anomali durasi (`48.202` nol, `852` `>180` mnt hingga `7.798,73` mnt), jarak & kecepatan (`147.158` jarak nol, `125` `>100` mil hingga `232.644,8` mil, `1.168` kecepatan `>80` mph), dan moneter (`14.688` total negatif, `480` total nol, maks `USD 10.003,70`).
- **5.2 Hasil Transformasi (Silver Layer v1 → v2 dan Gold Layer)**
  - Tabel komparasi Raw (`3.876.044`) → Silver v1 (`3.857.325`) → Silver v2 (`3.860.852`, retensi `99,6081%`, `+3.527` pickup valid dipulihkan termasuk `+805` transaksi `VendorID=6` dan `+281` pickup JFK `132`).
  - Rincian `15.192` baris *hard removal* bebas overlap (`0,3919%`), ringkasan flag kualitas (`94,64%` tanpa flag anomali), perbandingan statistik mentah vs metrik tervalidasi (`trip_distance stddev` turun dari `464,34` menjadi `4,33` mil), serta korelasi distribusi Raw vs Silver v2 (Pearson jam `0,999999`, Spearman zona `0,999984`, `0` perubahan peringkat Top-50).
  - Verifikasi 5 dataset Gold Layer: rekonsiliasi 100% (`3.860.852` pickup), `0` missing hours, profil *sparsity* lintas tier (`Top 10`: `0,11%` nol vs `Low >150`: `82,68%` nol), dan bukti *spot-check zero-leakage* (`PULocationID = 237`, `2026-09-15 18:00:00`: `260,1012 == 260,1012`).
- **5.3 Insight Analitik (Business Intelligence September 2026)**
  - **KPI Global:** `3.860.852` pickup (`128.695,07`/hari; `5.362,29`/jam), `3.847.834` non-reversal, `30,83%` Flex Fare, pendapatan bersih `USD 124.583.114,07`.
  - **Pola Temporal:** Jam puncak `18:00` (`267.143` pickup) vs jam sepi `04:00` (`32.138` pickup, rasio `8,3×`); jam `17:00` mencatat pendapatan bersih tertinggi (`USD 8,60 Jt`); perbandingan ternormalisasi Weekday (`5.346,91`/jam, 22 hari) vs Weekend (`5.404,61`/jam, 8 hari); peringkat hari tersibuk Kamis (`141.258`/hari) hingga tersepi Senin (`109.340`/hari, dipengaruhi Labor Day `7 Sep`).
  - **Pola Spasial & Divergensi Volume vs Pendapatan:** Dominasi Manhattan (`87,31%` pickup; `USD 94,55 Jt`) dan Queens (`8,88%` pickup namun `19,03%` pendapatan kota / `USD 23,70 Jt`); konsentrasi Top-10 zona (`33,03%`) dan Top-20 (`56,45%`); kontras `Upper East Side South` (#1 volume, `172.229` pickup, rata-rata `USD 23,26`/trip) vs `JFK Airport` (#3 volume `149.680`, namun #1 pendapatan `USD 11,96 Jt`, rata-rata `USD 80,85`/trip, `9,6%` pendapatan kota) serta karakteristik hiburan malam `East Village` (puncak `23:00`, `42,80%` akhir pekan).
- **5.4 Evaluasi Model Machine Learning**
  - **Seleksi Model pada Validation Set (`21–23 Sep`):** Tabel lengkap 6 kandidat + Naive `lag_168h`; pembahasan alasan ilmiah pemilihan `ridge_reg_0p1_024` (RMSE All-261 terendah `12,0061`, menang mutlak di seluruh metrik Top-50, 4,6× lebih cepat daripada `RF d10/t60`, serta menghindari distorsi *binning* pohon `maxBins=32` pada lonjakan ratusan pickup/jam).
  - **Performa Final pada *Locked Test Holdout* (`24–30 Sep`, `43.848` baris — ditegaskan bukan proyeksi masa depan):**
    - All 261 Zones: MAE `4,5495` (`-6,76%`), RMSE `12,2597` (`-19,28%`), $R^2$ `0,9564`, WAPE `21,36%`; `81,91%` prediksi dalam `±5` trip/jam dan `94,31%` dalam `±20` trip/jam.
    - Top 50 Operational Zones (`91,0%` volume): MAE `17,4498` (`-11,76%`), RMSE `27,4563` (`-19,50%`), $R^2$ `0,9244`, WAPE `17,25%`; `45/50` zona (90%) mengungguli Naive RMSE (perbaikan terbesar: `Midtown Center -44,4%`, `Midtown East -42,7%`).
    - Segmen Temporal: Peak Hours `17:00–21:00` (`-28,78%` RMSE), Weekday (`-23,06%`) vs Weekend (`-8,80%`).
  - **Interpretabilitas Model:** Analisis koefisien terstandardisasi (`lag_1h +28,88`, `lag_168h +20,21`, `lag_24h +10,39` diimbangi `rolling_std_7d -8,30` dan `rolling_avg_7d -8,14` sebagai peredam *over-shooting*).
  - **Pembahasan Jujur Performa Lintas Kelompok Zona & Keterbatasan Model:**
    - Kontras tegas performa antar-tier (`Top 10`, `High`, `Medium` = `99,47%` volume seluruhnya unggul `-16,39%` s.d. `-20,83%` vs Tier `Low >150` = `0,53%` volume dengan WAPE `287,94%`, $R^2$ `-2,0229`, kalah `+55,13%` RMSE dari Naive akibat *zero-inflation* + intercept linear global `+3,19` + *clipping* non-negatif).
    - Kontras antar-Borough (`Manhattan -20,14%`, `Brooklyn -13,17%`, `Queens -12,91%` vs `Bronx +4,23%`, `EWR +14,75%`, `Staten Island` hanya `107` trip/minggu WAPE `1713,61%`).
    - Kasus khusus zona padat: `East Village` (`79`) kalah dari Naive `lag_168h` (`48,25` vs `42,87`) karena lonjakan *nightlife* akhir pekan yang sangat terikat pola minggu lalu.
    - Keterbatasan struktural lainnya: cakupan 1 bulan, absennya variabel eksogen (cuaca/penerbangan/event), dan horizon *1-step-ahead*.
- **5.5 Hasil Dashboard**
  - Pembahasan tampilan, fungsionalitas, dan kesesuaian metrik pada 5 halaman aktual (`v1.0.0-gold-ml`, `6/6` artefak Serving Layer, `100%` lookup match): Ringkasan Utama, Analisis Permintaan, Analisis Zona, Prediksi Permintaan (dengan banner klarifikasi *Locked Test Holdout*), dan Rekomendasi Armada (`8` rekomendasi aktif, `6` prioritas tinggi, jendela `17:00–21:59`).
  - Catatan transparan atas ketiadaan peta choropleth poligon GeoJSON (opsional).
- **5.6 Hasil Pengujian Integrasi**
  - Rekapitulasi hasil pengujian *end-to-end* dari `docs/testing_log_024.md`: **33/35 test case `PASS` (`0` FAIL)** dan **2 test case opsional/sengaja dilewati (`NOT TESTED`)** — yaitu `TST-OP-01_024` (GeoJSON opsional) dan `TST-OP-02_024` (upload NiFi langsung ke Raw produksi bulan baru).
  - Sorotan khusus hasil uji integrasi Apache NiFi `1.28.1` → HDFS `/urbanpulse_024/nifi_test_024/`: transfer `7,27` detik provenance (`7` event sukses), ukuran & SHA-256 100% cocok (`67.374.896` B), `0` duplikasi setelah >2 siklus `ListFile`, file lokal utuh, file Raw produksi tetap tidak tersentuh (`2026-11-25 07:24`), dan independensi layanan saat NiFi dihentikan.
  - Verifikasi *reproducibility* reload model (`< 1e-4`) dan *immutability* HDFS Gold byte-per-byte.

### 8. BAB VI KESIMPULAN DAN SARAN
- **6.1 Kesimpulan**
  - Menjawab seluruh rumusan masalah secara ringkas, kuantitatif, dan berbasis bukti aktual (keberhasilan pipeline Big Data *Medallion* + uji NiFi terisolasi, efektivitas cleaning berbasis audit Silver v2, temuan kunci BI September 2026, keunggulan `ridge_reg_0p1_024` pada zona operasional utama beserta batasan segmen *zero-inflated*, serta keberhasilan Dashboard 5 halaman dan 33/33 uji aktual PASS).
- **6.2 Saran**
  - **Pengembangan Data & Pipeline:** Penambahan dataset multi-bulan untuk menangkap *annual seasonality* serta aktivasi alur NiFi menuju `/urbanpulse_024/raw/yellow_taxi/` saat dataset bulan baru tersedia; pengamanan NiFi dengan HTTPS + autentikasi bila dideploy pada jaringan bersama.
  - **Pengembangan Model ML:** Eksplorasi model *two-stage/hurdle* atau regresi Poisson/Tweedie untuk mengatasi *zero-inflation* pada Tier `Low (>150)` / outer boroughs; pengayaan fitur eksogen (cuaca historis NYC, jadwal kedatangan penerbangan JFK/LGA, kalender event kota); pengembangan *direct multi-horizon forecasting* (>1 jam ke depan).
  - **Pengembangan Dashboard:** Penambahan berkas opsional `taxi_zones_024.geojson` untuk mengaktifkan peta choropleth poligon interaktif NYC.

### 9. Daftar Pustaka
- Disusun konsisten mengikuti gaya sitasi yang dikonfirmasi (rekomendasi: **APA 7th Edition** atau **IEEE**), hanya memuat sumber primer resmi dan literatur akademis standar yang 100% dapat diverifikasi (rincian lengkap pada Bagian 5).

### 10. Lampiran
- **Lampiran A:** Ringkasan Konvensi Penamaan `_024` dan Log *Data Provenance* (`docs/naming_convention_024.md`).
- **Lampiran B:** Skema Lengkap 21 Kolom Asli NYC TLC & 28 Kolom Turunan `_024` pada Silver Layer v2, serta Skema 5 Dataset Gold Layer.
- **Lampiran C:** Daftar Konfigurasi Prosesor Flow Apache NiFi `urbanpulse_ingestion_024` & Cuplikan Hasil Uji JSON (`nifi/configs/ingestion_test_result_024.json`).
- **Lampiran D:** Ringkasan Matriks 35 Test Case Pengujian (`docs/testing_log_024.md`) dan Daftar Bukti Artefak (`docs/evidence_checklist_024.md`).
- **Lampiran E:** Daftar 15 Koefisien Terstandardisasi Model `ridge_reg_0p1_024` dan Tabel Evaluasi Lengkap Per-Segmen (`reports/ml_evaluation_report_024.md`).

---

## 3. Tabel Pemetaan Bab ke Sumber File dan Screenshot

> **Keterangan Klasifikasi Bukti Visual (`reports/figures/`):**
> - **[Langsung]:** Tangkapan layar langsung dari browser/layanan aktif (portal NYC TLC, Dashboard Streamlit, Apache NiFi UI, Hadoop NameNode Explorer).
> - **[Verifikasi Saat Ini]:** Tangkapan layar perintah *read-only* terhadap kondisi sistem aktual pada 25 November 2026 (bukan log eksekusi historis awal).
> - **[Visualisasi Artefak]:** Visualisasi ringkasan yang dihasilkan langsung dari berkas laporan/JSON terverifikasi (bukan tangkapan layar terminal historis saat eksekusi awal).

| No | Bab / Bagian Laporan | Sumber File Utama & Pendukung | Screenshot / Gambar di `reports/figures/` | Keterangan Kejujuran Gambar |
| :-: | :--- | :--- | :--- | :--- |
| 1 | **Halaman Judul & Identitas** | `PROJECT_BRIEF_024.md` *(sebagian besar data identitas `[PERLU KONFIRMASI]`)* | — *(Opsional: logo institusi — `[PERLU KONFIRMASI]`)* | — |
| 2 | **Abstrak** | `docs/methodology_024.md`, `docs/technical_documentation_024.md`, `reports/gold_layer_report_024.md`, `reports/ml_evaluation_report_024.md` | — | — |
| 3 | **BAB I Pendahuluan** | `PROJECT_BRIEF_024.md` (§1–3), `README.md`, `docs/methodology_024.md` (§1, §9.2), `docs/naming_convention_024.md` | — | — |
| 4 | **BAB II Landasan Teori** | Literatur resmi & akademis (lihat Bagian 5); konteks arsitektur dari `docs/methodology_024.md`, `docs/technical_documentation_024.md`, `docs/nifi_setup_024.md` | — *(Opsional: diagram konseptual buatan naskah)* | — |
| 5 | **BAB III Metodologi & Perancangan Sistem** | `docs/methodology_024.md` (§1–9), `docs/technical_documentation_024.md` (§1–5), `docs/nifi_setup_024.md` (§1, §3–4), `docs/naming_convention_024.md`, `docs/dashboard_architecture_024.md`, `reports/ml_readiness_and_plan_024.md`, `reports/zone_lookup_validation_024.md` | • `progress_02_dataset_source_024.png`<br>• `progress_04_ml_chronological_split_024.png` | • **[Langsung]** Portal resmi NYC TLC (25 Nov 2026 08:20 WIB)<br>• **[Visualisasi Artefak]** Skema *chronological split* dari `ml_readiness_and_plan_024.md` |
| 6 | **BAB IV Implementasi Sistem** | | | |
| 6.1 | *4.1 Environment & Infrastruktur* | `PROJECT_BRIEF_024.md` (§5–6), `docs/technical_documentation_024.md` (§2–3), `docs/development_log_024.md` (Progress 1) | • `progress_01_project_structure_024.png`<br>• `progress_01_wsl_environment_024.png`<br>• `progress_01_java_verification_024.png`<br>• `progress_01_hadoop_hdfs_024.png`<br>• `progress_01_spark_verification_024.png` | Seluruh 5 gambar **[Verifikasi Saat Ini]** (*read-only* 25 Nov 2026; dapat dipilih 2–3 gambar inti di BAB IV dan sisanya di Lampiran F) |
| 6.2 | *4.2 Implementasi Apache NiFi* | `docs/nifi_setup_024.md` (§2–4, §7–8), `docs/development_log_024.md` (Progress 6), `nifi/flows/urbanpulse_ingestion_024.json`, `nifi/configs/nifi_runtime_024.properties`, `nifi/configs/hdfs_connection_024.properties` | • `nifi_01_ui_overview_024.png`<br>• `nifi_02_ingestion_flow_024.png` | Keduanya **[Langsung]** dari Apache NiFi `1.28.1` Web UI aktif (`http://127.0.0.1:8090/nifi`) |
| 6.3 | *4.3 Implementasi Raw Layer* | `reports/raw_validation_report_024.md` (§1), `docs/development_log_024.md` (Progress 2.1–2.2), `docs/technical_documentation_024.md` (§4.1) | • `progress_02_raw_hdfs_024.png` | **[Verifikasi Saat Ini]** Output *read-only* `hdfs dfs -ls/du/stat` pada `/urbanpulse_024/raw/yellow_taxi/` (25 Nov 2026) |
| 6.4 | *4.4 Implementasi Silver Layer* | `reports/silver_cleaning_report_024.md` (§1, §8), `reports/silver_cleaning_impact_audit_024.md`, `docs/development_log_024.md` (Progress 2.3–2.5) | • `progress_02_silver_hdfs_verification_024.png` | **[Verifikasi Saat Ini]** Output *read-only* HDFS `/urbanpulse_024/processed/yellow_taxi_024` (8 partisi, `159,2 M`, 25 Nov 2026) |
| 6.5 | *4.5 Implementasi Gold Layer* | `reports/gold_layer_report_024.md` (§1–2), `docs/development_log_024.md` (Progress 3.1–3.6), `reports/zone_lookup_validation_024.md` | • `progress_03_gold_script_024.png`<br>• `progress_03_gold_hdfs_datasets_024.png` | Keduanya **[Verifikasi Saat Ini]** (tampilan kode `build_gold_layer_024.py` sorotan `TIMESTAMP_NTZ` & `hdfs dfs -ls/du` `/urbanpulse_024/analytics/`, 25 Nov 2026) |
| 6.6 | *4.6 Implementasi ML* | `reports/ml_evaluation_report_024.md` (§1–4), `docs/development_log_024.md` (Progress 4–5.1), `docs/technical_documentation_024.md` (§4.1–4.2) | • `progress_04_ml_readiness_audit_024.png` | **[Visualisasi Artefak]** Ringkasan audit kesiapan ML dari `ml_readiness_and_plan_024.md` |
| 6.7 | *4.7 Implementasi Dashboard* | `docs/technical_documentation_024.md` (§5.1), `docs/dashboard_architecture_024.md`, `docs/development_log_024.md` (Progress 5.3), `README.md` | — *(Tampilan hasil halaman dimuat pada BAB V §5.5)* | — |
| 7 | **BAB V Hasil dan Pembahasan** | | | |
| 7.1 | *5.1 Hasil Validasi Data* | `reports/raw_validation_report_024.md` (§2–6), `reports/raw_validation_summary_024.json` | • `progress_02_raw_validation_024.png` | **[Visualisasi Artefak]** Ringkasan artefak `raw_validation_report_024.md` |
| 7.2 | *5.2 Hasil Transformasi* | `reports/silver_cleaning_report_024.md` (§2–7), `reports/silver_cleaning_impact_audit_024.md`, `reports/gold_layer_report_024.md` (§2–3) | • `progress_02_silver_cleaning_024.png`<br>• `progress_02_silver_metrics_024.png`<br>• `progress_03_gold_verification_024.png`<br>• `progress_03_gold_ml_features_024.png` | Keempat gambar **[Visualisasi Artefak]** dari `silver_cleaning_report_024.md` dan `gold_layer_report_024.md` |
| 7.3 | *5.3 Insight Analitik* | `reports/gold_layer_report_024.md` (§4), `reports/zone_lookup_validation_024.md` (§2), `docs/development_log_024.md` (Progress 3.7) | • `dashboard_01_overview_024.png`<br>• `dashboard_02_demand_analysis_024.png`<br>• `dashboard_03_zone_analysis_024.png` *(rujuk silang dengan §5.5 agar tidak duplikat gambar)* | **[Langsung]** Tangkapan browser Dashboard `v1.0.0-gold-ml` mode Data Aktual (25 Nov 2026 11:49–11:51 WIB) |
| 7.4 | *5.4 Evaluasi Model* | `reports/ml_evaluation_report_024.md` (§3, §5–8), `reports/ml_readiness_and_plan_024.md` (§3.2, §6), `data/analytics/model_metrics_024.json` | • `progress_04_ml_benchmark_024.png`<br>• `dashboard_04_demand_prediction_024.png` *(rujuk silang dengan §5.5)* | • **[Visualisasi Artefak]** Tabel benchmark naif `lag_168h` dari `ml_readiness_and_plan_024.md`<br>• **[Langsung]** Halaman Prediksi Permintaan (*Locked Test Holdout* 24–30 Sep 2026) |
| 7.5 | *5.5 Hasil Dashboard* | `docs/technical_documentation_024.md` (§5.1), `docs/development_log_024.md` (Progress 5.3), `docs/testing_log_024.md` (`TST-P5-04/05_024`) | • `dashboard_01_overview_024.png`<br>• `dashboard_02_demand_analysis_024.png`<br>• `dashboard_03_zone_analysis_024.png`<br>• `dashboard_04_demand_prediction_024.png`<br>• `dashboard_05_recommendations_024.png` | Seluruh 5 gambar **[Langsung]** dari browser Dashboard Streamlit `v1.0.0-gold-ml` (25 Nov 2026 11:49–11:52 WIB) |
| 7.6 | *5.6 Hasil Pengujian Integrasi* | `docs/testing_log_024.md` (§A–G, 35 test case), `docs/nifi_setup_024.md` (§5–6), `nifi/configs/ingestion_test_result_024.json`, `reports/ml_evaluation_report_024.md` (§7) | • `nifi_03_processor_execution_provenance_024.png`<br>• `nifi_04_hdfs_verification_024.png` | Keduanya **[Langsung]** dari Apache NiFi Data Provenance & Hadoop NameNode Explorer (`/urbanpulse_024/nifi_test_024/`, 25 Nov 2026 13:05–13:06 WIB) |
| 8 | **BAB VI Kesimpulan & Saran** | `docs/methodology_024.md` (§8–9), `reports/ml_evaluation_report_024.md` (§8), `docs/nifi_setup_024.md` (§8), `docs/testing_log_024.md` | — | — |
| 9 | **Daftar Pustaka** | Lihat Bagian 5 (Daftar Pustaka Terverifikasi) | — | — |
| 10 | **Lampiran** | `docs/naming_convention_024.md`, `docs/testing_log_024.md`, `docs/evidence_checklist_024.md`, `nifi/configs/ingestion_test_result_024.json`, `reports/silver_cleaning_report_024.md`, `reports/ml_evaluation_report_024.md` | Sisa tangkapan layar verifikasi lingkungan dari `progress_01_*_024.png` (yang tidak dimuat di BAB IV) agar seluruh **27/27 PNG** terdokumentasi utuh | Mempertahankan label **[Verifikasi Saat Ini]** / **[Visualisasi Artefak]** pada setiap caption |

---

## 4. Identifikasi Informasi yang Masih Kurang / Perlu Konfirmasi

Berikut daftar informasi yang **tidak tersedia di dalam seluruh sumber proyek** atau memerlukan keputusan penulis sebelum masuk ke tahap penulisan naskah final:

### 4.1 Identitas Akademik & Format Kampus (`[PERLU KONFIRMASI]` — Kritis untuk Halaman Judul)
1. **Nama Lengkap Penyusun & NIM/NPM Lengkap:** Tidak tercantum di 12 dokumen sumber (hanya kode akhiran `_024` yang digunakan secara konsisten pada identifier proyek). Perlu dikonfirmasi apakah proyek dikerjakan secara **individu** atau **kelompok** beserta daftar nama dan NIM lengkap.
2. **Nama Institusi Pendidikan:** Nama Universitas, Fakultas, dan Program Studi.
3. **Detail Mata Kuliah:** Nama resmi mata kuliah (tercatat *Big Data dan Analitik*), kode mata kuliah (jika ada), kelas/seksi, serta **Semester & Tahun Akademik** (mis. Ganjil 2026/2027).
4. **Nama Dosen Pengampu:** Nama lengkap beserta gelar akademik dosen pengampu mata kuliah.
5. **Kota dan Tanggal Pengesahan/Penyerahan Laporan:** Kota kampus dan tanggal resmi pengumpulan laporan akhir.
6. **Pedoman Tata Tulis & Gaya Sitasi Kampus:**
   - Gaya sitasi yang diinginkan (**APA 7th Edition** atau **IEEE**).
   - Kelengkapan halaman depan yang diwajibkan (apakah memerlukan Lembar Pengesahan, Kata Pengantar, Abstrak Bahasa Inggris / *Abstract*, Daftar Isi, Daftar Tabel, Daftar Gambar, Daftar Lampiran).
   - Aturan tipografi spesifik kampus bila ada (mis. Times New Roman 12 pt spasi 1,5, atau standar modern PDF A4).

### 4.2 Klarifikasi Materi Teknis & Kelengkapan Arsip (`[PERLU KONFIRMASI]` — Opsional / Non-Blokir)
7. **Status Salinan Konfigurasi XML Hadoop & Log Historis Progress 1:** Berdasarkan `docs/evidence_checklist_024.md`, dua item berikut tidak tersimpan di workspace:
   - Salinan berkas mentah `/usr/local/hadoop/etc/hadoop/core-site.xml` dan `hdfs-site.xml` *(ringkasan parameter koneksi sudah tercatat di `nifi/configs/hdfs_connection_024.properties`)*.
   - Log historis instalasi awal Java/Hadoop/Spark, `hdfs namenode -format`, log perintah `hdfs dfs -put` awal Progress 2, serta log error mentah sebelum perbaikan *timezone shift* `+7 jam` di Progress 3.
   - *Rekomendasi:* Tidak perlu mengarang log historis; cukup laporkan secara jujur berbasis parameter di `nifi/configs/hdfs_connection_024.properties` dan bukti verifikasi kondisi saat ini.
8. **Penggunaan 3 PDF Referensi Mahasiswa di `references/`:** Sesuai `PROJECT_BRIEF_024.md`, ketiga PDF tersebut hanya menjadi acuan struktur/metodologi internal dan **tidak dikutip** di Daftar Pustaka. Mohon konfirmasi jika ada struktur khusus dari pedoman mata kuliah pada referensi tersebut yang ingin diadopsi (mis. penamaan bab atau subbab tertentu).
9. **Pemilihan Penempatan 27 Screenshot:** Apakah seluruh 27 screenshot di `reports/figures/` ingin dimuat seluruhnya (mis. ~14–16 gambar utama di BAB III–V + sisanya di Lampiran F), atau hanya gambar terpilih pada batang tubuh laporan?
10. **Status Git/GitHub:** Sesuai arahan, GitHub tidak dianggap wajib dan tidak akan diklaim sudah tersedia. Jika nanti dosen mewajibkan tautan repositori sebelum finalisasi PDF, bagian Lampiran dapat disisipkan tautan repositori pada tahap akhir.

---

## 5. Rekomendasi Daftar Pustaka Terverifikasi

Seluruh referensi berikut merupakan **sumber resmi dataset/teknologi yang benar-benar digunakan dalam proyek** atau **buku/makalah akademis fundamental yang dapat diverifikasi sepenuhnya** (tidak ada kutipan fiktif maupun DOI/halaman yang dikarang). Format di bawah disajikan dalam gaya **APA 7th Edition** (dapat disesuaikan ke IEEE pada tahap penulisan bila diperlukan).

### A. Sumber Data Resmi
1. New York City Taxi and Limousine Commission (NYC TLC). (2026). *TLC Trip Record Data (Yellow Taxi Trip Records & Data Dictionary)*. Pemerintah Kota New York. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. New York City Taxi and Limousine Commission (NYC TLC). (2026). *Taxi Zone Lookup Table (`taxi_zone_lookup.csv`)*. https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

### B. Dokumentasi Resmi Perangkat Lunak & Kerangka Kerja
3. Apache Software Foundation. (2025). *Apache Hadoop Documentation: HDFS Architecture*. https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
4. Apache Software Foundation. (2024). *Apache NiFi Documentation (Overview, User Guide, & Processors)*. https://nifi.apache.org/docs.html
5. Apache Software Foundation. (2025). *Apache Spark Documentation: PySpark & Machine Learning Library (MLlib) Guide*. https://spark.apache.org/docs/latest/ml-guide.html
6. Apache Software Foundation. (2025). *Apache Parquet Documentation*. https://parquet.apache.org/docs/
7. Plotly Technologies Inc. (2025). *Plotly Python Open Source Graphing Library Documentation*. https://plotly.com/python/
8. Snowflake Inc. (2025). *Streamlit Documentation*. https://docs.streamlit.io/

> *Catatan untuk Sumber A dan B:* Tahun publikasi/akses pada dokumen web resmi perlu diverifikasi tanggal akses terakhirnya saat penyusunan naskah final (mis. diakses 25 November 2026).

### C. Literatur Akademis & Referensi Teknis Fundamental
9. Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics. *Proceedings of the 11th Annual Conference on Innovative Data Systems Research (CIDR 2021)*.
10. Breiman, L. (2001). Random forests. *Machine Learning*, 45(1), 5–32.
11. Friedman, J. H. (2001). Greedy function approximation: A gradient boosting machine. *Annals of Statistics*, 29(5), 1189–1232.
12. Hoerl, A. E., & Kennard, R. W. (1970). Ridge regression: Biased estimation for nonorthogonal problems. *Technometrics*, 12(1), 55–67.
13. Hyndman, R. J., & Athanasopoulos, G. (2021). *Forecasting: Principles and practice* (3rd ed.). OTexts. https://otexts.com/fpp3/
14. Laney, D. (2001). *3D data management: Controlling data volume, velocity, and variety* (Application Delivery Strategies File 949). META Group.
15. Meng, X., Bradley, J., Yavuz, B., Sparks, E., Venkataraman, S., Liu, D., Freeman, J., Tsai, D. B., Amde, M., Owen, S., Xin, D., Xin, R., Franklin, M. J., Zadeh, R., Zaharia, M., & Talwalkar, A. (2016). MLlib: Machine learning in Apache Spark. *Journal of Machine Learning Research*, 17(34), 1–7.
16. Shvachko, K., Kuang, H., Radia, S., & Chansler, R. (2010). The Hadoop Distributed File System. *2010 IEEE 26th Symposium on Mass Storage Systems and Technologies (MSST)*, 1–10.
17. White, T. (2015). *Hadoop: The definitive guide* (4th ed.). O'Reilly Media.
18. Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM*, 59(11), 56–65.

---

## 6. Perkiraan Jumlah Halaman Laporan (Format A4 Siap PDF)

Estimasi disusun secara realistis dengan asumsi format akademik A4 (margin standar, ukuran huruf 11–12 pt, spasi 1,15–1,5, tabel terformat rapi, serta penyisipan gambar/screenshot proporsional):

| No | Bagian / Bab | Estimasi Halaman | Dasar Perhitungan |
| :-: | :--- | :---: | :--- |
| 1 | Halaman Judul, Identitas, & Daftar Isi/Tabel/Gambar | 3–4 | Cover (1), Kata Pengantar/Pengesahan opsional (0–1), Daftar Isi/Tabel/Gambar (2). |
| 2 | Abstrak | 1 | 1 halaman penuh (Bahasa Indonesia; +1 halaman jika ada *Abstract* Bahasa Inggris). |
| 3 | BAB I Pendahuluan | 3–4 | Latar belakang, 5 rumusan masalah, tujuan, manfaat, dan batasan masalah terstruktur. |
| 4 | BAB II Landasan Teori | 7–9 | 7 subbab teori (Big Data, Hadoop/HDFS, NiFi, Spark/PySpark, Data Lake/Medallion, ML Demand Forecasting + metrik MAE/RMSE/$R^2$/WAPE, Streamlit & Plotly). |
| 5 | BAB III Metodologi dan Perancangan Sistem | 8–10 | Karakteristik dataset, diagram arsitektur *end-to-end*, desain NiFi, Medallion Raw–Silver–Gold, *feature engineering* 276 dimensi, split kronologis, metrik evaluasi, desain Dashboard + 2 gambar. |
| 6 | BAB IV Implementasi Sistem | 8–10 | Infrastruktur & mitigasi kendala, konfigurasi NiFi (`1.28.1`), implementasi Raw, Silver v1→v2, Gold (`TIMESTAMP_NTZ`), MLlib, Dashboard + 7–9 screenshot inti. |
| 7 | BAB V Hasil dan Pembahasan | 12–15 | Tabel validasi Raw, transformasi Silver/Gold, insight BI mendalam, seleksi 6 model MLlib, evaluasi *Test Holdout* lintas segmen/zona/tier/Borough, keterbatasan jujur, 5 screenshot Dashboard, hasil pengujian integrasi (35 test case + 2 screenshot NiFi/HDFS). |
| 8 | BAB VI Kesimpulan dan Saran | 2–3 | Kesimpulan terukur berbasis bukti + saran pengembangan data, ML, dan Dashboard. |
| 9 | Daftar Pustaka | 2 | 18 referensi resmi & akademis terverifikasi. |
| 10 | Lampiran (A–F) | 6–10 | Provenance `_024`, skema 49 kolom Silver & 5 tabel Gold, ringkasan 35 test case, tabel koefisien & evaluasi lengkap, serta sisa screenshot verifikasi lingkungan Progress 1–4. |
| | **Total Estimasi** | **52–68 halaman** | **Batang tubuh (BAB I–VI): ~40–51 halaman**; keseluruhan termasuk halaman depan, daftar pustaka, dan lampiran: **~52–68 halaman** *(proporsional dan sangat memadai untuk proyek Big Data end-to-end dengan 27 bukti visual)*. |
