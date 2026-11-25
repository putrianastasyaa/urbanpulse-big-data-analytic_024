# Evidence Checklist — UrbanPulse: Intelligent Urban Taxi Analytics (`_024`)

> **Proyek:** UrbanPulse — Intelligent Urban Taxi Analytics  
> **Tanggal Pembaruan:** 25 November 2026  
> **Cakupan:** Progress 1 s.d. Progress 6 (inventaris faktual seluruh artefak kode, laporan, model, serving layer, konfigurasi NiFi, deliverable akhir DOCX/PDF/PPTX, serta **10 tangkapan layar PNG autentik** di `reports/figures/`)  
> **Catatan Audit Autentisitas Bukti:** Sebanyak **17 berkas PNG sintetis** Progress 1–4 (9 mockup terminal/editor HTML/CSS dan 8 kartu infografis HTML/CSS) telah **dihapus seluruhnya** dari `reports/figures/`, laporan akhir (DOCX/PDF), dan slide presentasi (PPTX) demi menjaga keaslian bukti. Hanya **10 tangkapan layar aktual dari browser terhadap layanan/situs nyata** yang dipertahankan, ditambah **1 diagram arsitektur ilustratif** (bukan screenshot) di dalam laporan dan presentasi. Data kuantitatif penting dari Progress 1–4 disajikan secara jujur melalui **tabel native di DOCX/PPTX** yang bersumber langsung dari laporan Markdown/JSON terverifikasi.

---

## Ringkasan Kesiapan Bukti

| Progress | Tersedia | Perlu Dilengkapi / Tidak Disertakan | Belum Dikerjakan / Opsional |
| :--- | :---: | :---: | :---: |
| **Progress 1 — Infrastruktur & Environment** | 3 | 7 *(2 log/XML historis + 5 screenshot CLI tidak disertakan)* | 1 *(Git/GitHub opsional)* |
| **Progress 2 — Ingestion, Validasi, & Silver Layer** | 11 | 5 *(5 screenshot CLI/ringkasan tidak disertakan; bukti di laporan/JSON & tabel native)* | 0 |
| **Progress 3 — Gold Layer & Analytical Dataset** | 5 | 4 *(4 screenshot CLI/kode/ringkasan tidak disertakan; bukti di kode, laporan/JSON & tabel native)* | 0 |
| **Progress 4 — Machine Learning Readiness Audit** | 2 | 3 *(3 kartu ringkasan tidak disertakan; bukti di laporan/JSON & tabel native)* | 0 |
| **Progress 5 — ML Training, Evaluasi, Serving Layer, & Dashboard** | 13 | 0 | 1 *(GeoJSON opsional)* |
| **Progress 6 — Integrasi Apache NiFi & Finalisasi Deliverable** | 11 | 0 | 0 |
| **Total (66 Item)** | **45** | **19** | **2** |

*(Rekapitulasi bukti visual autentik di `reports/figures/`: **10/10 berkas PNG autentik Tersedia** dan lulus verifikasi — terdiri atas `1` portal resmi NYC TLC Progress 2, `5` halaman Dashboard Streamlit `v1.0.0-gold-ml` Progress 5, dan `4` Web UI Apache NiFi `1.28.1` & Hadoop NameNode Explorer Progress 6. Seluruh 17 PNG sintetis sebelumnya telah dihapus tanpa menyisakan referensi rusak.)*

---

## Progress 1 — Infrastruktur dan Environment

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Dokumen Audit Lingkungan & Brief Proyek | Bukti spesifikasi host, WSL2, Java, Hadoop, Spark, disk, RAM, dan mitigasi kendala. | Laporan | **Tersedia** | `PROJECT_BRIEF_024.md` |
| 2 | Dokumen Konvensi Penamaan `_024` & Provenance | Bukti kepatuhan aturan penamaan akademik dan ketertelusuran dataset. | Dokumentasi | **Tersedia** | `docs/naming_convention_024.md` |
| 3 | Bukti Instalasi & Integrasi Apache NiFi *(Diselesaikan pada Progress 6)* | Bukti layer ingestion otomatis sesuai rencana arsitektur. | Dokumentasi / Konfigurasi / Screenshot | **Tersedia** | `docs/nifi_setup_024.md`, `nifi/flows/urbanpulse_ingestion_024.json`, `reports/figures/nifi_01_ui_overview_024.png`, `reports/figures/nifi_02_ingestion_flow_024.png`, `reports/figures/nifi_03_processor_execution_provenance_024.png`, `reports/figures/nifi_04_hdfs_verification_024.png` |
| 4 | Screenshot Struktur Direktori Proyek | Bukti visual organisasi folder workspace. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; struktur faktual terdokumentasi di `PROJECT_BRIEF_024.md` & BAB IV.1)* | — |
| 5 | Screenshot Environment WSL2 Ubuntu 24.04.4 | Bukti visual `lsb_release -a`, `uname -a`, `free -h`, `df -h`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; spesifikasi aktual tercatat di `PROJECT_BRIEF_024.md` & Tabel 4.1)* | — |
| 6 | Screenshot Verifikasi Java OpenJDK 17 | Bukti visual `java -version`, `echo $JAVA_HOME`, `/usr/lib/jvm`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; terverifikasi operasional di `docs/nifi_setup_024.md` & Tabel 4.1)* | — |
| 7 | Screenshot Verifikasi Hadoop 3.5.0 & HDFS | Bukti visual `hadoop version`, `jps -l`, `safemode`, `hdfs dfs -ls`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; bukti HDFS aktual terwakili oleh `nifi_04_hdfs_verification_024.png` & log eksekusi)* | — |
| 8 | Screenshot Verifikasi Apache Spark 4.0.4 | Bukti visual `spark-submit --version` & `python3 --version`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; terverifikasi dari 6 skrip PySpark/MLlib & laporan JSON aktual)* | — |
| 9 | Salinan Konfigurasi Hadoop (`core-site.xml`, `hdfs-site.xml`) | Bukti konfigurasi `fs.defaultFS=hdfs://localhost:9000` & `dfs.replication=1`. | Konfigurasi | `[PERLU DILENGKAPI]` | `/usr/local/hadoop/etc/hadoop/` *(referensi ringkas tercatat di `nifi/configs/hdfs_connection_024.properties`, salinan XML belum diarsipkan di `docs/`)* |
| 10 | Log Historis Instalasi Awal & `hdfs namenode -format` | Bukti kronologis instalasi sebelum audit 25 Nov 2026. | Log | `[PERLU DILENGKAPI]` | Tidak tersimpan di workspace |
| 11 | Bukti Repositori Git & Commit GitHub | Bukti version control untuk deliverable GitHub. | Log / Screenshot | **Belum Dikerjakan (Opsional)** | `.git/` belum diinisialisasi |

---

## Progress 2 — Data Ingestion, Raw Validation, dan Silver Layer

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Screenshot Portal Resmi NYC TLC Trip Record Data | Bukti keaslian sumber data publik NYC TLC *(tangkapan browser aktual 25 Nov 2026 08:20 WIB)*. | Screenshot | **Tersedia** | `reports/figures/progress_02_dataset_source_024.png` |
| 2 | Dataset Mentah Parquet (`2026-09`) | Bukti file asli (`67,374,896` byte, SHA-256 terverifikasi). | Output / Data | **Tersedia** | `data/raw/yellow_tripdata_2026-09_024.parquet` |
| 3 | Script Validasi Raw Layer | Bukti kode profiling PySpark read-only. | Kode | **Tersedia** | `spark/processing/validate_raw_yellow_taxi_024.py` |
| 4 | Laporan Validasi Raw Layer (Markdown) | Bukti `3,876,044` baris, `21` kolom, Flex Fare structural nulls, dan anomali. | Laporan | **Tersedia** | `reports/raw_validation_report_024.md` |
| 5 | Ringkasan Metrik Validasi Raw (JSON) | Bukti terstruktur statistik deskriptif & kualitas data Raw. | Output / Laporan | **Tersedia** | `reports/raw_validation_summary_024.json` |
| 6 | Script Audit Dampak Cleaning Silver Layer | Bukti kode audit disjoint overlap & uji mirror-reversal pair. | Kode | **Tersedia** | `spark/processing/audit_silver_cleaning_impact_024.py` |
| 7 | Laporan Audit Dampak Cleaning (Markdown) | Bukti evaluasi ilmiah yang mendasari revisi Silver v1 → v2. | Laporan | **Tersedia** | `reports/silver_cleaning_impact_audit_024.md` |
| 8 | Ringkasan Metrik Audit Dampak Cleaning (JSON) | Bukti kuantitatif korelasi jam/zona, mirror-pair `88.66%`, dan dekomposisi `18,719` baris. | Output / Laporan | **Tersedia** | `reports/silver_cleaning_impact_audit_024.json` |
| 9 | Script Cleaning Silver Layer v2 | Bukti implementasi cleaning, `28` kolom turunan `_024`, dan validasi read-back. | Kode | **Tersedia** | `spark/processing/clean_yellow_taxi_024.py` |
| 10 | Laporan Cleaning Silver Layer v2 (Markdown) | Bukti perbandingan Raw (`3,876,044`) → v1 (`3,857,325`) → v2 (`3,860,852`) & atomic swap. | Laporan | **Tersedia** | `reports/silver_cleaning_report_024.md` |
| 11 | Ringkasan Metrik Silver Layer v2 (JSON) | Bukti terstruktur `49` kolom, flag `_024`, dan verifikasi live HDFS. | Output / Laporan | **Tersedia** | `reports/silver_cleaning_summary_024.json` |
| 12 | Screenshot Direktori Raw Layer di HDFS | Bukti visual direktori `/urbanpulse_024/raw/yellow_taxi/`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; terverifikasi SHA-256 di `nifi/configs/ingestion_test_result_024.json`)* | — |
| 13 | Screenshot Output Validasi Raw Layer | Ringkasan temuan validasi Raw (`3,876,044` baris, `21` kolom). | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.1 native dari `raw_validation_summary_024.json`)* | — |
| 14 | Screenshot Output Cleaning Silver v2 | Ringkasan aturan hard removal & atomic swap Silver v2. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.2–5.3 native & Slide 6 native dari `silver_cleaning_summary_024.json`)* | — |
| 15 | Screenshot Metrik Perbandingan Silver v1 vs v2 | Perbandingan metrik Raw → v1 → v2 & metrik tervalidasi `*_valid_*_024`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.2–5.3 native & Slide 6 native)* | — |
| 16 | Screenshot Verifikasi HDFS Silver Layer v2 | Bukti visual HDFS `/urbanpulse_024/processed/yellow_taxi_024` (`159.2 M`, 8 partisi). | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; verifikasi live read-back tercatat di `silver_cleaning_summary_024.json`)* | — |

---

## Progress 3 — Gold Layer dan Analytical Dataset

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Script Pembangunan Gold Layer | Bukti kode agregasi 5 tabel Gold, perbaikan `TIMESTAMP_NTZ`, & panel fitur ML bebas leakage. | Kode | **Tersedia** | `spark/processing/build_gold_layer_024.py` |
| 2 | Laporan Gold Layer & Insight Awal BI (Markdown) | Bukti KPI aktual September 2026 (`USD 124.58M`, Top-10 zona, weekday vs weekend, zero-leakage). | Laporan | **Tersedia** | `reports/gold_layer_report_024.md` |
| 3 | Ringkasan Metrik Gold Layer (JSON) | Bukti skema, jumlah baris, ukuran HDFS, dan rekonsiliasi 100% ke-5 dataset Gold. | Output / Laporan | **Tersedia** | `reports/gold_layer_summary_024.json` |
| 4 | File Referensi Zone Lookup Resmi NYC TLC | Bukti file referensi `265` zona (`12,331` byte, SHA-256 terverifikasi). | Data Referensi | **Tersedia** | `data/reference/taxi_zone_lookup_024.csv` |
| 5 | Laporan Validasi Zone Lookup (Markdown & JSON) | Bukti kecocokan `261/261` (`100%`) zona aktif Gold secara read-only. | Laporan | **Tersedia** | `reports/zone_lookup_validation_024.md`, `reports/zone_lookup_validation_024.json` |
| 6 | Screenshot Script Gold Layer (`build_gold_layer_024.py`) | Tampilan kode `CAST(... AS TIMESTAMP_NTZ)`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; kode sumber asli tersedia langsung di `spark/processing/build_gold_layer_024.py`)* | — |
| 7 | Screenshot Direktori 5 Dataset Gold di HDFS | Bukti direktori `/urbanpulse_024/analytics/` (`2.3 M`). | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; terverifikasi di `gold_layer_summary_024.json` & Tabel 5.4 / Slide 7 native)* | — |
| 8 | Screenshot Verifikasi Rekonsiliasi Gold Layer | Ringkasan rekonsiliasi `3,860,852` pickup & `0` missing hours. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.4 native dari `gold_layer_summary_024.json`)* | — |
| 9 | Screenshot Ringkasan Fitur ML (`gold_demand_features_024`) | Profil sparsity & zero-leakage `260.1012`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.5 native & Slide 7 native)* | — |

---

## Progress 4 — Machine Learning Readiness Audit

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Laporan ML Readiness Audit & Rencana Implementasi (Markdown) | Bukti kesiapan `144,072` baris ML-ready, korelasi fitur, split kronologis, benchmark naif, & keterbatasan. | Laporan | **Tersedia** | `reports/ml_readiness_and_plan_024.md` |
| 2 | Ringkasan Metrik ML Readiness Audit (JSON) | Bukti terstruktur distribusi target, korelasi Pearson, split, & benchmark MAE/RMSE. | Output / Laporan | **Tersedia** | `reports/ml_readiness_audit_024.json` |
| 3 | Screenshot Hasil Audit Kesiapan ML | Ringkasan `144,072` ML-ready, `0` null, `lag_1h=0.9622`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 4.3 native dari `ml_readiness_audit_024.json`)* | — |
| 4 | Screenshot Skema Chronological Split | Ringkasan split `Sep 8–20 / 21–23 / 24–30`. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 3.3 native)* | — |
| 5 | Screenshot Benchmark Persistensi Naif (`lag_168h`) | Ringkasan sanity floor MAE/RMSE All-261 & Top-50. | Screenshot | **Tidak Disertakan** *(PNG sintetis dihapus; disajikan sebagai Tabel 5.8 native dari `ml_readiness_audit_024.json`)* | — |

---

## Progress 5 — ML Training, Evaluasi, Serving Layer, & Dashboard Data Aktual

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Script Training Spark MLlib | Bukti pelatihan 6 kandidat model (`Ridge`, `RandomForest`, `GBT`) + Naive, chronological split, & refit final. | Kode | **Tersedia** | `ml/training/train_demand_model_024.py` |
| 2 | Script Evaluasi Independen Spark MLlib | Bukti verifikasi reload model, evaluasi tersegmentasi, lookup enrichment, & cek immutability HDFS. | Kode | **Tersedia** | `ml/evaluation/evaluate_demand_model_024.py` |
| 3 | Artefak Model Terlatih (`PipelineModel`) & Metadata Run | Bukti model terbaik `ridge_reg_0p1_024` tersimpan & lolos reload verification. | Model / Output | **Tersedia** | `ml/models/ridge_reg_0p1_024/`, `ml/models/training_run_metadata_024.json` |
| 4 | Laporan Evaluasi Model ML (Markdown & JSON) | Bukti performa Test Holdout (All 261 RMSE `12.2597` `-19.28%`, $R^2$ `0.9564`; Top-50 WAPE `17.25%`) & koefisien fitur. | Laporan / Output | **Tersedia** | `reports/ml_evaluation_report_024.md`, `reports/ml_evaluation_summary_024.json` |
| 5 | Serving Layer Lokal (`6/6` Artefak di `data/analytics/`) | Bukti 4 tabel Gold ekspor read-only HDFS + `demand_predictions_024.parquet` (`43,848` baris) + `model_metrics_024.json`. | Output / Data | **Tersedia** | `data/analytics/gold_hourly_demand_024/`, `data/analytics/gold_zone_demand_024/`, `data/analytics/gold_temporal_patterns_024/`, `data/analytics/gold_zone_hourly_demand_024/`, `data/analytics/demand_predictions_024.parquet/`, `data/analytics/model_metrics_024.json` |
| 6 | Kode & Arsitektur Dashboard Streamlit (`v1.0.0-gold-ml`) | Bukti 5 halaman interaktif terhubung ke Data Aktual Gold & ML + enrichment `PULocationID == LocationID`. | Kode & Dokumentasi | **Tersedia** | `docs/dashboard_architecture_024.md`, `dashboard/app_024.py`, `dashboard/config_024.py`, `dashboard/data_loader_024.py`, `dashboard/pages/overview_024.py`, `dashboard/pages/demand_analysis_024.py`, `dashboard/pages/zone_analysis_024.py`, `dashboard/pages/demand_prediction_024.py`, `dashboard/pages/recommendations_024.py` |
| 7 | Konfigurasi Tema Streamlit & Requirements | Bukti dependensi & tema antarmuka UrbanPulse. | Konfigurasi | **Tersedia** | `.streamlit/config.toml`, `requirements.txt` |
| 8 | Screenshot Dashboard 1 — Ringkasan Utama (`Data Aktual`) | Bukti KPI aktual (`3.860.852` pickup, `USD 124,58 Jt`, `18:00 vs 04:00`, `Upper East Side South`, `6/6` artefak). | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/dashboard_01_overview_024.png` |
| 9 | Screenshot Dashboard 2 — Analisis Permintaan Temporal | Bukti filter tanggal/hari/periode, `5.346,9/jam` weekday vs `5.404,6/jam` weekend, & grafik temporal. | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/dashboard_02_demand_analysis_024.png` |
| 10 | Screenshot Dashboard 3 — Analisis Zona NYC | Bukti ranking zona, `JFK Airport USD 11,96 Jt`, Top-10 `33,03%`, & scatter Demand vs Revenue. | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/dashboard_03_zone_analysis_024.png` |
| 11 | Screenshot Dashboard 4 — Prediksi Permintaan ML | Bukti evaluasi Locked Test Holdout `24–30 Sep 2026` (MAE `4,55`, RMSE `12,26`, $R^2$ `0,9564`, WAPE `21,36%`). | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/dashboard_04_demand_prediction_024.png` |
| 12 | Screenshot Dashboard 5 — Rekomendasi Armada | Bukti `8` rekomendasi berbasis bukti (`6` prioritas tinggi, jendela `17:00–21:59`, kontras WAPE Top-50 vs Low). | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/dashboard_05_recommendations_024.png` |
| 13 | Backup Dokumentasi Progress 1–4 & Pra-Pembaruan Progress 1–6 | Bukti jejak audit & pelestarian versi dokumentasi proyek. | Backup | **Tersedia** | `docs/backup_progress_01-04_024/`, `docs/backup_progress_01-06_024/` |
| 14 | Geometri Zona NYC (`taxi_zones_024.geojson`) *(Opsional)* | Peta choropleth poligon NYC penuh pada `zone_analysis_024.py`. | Data Referensi | **Belum Dikerjakan (Opsional)** | `data/reference/taxi_zones_024.geojson` |

---

## Progress 6 — Integrasi Apache NiFi (`1.28.1`) & Finalisasi Deliverable

| No | Nama Bukti | Tujuan | Jenis Bukti | Status | Lokasi File / Target Path |
| :-: | :--- | :--- | :--- | :---: | :--- |
| 1 | Dokumentasi Lengkap Setup & Uji Apache NiFi | Bukti arsitektur ingestion, kompatibilitas Java 17, hasil uji `/urbanpulse_024/nifi_test_024/`, & panduan operasional. | Dokumentasi | **Tersedia** | `docs/nifi_setup_024.md` |
| 2 | Ekspor Definisi Flow NiFi (`urbanpulse_ingestion_024`) | Bukti 6 prosesor (`ListFile`, `FetchFile`, `UpdateAttribute`, `PutHDFS`, 2× `LogAttribute`) & seluruh koneksi. | Flow JSON | **Tersedia** | `nifi/flows/urbanpulse_ingestion_024.json` |
| 3 | Referensi Konfigurasi Koneksi HDFS & Runtime NiFi | Bukti parameter koneksi read-only Hadoop & alokasi JVM konservatif (`-Xms512m -Xmx1g`, `127.0.0.1:8090`). | Konfigurasi | **Tersedia** | `nifi/configs/hdfs_connection_024.properties`, `nifi/configs/nifi_runtime_024.properties` |
| 4 | Bukti JSON Verifikasi Integritas & Anti-Duplikasi Uji NiFi | Bukti ukuran `67,374,896` B, SHA-256 cocok 100%, `1` file tanpa duplikat, & file Raw produksi tetap utuh. | Output / Log | **Tersedia** | `nifi/configs/ingestion_test_result_024.json` |
| 5 | Screenshot NiFi 1 — Antarmuka Web UI (Root Canvas) | Bukti visual Apache NiFi `1.28.1` aktif di `http://127.0.0.1:8090/nifi` dengan Process Group `urbanpulse_ingestion_024`. | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/nifi_01_ui_overview_024.png` |
| 6 | Screenshot NiFi 2 — Diagram Lengkap Flow Ingestion | Bukti visual ke-6 prosesor berstatus `RUNNING` (`0` invalid, `0` error) dan seluruh jalur *success/failure*. | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/nifi_02_ingestion_flow_024.png` |
| 7 | Screenshot NiFi 3 — Bukti Eksekusi Prosesor (Data Provenance) | Bukti visual 7 event provenance aktual (`CREATE` → `FETCH` → `SEND` `64.25 MB` → `DROP`, `7.27s`). | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/nifi_03_processor_execution_provenance_024.png` |
| 8 | Screenshot NiFi 4 — Verifikasi Output HDFS di NameNode Explorer | Bukti visual `/urbanpulse_024/nifi_test_024/yellow_tripdata_2026-09_024.parquet` (`64.25 MB`, `1 of 1 entries`). | Screenshot *(Aktual Browser)* | **Tersedia** | `reports/figures/nifi_04_hdfs_verification_024.png` |
| 9 | Pembaruan 5 Dokumen Utama Proyek (Progress 1–6) | Konsistensi menyeluruh `development_log`, `testing_log`, `evidence_checklist`, `technical_documentation`, `methodology`. | Dokumentasi | **Tersedia** | `docs/development_log_024.md`, `docs/testing_log_024.md`, `docs/evidence_checklist_024.md`, `docs/technical_documentation_024.md`, `docs/methodology_024.md` |
| 10 | Laporan Akhir Akademik (DOCX & PDF) | Deliverable laporan akhir mata kuliah Big Data & Analitik (memuat 10 screenshot autentik + 1 diagram arsitektur ilustratif + tabel native; bebas gambar sintetis). | Laporan | **Tersedia** | `reports/final_024/laporan_akhir_urbanpulse_024.docx`, `reports/final_024/laporan_akhir_urbanpulse_024.pdf` |
| 11 | Materi Slide Presentasi & Skenario Demo Final (PPTX) | Deliverable presentasi 15 slide (memuat 9 screenshot autentik + 1 diagram arsitektur ilustratif + tabel native; bebas gambar sintetis). | Presentasi | **Tersedia** | `reports/final_024/presentasi_demo_urbanpulse_024.pptx` |

---

## Ringkasan Verifikasi 10 Screenshot Autentik di `reports/figures/`

Seluruh **10 berkas PNG autentik** di `reports/figures/` telah diverifikasi ada, dapat dibuka tanpa error, dan berasal langsung dari tampilan browser terhadap layanan/situs nyata (`25 Nov 2026`):
- **Progress 2 (1 PNG — portal web resmi NYC TLC):** `progress_02_dataset_source_024.png`.
- **Progress 5 (5 PNG — tangkapan browser langsung Dashboard Streamlit `v1.0.0-gold-ml` Data Aktual):** `dashboard_01_overview_024.png`, `dashboard_02_demand_analysis_024.png`, `dashboard_03_zone_analysis_024.png`, `dashboard_04_demand_prediction_024.png`, `dashboard_05_recommendations_024.png`.
- **Progress 6 (4 PNG — tangkapan browser langsung Apache NiFi `1.28.1` & Hadoop NameNode Explorer):** `nifi_01_ui_overview_024.png`, `nifi_02_ingestion_flow_024.png`, `nifi_03_processor_execution_provenance_024.png`, `nifi_04_hdfs_verification_024.png`.

*(Catatan: 17 berkas PNG sintetis Progress 1–4 telah dihapus permanen dari `reports/figures/`, DOCX/PDF, dan PPTX; seluruh metrik kuantitatifnya dipertahankan secara autentik dalam bentuk tabel native dan artefak laporan JSON/Markdown.)*
