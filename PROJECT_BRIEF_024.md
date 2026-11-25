# PROJECT BRIEF — UrbanPulse: Intelligent Urban Taxi Analytics

> **Status Dokumen:** Inisiasi Proyek (Tahap Audit Lingkungan & Perencanaan)  
> **Tanggal Pembuatan:** 25 November 2026  
> **Mata Kuliah:** Big Data dan Analitik (Tugas Akhir)  
> **Root Project:** `D:\A University\Big Data dan Analitik\Project Akhir`

---

## 1. Konteks & Ringkasan Proyek

**UrbanPulse — Intelligent Urban Taxi Analytics** adalah proyek aplikasi berbasis Big Data dan Machine Learning untuk menganalisis pola permintaan dan memprediksi kebutuhan layanan **Yellow Taxi di New York City (NYC)** berdasarkan dimensi **waktu** dan **lokasi (taxi zones)**.

Proyek ini memanfaatkan data transaksi perjalanan nyata dari **NYC Taxi and Limousine Commission (TLC)** dengan cakupan maksimal satu tahun terakhir, diolah menggunakan ekosistem Big Data end-to-end (ingestion, penyimpanan terdistribusi, pemrosesan paralel, machine learning, hingga visualisasi interaktif).

### Deliverable Utama
1. **Demonstrasi Aplikasi dengan UI Interaktif** (Streamlit + Plotly) yang menyajikan insight Business Intelligence (BI) dan hasil prediksi permintaan taksi.
2. **Laporan Akhir (PDF)** yang mencakup sumber data, alasan pemilihan topik, manajemen data (arsitektur & pipeline), insight baru berbasis hasil olahan nyata, serta rekomendasi operasional/strategis ke depan.
3. **Repositori Kode di GitHub** yang terdokumentasi rapi dan reproducible.

### Prinsip & Batasan Kerja
- **100% Data Nyata:** Tidak menggunakan dataset simulasi atau data sintetis untuk menggantikan data asli NYC TLC.
- **Berbasis Bukti:** Tidak membuat klaim hasil analisis, metrik model, atau insight sebelum data benar-benar diunduh dan diolah.
- **Konvensi Penamaan Wajib (`_024`):** Seluruh file, class, function, variabel, kolom hasil olahan, tabel, model, dan artefak buatan sendiri pada proyek wajib menggunakan akhiran `_024` selama tidak mengganggu kompatibilitas teknis. Pengecualian meliputi: kolom asli dataset NYC TLC, API/library/framework bawaan, file standar tooling (`README.md`, `.gitignore`, `.gitkeep`, `requirements.txt`), dan nama folder utama proyek. File dataset mentah dapat diberi akhiran `_024` pada nama file tanpa mengubah isi Parquet dengan provenance nama asli tercatat di `docs/naming_convention_024.md`.
- **Referensi Etis:** Tiga dokumen PDF referensi tugas mahasiswa (akan ditempatkan di folder `references/`) digunakan semata-mata sebagai acuan struktur laporan dan pendekatan metodologis, bukan untuk disalin.
- **Pengembangan Bertahap:** Lingkungan diverifikasi terlebih dahulu sebelum instalasi tambahan, pengunduhan dataset, dan pengkodean aplikasi.
- **Aman Terhadap File Eksisting:** Tidak mengubah atau menghapus file, konfigurasi sistem, maupun data proyek lain tanpa izin eksplisit.

---

## 2. Tujuan Proyek

1. **Membangun Pipeline Big Data End-to-End** untuk menyerap (*ingest*), menyimpan, membersihkan, dan mentransformasi data perjalanan Yellow Taxi NYC berskala jutaan baris.
2. **Menghasilkan Insight Business Intelligence (BI)** terkait dinamika spasial-temporal permintaan taksi (misalnya pola per jam, hari kerja vs akhir pekan, tren bulanan/musiman, zona pickup/dropoff utama, koridor perjalanan, durasi, jarak, dan karakteristik tarif/tip — seluruh angka aktual akan diturunkan murni dari hasil pengolahan data).
3. **Membangun Model Prediksi Permintaan (Spark MLlib)** untuk memprediksi jumlah permintaan taksi (*trip demand*) berdasarkan waktu dan lokasi/zona guna mendukung alokasi armada yang lebih efisien.
4. **Menyajikan Hasil dalam Aplikasi UI Interaktif** agar pengguna dapat mengeksplorasi insight BI dan simulasi/prediksi permintaan secara intuitif.

---

## 3. Rencana Sumber Data

- **Sumber Resmi:** NYC Taxi and Limousine Commission (TLC) — Trip Record Data  
  URL: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- **Jenis Data Utama:** Yellow Taxi Trip Records (format **Apache Parquet**).
- **Periode yang Direncanakan:** **Oktober 2025 – September 2026** (12 bulan penuh, memenuhi ketentuan maksimal satu tahun terakhir per November 2026).
- **Data Pendukung (Metadata Lokasi):**
  - `taxi_zone_lookup.csv` (pemetaan `LocationID` ke nama `Borough`, `Zone`, dan `service_zone`).
  - Geometri batas zona taksi NYC (*Taxi Zones Shapefile / GeoJSON*) untuk visualisasi peta choropleth di UI.
- **Catatan Verifikasi:** Ketersediaan seluruh file bulanan (khususnya bulan-bulan terbaru seperti Agustus–September 2026) akan diverifikasi langsung pada fase akuisisi data, mengingat TLC biasanya memiliki jeda publikasi 1–2 bulan.

---

## 4. Rencana Arsitektur & Stack Teknologi

```text
[NYC TLC Parquet Files + Zone Lookup]
                │
                ▼
     [1. Apache NiFi]          ── Ingestion otomatis & validasi awal
                │
                ▼
     [2. Hadoop HDFS]          ── Raw storage terdistribusi (hdfs://localhost:9000)
                │
                ▼
     [3. Apache PySpark]       ── Data cleaning, enrichment zona, transformasi,
                │                 dan agregasi Business Intelligence (Silver/Gold)
                ▼
     [4. Spark MLlib]          ── Feature engineering temporal-spasial, training,
                │                 evaluasi, dan inferensi model prediksi demand
                ▼
     [5. Serving Artifacts]    ── Tabel agregasi BI & output prediksi terstruktur
                │                 (Parquet/CSV ringan di ruang proyek)
                ▼
  [6. Streamlit + Plotly UI]   ── Dashboard BI interaktif & halaman prediksi
```

| Lapisan | Teknologi | Peran |
| :--- | :--- | :--- |
| Ingestion | Apache NiFi | Menyerap file Parquet & referensi zona ke HDFS |
| Raw Storage | Hadoop HDFS (3.5.0) | Penyimpanan raw layer terdistribusi |
| Processing & BI | PySpark (Spark 4.0.4) | Cleaning, feature engineering, agregasi BI |
| Machine Learning | Spark MLlib | Prediksi permintaan taksi per zona & waktu |
| Visualisasi & UI | Streamlit + Plotly | Dashboard interaktif untuk demonstrasi aplikasi |
| Version Control | Git & GitHub | Manajemen kode dan dokumentasi proyek |

---

## 5. Hasil Pemeriksaan Lingkungan Teknis (25 November 2026)

### 5.1 Kondisi Folder Project
- Lokasi: `D:\A University\Big Data dan Analitik\Project Akhir`
- Status saat ini: **Kosong** (belum ada file, subfolder `references/`, maupun inisialisasi `.git`).

### 5.2 Spesifikasi Host Windows
- **OS:** Microsoft Windows 11 Home Single Language (Build 26200, 64-bit)
- **CPU:** 13th Gen Intel Core i7-1355U (10 cores, 12 logical processors)
- **RAM:** 16 GB total (~15.55 GiB). Saat pemeriksaan, memori bebas tercatat sangat rendah (~356 MB) karena Docker Desktop aktif menjalankan 6 container dari proyek lain (`mongo1`, `mongo2`, `mongo3`, `tbdnr-projek-app-1`, `tbdnr-projek-mongo-1`, `tbdnr-projek-neo4j-1`).
- **Penyimpanan (Disk):**
  - `C:\` : 207.3 GB terpakai / **17.42 GB kosong** (93% penuh — perlu perhatian khusus)
  - `D:\` : 194.14 GB terpakai / **55.86 GB kosong** (lokasi folder proyek)
  - `E:\` : 1607.11 GB terpakai / **255.85 GB kosong**
- **Tools Windows:**
  - Git `2.47.0.windows.1` (terpasang)
  - Java JDK `20.0.2` (terpasang, `JAVA_HOME` belum di-set)
  - Python `3.10.6` (Laragon, default PATH; terpasang `numpy`, `pandas`, `pyarrow`) & Python `3.11.4`
  - Docker Desktop (terpasang & sedang berjalan pada WSL2)
  - `streamlit`, `plotly`, `pyspark`: belum terpasang

### 5.3 Status WSL 2 & Ekosistem Big Data (Ubuntu)
- **WSL 2:** Aktif dan dapat dijalankan (`Ubuntu` sebagai distribusi default, versi WSL 2).
- **OS WSL:** Ubuntu 24.04.4 LTS (Noble), systemd aktif, alokasi default RAM WSL `7.5 GiB` + Swap `2.0 GiB` (belum ada file `C:\Users\PUTRI ANASTASYA\.wslconfig`).
- **Komponen yang Sudah Terpasang di WSL Ubuntu:**
  - **OpenJDK 17.0.20** (`/usr/lib/jvm/java-17-openjdk-amd64`) — sudah dikonfigurasi di `~/.bashrc`
  - **Apache Hadoop 3.5.0** (`/usr/local/hadoop`) — mode pseudo-distributed (`hdfs://localhost:9000`, `dfs.replication=1`, data dir `/home/ubuntu/hadoopdata` yang saat ini berukuran `7.3 GB` dari penggunaan sebelumnya). Saat pemeriksaan, daemon HDFS/YARN belum berjalan.
  - **Apache Spark 4.0.4** (`/usr/local/spark`, build `hadoop3`) — sudah terpasang dan lolos verifikasi `spark-submit --version`.
  - **Python 3.12.3** (modul `venv` tersedia, tetapi `pip` / `python3-pip` belum terpasang).
  - **SSH (`ssh.socket`):** Terpasang dan aktif pada port 22.
- **Komponen yang Belum Terpasang:**
  - **Apache NiFi:** Belum ditemukan di Windows maupun di WSL Ubuntu.
  - **Python Environment Proyek (`venv`):** Belum ada `pip`, `pyspark`, `streamlit`, `plotly`, `pandas`, `pyarrow` di WSL Ubuntu.

---

## 6. Identifikasi Kebutuhan & Potensi Kendala Teknis

| No | Area | Temuan / Potensi Kendala | Tingkat Risiko | Rekomendasi Mitigasi |
| :-: | :--- | :--- | :---: | :--- |
| 1 | **Ruang Disk `C:\`** | Tersisa hanya **17.42 GB**. Virtual disk WSL2 (`ext4.vhdx`) dan Docker berada di `C:\`, dan `/home/ubuntu/hadoopdata` sudah memakai 7.3 GB. Penambahan NiFi + HDFS data + shuffle Spark bisa menghabiskan `C:\`. | **Tinggi** | Gunakan format **Parquet** (jauh lebih ringkas daripada CSV, ~600 MB–1.2 GB untuk 12 bulan). Simpan staging file, hasil agregasi, dan output UI di Drive `D:\` (atau arsip di `E:\`). Konfigurasi retensi rendah pada NiFi repositories. Jangan menghapus file tugas lama tanpa izin. |
| 2 | **Kapasitas RAM (16 GB)** | Saat ini RAM bebas Windows sangat tipis karena 6 container Docker proyek lain sedang berjalan. Menjalankan Docker lain + Hadoop + NiFi + Spark + Streamlit bersamaan berisiko OOM. | **Tinggi** | Hentikan container proyek lain saat bekerja pada UrbanPulse. Jalankan pipeline secara **bertahap (stage-by-stage)**: jalankan NiFi → ingest ke HDFS → stop NiFi → jalankan PySpark/MLlib → ekspor hasil serving → jalankan UI Streamlit dari serving layer. Batasi JVM heap NiFi & memori driver Spark (`spark.driver.memory`). |
| 3 | **Konflik Port `8088`** | Port `127.0.0.1:8088` sedang dipakai oleh container `tbdnr-projek-app-1`. Port `8088` adalah port default **YARN ResourceManager Web UI**. | **Sedang** | Hentikan container tersebut sebelum menyalakan YARN (`start-yarn.sh`), atau jalankan Spark dalam mode `local[*]` yang membaca langsung dari HDFS (`hdfs://localhost:9000`) tanpa keharusan menyalakan YARN setiap saat. |
| 4 | **Instalasi Apache NiFi & Versi Java** | NiFi belum terpasang. **NiFi 2.x** membutuhkan **Java 21**, sedangkan WSL saat ini memakai **OpenJDK 17** (yang optimal untuk Hadoop 3.5.0 & Spark 4.0.4). Selain itu, integrasi `PutHDFS` perlu disesuaikan dengan versi NiFi dan konfigurasi Hadoop 3.5.0. | **Sedang** | Tentukan opsi terbaik bersama sebelum instalasi: (a) jalankan NiFi 2.x dengan OpenJDK 21 terpisah khusus untuk `NIFI_JAVA_HOME` (tanpa mengubah Java 17 milik Hadoop/Spark), (b) jalankan NiFi via Docker container pada jaringan host/WSL, atau (c) NiFi 1.28.x di Java 17 jika membutuhkan kompatibilitas `PutHDFS` bawaan yang paling stabil. |
| 5 | **Python `pip` & Virtual Env di WSL** | `python3-pip` belum terpasang di WSL Ubuntu; environment `HADOOP_HOME`/`SPARK_HOME` hanya dimuat pada interactive shell (`.bashrc`). | **Rendah** | Instal `python3-pip` di WSL Ubuntu, buat `.venv` khusus proyek, dan buat script helper environment agar variabel `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, serta `SPARK_LOCAL_IP=127.0.0.1` konsisten saat dijalankan. |
| 6 | **Warning `/etc/wsl.conf` & Hostname Spark** | Muncul warning `Unknown key 'network.generateHost' in /etc/wsl.conf:9` (typo dari `generateHosts`) dan warning Spark terkait resolusi hostname `ubuntu` ke `127.0.1.1`. | **Rendah** | Set `SPARK_LOCAL_IP=127.0.0.1` saat menjalankan Spark. Perbaikan `/etc/wsl.conf` hanya dilakukan jika diizinkan pengguna. |
| 7 | **Ketersediaan Data Terbaru TLC** | Periode target Oktober 2025 – September 2026 perlu dicek apakah seluruh 12 file Parquet sudah dipublikasikan di server NYC TLC. | **Sedang** | Verifikasi daftar URL resmi NYC TLC pada fase akuisisi; jika bulan terakhir belum rilis, sesuaikan jendela 12 bulan terbaru yang tersedia secara resmi. |
| 8 | **File Referensi PDF Belum di Folder Proyek** | Folder `references/` belum ada dan 3 PDF referensi mahasiswa belum ditempatkan di dalam root project. | **Rendah** | Minta pengguna menempatkan 3 PDF ke `references/` sebelum finalisasi desain arsitektur detail dan kerangka laporan. |

---

## 7. Struktur Direktori Proyek & Konvensi Penamaan (`_024`)

Struktur berikut telah disiapkan di root proyek dengan pemisahan jelas antara data mentah, hasil pemrosesan, kode pipeline, model ML, dan laporan. Seluruh artefak dan identifier buatan sendiri mengikuti aturan akhiran **`_024`** (rincian lengkap dan log provenance dataset mentah terdokumentasi di `docs/naming_convention_024.md`):

```text
Project Akhir/
├── PROJECT_BRIEF_024.md          # Dokumen konteks, hasil audit, dan rencana kerja
├── README.md                     # Dokumentasi proyek untuk GitHub (standar tooling)
├── .gitignore                    # Mengecualikan raw data besar, venv, cache, model biner (standar tooling)
├── references/                   # 3 PDF referensi tugas mahasiswa (acuan lokal)
├── data/
│   ├── raw/                      # Dataset mentah NYC TLC (Parquet berakhiran _024, isi asli tidak diubah)
│   ├── reference/                # Metadata lokasi (taxi_zone_lookup, shapefile/GeoJSON zona NYC)
│   ├── processed/                # Data hasil cleaning & enrichment (Silver layer)
│   └── analytics/                # Tabel agregasi BI & hasil prediksi untuk serving layer UI (Gold layer)
├── nifi/
│   ├── flows/                    # Flow JSON / template Apache NiFi (*_024)
│   └── configs/                  # Konfigurasi koneksi & parameter NiFi–HDFS (*_024)
├── spark/
│   ├── ingestion/                # Script penyerapan/validasi awal ke/dari HDFS (*_024.py)
│   ├── processing/               # Job PySpark untuk cleaning, enrichment, & agregasi BI (*_024.py)
│   └── scripts/                  # Utilitas & helper eksekusi Spark (*_024)
├── ml/
│   ├── training/                 # Script feature engineering & pelatihan model Spark MLlib (*_024.py)
│   ├── evaluation/               # Script & hasil evaluasi metrik model (*_024)
│   └── models/                   # Artefak model ML terlatih (*_024)
├── dashboard/
│   ├── pages/                    # Halaman multi-page Streamlit + Plotly (*_024.py)
│   └── assets/                   # Aset visual & styling UI
├── reports/
│   └── figures/                  # Grafik, peta, & screenshot untuk laporan akhir PDF
├── docs/
│   └── naming_convention_024.md  # Aturan wajib akhiran _024, pengecualian, & provenance dataset
└── scripts/                      # Script otomasi proyek & helper environment (*_024)
```

---

## 8. Rencana Tahapan Pengerjaan (Roadmap)

### Fase 0 — Inisiasi & Audit Lingkungan *(Selesai)*
- [x] Memeriksa isi folder proyek.
- [x] Memverifikasi Windows, WSL 2 Ubuntu, Hadoop 3.5.0, Spark 4.0.4, Java, Python, Docker, RAM, dan ruang disk.
- [x] Mengidentifikasi kebutuhan serta kendala teknis dan mitigasinya.
- [x] Menyusun `PROJECT_BRIEF_024.md`.
- [x] Menyiapkan struktur folder proyek dan menetapkan konvensi penamaan `_024` (`docs/naming_convention_024.md`).

### Fase 1 — Review Referensi & Finalisasi Desain
- [ ] Menempatkan 3 PDF referensi mahasiswa ke folder `references/`.
- [ ] Menganalisis struktur laporan, kedalaman analisis BI, dan pendekatan ML dari referensi.
- [ ] Memfinalisasi rumusan pertanyaan bisnis (BI), definisi target prediksi (mis. `pickup_count_024` per zona per jam), metrik evaluasi ML (mis. RMSE, MAE, R²), serta arsitektur detail NiFi–HDFS–Spark–Streamlit.
- [ ] Inisialisasi repositori Git.

### Fase 2 — Penyiapan & Verifikasi Lingkungan Big Data
- [ ] Memastikan kapasitas RAM & port aman (koordinasi penghentian container Docker proyek lain bila diperlukan).
- [ ] Menguji startup HDFS (`start-dfs.sh`) dan operasi dasar HDFS (`hdfs dfs -ls`) di WSL Ubuntu.
- [ ] Menguji integrasi PySpark membaca/menulis ke HDFS dengan `SPARK_LOCAL_IP=127.0.0.1`.
- [ ] Memilih metode instalasi dan mengonfigurasi **Apache NiFi** (native WSL atau Docker) serta menguji koneksi ingestion ke HDFS.
- [ ] Menyiapkan virtual environment Python (`pip`, `pyspark`, `streamlit`, `plotly`, `pandas`, `pyarrow`).

### Fase 3 — Akuisisi Data Nyata & Ingestion (Apache NiFi → HDFS)
- [ ] Memverifikasi ketersediaan file Yellow Taxi Parquet periode Oktober 2025 – September 2026 di situs resmi NYC TLC beserta `taxi_zone_lookup.csv` dan geometri zona NYC.
- [ ] Menyalin dataset asli ke `data/raw/` dan `data/reference/` di Drive `D:\` (dengan akhiran `_024` tanpa mengubah isi file, serta mencatat provenance di `docs/naming_convention_024.md`).
- [ ] Membangun dan menjalankan flow **Apache NiFi** untuk menyalurkan dataset ke direktori raw HDFS (mis. `/urbanpulse_024/raw/yellow_taxi/` dan `/urbanpulse_024/raw/reference/`).
- [ ] Mendokumentasikan flow NiFi (screenshot & ekspor flow JSON/template `*_024`) dan bukti penyimpanan HDFS untuk laporan.

### Fase 4 — Data Cleaning, Transformasi & Business Intelligence (PySpark)
- [ ] Profiling skema, jumlah baris, nilai kosong, dan anomali pada seluruh 12 bulan data nyata.
- [ ] Pembersihan data (filter timestamp valid sesuai rentang, `passenger_count`, `trip_distance`, `fare_amount`, `total_amount`, `PULocationID`/`DOLocationID` valid — kolom asli tetap dipertahankan, kolom turunan memakai akhiran `_024`).
- [ ] Enrichment dengan `taxi_zone_lookup.csv` (nama zona & borough untuk pickup dan dropoff).
- [ ] Pembuatan tabel agregasi BI (`*_024`) dan penyimpanan hasil teragregasi ke HDFS & serving layer (`data/analytics/`).

### Fase 5 — Pemodelan Machine Learning (Spark MLlib)
- [ ] Agregasi data ke granularity prediksi (mis. `pickup_hour_024` × `PULocationID` → `pickup_count_024`).
- [ ] Feature engineering berbasis waktu dan lokasi (jam, hari dalam minggu, akhir pekan, bulan, fitur lag/rolling demand, atribut zona/borough).
- [ ] Pembagian data berbasis waktu (*time-based split*, bukan random split, agar tidak terjadi *data leakage*).
- [ ] Pelatihan dan perbandingan beberapa model regresi di **Spark MLlib** (mis. Linear Regression / Ridge sebagai baseline, Random Forest Regressor, Gradient-Boosted Trees Regressor).
- [ ] Evaluasi kinerja model secara objektif (RMSE, MAE, R²), analisis error per zona/jam, dan generasi output prediksi mendatang untuk UI.

### Fase 6 — Pengembangan UI Interaktif (Streamlit + Plotly)
- [ ] Membangun halaman **Executive Overview** (KPI utama & tren keseluruhan).
- [ ] Membangun halaman **Spatial-Temporal BI Analytics** (peta interaktif zona NYC, heatmap jam × hari, top koridor & analisis tarif/tip).
- [ ] Membangun halaman **Demand Prediction & Scenario Explorer** (prediksi permintaan berdasarkan zona & waktu, perbandingan aktual vs prediksi, serta rekomendasi penempatan armada).
- [ ] Membangun halaman **Data Architecture & Pipeline Transparency** (ringkasan sumber data, proses cleaning, metrik kualitas data, dan evaluasi model).
- [ ] Uji coba demonstrasi aplikasi secara menyeluruh.

### Fase 7 — Dokumentasi GitHub & Penyusunan Laporan PDF
- [ ] Merapikan kode, menulis `README.md` lengkap (cara setup, arsitektur, screenshot UI/NiFi/HDFS/Spark), dan push ke GitHub.
- [ ] Menyusun laporan akhir PDF sesuai seluruh kriteria tugas: sumber data, alasan pemilihan topik, manajemen data, insight baru berbasis bukti olahan data, dan rekomendasi ke depan.

---

## 9. Keputusan yang Perlu Disepakati di Langkah Berikutnya

Sebelum masuk ke Fase 1 dan Fase 2, hal-hal berikut perlu dikonfirmasi:
1. Penempatan **3 PDF referensi mahasiswa** ke folder `references/` agar dapat segera ditelaah.
2. Pilihan metode menjalankan **Apache NiFi**:
   - **Opsi A (Direkomendasikan untuk hemat RAM & minim konflik Docker):** Instalasi native di WSL Ubuntu (dengan Java 21 terpisah khusus NiFi 2.x, atau NiFi 1.28.x di Java 17 untuk kompatibilitas `PutHDFS` paling mulus).
   - **Opsi B:** Menjalankan NiFi via container Docker (dengan catatan container proyek lain dihentikan sementara agar RAM cukup).
3. Konfirmasi apakah 6 container Docker yang sedang berjalan (`mongo1-3`, `tbdnr-projek-*`) boleh dihentikan sementara pada saat kita nanti menjalankan Hadoop/NiFi/Spark (terutama untuk membebaskan RAM dan port `8088`).
