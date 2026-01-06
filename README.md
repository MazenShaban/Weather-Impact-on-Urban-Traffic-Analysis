# 🚗 Urban Traffic Risk Analytics (Big Data Pipeline)

An end-to-end Big Data project that ingests, cleans, stores, and analyzes urban traffic and weather data to predict congestion risks. This pipeline leverages a modern Data Lake architecture using **MinIO (Object Store)** and **Hadoop HDFS**, performs advanced statistical modeling (**Monte Carlo Simulations** & **Factor Analysis**), and visualizes insights via interactive **Streamlit Dashboards**.

---

## 🏗️ Architecture

The project follows a multi-layered "Medallion" architecture:

1.  **Bronze Layer (Raw Ingestion):** Python scripts generate/ingest raw CSV data into MinIO.
2.  **Silver Layer (Cleaning):** Jupyter Notebooks clean and convert data to **Parquet** format, stored in MinIO.
3.  **Distributed Storage (HDFS):** Cleaned data is replicated from MinIO to a Hadoop HDFS cluster for distributed accessibility.
4.  **Gold Layer (Analytics Ready):** Data is merged and aggregated.
5.  **Analytics Engine:**
    * **Monte Carlo Simulation:** Estimates the probability of traffic jams under specific weather conditions ($N=5000$ runs).
    * **Factor Analysis:** Uses Factor Analysis (Kaiser Method, Scree Plots) to identify latent variables driving traffic stress.
6.  **Presentation Layer:** Two Streamlit dashboards (Static Reports & Interactive Analytics).

---

## 🛠️ Tech Stack

* **Infrastructure:** Docker, Docker Compose
* **Storage:** MinIO (S3 Compatible), Hadoop HDFS (NameNode/DataNode)
* **Processing:** Python 3.14, Pandas, PyArrow
* **Analytics:** NumPy, Scikit-Learn, FactorAnalyzer, SciPy
* **Visualization:** Matplotlib, Seaborn, Streamlit

---

## 📂 Project Structure

```text
├── docker-compose.yml       # Infrastructure definition (MinIO + Hadoop)
├── hadoop.env               # HDFS Environment variables
├── scripts/                 # ETL & Analysis Scripts
│   ├── weather_raw.py       # Phase 1: Data Generation
│   ├── traffic_raw.py
│   ├── ingest_to_hdfs.py    # Phase 3: Bridge MinIO -> HDFS
│   ├── merge_datasets.py    # Phase 4: Silver -> Gold Merge
│   ├── monte_carlo.py       # Phase 5: Predictive Simulation
│   ├── factor_analysis.py   # Phase 6: Dimensionality Reduction
│   ├── dashboard_v1.py      # Phase 7: Static Reporting Dashboard
│   └── dashboard_v2.py      # Phase 7: Interactive Analysis Dashboard
├── notebooks/               # Phase 2: Cleaning (Jupyter)
│   ├── 01_clean_weather.ipynb
│   └── 02_clean_traffic.ipynb
└── data/                    # Local staging (optional)
