**🚖 NYC Taxi Data Engineering**  

 **🛠️ End-to-End ETL + Warehouse + Analytics Pipeline**

Build a production-style data engineering workflow to ingest, clean, transform, warehouse, and analyze millions of NYC taxi trip records using Python, Pandas, Parquet, and DuckDB.

**⭐ Project Summary**

This repository demonstrates a complete data engineering solution for the New York City taxi dataset — optimized for large-scale processing, data quality, and analytical insights.

## 📌 Why This Project Matters

This pipeline showcases real data engineering skills including:

    ✨ Scalable batch ingestion  
    ✨ Schema drift handling  
    ✨ Analytics-ready data warehouse modeling  
    ✨ Clean ETL architecture  
    ✨ SQL-based KPI generation  
    ✨ Modular, resume-ready code structure

**🏗️ High-Level Architecture**

📂 Raw CSV Files (Millions of Rows)  
↓  
📦 Chunked Ingestion (Pandas)  
↓  
📌 Data Cleaning + Normalization  
↓  
🗃️ Parquet Data Lake (Optimized Storage)  
↓  
🧠 DuckDB Warehouse (Fast SQL Analytics)
↓  
📊Queries  

**🧠 Core Features**

✔️ Chunked data ingestion (handles large files)  
✔️ Data quality checks & cleaning  
✔️ Parquet storage for efficient reloading  
✔️ DuckDB analytic warehouse for SQL queries  
✔️ Modular codebase with clear separation of concerns  
✔️ Resilient to schema changes  
✔️ Analytical KPI outputs and dashboards

**📁 Repository Structure**
```text
📦 nyc-taxi-data-engineering
┣ 📂 src/ # Modular ETL + analytics code
┣ 📄 requirements.txt # Dependencies
┣ 📄 README.md # Project overview
┗ 📄 sample_data/ # Sample taxi data for testing
