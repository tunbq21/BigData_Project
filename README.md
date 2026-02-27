# 💠 Azure Big Data Pipeline (Medallion Architecture)

> **Architected a scalable end-to-end ETL pipeline transforming multi-source raw data into business-ready datasets using Microsoft Azure.**

---

## 🛠 Tech Stack Overview

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestration** | **Azure Data Factory (ADF)** | Automated ingestion & validation |
| **Storage** | **ADLS Gen2 & Delta Lake** | Scalable Data Lakehouse |
| **Compute** | **Azure Databricks (PySpark)** | Distributed ETL & Schema enforcement |
| **Warehouse** | **Azure Synapse Analytics** | SQL-based aggregation for BI |

---

## 🏗 Medallion Architecture Implementation

### 🥉 Bronze Layer (Ingestion)
* **Objective:** Automated data movement from multi-source raw systems.
* **Implementation:** Orchestrated ingestion with **Azure Data Factory**.
* **Key Feature:** Implemented **Data Validation Checks** before landing records to ensure raw data integrity.

### 🥈 Silver Layer (Transformation)
* **Objective:** Cleaned, filtered, and augmented data for downstream use.
* **Implementation:** Utilized **PySpark on Databricks** for complex transformations.
* **Key Feature:** Enforced **Strict Schemas** and deduplication, significantly improving data quality via Delta Lake.

### 🥇 Gold Layer (Aggregated)
* **Objective:** Business-level aggregates and KPIs for high-performance reporting.
* **Implementation:** Developed SQL-based logic in **Azure Synapse Analytics**.
* **Key Feature:** Derived **Key Business Metrics**, enabling lightning-fast BI dashboard consumption.

---

## 🌟 Key Engineering Highlights

* ⚙️ **Scalability:** Architected a pipeline capable of handling high-volume, multi-source raw data.
* 🛡️ **Reliability:** Leveraged Delta Lake’s ACID transactions to ensure 100% data consistency.
* 📊 **Performance:** Optimized SQL aggregation logic to reduce BI query latency.

---

## 📂 Project Structure

```bash
├── 📁 adf-pipelines/         # Azure Data Factory JSON & Linked Services
├── 📁 databricks-notebooks/  # PySpark ETL (Bronze-to-Silver-to-Gold)
├── 📁 synapse-scripts/       # SQL Views & Business Aggregations
└── 📁 documentation/         # Architecture diagrams & Metadata
