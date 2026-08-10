# Databricks Banking Analytics Platform

An enterprise-grade Databricks data engineering solution implementing an end-to-end Medallion Architecture (Bronze, Silver, Gold) for banking operations, customer risk modeling, transaction processing, and executive reporting.

## 🏛 Architecture Overview

```
+------------------------+      +-------------------------+      +-------------------------+      +-------------------------+
|     Source Layer       | ---> |      Bronze Layer       | ---> |      Silver Layer       | ---> |       Gold Layer        |
| (SQL Server & Blobs)   |      | (Raw Ingestion / Delta) |      | (Cleansed / Conformed)  |      | (Aggregated / Business) |
+------------------------+      +-------------------------+      +-------------------------+      +-------------------------+
            |                                                                                                  |
            v                                                                                                  v
+------------------------+                                                                          +-------------------------+
|    Control Metadata    |                                                                          |   Lakeview Dashboard    |
| (pipeline_metadata)    |                                                                          | (NeoBank KPI Metrics)   |
+------------------------+                                                                          +-------------------------+
```

## 📁 Directory Structure

```
banking-project/
├── 00_Source_Files/
│   ├── 01_SQL_Server/             # SQL Server DDL and seed scripts
│   │   ├── 01_Create_Tables.sql   # Relational tables (Customers, Accounts, Transactions, etc.)
│   │   ├── 02_Insert_Historical_data.sql # Initial historical load data
│   │   └── 03_Incrementat_data.sql       # Incremental transaction & update feeds
│   └── 02_Blob/                   # Raw CSV landing zone feeds
│       ├── credit_bureau_reports_1.csv / 2_incremental.csv
│       └── payment_gateway_logs_1.csv / 2_incremental.csv
├── 01_Setup_Metadata/
│   ├── 01_Setup_Metadata.sql      # Pipeline metadata table definition and table configs
│   └── 02_Check_Metadata.sql      # Diagnostic & watermark monitoring queries
├── 02_Source_to_Silver/
│   ├── 00_Setup_Secret_Scope.py   # Databricks Secret Scope & Key Vault credentials
│   ├── 01_Read_Tables_List.py     # Metadata reader utility for active source tables
│   ├── 02_Read_Table_Parameters.py# Dynamic notebook execution parameter parser
│   ├── 03_Source_to_Bronze.py     # Ingestion engine (JDBC/Blob -> Delta Bronze)
│   └── 04_Bronze_to_Silver.py     # Transformation engine (Bronze -> Silver SCD1/SCD2)
├── 03_Silver_to_Gold/
│   ├── 01_Silver_to_Gold_Driver.py# Orchestrator for Gold layer transformations
│   └── gold_transformations/      # Business domain aggregation modules
│       ├── branch_performance.py  # Branch financial metrics & deposit volumes
│       ├── customer_360.py        # Customer 360 balance & risk summary
│       ├── daily_bank_kpi.py      # Bank-wide daily KPI metrics
│       ├── risk_customer_summary.py# High-risk account & credit default flags
│       └── transaction_channel_summary.py # Channel distribution (Mobile, ATM, Branch, Online)
├── 04_Email_Notification/
│   └── 01_Send_Email.py           # Automated pipeline run status and failure alerts
├── 05_Dashboard/
│   └── NeoBank_Dashboard.lvdash.json # Databricks Lakeview Dashboard spec
└── README.md
```

## ⚙️ Key Technical Features

### 1. Metadata-Driven Ingestion
The pipeline utilizes a `pipeline_metadata` table to dynamically control table processing:
- Source connection properties (Schema, Table, Source System)
- Ingestion mode (`Full` vs `Incremental`)
- High-watermark tracking (`Watermark_Column`, `Watermark_Value`)
- Target Delta Lake paths and catalog locations

### 2. Medallion Data Pipeline
- **Bronze Layer**: Preserves raw schema from SQL Server JDBC connections and Azure Blob CSV feeds without mutation.
- **Silver Layer**: Enforces strict schema, performs type casting, standardizes null handling, deduplicates records, and applies Delta MERGE operations.
- **Gold Layer**: Curates business-level aggregations tailored for BI tools, analytics, and operational dashboards.

### 3. Executive Dashboards & Alerting
- Integrated **Databricks Lakeview Dashboard** JSON (`NeoBank_Dashboard.lvdash.json`) displaying key banking metrics (Daily Active Accounts, High-Value Transaction Flags, Loan Defaults, Channel Distribution).
- SMTP notification module (`01_Send_Email.py`) providing automated execution reporting.

## 🚀 Execution Guide

1. **Metadata & Secret Setup**:
   - Run `02_Source_to_Silver/00_Setup_Secret_Scope.py` to configure Key Vault / Secret Scopes.
   - Execute `01_Setup_Metadata/01_Setup_Metadata.sql` to initialize metadata tracking tables.

2. **Ingestion Execution (Source to Silver)**:
   - Run `02_Source_to_Silver/03_Source_to_Bronze.py` to ingest raw landing data.
   - Run `02_Source_to_Silver/04_Bronze_to_Silver.py` to transform and MERGE into Delta Silver tables.

3. **Gold Aggregations**:
   - Run `03_Silver_to_Gold/01_Silver_to_Gold_Driver.py` to compute domain metrics.

4. **Dashboard Deployment**:
   - Import `05_Dashboard/NeoBank_Dashboard.lvdash.json` into Databricks Dashboards.
