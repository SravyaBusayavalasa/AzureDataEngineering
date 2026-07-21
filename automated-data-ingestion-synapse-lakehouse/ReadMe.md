# Azure Synapse REST API Ingestion & Taxi Data Analytics

This repository contains a unified, enterprise-grade cloud data engineering ecosystem developed within **Azure Synapse Analytics**. The project is split into two advanced, production-scale analytical workstreams:

1. **Metadata-Driven Application Ingestion Framework**: A fully parametric pipeline that dynamically fetches semi-structured data from multiple REST API endpoints (`posts`, `comments`, `users`, `albums`, `todos`, `photos`) utilizing an inline JSON configuration matrix. Complex parent-child array hierarchies are evaluated at runtime and flattened using Azure Data Flows before being committed to an Azure SQL Database.
2. **NYC Yellow Taxi Delta Lakehouse Architecture**: A highly scalable big data pipeline that extracts monthly columnar files from an HTTP source, normalizes and partitions the records using an Apache Spark compute layer into ACID-compliant Delta Lake formats, and utilizes cost-optimized Serverless SQL views to serve a dynamic, rolling 2-month data frame straight to Power BI.

## Project Structure

```text
automated-api-ingestion-synapse-lakehouse/
│
├── 📁 pipelines/                       # Data Factory / Synapse Orchestration Workflows
│   ├── 📁 ingestion-api/               # REST API pipelines
│   ├── 📁 dataflows/                   # Chained JSON array flattening logic
│   └── 📁 ingestion-nyc_taxi_data/      # NYC taxi data ingestion pipelines
│
├── 📁 metadata/                        # JSON metadata array with relative URLs and configurations
│
├── 📁 datasets_json/                   # Dataset definitions
│
├── 📁 scripts/                         # Source Code & Query Compute Layers
│   ├── 📁 sql/                         # Synapse Serverless T-SQL Assets & Views
│   │   ├── DDL.sql                     # Table creation scripts to map flattened JSON
│   │   └── nyc_serverless_datasetup.sql # External datasources, views, credentials, and serverless setup
│   │
│   └── 📁 python/                      # PySpark Notebooks for Delta Lake execution
│       └── taxi_delta_processing.py
│
├── 📁 reports/                         # Semantic Visualization Layers
│   └── YellowtaxiReporting.pbix
│
├── 📁 docs/                            # Project documentation, architectural canvases & layouts
│   ├── 📁 images/                      # Architecture diagrams
│   ├── 📁 reports/                     # Report screenshots and visualization assets
│   └── 📁 ProjectDocumentation/        # Detailed project documentation pages
│
└── README.md                           # Main portfolio presentation page
```

## Tech Stack & Azure Core Services

- **Orchestration & Ingestion**: Azure Synapse Pipelines (Parameterized REST & HTTP Connectors)
- **Big Data Compute Layer**: Apache Spark Runtime (PySpark Notebooks for partition optimization)
- **Transformation Engine**: Synapse Data Flows (Complex conditional JSON parsing matrices)
- **Storage Tier**: Azure Data Lake Storage Gen2 (ADLS Gen2) wrapped in a Delta Lake transaction ecosystem
- **Modern Serving Layer**: Synapse Serverless SQL Pools (Metadata-driven `OPENROWSET` parsing)
- **Business Intelligence**: Power BI Desktop (Direct Query / Import semantic analytics modeling)

## Key Engineering Implementations

### 1. Hardcoded Metadata-Driven Ingestion with Structural Route Splitting

To avoid hardcoding independent activities for similar application interfaces, a single generic pipeline loops over a static metadata JSON array parameter containing pre-defined ingestion, destination, and nested structural mapping properties:

```json
/* Ingestion Metadata Parameter Control */
@json('[
  { "RelativeURL": "posts", "TargetFile": "posts", "TargetTable": "Gold_Social_Posts", "IsNested": false },
  { "RelativeURL": "comments", "TargetFile": "comments", "TargetTable": "Gold_Social_Comments", "IsNested": false },
  { "RelativeURL": "users", "TargetFile": "users", "TargetTable": "Gold_Social_Users", "IsNested": true },
  { "RelativeURL": "albums", "TargetFile": "albums", "TargetTable": "Gold_Social_Albums", "IsNested": false },
  { "RelativeURL": "todos", "TargetFile": "todos", "TargetTable": "Gold_Social_Todos", "IsNested": false },
  { "RelativeURL": "photos", "TargetFile": "photos", "TargetTable": "Gold_Social_Photos", "IsNested": false }
]')
```

- **Control Flow Execution**: A `ForEach` loop reads this collection, dynamically evaluating attributes like `@item().RelativeURL` for source connectivity, and routes files directly to ADLS Gen2 landing zones.
- **Conditional Transformation Routing**: Downstream, an `If Activity` checks the boolean status of `@item().IsNested`. If false, it executes a highly performant, straight `Copy Activity`. If true (as required for the complex `users` object), it passes the payload into an Azure Data Flow where chained Flatten nodes parse independent child structures into relational columns.

### 2. Scalable PySpark Delta Conversion & Temporal Partitioning

For the high-volume NYC Yellow Taxi workflow, monthly files are dynamically ingested from an HTTP endpoint using relative URL generation string formatting (`yellow_tripdata_2026-@{item()}.parquet`).

Once written to the data lake, an Apache Spark Notebook processes the raw Parquet records, appends dynamic calendar keys, and outputs them into optimized Delta Lake structures partitioned cleanly by year and month. This layout enforces strict ACID compliance and allows the query engine to completely bypass irrelevant files during full dataset scanning runs.

### 3. Cost-Optimized Serverless Database Views

Synapse Serverless SQL query charges are based on the volume of data scanned ($/TB). To optimize cost controls:

- The semantic layer utilizes an advanced T-SQL View backed by a Common Table Expression (CTE) and a `CROSS JOIN`.
- Instead of running costly, repetitive full-table scans to find date parameters, the view evaluates `MAX(tpep_pickup_datetime)` dynamically against the Delta Lake transaction logs.
- This forces the Serverless engine to execute metadata pruning, skipping older raw files entirely and scanning only the folders containing data for the rolling 2-month threshold.

## Core Business Insights Extracted (Power BI Layer)

The finalized dashboard model targets exactly the trailing 60 days of enterprise fleet data, exposing:

- **Revenue Trends vs. Ride Volume Velocity**: Compares `SUM(fare_amount)` alongside a row-level `COUNT(*)` to identify whether monthly variations are driven by customer demand spikes or pricing adjustments.
- **Vendor Market Share Distribution**: Isolates ride volume and financial velocity metrics across distinct competing vendor groups.
- **Anomalous Log Detection**: Captures extreme operational outliers using `MAX(DATEDIFF(...))` calculations to isolate logging errors or extreme trip exceptions.
- **Demand Hotspots**: Isolates the Top 5 busiest pickup and drop-off zones using automated horizontal Top-N ranking filters.
- **Fleet Distance & Temporal Efficiency Yields**: Tracks profitability metrics across two dimensions—measuring financial yield generated per mile traveled (`Yield_Per_Mile`) alongside time utilization density (`Yield_Per_Minute`) to capture true congestion bottlenecks.

## Deployment & Runbook Guide

### Step 1: Establish Workspace Git Integration
Link your active Azure Synapse Workspace directly to this GitHub repository branch via the **Manage** -> **Git Configuration** panel in your Synapse Studio UI for native version control.

### Step 2: Configure Storage Security & Access Control
Because Synapse Serverless SQL queries execute via the workspace's system identity rather than your individual login, you must grant permissions to the storage account. Navigate to your Azure Storage Account (ADLS Gen2), select **Access Control (IAM)**, and assign the **Storage Blob Data Reader** role explicitly to your Synapse Workspace Managed Identity Name.

### Step 3: Execute Database Setup Scripts
Connect to your serverless on-demand endpoint and run your database preparation scripts in sequence:

1. Run `DDL.sql` to generate tables to map `users`, `todos`, `comments`, `posts`, `albums`, `photos`, & `orders`.
2. Run `nyc_serverless_datasetup.sql` for External datasource, views, credentials, and serverless setup.

### Step 4: Map Visualization Parameters
Open `reports/YellowTaxiReporting.pbix` in Power BI Desktop, edit the data source connection configurations to point to your specific on-demand workspace endpoint URL, and run an initial data import refresh to populate the visualization canvas.
