# Healthcare Lakehouse Project

A comprehensive data lakehouse implementation built on **Databricks** and **AWS S3** for processing and analyzing CMS (Centers for Medicare & Medicaid Services) healthcare claims data.

## 📊 Dataset Overview

| Dataset | Size | Description |
|---------|------|-------------|
| **Provider Drug** | ~3.5 GB | Prescription drug claims by provider |
| **Provider** | ~550 MB | Healthcare provider information |

## 🏗️ Architecture

This project implements a **Medallion Architecture** with three distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐     │
│  │ top_drugs   │  │ top_providers│  │ state_summary       │     │
│  └─────────────┘  └─────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐     │
│  │ provider    │  │ drug        │  │ fact_prescriptions  │     │
│  │ (Dimension) │  │ (Dimension) │  │ (Fact)              │     │
│  └─────────────┘  └─────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                              │
│  ┌─────────────────────┐  ┌─────────────────────────────┐      │
│  │ by_provider         │  │ by_provider_drug            │      │
│  │ (550 MB)            │  │ (3.5 GB)                    │      │
│  └─────────────────────┘  └─────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────────┐
│                      RAW DATA (S3)                               │
│  s3://healthcare-claims-lakehouse/raw/cms/                      │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline Orchestration

![Databricks Workflow](images/health-cms-workflow.png)

**Workflow Performance:**
- **Total Runtime:** 3m 17s (Serverless compute)
- **Tasks:** 4 orchestrated jobs (dim_processing → fact_processing → gold_processing)
- **Compute:** Databricks Serverless with performance optimization
- **Lineage:** 1 upstream table, 3 downstream tables

## 📁 Project Structure

```
healthcare_lakehouse_project/
├── 01_setup/
│   ├── setup-catalog.ipynb      # Create catalog and schemas
│   └── utilities.py             # Shared variables
├── 02_ingestion/
│   ├── 01-bronze_ingest_provider.ipynb   # Ingest provider data
│   └── 02-bronze_ingest_drug.ipynb       # Ingest drug data
├── 03_processing/
│   ├── 01-silver-provider.ipynb          # Process provider silver layer
│   └── 02-silver-drugs.ipynb             # Process drug silver layer
├── 04-insights/
│   └── 01-gold-layer.ipynb               # Create gold aggregations
├── data/
│   ├── provider.csv             # Sample provider data
│   └── provider_drugs.csv       # Sample provider-drug data
├── images/
│   └── health-cms-workflow.png  # Databricks workflow orchestration
├── README.md
└── RESUME_INSIGHTS.md           # Resume bullet points and project insights
```

## 🚀 Pipeline Execution Order

Run the notebooks in the following sequence:

### Phase 1: Setup
1. **`01_setup/setup-catalog.ipynb`**
   - Creates Unity Catalog: `healthcare_lakehouse`
   - Creates schemas: `bronze`, `silver`, `gold`

### Phase 2: Bronze Ingestion
2. **`02_ingestion/01-bronze_ingest_provider.ipynb`**
   - Ingests provider data (~550 MB) from S3
   - Uses Auto Loader with 256MB trigger size
   - Creates table: `healthcare_lakehouse.bronze.by_provider`

3. **`02_ingestion/02-bronze_ingest_drug.ipynb`**
   - Ingests provider-drug data (~3.5 GB) from S3
   - Uses Auto Loader with 128MB trigger size
   - Creates table: `healthcare_lakehouse.bronze.by_provider_drug`

### Phase 3: Silver Processing
4. **`03_processing/01-silver-provider.ipynb`**
   - Cleans and standardizes provider data
   - Creates dimension table: `healthcare_lakehouse.silver.provider`
   - Creates fact table: `healthcare_lakehouse.silver.fact_prescriptions`

5. **`03_processing/02-silver-drugs.ipynb`**
   - Cleans and standardizes drug data
   - Creates dimension table: `healthcare_lakehouse.silver.drug`
   - Creates fact table: `healthcare_lakehouse.silver.by_provider_drug`

### Phase 4: Gold Aggregations
6. **`04-insights/01-gold-layer.ipynb`**
   - Creates aggregated business tables:
     - `healthcare_lakehouse.gold.top_drugs`
     - `healthcare_lakehouse.gold.top_providers`
     - `healthcare_lakehouse.gold.state_summary`

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Databricks** | Unified analytics platform |
| **Apache Spark** | Distributed data processing |
| **Delta Lake** | ACID transactions and data reliability |
| **AWS S3** | Data lake storage |
| **Unity Catalog** | Data governance and catalog management |
| **Auto Loader** | Incremental data ingestion |
| **Databricks Workflows** | Pipeline orchestration and scheduling |

## 📋 Data Modeling

### Silver Layer - Star Schema Design

```
                    ┌─────────────────────┐
                    │  fact_prescriptions │
                    │  - prscrbr_npi      │
                    │  - brnd_name        │
                    │  - gnrc_name        │
                    │  - tot_clms         │
                    │  - tot_drug_cst     │
                    │  - tot_benes        │
                    └─────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
    ┌─────────────────┐       ┌─────────────────┐
    │    provider     │       │      drug       │
    │  - provider_id  │       │   - drug_id     │
    │  - prscrbr_npi  │       │   - gnrc_name   │
    │  - first_name   │       │   - brnd_name   │
    │  - last_org_name│       └─────────────────┘
    │  - city         │
    │  - state        │
    │  - country      │
    └─────────────────┘
```

## 📊 Gold Layer Tables

### 1. top_drugs
| Column | Type | Description |
|--------|------|-------------|
| drug_id | long | Unique drug identifier |
| total_cost | double | Sum of all drug costs |
| total_claims | long | Total number of claims |
| cost_per_claim | double | Average cost per claim |
| gnrc_name | string | Generic drug name |

### 2. top_providers
| Column | Type | Description |
|--------|------|-------------|
| provider_id | long | Unique provider identifier |
| total_cost | double | Total prescription costs |
| total_claims | long | Total claims submitted |
| total_beneficiaries | long | Total unique beneficiaries |
| unique_drugs | long | Count of distinct drugs prescribed |
| cost_per_claim | double | Average cost per claim |
| prscrbr_npi | string | Provider NPI |
| prscrbr_first_name | string | Provider first name |
| prscrbr_last_org_name | string | Provider last/org name |
| prscrbr_city | string | Provider city |
| prscrbr_state_abrvtn | string | Provider state |
| prscrbr_cntry | string | Provider country |

### 3. state_summary
| Column | Type | Description |
|--------|------|-------------|
| prscrbr_state_abrvtn | string | State abbreviation |
| total_cost | double | Total healthcare spending |
| total_claims | long | Total claims in state |
| total_beneficiaries | long | Total beneficiaries |
| unique_drugs | long | Distinct drugs prescribed |
| unique_providers | long | Distinct providers |
| cost_per_claim | double | Average cost per claim |
| cost_per_beneficiary | double | Average cost per beneficiary |
| claims_per_beneficiary | double | Average claims per beneficiary |

## 🔧 Configuration

### Widget Parameters

Each notebook supports runtime configuration via Databricks widgets:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog` | `healthcare_lakehouse` | Unity Catalog name |
| `data_source` | `cms` | Source system identifier |

### S3 Bucket Structure

```
s3://healthcare-claims-lakehouse/
├── raw/
│   └── cms/
│       ├── by_provider/
│       └── by_provider_drug/
└── checkpoints/
    └── cms/
        ├── by_provider/
        │   ├── schema/
        │   └── checkpoint/
        └── by_provider_drug/
            ├── schema/
            └── checkpoint/
```

## ✨ Key Features

- **Auto Loader**: Efficiently processes new files as they arrive in S3
- **Schema Evolution**: Automatically handles new columns
- **Data Lineage**: Tracks file metadata (name, size, timestamp)
- **Change Data Feed**: Enabled on Delta tables for SCD Type 1/2
- **Partitioning**: State-based partitioning for optimized queries
- **Data Quality**: Null handling, type casting, deduplication
- **Derived Metrics**: Cost per claim, cost per beneficiary, etc.

## 📈 Data Transformations

### Bronze → Silver
1. Drop metadata columns
2. Standardize invalid values (NULL, NA, *, empty)
3. Type casting (INT, DOUBLE)
4. Column name standardization (lowercase)
5. Text standardization (UPPERCASE, TRIM)
6. Deduplication
7. Dimensional modeling (Star Schema)

### Silver → Gold
1. Join fact tables with dimensions
2. Aggregate by business entities (drug, provider, state)
3. Calculate derived metrics
4. Create business-ready tables

## 🔍 Sample Queries

```sql
-- Top 10 drugs by total cost
SELECT gnrc_name, total_cost, total_claims, cost_per_claim
FROM healthcare_lakehouse.gold.top_drugs
ORDER BY total_cost DESC
LIMIT 10;

-- Top 10 providers by total cost
SELECT prscrbr_first_name, prscrbr_last_org_name, 
       prscrbr_city, prscrbr_state_abrvtn,
       total_cost, total_claims, unique_drugs
FROM healthcare_lakehouse.gold.top_providers
ORDER BY total_cost DESC
LIMIT 10;

-- State healthcare spending summary
SELECT prscrbr_state_abrvtn, total_cost, total_claims,
       cost_per_claim, cost_per_beneficiary
FROM healthcare_lakehouse.gold.state_summary
ORDER BY total_cost DESC;
```

## 📝 Notes

- Notebooks are in `.ipynb` format
- Sample data files are included in the `data/` directory for local testing
- Full CMS dataset available at [data.cms.gov](https://data.cms.gov) (119M+ records, $18B+ spending)
- Checkpoints should be cleaned before re-running ingestion notebooks
- Delta Change Data Feed is enabled for audit and SCD scenarios
- See `RESUME_INSIGHTS.md` for resume bullet points and project insights

## 👨‍💻 Author

Built on Databricks with AWS S3 for CMS Healthcare Claims data processing.
