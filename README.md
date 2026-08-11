# 📊 NGO Data Engineering Pipeline (Databricks & Medallion Architecture)

## 📌 Project Overview
This project delivers an end-to-end data pipeline built on **Databricks** using **PySpark** and **Delta Lake**. The pipeline processes raw NGO (Non-Governmental Organization) operational datasets—including beneficiaries, case management logs, and services rendered—and transforms them following the industry-standard **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🏗️ Architecture & Workflow Structure

     [ Raw CSV Files ]
             │
             ▼
  🥉 01_bronze (Ingestion)

  Raw Delta Storage

Metadata Lineage (_ingested_at, _source_file)
              │
              ▼
🥈 02_silver (Cleansing & Quality Checks)

Type Casting & Null Filtering

Window-based Deduplication

Automated Quality Assertions (gold_quality_summary)
                      │
                      ▼
🥇 03_gold (Analytics & KPIs)

Daily Service KPIs (gold_daily_services)

Beneficiary 360 View (gold_case_360)

Interactive Visualizations

---

## 📁 Repository Structure

```text
.
├── 00_config.ipynb          # Catalog/Schema setup & environment paths
├── 01_bronze.ipynb          # Ingestion layer with metadata enrichment
├── 02_silver.ipynb          # Quality enforcement & deduplication layer
├── 03_gold.ipynb            # Aggregated analytics & visualization layer
└── README.md                # Technical documentation & project answers



##⚙️ Databricks Workflow OrchestrationThe pipeline is fully orchestrated using Databricks Workflows (Jobs) under the job named NGO_Pipeline_Orchestration.
🔄 Execution Order & Dependencies:$$\text{00\_config} \longrightarrow \text{01\_bronze} \longrightarrow \text{02\_silver} \longrightarrow \text{03\_gold}$$🧪 Automated Data Quality Assertions (gold_quality_summary)Data quality is programmatically checked in the Silver transformation phase and stored in gold_quality_summary:Check NameTarget TableValidation Logicnull_case_idssilver_casesVerifies case_id IS NOT NULLduplicate_casessilver_casesValidates primary key uniquenessnull_service_idssilver_servicesVerifies service_id IS NOT NULLduplicate_servicessilver_servicesValidates service primary key uniqueness
❓ Technical Evaluation Q&A
1. Why did you choose each Bronze, Silver, and Gold transformation?
Bronze: Preserved raw schema without modification (inferSchema=False) to guarantee zero data loss, while adding lineage columns (_ingested_at, _source_file) for operational traceability.

Silver: Enforced structural integrity by casting date types, filtering out records missing primary keys (case_id, service_id), and stripping out duplicates via PySpark Window functions over ingestion timestamps.

Gold: Aggregated verified Silver records into business-ready KPIs (gold_daily_services, gold_case_360) optimized for operational reporting and dashboarding.

2. Which rows were rejected or removed, and how can they be traced?
Rows missing valid primary keys (NULL values) and historical duplicates were filtered out during Silver processing.

Filtered metrics are logged automatically into the gold_quality_summary table, recording failure counts per check along with execution timestamps for full auditability.

3. How does your pipeline prevent duplicate records when it is rerun?
Deduplication is handled via Window.partitionBy("primary_key").orderBy(col("_ingested_at").desc()) using row_number() == 1 to ensure only the latest record is retained.

Overwrite modes (.mode("overwrite")) on managed Delta tables ensure full pipeline idempotency upon re-execution.

4. Which metric definitions could be misunderstood by a business user?
unique_cases_served: Users might confuse total service delivery events (total_services) with unique beneficiary individuals/cases served.

service_date: Derived strictly from actual_date (actual service delivery date) rather than expected_date (scheduled date).

5. What would change if the source files arrived every hour instead of once?
The current batch .mode("overwrite") model would transition to continuous/streaming ingestion using Databricks Auto Loader (spark.readStream) combined with Delta Lake MERGE INTO (Upserts) for stateful streaming.

6. What is the largest data-quality risk in your solution?
Referential Integrity Violation: Service records (services) referencing a case_id that does not exist in the primary beneficiary registry (cases).

7. How would you monitor the pipeline in a production environment?
Configure real-time alerts (Slack/Email) on Job failure within Databricks Workflows.

Set automated alerting queries on gold_quality_summary status logs if any status switches to FAILED.

Monitor compute execution SLAs using Databricks System Tables (system.lakeflow.jobs).





