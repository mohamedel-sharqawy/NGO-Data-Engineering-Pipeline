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



# ⚙️ Databricks Workflow Orchestration

The pipeline is fully orchestrated using **Databricks Workflows (Jobs)** under the job named `NGO_Pipeline_Orchestration`.

## 🔄 Execution Order & Dependencies

```text
00_config → 01_bronze → 02_silver → 03_gold
```

## 🧪 Automated Data Quality Assertions

Data quality is programmatically checked during the **Silver transformation phase** and stored in the `gold_quality_summary` table.

| Check Name           | Target Table      | Validation Logic                         |
| -------------------- | ----------------- | ---------------------------------------- |
| `null_case_ids`      | `silver_cases`    | Verifies `case_id IS NOT NULL`           |
| `duplicate_cases`    | `silver_cases`    | Validates primary key uniqueness         |
| `null_service_id`    | `silver_services` | Verifies `service_id IS NOT NULL`        |
| `duplicate_services` | `silver_services` | Validates service primary key uniqueness |

---

# ❓ Technical Evaluation Q&A

## 1. Why did you choose each Bronze, Silver, and Gold transformation?

### 🥉 Bronze

Preserved the raw source schema without modification using `inferSchema=False` to guarantee zero data loss, while adding lineage columns:

* `_ingested_at`
* `_source_file`

These columns provide operational traceability and allow each record to be traced back to its source and ingestion time.

### 🥈 Silver

Enforced structural and data integrity by:

* Casting date columns to the appropriate data types.
* Filtering out records missing primary keys (`case_id`, `service_id`).
* Removing duplicate records using PySpark `Window` functions ordered by ingestion timestamp.

### 🥇 Gold

Aggregated verified Silver records into business-ready KPIs, including:

* `gold_daily_services`
* `gold_case_360`

These Gold tables are optimized for operational reporting and dashboarding.

---

## 2. Which rows were rejected or removed, and how can they be traced?

Rows that were rejected during Silver processing include:

* Records with missing or invalid primary keys (`NULL` values).
* Historical duplicate records.

Filtered metrics are logged automatically into the `gold_quality_summary` table, recording failure counts for each data-quality check along with execution timestamps for full auditability.

---

## 3. How does your pipeline prevent duplicate records when it is rerun?

Deduplication is handled using a PySpark Window function:

```python
Window.partitionBy("primary_key") \
    .orderBy(col("_ingested_at").desc())
```

The latest record is retained using:

```python
row_number() == 1
```

This ensures that only the most recent version of each record is kept.

Additionally, `.mode("overwrite")` is used for managed Delta tables, ensuring full pipeline idempotency when the pipeline is re-executed.

---

## 4. Which metric definitions could be misunderstood by a business user?

### `unique_cases_served`

Users might confuse the number of **unique cases served** with the total number of service delivery events (`total_services`).

* `total_services` = Total service delivery events.
* `unique_cases_served` = Number of distinct beneficiary cases that received services.

### `service_date`

`service_date` is derived strictly from `actual_date`, representing the **actual service delivery date**, rather than `expected_date`, which represents the scheduled date.

---

## 5. What would change if the source files arrived every hour instead of once?

The current batch-based `.mode("overwrite")` approach would transition toward incremental or streaming ingestion.

The pipeline could use:

* **Databricks Auto Loader**
* `spark.readStream`
* Delta Lake `MERGE INTO` for upserts

This would allow the pipeline to continuously process newly arriving files while maintaining state and handling incremental updates.

---

## 6. What is the largest data-quality risk in your solution?

The largest data-quality risk is **referential integrity violation**.

For example, a record in the `services` table may reference a `case_id` that does not exist in the primary beneficiary registry (`cases`).

This could lead to orphan service records and inaccurate business KPIs.

A future improvement would be to implement explicit referential integrity checks between `silver_services` and `silver_cases`.

---

## 7. How would you monitor the pipeline in a production environment?

### 🔔 Job Failure Alerts

Configure real-time **Slack/Email alerts** for Databricks Workflow failures.

### 🧪 Data Quality Monitoring

Set up automated alerting based on the status logs stored in `gold_quality_summary`.

For example, trigger an alert whenever a data-quality check changes to:

```text
FAILED
```

### ⏱️ Pipeline SLA Monitoring

Monitor compute execution time and pipeline SLAs using **Databricks System Tables**, such as:

```text
system.lakeflow.jobs
```

This would help identify job failures, performance degradation, and SLA violations.

---

## 📌 Summary

The pipeline follows a structured **Bronze → Silver → Gold** architecture with:

* Raw data preservation and lineage tracking in Bronze.
* Data cleaning, validation, and deduplication in Silver.
* Business-ready KPIs and reporting tables in Gold.
* Automated data-quality assertions.
* Workflow-based orchestration and dependency management.
* Idempotent pipeline execution.
* Production monitoring and alerting capabilities.
