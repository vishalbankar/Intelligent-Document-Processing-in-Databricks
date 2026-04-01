# Intelligent-Document-Processing-in-Databricks
Every day SafeGuard receives hundreds of claim forms as PDFs — auto accidents, hospital bills, property damage reports.

# Problems it faces
- With 500+ claims monthly across branches, there's always a backlog
- Financial Risk and
- Management Has No Real-Time Data Visibility

> **Automated extraction, validation, and analytics of insurance claim PDFs using Databricks `ai_parse_document`, PySpark, Delta Lake, and Genie.**

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Solution Architecture](#-solution-architecture)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Dataset](#-dataset)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Gold Table Schema](#-gold-table-schema)
- [Validation Logic](#-validation-logic)
- [Genie — Natural Language Analytics](#-genie--natural-language-analytics)
- [Project Structure](#-project-structure)
- [Setup & How to Run](#-setup--how-to-run)
- [Databricks Free Edition — Constraints & Workarounds](#-databricks-free-edition--constraints--workarounds)
- [Screenshots](#-screenshots)
- [What I Learned](#-what-i-learned)
- [Connect](#-connect)

## 📖 Project Overview

This project builds an end-to-end **Intelligent Document Processing (IDP)** pipeline that automatically extracts structured financial data from unstructured insurance claim PDF forms.

The pipeline handles three claim types — **Auto Accident**, **Health/Medical**, and **Property Damage** — each with a different document layout, different field names, and different financial structure. Extracted data is validated mathematically and stored in a **Gold Delta table** connected to **Genie** for natural language analytics.

> **"80% of enterprise data exists in unstructured formats. This pipeline solves the problem of data trapped in documents — at scale, automatically, with built-in validation."**

---

## 🚨 Business Problem

An insurance company receives **500+ PDF claim forms every month** across three claim types. The current process:

```
PDF arrives → Adjuster manually opens it → Reads it → Types data into Excel
           → Manager runs weekly report → 7-10 day backlog
```

**Three critical problems:**

| Problem | Business Impact |
|---|---|
| **Speed** — manual extraction is slow | 7-10 day processing backlog. Customer churn. |
| **Accuracy** — human transcription errors | One wrong digit: Rs. 3,37,000 paid instead of Rs. 33,70,000 |
| **Visibility** — no real-time data | Management waits for a weekly Excel report |

**This pipeline eliminates all three problems.**

---

## 🏗️ Architecture

<img width="912" height="1952" alt="arhitecture_dia" src="https://github.com/user-attachments/assets/6ff60c48-b5da-4b4a-92b5-ba88ebbbfd43" />
```



┌─────────────────────────────────────────────────────────────────┐
│                    SOURCE — PDF Claim Forms                     │
│         Auto Accident  │  Health/Medical  │  Property Damage    │
└────────────────────────┬────────────────────────────────────────┘
                         │ Upload
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│         UNITY CATALOG — Managed Volume (Landing Zone)           │
│      /Volumes/insurance/landing/raw_invoices/  (50 PDFs)        │
└────────────────────────┬────────────────────────────────────────┘
                         │ spark.read.format("binaryFile")
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION — raw_df                             │
│             path │ content (binary) │ length │ modificationTime │
└────────────────────┬────────────────────────────────────────────┘
                     │ ai_parse_document(content)
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           PARSING — parsed_doc (VARIANT)                        │
│   OCR + Semantic Layout Understanding → elements array          │
└────────────────────┬────────────────────────────────────────────┘
                     │ variant_get → from_json → explode
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           ELEMENTS — elements_df                                │
│   path │ element_id │ element_type │ content │ page_id │ bbox   │
│   ~600 rows (one per document element across all 50 PDFs)       │
└────────┬───────────────────────────┬──────────────────┬─────────┘
         │ AUTO filter               │ HLTH filter      │ PROP filter
         ▼                           ▼                  ▼
    auto_lines                 health_lines         prop_lines
    (18 rows)                  (16 rows)            (16 rows)
         │                           │                  │
         └───────────┬───────────────┘                  │
                     │    unionByName(allowMissingColumns=True)
                     ▼                  
         claims_combined (50 rows — all fields)
                     │   Numeric conversion + Validation
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              GOLD TABLE — insurance.gold.claims_gold             │
│         50 rows │ 20 columns │ Delta format │ Validated          │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
              GENIE — Natural Language Analytics
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| **Platform** | Databricks Free Edition | Serverless compute, notebooks, jobs |
| **Storage** | Unity Catalog Managed Volume | Raw PDF file storage — replaces S3 |
| **Compute** | Apache Spark (PySpark) | Distributed PDF processing |
| **Core Function** | `ai_parse_document` | AI-powered PDF parsing and OCR |
| **Data Format** | Delta Lake | ACID-compliant Gold table storage |
| **Governance** | Unity Catalog | Catalog, schema, table management |
| **Analytics** | Genie | Natural language SQL querying |
| **Language** | Python (PySpark) + SQL | Pipeline code |

---

## 🔄 Pipeline Walkthrough

### Step 1 — Setup_IDP notebook [notebook](setup_IDP.ipynb)
### Step 2 — invoce_ai_document_parser notebook [notebook](invoice_ai_document_parser.ipynb)

---

## 📊 Gold Table Schema

**Table:** `insurance.gold.claims_gold`

| Column | Type | Description | Applies To |
|---|---|---|---|
| `path` | STRING | Full Volume path of source PDF | All |
| `file_name` | STRING | PDF filename only | All |
| `claim_type` | STRING | AUTO / HLTH / PROP | All |
| `claim_type_full` | STRING | Auto Accident / Health / Property Damage | All |
| `claim_status` | STRING | PENDING / APPROVED / SETTLED / REJECTED etc. | All |
| `total_claim_amount` | DOUBLE | Extracted total from PDF | All |
| `calculated_total` | DOUBLE | Sum of line items (computed) | All |
| `diff` | DOUBLE | Absolute difference between extracted and calculated | All |
| `is_consistent` | BOOLEAN | True if diff ≤ Rs. 1.00 | All |
| `status` | STRING | OK / MISSING_TOTAL / TOTAL_MISMATCH / EXTRACTION_FAILED | All |
| `vehicle_repair` | DOUBLE | Vehicle body repair cost | AUTO only |
| `spare_parts` | DOUBLE | Spare parts replacement | AUTO only |
| `towing_charges` | DOUBLE | Towing and recovery | AUTO only |
| `survey_fee` | DOUBLE | Surveyor assessment fee | AUTO only |
| `room_charges` | DOUBLE | Hospital room charges | HLTH only |
| `ot_fee` | DOUBLE | Operation theatre and surgeon fee | HLTH only |
| `anaesthesia` | DOUBLE | Anaesthesia charges | HLTH only |
| `medicines` | DOUBLE | Medicines and consumables | HLTH only |
| `diagnostic_tests` | DOUBLE | Blood tests, scans etc. | HLTH only |
| `icu_charges` | DOUBLE | ICU charges (optional) | HLTH only |
| `structural_damage` | DOUBLE | Building structure repair | PROP only |
| `machinery_damage` | DOUBLE | Machinery and equipment | PROP only |
| `stock_loss` | DOUBLE | Raw material / stock loss | PROP only |
| `electrical` | DOUBLE | Electrical rewiring | PROP only |
| `debris_removal` | DOUBLE | Cleaning and debris removal | PROP only |
| `business_loss` | DOUBLE | Consequential / business loss | PROP only |

> Columns marked "AUTO/HLTH/PROP only" contain `NULL` for other claim types — this is by design.

---

## ✅ Validation Logic

### Formula Per Claim Type

```
AUTO:  vehicle_repair + spare_parts + towing_charges + survey_fee  ≈  total_claim_amount
HLTH:  room_charges + ot_fee + anaesthesia + medicines + diagnostic_tests + icu_charges  ≈  total_claim_amount
PROP:  structural_damage + machinery_damage + stock_loss + electrical + debris_removal + business_loss  ≈  total_claim_amount
```

### Tolerance

**Rs. 1.00** — accounts for floating point precision accumulation across 4-6 additions on large Indian currency amounts.

### Status Flags

| Status | Meaning |
|---|---|
| `OK` | All fields extracted. Amounts mathematically consistent. |
| `MISSING_TOTAL` | `total_claim_amount` could not be extracted from PDF. |
| `EXTRACTION_FAILED` | Line items could not be extracted — unusual PDF layout. |
| `TOTAL_MISMATCH` | All fields extracted but line items do not sum to total. |

---

## 🤖 Genie — Natural Language Analytics

Once the Gold table is ready, connect it to Genie in Databricks:

**Catalog → gold → claims_gold → Open in Genie**

### Sample Questions

```
"What is the total claim amount for auto accidents this quarter?"
"Which claim type has the highest average payout?"
"How many claims have TOTAL_MISMATCH status?"
"Show me monthly claim trend for FY 2024-25"
"Which months had the highest property damage claims?"
"Top 5 claims by total amount across all types"
"How many claims are currently in PENDING SURVEY status?"
"Average deductible across settled auto claims"
```

---

## 📂 Project Structure

```
Intelligent-Document-Processing-in-Databrick
│
├── notebooks/
│   └── insurance_claim_parser.ipynb    ← Main pipeline notebook
|   └── setup_IDP.ipynb                 ← structural setup (Create Catalog Schemas , tables and Volume)
│
│
├── data
│   ├── CLM-AUTO-2024-00423.pdf
│   ├── CLM-HLTH-2024-00871.pdf
│   └── CLM-PROP-2024-00156.pdf
│
└── README.md
```

---

## 🚀 Setup & How to Run

### Prerequisites

- Databricks Free Edition account — [Sign up here](https://www.databricks.com/try-databricks)
- No external services required — everything runs within Databricks

### Step 1 — Create Catalog and Volume

Run in a Databricks SQL editor or notebook:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS insurance
COMMENT 'SafeGuard Insurance IDP pipeline';

-- Create schema for raw file storage
CREATE SCHEMA IF NOT EXISTS insurance.landing
COMMENT 'Landing zone for raw PDFs';

-- Create managed volume
CREATE VOLUME IF NOT EXISTS insurance.landing.raw_invoices
COMMENT 'Upload all claim PDFs here';

-- Create gold schema
CREATE SCHEMA IF NOT EXISTS insurance.gold
COMMENT 'Validated claim output tables';
```

### Step 2 — Upload PDFs

1. Open Databricks Catalog Explorer
2. Navigate to `insurance → landing → raw_invoices`
3. Click **Upload to Volume**
4. Drag and drop all 50 PDF files

### Step 3 — Run the Notebook

1. Import `insurance_claim_parser.ipynb` into your Databricks workspace
2. Attach to **Serverless compute**
3. Run all cells in order
4. Expected output: `Total records: 50` and `Gold table ready: insurance.gold.claims_gold`

### Step 4 — Connect Genie

1. In Databricks, go to **Genie** (left sidebar)
2. Click **New Genie Space**
3. Select table: `insurance.gold.claims_gold`
4. Start asking questions in plain English

---

## Databricks Free Edition — Constraints & Workarounds

| Constraint | Workaround Used |
|---|---|
| No classic clusters — serverless only | All code uses DataFrame API (no RDD) |
| No `dbfs:/` mounts | Unity Catalog Volumes (`/Volumes/...` path) |
| Single workspace | Use schemas to separate concerns: `landing`, `gold` |
| No commercial use | This project is for portfolio/learning purposes only |

---

## 📸 Screenshots

| Step | What You See |
|---|---|
| Volume upload | 50 PDF files visible in Databricks Catalog Explorer |
| `raw_df` | path, content (binary), length, modificationTime columns |
| `elements_df` | element_id, element_type (table/section_header/text), content |
| `claims_combined` | All 50 rows with type-specific fields populated |
| `claim_gold` | status = OK / TOTAL_MISMATCH with calculated_total and diff |
| Genie | Natural language query returning SQL-backed table results |

---

## 💡 What I Learned

**`ai_parse_document` is not OCR.** Traditional OCR returns flat text. This function returns a structured semantic map — element types, bounding boxes, page positions. That structural understanding is what makes reliable field extraction possible at scale.

**Real PDFs are inconsistent.** The same field (`Total Claim Amount`) appeared in different element types across different PDFs — sometimes inside a `table`, sometimes as a standalone `section_header`. Production pipelines must engineer for variability, not just the happy path.

**Data quality is not a final step.** Validation logic — mathematical checks, NULL handling, status flags — must be built into the pipeline from the beginning, not bolted on afterward.

**Simple architecture over complexity.** Two layers (Volume → Gold) is cleaner and more maintainable than a six-layer medallion for a project of this scope. Architecture should match the problem, not demonstrate knowledge of patterns.

**`lead()` and `lag()` Window functions are underrated.** The ability to look at adjacent rows within a partition solves a class of document parsing problems that pure regex cannot handle.

---

## 🤝 Connect

**Built by:** Vishaal Bankar
**LinkedIn:** [Connect with me on LinkedIn URL](https://www.linkedin.com/in/vishaal-bankar-91866a26b/)
**GitHub:** [GitHub URL](https://github.com/vishalbankar)

If you're working on document processing, data extraction, or Lakehouse architecture — I'd love to connect.

---

*Built entirely on Databricks Free Edition. No paid services. No external APIs. Just data engineering.*
