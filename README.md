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
- [Solution Architecture](#-solution-architecture)
- [Tech Stack](#-tech-stack)
- [Dataset](#-dataset)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Gold Table Schema](#-gold-table-schema)
- [Validation Logic](#-validation-logic)
- [Genie — Natural Language Analytics](#-genie--natural-language-analytics)
- [Key Engineering Challenges Solved](#-key-engineering-challenges-solved)
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

## 🏗️ Solution Architecture
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
