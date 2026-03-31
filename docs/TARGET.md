
# 📄 Project Assignment (PM → Data Engineer)

## Project Name

**Global E-Commerce Sales Analytics Platform**

## Data Source

Kaggle (public datasets)

Recommended dataset:

* *Online Retail Dataset* OR any **transactional e-commerce dataset**

---

# 1️⃣ Business Context

The company operates an international e-commerce platform and wants to:

* Understand customer purchasing behavior
* Analyze product performance
* Monitor revenue trends
* Identify top markets and customer segments

Currently, data is stored in raw CSV files with **no structured analytics layer**.

---

# 2️⃣ Project Objective

Build a **batch data pipeline and analytics warehouse** that:

1. Ingests raw transactional sales data
2. Cleans and standardizes the dataset
3. Models the data into a warehouse schema
4. Produces business-level analytics
5. Enables reporting for stakeholders

---

# 3️⃣ Source Data Description

The dataset will include transactional records with fields like:

| Field        | Description           |
| ------------ | --------------------- |
| invoice_no   | Transaction ID        |
| stock_code   | Product ID            |
| description  | Product name          |
| quantity     | Number of items       |
| invoice_date | Transaction timestamp |
| unit_price   | Price per item        |
| customer_id  | Unique customer ID    |
| country      | Customer location     |

---

# 4️⃣ Core Business Questions

Your system must answer:

* What are total sales over time?
* Which products generate the most revenue?
* Who are the top customers?
* Which countries generate the most sales?
* What are seasonal sales trends?

---

# 5️⃣ Data Engineering Requirements

---

# EPIC 1 — Data Ingestion

## Task 1.1 — Raw Data Acquisition

**Requirements**

* Download dataset from Kaggle
* Store raw files in project storage
* Preserve original format

**Deliverables**

* Raw dataset stored in `/data/raw`

---

## Task 1.2 — Data Validation

**Requirements**

* Check for:

  * Missing values
  * Duplicate records
  * Invalid data types

**Deliverables**

* Data quality report

---

# EPIC 2 — Data Cleaning & Transformation

## Task 2.1 — Data Cleaning

**Requirements**

* Remove null or invalid records
* Handle negative quantities (returns)
* Normalize column names
* Convert timestamps

---

## Task 2.2 — Feature Engineering

Create derived fields:

| Field       | Description              |
| ----------- | ------------------------ |
| total_price | quantity × unit_price    |
| year        | Extract from date        |
| month       | Extract from date        |
| day         | Extract from date        |
| day_of_week | Weekday                  |
| is_return   | Flag for returned orders |

---

# EPIC 3 — Data Modeling

## Task 3.1 — Design Star Schema ⭐

You must implement a **star schema**.

### Dimension Tables

* customers
* products
* dates
* countries

### Fact Table

* sales transactions

---

## Task 3.2 — Schema Requirements

* Define primary keys
* Define foreign keys
* Ensure referential integrity
* Optimize for analytical queries

---

# EPIC 4 — Data Warehouse Implementation

## Task 4.1 — Database Setup

**Requirements**

* Create warehouse schema
* Create all tables
* Add indexes where necessary

---

## Task 4.2 — Data Loading

**Requirements**

* Load dimension tables first
* Load fact table after
* Ensure no duplicate transactions

---

# EPIC 5 — Aggregation Layer

## Task 5.1 — Business Metrics Tables

Create aggregated datasets:

---

### Sales Summary Table

| Metric          | Description       |
| --------------- | ----------------- |
| total_revenue   | Sum of sales      |
| total_orders    | Count of invoices |
| avg_order_value | Revenue per order |

---

### Product Performance Table

| Metric        | Description         |
| ------------- | ------------------- |
| total_sales   | Revenue per product |
| quantity_sold | Units sold          |

---

### Customer Analytics Table

| Metric         | Description          |
| -------------- | -------------------- |
| lifetime_value | Total customer spend |
| total_orders   | Orders per customer  |

---

### Country-Level Analysis

| Metric              | Description        |
| ------------------- | ------------------ |
| revenue_per_country | Total revenue      |
| order_count         | Orders per country |

---

# EPIC 6 — Reporting Requirements

---

## Report 1 — Revenue Trends

**Goal:** Track business growth

* Revenue by day / month
* Trend over time

---

## Report 2 — Top Products

**Goal:** Identify best-selling products

* Top products by revenue
* Top products by quantity

---

## Report 3 — Customer Segmentation

**Goal:** Identify valuable customers

* Top 10 customers
* Repeat vs one-time customers

---

## Report 4 — Geographic Analysis

**Goal:** Understand market distribution

* Revenue by country
* Order volume by country

---

## Report 5 — Returns Analysis

**Goal:** Monitor product returns

* Number of returned orders
* Return rate per product

---

# EPIC 7 — Pipeline Requirements

---

## Task 7.1 — Pipeline Execution

Pipeline must include:

1. Extract
2. Transform
3. Load

Must run **end-to-end with one command**.

---

## Task 7.2 — Incremental Strategy

**Requirements**

* Detect new transactions
* Load only new data
* Avoid duplication

---

## Task 7.3 — Logging

Log:

* Pipeline start/end
* Records processed
* Errors

---

# EPIC 8 — Documentation

---

## Task 8.1 — README Content

Must include:

* Project overview
* Architecture diagram
* Data model explanation
* Pipeline workflow
* Setup instructions

---

# 🎯 Final Deliverables

You must deliver:

* ETL pipeline code
* Cleaned dataset
* Data warehouse schema
* Aggregated tables
* Analytical reports
* Documentation

---

# 📈 Skills You Will Gain

This project will strengthen:

* Batch ETL pipelines
* Data cleaning at scale
* Star schema modeling
* Business-driven analytics
* Data warehouse design

---

# 🚀 Stretch Goals (Highly Recommended)

* Add slowly changing dimensions (SCD)
* Build data quality checks
* Add partitioning strategy
* Optimize query performance
* Add orchestration (scheduled runs)

---

# 🧠 PM Final Note

This project simulates a **real analytics team request**.

If you complete it properly, you will demonstrate:

* Strong understanding of **data modeling**
* Ability to build **production-style pipelines**
* Capability to translate **business questions into data systems**

---

If you want next level (🔥), I can give you:

👉 **Streaming (Kafka-style) real-time project**
👉 **FAANG-level system design version of this project**

Just tell me 👍
