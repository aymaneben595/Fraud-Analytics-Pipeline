# 💳 Credit Card Fraud Analytics & ML Pipeline

---

## 📘 Project Background

This project showcases a complete **end-to-end data analytics and machine learning workflow** focused on **credit card fraud detection**, covering every stage from **data ingestion in SQL** and **model development in Python** to **interactive visualization in Power BI**.

It mirrors the real-world operations of a **Risk & Fraud Automation team**, integrating **ETL**, **feature engineering**, **predictive modeling**, and **business intelligence** into one cohesive analytical solution.

### Key KPIs Tracked
* **Total Transactions**
* **Total Fraud Cases**
* **Fraud Rate (%)**
* **Model Accuracy**
* **ROC-AUC (Model Discrimination Power)**

The analysis and insights are structured around four main areas:
1.  **Temporal Fraud Patterns:** How fraud activity fluctuates by time of day
2.  **User Behavior & Risk:** Identifying high-risk spending behaviors
3.  **Model Performance Benchmarking:** Comparing machine learning models
4.  **Feature Importance & Risk Drivers:** Understanding what drives fraud patterns

---

🔗 **SQL ETL Script:**
[View ETL & Feature Engineering (fraud_pipeline.sql)](fraud_pipeline.sql)

🐍 **Python Modeling Script:**
[View EDA, Modeling & BI Export (fraud_pipeline.py)](fraud_pipeline.py)

📊 **Power BI Dashboard:**
[⬇️ Download Fraud Analytics Dashboard.pbix](Fraud%20Analytics%20Dashboard.pbix)

---

## 🚀 Project Workflow

This project was executed in three main stages to move from raw data to a finished intelligence product.

### 1. SQL: ETL & Feature Engineering
* Designed a **PostgreSQL schema** for fraud analytics.
* Loaded the raw dataset into a `transactions_raw` table.
* Cleaned data by removing duplicates and handling potential missing values.
* Performed transformations, such as converting time-in-seconds to proper timestamps.
* Engineered key analytical features:
    * `transaction_hour`
    * `is_night_transaction` (12 AM–6 AM)
    * `transaction_amount_log`
    * `transactions_per_user_last_24h`
    * `avg_amount_last_7days`
* Exported a fully prepared dataset: `transactions_clean`.

### 2. Python: Modeling & EDA
* Loaded the `transactions_clean` table directly from PostgreSQL.
* Performed detailed **Exploratory Data Analysis (EDA)**.
* Addressed the severe class imbalance using **SMOTE** (Synthetic Minority Over-sampling Technique).
* Trained and evaluated three classification models:
    * Logistic Regression
    * Random Forest
    * XGBoost
* Generated export-ready summary CSV files for Power BI:
    * `model_performance_summary.csv`
    * `fraud_summary_by_day.csv`
    * `fraud_summary_by_user.csv`

### 3. Power BI: Visualization
* Integrated the summary CSV outputs into an interactive dashboard.
* Built KPI cards, trend charts, and risk tables for at-a-glance insights.
* Structured the report into four pages: Overview, Temporal Analysis, Behavioral Analysis, and Model Insights.

---

## 🧩 Data Pipeline & Feature Engineering

The raw **Kaggle credit card dataset** was transformed through SQL-based ETL (`fraud_pipeline.sql`):

* **Ingestion:** Loaded `creditcard.csv` into `transactions_raw`.
* **Cleaning:** Deduplicated and verified data quality.
* **Transformation:** Converted the time (in seconds) into real timestamps (starting `2013-01-01`).
* **Synthetic User IDs:** Created simulated `card_id` values for behavioral analysis.
* **Feature Engineering:**
    * `transaction_hour`
    * `is_night_transaction`
    * `transaction_amount_log`
    * Rolling metrics (`avg_amount_last_7days`, `transactions_per_user_last_24h`)

✅ Final output: **`fraud.transactions_clean`** – a clean, feature-rich table ready for model training.

---

## 📈 Executive Summary

### Overview of Findings

From **283,700 transactions**, the system identified **473 fraudulent cases**, uncovering distinct behavioral trends and strong model results.

| Metric | Value |
| --- | --- |
| **Total Transactions** | 283,700 |
| **Fraud Cases** | 473 |
| **Fraud Rate (Sample)** | 16.7% *(see caveats)* |
| **Average ROC-AUC** | 0.98 |

The **XGBoost** model emerged as the most effective, delivering:
* **Accuracy:** 96%
* **Precision:** 0.85
* **Recall:** 0.82
* **ROC-AUC:** 0.98

<p align="center">
  <img src="Images/b.PNG" alt="Overview Dashboard Snapshot">
</p>

---

## 🔍 Insights Deep Dive

### 🕒 Category 1: Temporal Analysis: When Does Fraud Peak?

* Legitimate transactions cluster between **10 AM–10 PM**.
* Fraud spikes in **early morning hours (2–4 AM)**.
* **4 AM** records the **highest fraud rate (1.45%)**.
* This pattern suggests fraudsters act when transaction volume is lowest, indicating **reduced oversight**.

<p align="center">
  <img src="Images/c.PNG" alt="Transactions by Hour Analysis">
</p>

---

### 👥 Category 2: User Behavior & Risk: Who Are the Riskiest Users?

* **High-risk users:** `card_1562`, `card_0535`, `card_1212` (fraud >1.5%)
* **Low-risk, high-volume users:** `card_0267`, `card_0837` (140+ transactions, 0% fraud)
* Spending patterns show **no direct correlation** with fraud likelihood.

<p align="center">
  <img src="Images/d.PNG" alt="User Behavior & Risk Dashboard">
</p>

---

### 🤖 Category 3: Model Performance: Which Model Performs Best?

| Model | Accuracy | Precision | Recall | F1-Score | ROC-AUC |
| --- | --- | --- | --- | --- | --- |
| **XGBoost** | 0.96 | 0.85 | 0.82 | 0.84 | **0.98** |
| Random Forest | 0.93 | 0.45 | 0.84 | 0.58 | 0.98 |
| Logistic Regression | 0.90 | 0.10 | 0.15 | 0.11 | 0.92 |

**XGBoost** achieves the best trade-off between Recall and Precision, minimizing both missed frauds and false alarms.
**Random Forest** performs well but generates more false positives.
**Logistic Regression** struggles with severe class imbalance.

<p align="center">
  <img src="Images/e.PNG" alt="Model Insights & Features Dashboard">
</p>

---

### 🧠 Category 4: Key Fraud Indicators: What Drives Risk?

* **Top XGBoost features:** `v14`, `v4`, `v12`, `v10`
* **Top Random Forest features:** `v14`, `v12`, `v17`
* Low feature correlation → diverse signal sources and better model generalization

These anonymized PCA components likely capture hidden patterns such as timing irregularities or spending inconsistencies.

---

## 💡 Recommendations & Business Actions

1.  **Deploy XGBoost for Real-Time Scoring**
    * Its 0.98 ROC-AUC and high Recall make it ideal for production.

2.  **Introduce Night-Time Alerts**
    * Apply stricter limits or enforce 2FA between midnight and 6 AM.

3.  **Prioritize High-Risk Profiles for Review**
    * Focus on accounts like `card_1562` and `card_0535`.

4.  **Investigate Latent Features**
    * Collaborate with data engineers to interpret the key PCA components (`v14`, `v12`) and their real-world meaning.

---

## ⚙️ Assumptions & Caveats

* **Synthetic User IDs:** `card_id` was simulated for analytical grouping only.
* **Timestamp Construction:** Based on `2013-01-01` baseline plus time offsets.
* **Fraud Rate (16.7%)** reflects an **oversampled dataset** for visualization; the actual rate is **≈0.17%**.
* **Feature Names (`V1–V28`)** are anonymized PCA components with no direct business labels.

---

<p align="center">
  <i>Created by Aïmane Benkhadda / Personal Data Analytics Project (PostgreSQL, Python, Power BI)</i><br>
  <a href="mailto:aymanebenkhadda5959@gmail.com">aymanebenkhadda5959@gmail.com</a>
</p>
