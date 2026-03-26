# Cloud based data pipeline

## Project Overview

This project builds an **end-to-end cloud-based data pipeline** using Microsoft Azure to analyze **supply chain and inventory data**. It includes data ingestion, storage, transformation, and visualization using modern data engineering tools.

---

## Architecture

```
Data Source -> Azure Data Factory -> Azure Data Lake -> Databricks -> Azure SQL -> Power BI
```

---

## Technologies Used

* Azure Data Factory (ADF)
* Azure Data Lake Storage Gen2 (ADLS)
* Azure Databricks (PySpark)
* Azure SQL Database
* Power BI

---

##  Dataset Description
* inventory data
* stock data



## Data Pipeline Steps

### 1️. Data Ingestion

* Data is ingested using **Azure Data Factory**
* Source: CSV 
* Stored in **Azure Data Lake (raw layer)**

---

### 2️. Data Storage

* Raw data stored in `/raw`
* Processed data stored in `/processed`


---

### 3️. Data Transformation (Databricks)

Key transformations:

* Data cleaning (null handling, duplicates removal)
* Data type conversion (string → int/date)
* Feature engineering:

  * delivery_status (On-Time / Delayed)
  * reorder_flag (Yes / No)


---

### 4️. Data Loading

* Clean data stored in:

  * **Delta format (Data Lake)**
  * **Azure SQL Database**

---

### 5️. Data Visualization (Power BI)

##  Dashboards

### 1️. Delivery Performance Dashboard

* Avg delivery time by region
* Delayed vs On-Time orders


### 2️. Inventory Dashboard

* Stock levels by warehouse
* Reorder alerts



### 3️. Cost Analysis Dashboard

* Transport cost trends (Line Chart)
* Supplier comparison (Bar Chart)



### 4️. Regional Insights Dashboard

* Orders per region
* Delivery efficiency



##  Key Metrics

* Total Orders
* Delivery Efficiency (%)
* Total Transport Cost
* Reorder Alerts Count

---

## Automation

* Pipelines scheduled using **ADF Triggers**
* Supports:

  * Daily ingestion
  * Hourly updates
* Databricks notebooks triggered automatically

---

## Setup Instructions

1. Clone the repository:

```
git clone https://github.com/ENUKOLUnaga/Customer-Behavior-Analytics-on-Azure.git
```

2. Configure Azure Services:

* Create Data Factory
* Create Data Lake
* Setup Databricks workspace

3. Upload datasets to Data Lake

4. Run ADF pipeline

5. Execute Databricks notebooks

6. Connect Power BI to Azure SQL

---

## Key Features

* End-to-end ETL pipeline
* Scalable cloud architecture
* Real-time analytics readiness
* Interactive dashboards


---
## Analytic reports
<img width="1113" height="558" alt="Screenshot 2026-03-25 142119" src="https://github.com/user-attachments/assets/32820d32-0950-4c3e-ba5c-8fa532aa8f97" />
<img width="1091" height="625" alt="Screenshot 2026-03-25 145920" src="https://github.com/user-attachments/assets/50a2575b-db01-408a-a674-dbd37c054fee" />
<img width="986" height="647" alt="Screenshot 2026-03-25 144559" src="https://github.com/user-attachments/assets/fdab3b84-9c35-4fc7-b2de-d26915356849" />
<img width="1168" height="659" alt="Screenshot 2026-03-26 145615" src="https://github.com/user-attachments/assets/c4a6e327-a26b-4c39-ae6f-4251b33cd35b" />



## Conclusion

This project demonstrates how to build a **modern data engineering pipeline on Azure**, transforming raw data into actionable business insights through scalable architecture and visualization tools.

---
