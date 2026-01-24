## Project Overview
This repository contains the full source code, infrastructure as code (IaC), and analytical models for my **MBA Thesis in Data Science & Analytics at USP/Esalq**. 

The research addresses a critical gap in public administration: the objective measurement of investment efficiency. By cross-referencing **Federal Fund-to-Fund transfers** with **Socioeconomic Indicators** from the 2010 and 2022 IBGE Censuses, this project aims to identify statistical anomalies that may suggest administrative inefficiency or potential corruption risks.

### Key Features
* **Production-Grade ETL:** Automated pipelines orchestrated via **Apache Airflow (MWAA)** to ingest data from the IBGE SIDRA API and the Brazilian Transparency Portal.
* **Cloud-Native Architecture:** A Medallion-structured Data Lake (Bronze, Silver, Gold) implemented on **Amazon S3**.
* **Statistical Rigor:** Multiple Linear Regression and Geospatial Analysis conducted within **Amazon SageMaker** to isolate the impact of federal funding on social development.
* **Global Standards:** All documentation, code comments, and analysis are written in **English** to support international career mobility and academic peer review.