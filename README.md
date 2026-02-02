## Project Overview
This repository contains the full source code, infrastructure as code (IaC), and analytical models for my **MBA Thesis in Data Science & Analytics at USP/Esalq**. 

The research addresses a critical gap in public administration: the objective measurement of investment efficiency. By cross-referencing **Federal Fund-to-Fund transfers** with **Socioeconomic Indicators** from the 2010 and 2022 IBGE Censuses, this project aims to identify statistical anomalies that may suggest administrative inefficiency or potential corruption risks.

### Key Features
* **Production-Grade ETL:** Automated pipelines orchestrated via **Apache Airflow (MWAA)** to ingest data from the IBGE SIDRA API and the Brazilian Transparency Portal.
* **Cloud-Native Architecture:** A Medallion-structured Data Lake (Bronze, Silver, Gold) implemented on **Amazon S3**.
* **Statistical Rigor:** Multiple Linear Regression and Geospatial Analysis conducted within **Amazon SageMaker** to isolate the impact of federal funding on social development.
* **Global Standards:** All documentation, code comments, and analysis are written in **English** to support international career mobility and academic peer review.

## Project Status

**Timeline:** January 26 - February 7, 2026  
**Current Phase:** ✅ Bronze Layer Complete | ⏳ Silver Layer (Next)  
**Final Submission:** February 7, 2026

### Completed Milestones
- ✅ **Bronze I (IBGE):** 8 socioeconomic datasets (2010 & 2022)
- ✅ **Bronze II (Transparency):** Federal transfers 2013-2015 + 4 sanctions datasets
- ✅ Smart incremental ingestion with metadata tracking
- ✅ Comprehensive audit logging and documentation

See [`docs/BRONZE_LAYERS_SUMMARY.md`](docs/BRONZE_LAYERS_SUMMARY.md) for complete implementation details.

## Research Methodology

**Objective:** Detect public spending efficiency anomalies through correlation analysis

**Data Sources:**
- **IBGE Census (2010 & 2022):** Socioeconomic baseline and outcome measurements
- **Transparency Portal (2010-2022):** Federal fund-to-fund transfer records
- **CGU/TCU Registries:** Compliance and sanctions data

**Analytical Approach:**
- Multiple Linear Regression to isolate transfer impact on outcomes
- Geospatial analysis for regional efficiency patterns
- Anomaly detection for outlier identification
- Time-series analysis for temporal trends

**Temporal Structure:**
```
Census 2010 (Baseline) → Federal Transfers 2010-2022 (Treatment) → Census 2022 (Outcome)
```

## Quick Start

### For Development
```bash
# Clone repository
git clone <repository-url>
cd public-compliance-data-analysis

# Setup environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.\.venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials
aws configure
```

### Run Bronze Layer Ingestion
```bash
# Run both Bronze I (IBGE) and Bronze II (Transparency)
./scripts/run_ingestion.sh

# Run only IBGE ingestion
./scripts/run_ingestion.sh --only-ibge

# Run only Transparency ingestion
./scripts/run_ingestion.sh --only-transparency
```

**Environment Variables Required:**
```bash
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
TRANSPARENCY_API_KEY=<your-api-key>
S3_BUCKET_NAME=enok-mba-thesis-datalake
```

See [`docs/BRONZE_LAYERS_SUMMARY.md`](docs/BRONZE_LAYERS_SUMMARY.md) for detailed setup and troubleshooting.

## Repository Structure

```
public-compliance-data-analysis/
├── config/
│   ├── ibge_metadata.json          # 8 Census datasets (2010 & 2022)
│   └── transparency_metadata.json  # 17 datasets (13 transfers + 4 compliance)
├── src/
│   ├── ingestion/
│   │   ├── ibge_client.py          # IBGE SIDRA API client
│   │   └── transparency_client.py  # Transparency Portal client
│   ├── transformation/             # Silver phase (JSON → Parquet)
│   └── analysis/                   # Gold phase & ML models
├── tests/
│   ├── test_ibge_metadata.py       # IBGE endpoint validation
│   └── test_transparency_ingestion.py  # Transparency pipeline tests
├── docs/
│   ├── BRONZE_LAYERS_SUMMARY.md    # Complete Bronze I & II documentation
│   └── data_sources.log            # Audit trail of all ingestions
├── infra/
│   └── terraform/                  # AWS infrastructure as code
├── notebooks/                      # Jupyter analysis notebooks
└── requirements.txt                # Python dependencies
```

## Data Pipeline Architecture

### Medallion Layers

**Bronze Layer (Raw Data)** ✅
- IBGE Census 2010 & 2022 (8 datasets)
- Federal Transfers 2013-2015 (3 years available in API)
- CGU/TCU Compliance Registries (4 datasets: CEIS, CNEP, CEAF, CEPIM)

**Silver Layer (Cleaned & Normalized)** ⏳
- Parquet format with enforced schemas
- Standardized municipality codes (IBGE 7-digit)
- ISO 8601 date formats

**Gold Layer (Analytical)** ⏳
- Municipality-level efficiency metrics
- Compliance risk indicators
- Feature store for ML models

## Technical Stack

- **Language:** Python 3.12
- **Cloud:** AWS (S3, SageMaker, MWAA)
- **Data Formats:** JSON (Bronze), Parquet (Silver/Gold)
- **Key Libraries:** boto3, requests, pandas, pyarrow
- **IaC:** Terraform
- **Version Control:** Git (squashed PR workflow)

## Documentation

- **Bronze Layers:** Complete implementation guide in [`docs/BRONZE_LAYERS_SUMMARY.md`](docs/BRONZE_LAYERS_SUMMARY.md)
- **API Documentation:** Inline docstrings following Google style
- **Data Sources:** Audit trail in `docs/data_sources.log` for reproducibility
- **Commit History:** Squashed commits with comprehensive messages

## Bibliographic References

### Medallion Architecture (Bronze, Silver, Gold Layers)

The ETL pipeline implementation follows the **Medallion Architecture** pattern, a multi-layered data lake design that progressively refines data quality:

1. **Databricks. (2023).** *What is a Medallion Architecture?* Databricks Documentation.  
   Available at: https://www.databricks.com/glossary/medallion-architecture  
   - Defines the three-layer pattern: Bronze (raw), Silver (validated), Gold (aggregated)
   - Industry standard for data lake organization and data quality management

2. **Armbrust, M., et al. (2020).** *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores.*  
   Proceedings of the VLDB Endowment, 13(12), 3411-3424.  
   - Academic foundation for incremental data processing and schema evolution
   - ACID transactions in data lakes using checkpoint mechanisms

3. **Inmon, W. H. (2005).** *Building the Data Warehouse* (4th ed.). Wiley.  
   - Foundational concepts of data staging and progressive transformation
   - Separation of operational and analytical data concerns

4. **Kimball, R., & Ross, M. (2013).** *The Data Warehouse Toolkit* (3rd ed.). Wiley.  
   - ETL best practices: extraction, transformation, and loading patterns
   - Dimensional modeling for analytical workloads

5. **AWS. (2024).** *AWS Lake Formation Best Practices.*  
   Amazon Web Services Documentation.  
   Available at: https://docs.aws.amazon.com/lake-formation/  
   - Cloud-native implementation patterns for S3-based data lakes
   - Security and governance frameworks for public sector data

### Implementation Adaptations

This project adapts the Medallion Architecture for public sector compliance analysis:
- **Bronze Layer:** Raw JSON from government APIs with full audit trails
- **Silver Layer:** Schema-validated Parquet with standardized IBGE municipality codes
- **Gold Layer:** Pre-aggregated analytics optimized for regression models and geospatial analysis

## Academic Context

**Institution:** USP/Esalq - MBA in Data Science & Analytics  
**Thesis Advisor:** Prof. Dr. Carlos Nabil  
**Submission Deadline:** February 7, 2026  
**Legal Framework:** Lei de Acesso à Informação (LAI - Law 12.527/2011)

**Data Ethics:**
- All data is publicly available under Brazilian transparency laws
- No personal identifiable information (PII) collected
- Aggregated analysis only - no individual entity profiling
- Compliance with academic research ethics standards

## License

This project is developed for academic purposes as part of an MBA thesis. All code and documentation are subject to academic integrity policies of USP/Esalq.

## Contact

**Author:** Enok Antônio de Jesus  
**Program:** MBA Data Science & Analytics  
**Institution:** Universidade de São Paulo (USP) - Esalq