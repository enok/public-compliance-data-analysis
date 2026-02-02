# STEP 2: SILVER LAYER - Data Transformation

**Last Updated:** 2026-02-01  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Bucket:** `enok-mba-thesis-datalake`

---

## ⚠️ IMPORTANT: Bronze Data Re-Ingestion Required

**Issue Found During Review (2026-01-30):**
The IBGE income configuration had incorrect variable codes:

| Dataset | Old Variable | New Variable | Description |
|---------|-------------|--------------|-------------|
| income_2010 | v=841 (person count) | **v=842** | Average monthly income (R$) |
| income_2022 | v=13604 (person count) | **v=13431** | Average per capita income (R$) |

**Action Required:** Re-run Bronze ingestion to fetch correct income values:
```bash
./scripts/run_ingestion.sh --only-ibge
```

---

## Overview

The Silver layer transforms raw Bronze data into normalized, cleaned, and analysis-ready tables. This layer:
- Standardizes schemas across data sources
- Extracts municipality codes and geographic hierarchies
- Handles missing values and data type conversions
- Removes duplicates and validates data quality

---

## Architecture

### Input (Bronze Layer)
```
s3://enok-mba-thesis-datalake/bronze/
├── ibge/
│   ├── census_2010_pop.json
│   ├── census_2022_pop.json
│   ├── census_2010_sanitation.json
│   ├── census_2022_sanitation.json
│   ├── census_2010_literacy.json
│   ├── census_2022_literacy.json
│   ├── census_2010_income.json
│   └── census_2022_income.json
└── transparency/
    ├── federal_transfers_2013.json
    ├── federal_transfers_2014.json
    ├── federal_transfers_2015.json
    ├── ceis_compliance.json
    ├── cnep_compliance.json
    ├── ceaf_compliance.json
    └── cepim_compliance.json
```

### Output (Silver Layer)
```
s3://enok-mba-thesis-datalake/silver/
├── dim_municipalities/
│   ├── data.parquet
│   └── data.json
├── fact_population/
│   ├── data.parquet
│   └── data.json
├── fact_sanitation/
│   ├── data.parquet
│   └── data.json
├── fact_literacy/
│   ├── data.parquet
│   └── data.json
├── fact_income/
│   ├── data.parquet
│   └── data.json
├── fact_federal_transfers/
│   ├── data.parquet
│   └── data.json
└── fact_sanctions/
    ├── data.parquet
    └── data.json
```

---

## Smart Caching & Incremental Processing

**Implemented:** 2026-02-01

The Silver layer uses intelligent caching to avoid unnecessary reprocessing:

### How It Works

1. **Metadata Tracking**: Each output table stores metadata about its source files
   - Source file MD5 hashes (from S3 ETag)
   - Processing timestamp
   - Record count
   - Stored in `_metadata.json` files alongside data

2. **Change Detection**: Before processing, transformer checks:
   - Does output file exist?
   - Have any source files changed? (MD5 comparison)
   - If no changes detected → skip processing

3. **Automatic Reprocessing**: Transformation runs when:
   - Output doesn't exist (first run)
   - Source files have changed (new data)
   - New source files added (expanded coverage)

### Metadata Files

Each Silver table has an associated metadata file:
```
silver/
├── dim_municipalities/
│   ├── data.parquet
│   └── _metadata.json          # Tracks source file hashes
├── fact_population/
│   ├── data.parquet
│   └── _metadata.json
└── fact_federal_transfers/
    ├── data.parquet
    └── _metadata.json
```

### Benefits

- **Faster Runs**: Skip unchanged tables (seconds vs minutes)
- **Idempotent**: Safe to run multiple times
- **Incremental**: Only process what changed
- **Audit Trail**: Metadata shows when/why tables were updated

### Example Metadata

```json
{
  "output_file": "silver/fact_population/data.parquet",
  "source_files": {
    "bronze/ibge/census_2010_pop.json": "abc123def456...",
    "bronze/ibge/census_2022_pop.json": "789xyz012..."
  },
  "record_count": 11140,
  "processed_at": "2026-02-01T20:30:00"
}
```

---

## Schemas

### dim_municipalities (Dimension Table)
Master table for Brazilian municipalities with geographic hierarchy.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | 7-digit IBGE municipality code (PK) |
| municipality_name | string | Municipality name |
| state_code | string | 2-digit state code |
| state_name | string | State name |
| region_code | string | Region code (1-5) |
| region_name | string | Region name |

### fact_population
Population data from Census 2010 and 2022.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | FK to dim_municipalities |
| year | integer | Census year (2010, 2022) |
| total_population | integer | Total resident population |
| urban_population | integer | Urban area population (nullable) |
| rural_population | integer | Rural area population (nullable) |

### fact_sanitation
Sanitation infrastructure from Census 2010 and 2022.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | FK to dim_municipalities |
| year | integer | Census year |
| total_households | integer | Total permanent private households |
| households_with_water | integer | Households with piped water (nullable) |
| households_with_sewage | integer | Households with sewage system (nullable) |
| households_with_garbage_collection | integer | Households with garbage collection (nullable) |
| water_coverage_pct | float | Percentage with water access (nullable) |
| sewage_coverage_pct | float | Percentage with sewage access (nullable) |

### fact_literacy
Literacy rates from Census 2010 and 2022.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | FK to dim_municipalities |
| year | integer | Census year |
| population_15_plus | integer | Population 15 years or older |
| literate_population | integer | Literate population 15+ (nullable) |
| literacy_rate | float | Literacy rate percentage (nullable) |

### fact_income
Income data from Census 2010 and 2022.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | FK to dim_municipalities |
| year | integer | Census year |
| avg_income | float | Average nominal monthly income (BRL) |
| median_income | float | Median income (nullable) |
| population_with_income | integer | Population with income (nullable) |

### fact_federal_transfers
Federal transfers to municipalities (2013-2015).

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | FK to dim_municipalities (nullable) |
| year | integer | Transfer year |
| month | integer | Transfer month |
| transfer_amount | float | Transfer amount (BRL) |
| transfer_type | string | Type of transfer |
| source_agency | string | Source federal agency |

### fact_sanctions
Compliance sanctions from all registries.

| Column | Type | Description |
|--------|------|-------------|
| sanction_id | string | Unique sanction identifier (PK) |
| registry_type | string | Registry source (CEIS, CNEP, CEAF, CEPIM) |
| sanctioned_entity | string | Name of sanctioned entity/person |
| entity_type | string | Type: PF (individual) or PJ (company) |
| cpf_cnpj | string | Masked CPF or CNPJ |
| sanction_type | string | Type of sanction applied |
| sanction_start_date | date | Sanction start date |
| sanction_end_date | date | Sanction end date (nullable) |
| sanctioning_agency | string | Agency that applied the sanction |
| state_code | string | State code (nullable) |
| municipality_code | string | Municipality code (nullable) |

---

## Implementation

### Code Structure
```
src/processing/
├── __init__.py              # Module exports
├── base_transformer.py      # Abstract base class
├── ibge_transformer.py      # IBGE Census transformations
└── transparency_transformer.py  # Transparency Portal transformations

config/
└── silver_schemas.json      # Schema definitions and mappings

scripts/
└── run_transformation.sh    # Orchestration script
```

### Key Classes

#### BaseTransformer
Abstract base class providing:
- S3 read/write operations (JSON and Parquet)
- Municipality code extraction and validation
- State/region mapping utilities
- Safe type conversions (int, float, date)
- Schema validation and enforcement
- Processing logging

#### IBGETransformer
Transforms IBGE Census data:
- Builds municipalities dimension table
- Transforms population, sanitation, literacy, income facts
- Handles SIDRA API JSON format

#### TransparencyTransformer
Transforms Transparency Portal data:
- Transforms federal transfers (with municipality linking)
- Transforms sanctions from all 4 registries (CEIS, CNEP, CEAF, CEPIM)
- Masks sensitive document numbers (CPF/CNPJ)

---

## Execution

### Full Transformation
```bash
./scripts/run_transformation.sh
```

### IBGE Only
```bash
./scripts/run_transformation.sh --only-ibge
```

### Transparency Only
```bash
./scripts/run_transformation.sh --only-transparency
```

### Direct Python Execution
```bash
# IBGE
python -m src.processing.ibge_transformer

# Transparency
python -m src.processing.transparency_transformer
```

---

## Data Quality Measures

### Validation Rules
1. **Municipality codes**: Must be 7-digit, valid state prefix (11-53)
2. **Numeric values**: Safe conversion with null handling for invalid data
3. **Dates**: Multiple format parsing (ISO, Brazilian dd/mm/yyyy)
4. **Duplicates**: Removed based on primary key columns

### Privacy Protection
- CPF numbers masked: `***.***XXX-XX`
- CNPJ numbers masked: `**.***.***/ XXXX-XX`
- No PII stored in plain text

### Audit Trail
- All transformations logged to `docs/processing.log`
- Includes: timestamp, dataset, status, record counts, S3 keys

---

## Known Limitations

1. **Federal Transfers**: Municipality codes not always available in source data
2. **Sanctions**: Geographic location (municipality) often missing
3. **IBGE Data**: Detailed breakdowns (urban/rural) may require additional parsing
4. **Date Coverage**: Federal transfers only available 2013-2015

---

## Next Steps (Gold Layer)

1. **Aggregations**: Municipality and state-level summaries
2. **Feature Engineering**: 
   - Population change (2010→2022)
   - Sanitation improvement rates
   - Literacy change metrics
   - Income growth indicators
3. **Compliance Metrics**:
   - Sanctions per capita by municipality
   - Sanctions by type and region
4. **Analysis-Ready Tables**:
   - Correlation datasets
   - Regression analysis inputs
   - Visualization-ready summaries

---

## Dependencies

```
pandas>=2.2.2
pyarrow>=15.0.0
boto3>=1.34.0
python-dotenv>=1.0.0
```

---

## Testing

```bash
# Run all Silver layer tests
pytest tests/processing/ -v

# Run transformer tests
pytest tests/processing/test_transformers.py -v

# Run smart caching tests
pytest tests/processing/test_smart_caching.py -v

# Run complete test suite (includes unit, integration, and validation)
./scripts/run_tests.sh
```

**Test Coverage:**
- 16 transformer unit tests (BaseTransformer, IBGETransformer, TransparencyTransformer)
- 12 smart caching tests (metadata tracking, change detection, skip logic)
- Integration tests for full pipeline validation

---

**End of Silver Layer Summary**
