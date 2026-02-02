# STEP 3: GOLD LAYER - Analytics & Aggregations

**Last Updated:** 2026-02-01  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Bucket:** `enok-mba-thesis-datalake`

---

## Overview

The Gold layer transforms Silver layer data into analysis-ready aggregations and metrics. This layer:
- Aggregates data at municipality and state levels
- Calculates change metrics between census years (2010→2022)
- Creates feature-engineered variables for regression analysis
- Produces analysis-ready compliance datasets

---

## Architecture

### Input (Silver Layer)
```
s3://enok-mba-thesis-datalake/silver/
├── dim_municipalities/data.parquet
├── fact_population/data.parquet
├── fact_sanitation/data.parquet
├── fact_literacy/data.parquet
├── fact_income/data.parquet
├── fact_federal_transfers/data.parquet
└── fact_sanctions/data.parquet
```

### Output (Gold Layer)
```
s3://enok-mba-thesis-datalake/gold/
├── agg_municipality_socioeconomic/
│   ├── data.parquet
│   ├── data.json
│   └── .metadata.json
├── agg_state_summary/
│   ├── data.parquet
│   ├── data.json
│   └── .metadata.json
├── agg_sanctions_summary/
│   ├── data.parquet
│   ├── data.json
│   └── .metadata.json
└── analysis_compliance/
    ├── data.parquet
    ├── data.json
    └── .metadata.json
```

---

## Gold Tables

### 1. agg_municipality_socioeconomic

Municipality-level socioeconomic aggregation with change metrics between census years.

| Column | Type | Description |
|--------|------|-------------|
| municipality_code | string | 7-digit IBGE municipality code (PK) |
| municipality_name | string | Municipality name |
| state_code | string | 2-digit state code |
| state_name | string | State name |
| region_code | string | Region code (1-5) |
| region_name | string | Region name |
| population_2010 | integer | Population in 2010 |
| population_2022 | integer | Population in 2022 |
| population_change_pct | float | Population change % (2010→2022) |
| literacy_rate_2010 | float | Literacy rate 2010 (%) |
| literacy_rate_2022 | float | Literacy rate 2022 (%) |
| literacy_change_pp | float | Literacy change in percentage points |
| avg_income_2010 | float | Average income 2010 (BRL) |
| avg_income_2022 | float | Average income 2022 (BRL) |
| income_change_pct | float | Income change % (2010→2022) |
| households_2010 | integer | Total households 2010 |
| households_2022 | integer | Total households 2022 |
| households_change_pct | float | Households change % (2010→2022) |

**Use Cases:**
- Municipality-level trend analysis
- Identifying fastest/slowest growing municipalities
- Correlating socioeconomic changes with compliance outcomes

---

### 2. agg_state_summary

State-level aggregated summaries for regional analysis.

| Column | Type | Description |
|--------|------|-------------|
| state_code | string | 2-digit state code (PK) |
| state_name | string | State name |
| region_code | string | Region code (1-5) |
| region_name | string | Region name |
| municipality_count | integer | Number of municipalities |
| total_population_2010 | integer | Total population 2010 |
| total_population_2022 | integer | Total population 2022 |
| population_change_pct | float | Population change % |
| avg_income_2022 | float | Average income 2022 (BRL) |
| total_sanctions | integer | Total sanctions count |
| sanctions_pf | integer | Sanctions against individuals (PF) |
| sanctions_pj | integer | Sanctions against companies (PJ) |
| sanctions_per_100k | float | Sanctions per 100k population |

**Use Cases:**
- State-level comparisons
- Regional compliance analysis
- Identifying high/low sanction rate states

---

### 3. agg_sanctions_summary

Sanctions aggregations by registry type (CEIS, CNEP, CEAF, CEPIM).

| Column | Type | Description |
|--------|------|-------------|
| registry_type | string | Registry source (PK) |
| total_sanctions | integer | Total sanctions count |
| sanctions_pf | integer | Sanctions against individuals |
| sanctions_pj | integer | Sanctions against companies |
| pj_ratio_pct | float | Percentage of PJ sanctions |
| unique_agencies | integer | Unique sanctioning agencies |
| earliest_sanction | date | Earliest sanction date |
| latest_sanction | date | Latest sanction date |
| [state columns] | integer | Sanctions count per state |

**Use Cases:**
- Understanding sanctions distribution by registry
- Comparing registry characteristics
- Temporal analysis of sanctions

---

### 4. analysis_compliance

Analysis-ready dataset for regression and correlation analysis at state level.

| Column | Type | Description |
|--------|------|-------------|
| state_code | string | 2-digit state code (PK) |
| state_name | string | State name |
| region_code | string | Region code |
| region_name | string | Region name |
| n_municipalities | integer | Number of municipalities |
| population | integer | Total population 2022 |
| avg_literacy_rate | float | Average literacy rate 2022 (%) |
| avg_income | float | Average income 2022 (BRL) |
| n_sanctions | integer | Total sanctions count |
| n_sanctions_ceis | integer | CEIS sanctions count |
| n_sanctions_cnep | integer | CNEP sanctions count |
| n_sanctions_ceaf | integer | CEAF sanctions count |
| n_sanctions_cepim | integer | CEPIM sanctions count |
| sanctions_per_100k | float | Sanctions per 100k population |
| log_population | float | Log-transformed population |
| log_income | float | Log-transformed income |
| is_norte | integer | Dummy: Norte region |
| is_nordeste | integer | Dummy: Nordeste region |
| is_sudeste | integer | Dummy: Sudeste region |
| is_sul | integer | Dummy: Sul region |
| is_centro_oeste | integer | Dummy: Centro-Oeste region |

**Use Cases:**
- Regression analysis (sanctions ~ socioeconomic factors)
- Correlation analysis between compliance and development
- Regional comparisons with control variables

---

## Feature Engineering

### Change Metrics
- **Percentage Change:** `((new - old) / old) * 100`
- **Percentage Points:** `new_rate - old_rate` (for rates like literacy)

### Normalized Metrics
- **Sanctions per 100k:** `(n_sanctions / population) * 100000`
- Enables fair comparison across states of different sizes

### Log Transformations
- **log_population:** Natural log of population
- **log_income:** Natural log of income
- Useful for regression analysis with skewed distributions

### Dummy Variables
- Regional dummies for regression models
- Reference category can be dropped as needed

---

## Implementation

### Code Structure
```
src/processing/
├── __init__.py              # Module exports (includes GoldTransformer)
├── base_transformer.py      # Abstract base class
├── ibge_transformer.py      # Bronze → Silver (IBGE)
├── transparency_transformer.py  # Bronze → Silver (Transparency)
└── gold_transformer.py      # Silver → Gold (NEW)

config/
└── silver_schemas.json      # Schema definitions (includes Gold schemas)

scripts/
├── run_transformation.sh    # Silver layer script
└── run_gold_transformation.sh  # Gold layer script (NEW)
```

### GoldTransformer Class

Main methods:
- `transform()` - Execute all Gold transformations
- `_transform_municipality_socioeconomic()` - Build municipality aggregation
- `_transform_state_summary()` - Build state summaries
- `_transform_sanctions_summary()` - Build sanctions aggregation
- `_transform_analysis_compliance()` - Build analysis dataset

Utility methods:
- `_read_silver_parquet()` - Read Parquet from Silver layer
- `_calculate_change_pct()` - Calculate percentage change
- Inherits from `BaseTransformer`: schema validation, S3 I/O, smart caching

---

## Execution

### Full Gold Transformation
```bash
./scripts/run_gold_transformation.sh
```

### Direct Python Execution
```bash
python -m src.processing.gold_transformer
```

### Prerequisites
Silver layer must be populated first:
```bash
./scripts/run_transformation.sh
```

---

## Smart Caching

Like Silver layer, Gold layer uses intelligent caching:

1. **Metadata Tracking:** Each output stores source file hashes
2. **Change Detection:** Compares current vs stored hashes
3. **Skip Logic:** Skips processing if sources unchanged

### Benefits
- Fast re-runs when sources unchanged
- Automatic reprocessing when Silver data updates
- Idempotent - safe to run multiple times

---

## Data Quality

### Validation
- Schema validation via `validate_schema()`
- Type enforcement (integers, floats, strings)
- Null handling for optional fields

### Aggregation Rules
- Population: Sum across municipalities
- Income/Literacy: Average across municipalities
- Sanctions: Count with proper deduplication

### Edge Cases
- Missing data: Graceful handling with NULL values
- Division by zero: Protected in ratio calculations
- Log of zero: Returns NULL for invalid inputs

---

## Testing

```bash
# Run Gold layer tests
pytest tests/processing/test_gold_transformer.py -v

# Run all processing tests
pytest tests/processing/ -v

# Run complete test suite
./scripts/run_tests.sh
```

**Test Coverage:**
- GoldTransformer initialization and configuration
- Change percentage calculations
- Sanctions aggregation logic
- Analysis dataset feature engineering
- Region dummy variable creation

---

## Dependencies

Same as Silver layer:
```
pandas>=2.2.2
pyarrow>=15.0.0
boto3>=1.34.0
numpy>=1.26.0
```

---

## Analysis Examples

### Correlation Analysis
```python
import pandas as pd

# Load analysis dataset
df = pd.read_parquet('gold/analysis_compliance/data.parquet')

# Correlation: sanctions vs socioeconomic factors
correlations = df[['sanctions_per_100k', 'avg_literacy_rate', 'avg_income', 'log_population']].corr()
print(correlations)
```

### Regression Setup
```python
import statsmodels.api as sm

# Dependent variable
y = df['sanctions_per_100k']

# Independent variables (with region dummies)
X = df[['log_income', 'avg_literacy_rate', 'log_population', 
        'is_norte', 'is_nordeste', 'is_sul', 'is_centro_oeste']]
X = sm.add_constant(X)

# Fit OLS
model = sm.OLS(y, X).fit()
print(model.summary())
```

### Regional Comparison
```python
# Sanctions by region
regional = df.groupby('region_name').agg({
    'n_sanctions': 'sum',
    'population': 'sum',
    'sanctions_per_100k': 'mean'
}).round(2)
print(regional)
```

---

## Known Limitations

1. **Sanctions Geography:** Many sanctions lack state/municipality codes
2. **Federal Transfers:** Limited date range (2013-2015) in source data
3. **Census Years:** Only 2010 and 2022 available for comparison
4. **State-Level Analysis:** Municipality-level sanctions data often unavailable

---

## Next Steps

1. **Visualization Layer:** Dashboards and charts
2. **Extended Analysis:** Time-series if more data becomes available
3. **Machine Learning:** Classification models for compliance risk
4. **Geographic Analysis:** Municipality-level when data permits

---

**End of Gold Layer Summary**
