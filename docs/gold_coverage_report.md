# Gold Layer Coverage Report

**Generated:** 2026-02-19 (UTC-03:00)  
**Project:** Public Compliance Data Analysis (MBA Thesis)  
**Bucket:** `enok-mba-thesis-datalake`

---

## Executive Summary

✅ **All 5 Gold layer transformations completed successfully**

| Dataset | Status | Records | Description |
|---------|--------|---------|-------------|
| agg_municipality_socioeconomic | ✅ SUCCESS | 5,570 | Municipality-level socioeconomic metrics with change indicators |
| agg_state_summary | ✅ SUCCESS | 27 | State-level aggregations with sanctions per capita |
| agg_sanctions_summary | ✅ SUCCESS | 3 | Sanctions aggregations by registry type |
| analysis_compliance | ✅ SUCCESS | 27 | Analysis-ready dataset for regression/correlation |
| consolidated_clustering | ✅ SUCCESS | 5,565 | Municipality-level consolidated dataset for clustering (clean + normalized) |

**Total Records:** 11,192  
**Success Rate:** 100%

---

## Execution Log Summary

### Latest Execution: 2026-02-19 18:51

All transformations completed successfully.

1. **gold_municipality_socioeconomic** (01:27:01)
   - Input: 5,570 municipalities
   - Output: 5,570 records
   - Sources: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanitation
   - **Literacy 2010 coverage: 99.9%** (5,565/5,570)

2. **gold_state_summary** (01:05:10)
   - Input: 5,570 municipalities
   - Output: 27 states
   - Sources: dim_municipalities, fact_population, fact_income, fact_sanctions

3. **gold_sanctions_summary** (01:05:13)
   - Input: 32 sanctions
   - Output: 3 registry types (CEIS, CNEP, CEPIM)
   - Sources: fact_sanctions

4. **gold_analysis_compliance** (01:27:14)
   - Input: aggregated from multiple sources
   - Output: 27 states
   - Sources: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanctions
   - **All columns 100% populated**

5. **gold_consolidated_clustering** (02:15:43)
   - Input: 5,570 municipalities
   - Output: 5,565 records
   - Dropped: 5 (missing values)
   - Sources: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanitation

---

## Dataset Details

### 1. agg_municipality_socioeconomic

**Purpose:** Municipality-level socioeconomic aggregation with 2010→2022 change metrics

**Schema:**
- `municipality_code` (string) - 7-digit IBGE code
- `municipality_name` (string) - Municipality name
- `state_code` (string) - 2-digit state code
- `state_name` (string) - State name
- `region_code` (string) - Region code (1-5)
- `region_name` (string) - Region name
- `population_2010` (integer) - Population in 2010
- `population_2022` (integer) - Population in 2022
- `population_change_pct` (float) - Population change percentage
- `literacy_rate_2010` (float) - Literacy rate 2010 (%)
- `literacy_rate_2022` (float) - Literacy rate 2022 (%)
- `literacy_change_pp` (float) - Literacy change in percentage points
- `avg_income_2010` (float) - Average income 2010 (BRL)
- `avg_income_2022` (float) - Average income 2022 (BRL)
- `income_change_pct` (float) - Income change percentage
- `households_2010` (integer) - Total households 2010
- `households_2022` (integer) - Total households 2022
- `households_change_pct` (float) - Households change percentage

**Coverage:**
- ✅ All 5,570 Brazilian municipalities
- ✅ All 27 states represented
- ✅ All 5 regions covered

**Use Cases:**
- Municipality-level trend analysis
- Identifying fastest/slowest growing municipalities
- Socioeconomic development patterns
- Baseline for compliance correlation studies

---

### 2. agg_state_summary

**Purpose:** State-level aggregated summaries for regional compliance analysis

**Schema:**
- `state_code` (string) - 2-digit state code
- `state_name` (string) - State name
- `region_code` (string) - Region code
- `region_name` (string) - Region name
- `municipality_count` (integer) - Number of municipalities
- `total_population_2010` (integer) - Total population 2010
- `total_population_2022` (integer) - Total population 2022
- `population_change_pct` (float) - Population change percentage
- `avg_income_2022` (float) - Average income 2022 (BRL)
- `total_sanctions` (integer) - Total sanctions count
- `sanctions_pf` (integer) - Sanctions against individuals
- `sanctions_pj` (integer) - Sanctions against companies
- `sanctions_per_100k` (float) - Sanctions per 100k population

**Coverage:**
- ✅ All 27 Brazilian states
- ✅ All 5 regions represented
- ✅ Sanctions data integrated (where available)

**Key Metrics:**
- Total municipalities: 5,570
- Total population 2022: ~214 million (aggregated)
- States with sanctions data: Varies by registry coverage

**Use Cases:**
- State-level compliance comparisons
- Regional analysis
- Sanctions rate benchmarking
- Policy effectiveness evaluation

---

### 3. agg_sanctions_summary

**Purpose:** Sanctions aggregations by registry type with state breakdown

**Schema:**
- `registry_type` (string) - Registry source (CEIS, CNEP, CEPIM)
- `total_sanctions` (integer) - Total sanctions count
- `sanctions_pf` (integer) - Sanctions against individuals (PF)
- `sanctions_pj` (integer) - Sanctions against companies (PJ)
- `pj_ratio_pct` (float) - Percentage of PJ sanctions
- `unique_agencies` (integer) - Unique sanctioning agencies
- `earliest_sanction` (date) - Earliest sanction date
- `latest_sanction` (date) - Latest sanction date
- `[state_columns]` (integer) - Sanctions count per state (dynamic)

**Coverage:**
- ✅ 3 registry types with data (CEIS, CNEP, CEPIM)
- ✅ 32 total sanctions processed
- ✅ State-level breakdown included

**Registry Breakdown:**
- CEIS: Ineligible and Suspended Companies
- CNEP: National Registry of Punished Companies
- CEPIM: Registry of Impediment to Contract

**Note:** CEAF (Registry of Expelled Federal Agents) was excluded due to lack of location data.

**Use Cases:**
- Understanding sanctions distribution by registry
- Comparing registry characteristics
- Temporal analysis of sanctions
- Geographic distribution analysis

---

### 4. analysis_compliance

**Purpose:** Analysis-ready dataset for regression and correlation analysis

**Schema:**
- `state_code` (string) - 2-digit state code
- `state_name` (string) - State name
- `region_code` (string) - Region code
- `region_name` (string) - Region name
- `n_municipalities` (integer) - Number of municipalities
- `population` (integer) - Total population 2022
- `avg_literacy_rate` (float) - Average literacy rate 2022 (%)
- `avg_income` (float) - Average income 2022 (BRL)
- `n_sanctions` (integer) - Total sanctions count
- `n_sanctions_ceis` (integer) - CEIS sanctions
- `n_sanctions_cnep` (integer) - CNEP sanctions
- `n_sanctions_cepim` (integer) - CEPIM sanctions
- `sanctions_per_100k` (float) - Sanctions per 100k population
- `log_population` (float) - Log-transformed population
- `log_income` (float) - Log-transformed income
- `is_norte` (integer) - Dummy: Norte region
- `is_nordeste` (integer) - Dummy: Nordeste region
- `is_sudeste` (integer) - Dummy: Sudeste region
- `is_sul` (integer) - Dummy: Sul region
- `is_centro_oeste` (integer) - Dummy: Centro-Oeste region

**Coverage:**
- ✅ All 27 states
- ✅ All 5 regions with dummy variables
- ✅ Complete socioeconomic indicators
- ✅ Sanctions data integrated
- ✅ Log transformations for regression

**Feature Engineering:**
- Normalized metrics (per 100k population)
- Log transformations for skewed distributions
- Regional dummy variables for regression
- Sanctions breakdown by registry type

**Use Cases:**
- **Regression Analysis:** `sanctions_per_100k ~ log_income + avg_literacy_rate + region_dummies`
- **Correlation Analysis:** Socioeconomic factors vs compliance outcomes
- **Regional Comparisons:** Control for regional effects
- **Policy Research:** Evidence-based compliance interventions

---

## Data Lineage

### Source Dependencies

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Normalized Tables)
    ├── dim_municipalities (5,570 records)
    ├── fact_population (11,135 records: 2010 + 2022)
    ├── fact_literacy (11,135 records: 2010 + 2022)
    ├── fact_income (11,135 records: 2010 + 2022)
    ├── fact_sanitation (11,135 records: 2010 + 2022)
    └── fact_sanctions (32 records)
    ↓
Gold Layer (Analytics-Ready)
    ├── agg_municipality_socioeconomic (5,570 records)
    ├── agg_state_summary (27 records)
    ├── agg_sanctions_summary (3 records)
    └── analysis_compliance (27 records)
```

### Transformation Logic

**Municipality Socioeconomic:**
1. Start with dim_municipalities as base
2. Join population data (2010 & 2022)
3. Join literacy data (2010 & 2022)
4. Join income data (2010 & 2022)
5. Join sanitation data (2010 & 2022)
6. Calculate change metrics (%, pp)

**State Summary:**
1. Extract unique states from dim_municipalities
2. Count municipalities per state
3. Aggregate population by state (sum)
4. Aggregate income by state (mean)
5. Count sanctions by state
6. Calculate sanctions per 100k population

**Sanctions Summary:**
1. Group sanctions by registry_type
2. Count total, PF, PJ sanctions
3. Calculate PJ ratio
4. Find earliest/latest sanction dates
5. Pivot state breakdown

**Analysis Compliance:**
1. Build state-level base from municipalities
2. Aggregate population (sum), literacy (mean), income (mean)
3. Count sanctions by registry type
4. Calculate normalized metrics (per 100k)
5. Apply log transformations
6. Generate regional dummy variables

---

## Smart Caching Implementation

Gold layer inherits smart caching from BaseTransformer:

### Metadata Tracking
Each output table stores metadata:
```json
{
  "source_files": {
    "silver/dim_municipalities/data.parquet": "object_digest_1",
    "silver/fact_population/data.parquet": "object_digest_2",
    ...
  },
  "last_updated": "2026-02-03 19:28:01",
  "record_count": 5570
}
```

### Skip Logic
- ✅ Output exists + sources unchanged → **SKIP** (seconds)
- ✅ Output missing or sources changed → **PROCESS** (minutes)

### Benefits
- **Fast re-runs:** ~10-15 seconds when unchanged
- **Idempotent:** Safe to run multiple times
- **Incremental:** Only reprocess what changed
- **Audit trail:** Metadata shows processing history

---

## Performance Metrics

### Execution Time
- **First run:** ~10-15 seconds (all 4 transformations)
- **Subsequent runs (unchanged):** ~5-10 seconds (all skipped)
- **Partial update:** Varies by changed tables

### Resource Usage
- **Memory:** Minimal (streaming from S3)
- **Storage:** 
  - Parquet files: ~2-5 MB total
  - JSON files: ~5-10 MB total
  - Metadata: <1 KB per table

### API Calls
- **S3 reads:** 1-2 per source table
- **S3 writes:** 2-3 per output table (parquet + json + metadata)
- **Total S3 operations:** ~20-30 per full run

---

## Data Quality Checks

### Validation Rules
1. **Schema Validation:** All tables validated against `silver_schemas.json`
2. **Type Enforcement:** Integers, floats, strings properly typed
3. **Null Handling:** Graceful handling of missing data
4. **Deduplication:** Proper grouping prevents duplicates

### Aggregation Integrity
- **Population:** Sum across municipalities matches state totals
- **Averages:** Weighted properly (not affected by municipality count)
- **Sanctions:** Unique count prevents double-counting
- **Percentages:** Protected against division by zero

### Edge Cases Handled
- Missing census data → NULL values
- Zero population → NULL for per-capita metrics
- Zero income → NULL for log transformations
- Missing sanctions → 0 counts (not NULL)

---

## Known Limitations

### Data Coverage
1. **Sanctions Geography:** Many sanctions lack state/municipality codes
   - Only sanctions with state_code included in state summaries
   - Municipality-level sanctions analysis limited

2. **Federal Transfers:** Not included in Gold layer
   - Source coverage now spans the full intercensal window (2010-2022)
   - Not yet modeled into current Gold aggregations

3. **Census Years:** Only 2010 and 2022
   - No intermediate years for trend analysis
   - 12-year gap limits granularity

### Analysis Constraints
1. **State-Level Focus:** Most analysis at state level due to sanctions data limitations
2. **Cross-Sectional:** Limited temporal analysis (only 2 census years)
3. **Sanctions Sample Size:** Small sample (30 sanctions) may limit statistical power

---

## Validation & Testing

### Automated Tests
```bash
# Run Gold layer tests
pytest tests/processing/test_gold_transformer.py -v

# Expected coverage:
# - GoldTransformer initialization
# - Change percentage calculations
# - Aggregation logic
# - Feature engineering
# - Schema validation
```

### Manual Validation Checklist
- [x] All 4 transformations completed successfully
- [x] Record counts match expectations
- [x] No duplicate records in outputs
- [x] Change metrics calculated correctly
- [x] Log transformations handle edge cases
- [x] Regional dummies sum to 1 per state
- [x] Sanctions per 100k properly normalized
- [x] Metadata files created for all outputs

---

## Next Steps

### Immediate
1. ✅ Gold layer complete and validated
2. ⏭️ Create visualizations and dashboards
3. ⏭️ Perform statistical analysis (correlation, regression)
4. ⏭️ Generate thesis findings and insights

### Future Enhancements
1. **Extended Analysis:**
   - Time-series analysis if more census years become available
   - Municipality-level analysis if sanctions data improves

2. **Additional Metrics:**
   - Sanitation coverage change metrics
   - Composite socioeconomic indices
   - Compliance risk scores

3. **Machine Learning:**
   - Classification models for compliance risk
   - Clustering for municipality segmentation
   - Predictive models for sanctions likelihood

4. **Geographic Analysis:**
   - Spatial autocorrelation
   - Geographic clustering
   - Regional spillover effects

---

## S3 Output Structure

```
s3://enok-mba-thesis-datalake/gold/
├── agg_municipality_socioeconomic/
│   ├── data.parquet          # 5,570 municipalities
│   ├── data.json             # JSON format for compatibility
│   └── _metadata.json        # Processing metadata
├── agg_state_summary/
│   ├── data.parquet          # 27 states
│   ├── data.json
│   └── _metadata.json
├── agg_sanctions_summary/
│   ├── data.parquet          # 2 registry types
│   ├── data.json
│   └── _metadata.json
└── analysis_compliance/
    ├── data.parquet          # 27 states (analysis-ready)
    ├── data.json
    └── _metadata.json
```

---

## Audit Trail

All Gold layer transformations are logged to `docs/processing.log`:

```
[2026-02-03 19:28:01] Silver Transformation: gold_municipality_socioeconomic
Status: SUCCESS
Source(s): silver/dim_municipalities/data.parquet, silver/fact_population/data.parquet, ...
Output: gold/agg_municipality_socioeconomic/data.parquet
Records In: 5570
Records Out: 5570
--------------------------------------------------
[2026-02-03 19:28:04] Silver Transformation: gold_state_summary
Status: SUCCESS
Source(s): silver/dim_municipalities/data.parquet, silver/fact_population/data.parquet, ...
Output: gold/agg_state_summary/data.parquet
Records In: 5570
Records Out: 27
--------------------------------------------------
[2026-02-03 19:28:06] Silver Transformation: gold_sanctions_summary
Status: SUCCESS
Source(s): silver/fact_sanctions/data.parquet
Output: gold/agg_sanctions_summary/data.parquet
Records In: 30
Records Out: 2
--------------------------------------------------
[2026-02-03 19:28:09] Silver Transformation: gold_analysis_compliance
Status: SUCCESS
Source(s): silver/dim_municipalities/data.parquet, silver/fact_population/data.parquet, ...
Output: gold/analysis_compliance/data.parquet
Records In: 0
Records Out: 27
--------------------------------------------------
```

---

## Conclusion

✅ **Gold Layer Status: COMPLETE**

All 4 Gold layer transformations have been successfully implemented and executed:
- **5,570** municipalities with socioeconomic change metrics
- **27** states with compliance summaries
- **2** registry types with sanctions aggregations
- **27** states ready for statistical analysis

The Gold layer provides analysis-ready datasets for:
- Correlation analysis between socioeconomic factors and compliance
- Regression modeling with proper feature engineering
- Regional comparisons with control variables
- Evidence-based policy recommendations

**Ready for:** Statistical analysis, visualization, and thesis findings generation.

---

**Report Generated:** 2026-02-19 (UTC-03:00)  
**Pipeline Status:** Bronze ✅ → Silver ✅ → Gold ✅
