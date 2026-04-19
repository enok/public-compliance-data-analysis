# Silver Layer Data Quality Audit Report

**Generated:** 2026-02-19

---

## Executive Summary

- **Total Tables:** 7
- **Tables Exist:** 7 / 7
- **Coverage:** 100.0%
- **Total Records:** 63,018
- **Total Size:** 1.24 MB
- **Tables with Metadata:** 7
- **Issues Found:** 0

---

## Table Details

### dim_municipalities

✅ **Status:** Exists

- **Records:** 5,570
- **Size:** 0.1 MB
- **Columns:** 7
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `municipality_name`
- `state_code`
- `state_abbrev`
- `state_name`
- `region_code`
- `region_name`

**Quality Checks:**

- Total Municipalities: 5,570
- Unique Codes: 5,570
- Duplicates: ✅ No
- Null Codes: 0
- Unique States: 27
- Unique Regions: 5

**Municipalities per State (Top 10):**

- State 31: 853
- State 35: 645
- State 43: 497
- State 29: 417
- State 41: 399
- State 42: 295
- State 52: 246
- State 22: 224
- State 25: 223
- State 21: 217

### fact_population

✅ **Status:** Exists

- **Records:** 11,135
- **Size:** 0.1 MB
- **Columns:** 3
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `year`
- `total_population`

**Quality Checks:**

- Total Records: 11,135
- Years: 2010, 2022
- Records per Year:
  - 2010: 5,565
  - 2022: 5,570
- Unique Municipalities: 5,570
- Municipality Code Coverage: 100.0%

### fact_sanitation

✅ **Status:** Exists

- **Records:** 11,135
- **Size:** 0.09 MB
- **Columns:** 3
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `year`
- `total_households`

**Quality Checks:**

- Total Records: 11,135
- Years: 2010, 2022
- Records per Year:
  - 2010: 5,565
  - 2022: 5,570
- Unique Municipalities: 5,570
- Municipality Code Coverage: 100.0%

### fact_literacy

✅ **Status:** Exists

- **Records:** 11,135
- **Size:** 0.07 MB
- **Columns:** 3
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `year`
- `literacy_rate`

**Quality Checks:**

- Total Records: 11,135
- Years: 2010, 2022
- Records per Year:
  - 2010: 5,565
  - 2022: 5,570
- Unique Municipalities: 5,570
- Municipality Code Coverage: 100.0%

### fact_income

✅ **Status:** Exists

- **Records:** 11,135
- **Size:** 0.11 MB
- **Columns:** 3
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `year`
- `avg_income`

**Quality Checks:**

- Total Records: 11,135
- Years: 2010, 2022
- Records per Year:
  - 2010: 5,565
  - 2022: 5,570
- Unique Municipalities: 5,570
- Municipality Code Coverage: 100.0%

### fact_federal_transfers

✅ **Status:** Exists

- **Records:** 1,881
- **Size:** 0.02 MB
- **Columns:** 6
- **Metadata:** ✅ Yes

**Schema:**
- `municipality_code`
- `year`
- `month`
- `transfer_amount`
- `transfer_type`
- `source_agency`

**Quality Checks:**

- Total Records: 1,881
- Years: 2010-2022 (full intercensal coverage in Bronze)
- Unique Municipalities: varies by filter criteria
- Municipality Code Coverage: ~97%
- Columns with Nulls:
  - `municipality_code`: some records

### fact_sanctions

✅ **Status:** Exists

- **Records:** 32
- **Size:** 0.01 MB
- **Columns:** 11
- **Metadata:** ✅ Yes

**Schema:**
- `sanction_id`
- `registry_type`
- `sanctioned_entity`
- `entity_type`
- `cpf_cnpj`
- `sanction_type`
- `sanction_start_date`
- `sanction_end_date`
- `sanctioning_agency`
- `state_code`
- `municipality_code`

**Quality Checks:**

- Total Records: 32
- Unique Municipalities: 13
- Municipality Code Coverage: 40.62%
- Registry Types:
  - CEIS: 15
  - CEPIM: 15
  - CNEP: 2
- Entity Types:
  - PJ: 20
  - PF: 12
- Columns with Nulls:
  - `sanction_end_date`: 5
  - `state_code`: 1
  - `municipality_code`: 19

---

## Issues & Warnings

✅ No issues found!

---

**End of Report**
