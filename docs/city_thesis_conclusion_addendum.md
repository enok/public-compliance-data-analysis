# City Thesis Conclusion Addendum (Municipality-Level Supervised Analysis)

**Run date:** April 8, 2026  
**Command:** `python scripts/run_city_full_analysis.py --aws-profile '' --output-dir /tmp/city_full_analysis_run`  
**Dataset:** `gold/analysis_compliance_municipality`

## Evidence Snapshot

- Municipality sample used in modeling: **5,570**
- Strongest linear association with `sanctions_per_100k`: **`log_income`** (`r = 0.149`)
- OLS fit (HC3 robust errors): **R² = 0.024**, adjusted **R² = 0.023**
- Best ML benchmark model: **ElasticNet**, test **R² = 0.030**

## Cluster Outcome Contrast

- Cluster 0: `n=821`, avg sanctions/100k `= 5.514`, avg transfers `= 114,117.40`
- Cluster 1: `n=2,158`, avg sanctions/100k `= 1.845`, avg transfers `= 2,263.60`
- Cluster 2: `n=2,584`, avg sanctions/100k `= 7.583`, avg transfers `= 1,172.09`
- Cluster 3: `n=2`, avg sanctions/100k `= 0.367`, avg transfers `= 4,298,406.99`

## Interpretation

The city-level extension preserves the direction of the core thesis narrative (income/capacity proxies remain relevant), but explanatory power is low at municipality level. This supports a cautious interpretation centered on heterogeneity, sparse geolocation coverage in sanctions records, and the need for richer predictors and temporal designs.
