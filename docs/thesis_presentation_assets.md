# Thesis Presentation Assets (QGIS + Power BI)

This workflow generates two presentation-ready packages from the current Gold datasets and city-level thesis extension:

- a QGIS package for Brazil map storytelling (states + city cluster context)
- a Power BI package for dashboard storytelling (tables, relationships, DAX starter measures)

## Run Command

```bash
python scripts/build_thesis_presentation_assets.py \
  --aws-profile '' \
  --output-dir docs/thesis_presentation_assets \
  --min-k 2 \
  --max-k 10 \
  --peer-top-n 3 \
  --download-municipality-boundaries
```

Notes:

- `--download-municipality-boundaries` is optional and downloads a larger official IBGE ZIP (`BR_Municipios_2022.zip`).
- State boundaries (`BR_UF_2022.zip`) are always downloaded and converted to a thesis-enriched GeoJSON.

## QGIS Package Output

Generated under `docs/thesis_presentation_assets/qgis/`:

- `brazil_states_final_findings.geojson`
- `state_findings_table.csv`
- `city_findings_with_cluster.csv`
- `city_cluster_k_diagnostics.csv`
- `city_cluster_outcome_summary.csv`
- `city_same_cluster_peers.csv`
- `BR_UF_2022.zip` (IBGE source)
- `BR_Municipios_2022.zip` (optional IBGE source)
- `README_qgis_map.md` (map setup instructions)

Recommended thesis maps:

1. State choropleth by `sanctions_per_100k_state`.
2. State categorical map by `dominant_cluster`.
3. Municipal join map by `cluster` or `sanctions_per_100k` (optional, if municipality ZIP is loaded).

## Power BI Package Output

Generated under `docs/thesis_presentation_assets/powerbi/`:

- `tables/dim_region.csv`
- `tables/dim_state.csv`
- `tables/dim_cluster.csv`
- `tables/fact_state_findings.csv`
- `tables/fact_city_findings.csv`
- `tables/fact_cluster_state_mix.csv`
- `tables/fact_city_peer_benchmark.csv`
- `tables/fact_model_benchmark.csv`
- `powerbi_measures.dax`
- `powerbi_relationships.md`
- `powerbi_dashboard_storyboard.md`
- `thesis_theme.json`

### Suggested Dashboard Pages

1. Executive summary (cards + map + benchmark bars)
2. State findings (matrix + cluster share by state)
3. City findings (scatter and distributions by cluster)
4. Cluster outcomes (sanctions/transfers contrast)
5. Peer benchmark drillthrough (same-cluster nearest peers)

## Data Sources Used

- `gold/analysis_compliance`
- `gold/analysis_compliance_municipality`
- `gold/consolidated_clustering`
- official IBGE boundaries (municipio 2022, Brazil/BR)

## Caveat For Defense Narrative

These visuals support an associational analysis. Keep the thesis framing explicit:

- sanctions indicators are not direct accusations
- observed relationships do not imply causality
- city-level heterogeneity and reporting/detection bias remain key limitations
