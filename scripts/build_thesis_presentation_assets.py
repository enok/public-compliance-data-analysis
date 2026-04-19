#!/usr/bin/env python3
"""
Build thesis presentation assets for:
- QGIS (Brazil map layers + join-ready findings)
- Power BI (star-schema CSVs + DAX starter measures + dashboard storyboard)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

# Ensure `src` imports work when running as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.city_clustering import build_same_cluster_peer_table, cluster_cities
from src.analysis.city_full_analysis import (
    run_city_ml_analysis,
    run_city_statistical_analysis,
    summarize_city_clusters_for_outcomes,
)
from src.analysis.data_loader import GoldDataLoader
from src.analysis.presentation_assets import (
    build_powerbi_measures_dax,
    build_powerbi_tables,
    build_state_final_findings,
    build_state_geojson_from_shapefile,
    download_ibge_boundary_zip,
    summarize_file_paths,
    write_powerbi_table_bundle,
    write_text_files,
)


def _build_qgis_readme() -> str:
    return """# QGIS Thesis Map Package

This folder includes a ready map layer for Brazil states and tables to map city-level findings.

## Files

- `brazil_states_final_findings.geojson`: state polygons already enriched with thesis metrics.
- `state_findings_table.csv`: same state metrics in tabular format.
- `city_findings_with_cluster.csv`: municipality metrics + cluster assignment.
- `city_cluster_k_diagnostics.csv`: silhouette/inertia by tested K.
- `city_cluster_outcome_summary.csv`: average sanctions and transfers by city cluster.
- `city_same_cluster_peers.csv`: nearest peer municipalities inside each cluster.
- `BR_UF_2022.zip`: official IBGE states boundary source.
- `BR_Municipios_2022.zip` (optional): official IBGE municipalities polygons.

## Recommended QGIS Layers (presentation)

1. Load `brazil_states_final_findings.geojson`.
2. Style (graduated) by `sanctions_per_100k_state` with 5 classes (Quantile).
3. Label by `state_name`.
4. Add a second graduated style by `dominant_cluster` (categorical) for a cluster narrative map.
5. If municipality polygons are available:
   - load `BR_Municipios_2022.zip` directly in QGIS
   - join with `city_findings_with_cluster.csv` using:
     - polygon field: `CD_MUN`
     - table field: `municipality_code`
   - style by `cluster` or `sanctions_per_100k`.

## Suggested Presentation Captions

- "State-level sanctions intensity (per 100k) with city-level cluster mix enrichment."
- "Dominant city cluster by state and share of municipalities in the dominant profile."
- "Municipality peer benchmarking inside cluster-defined comparable groups."
"""


def _build_powerbi_relationships_md() -> str:
    return """# Power BI Data Model Relationships

Create these relationships after importing CSV tables from `tables/`:

1. `dim_region[region_code]` 1->* `dim_state[region_code]`
2. `dim_state[state_code]` 1->* `fact_state_findings[state_code]`
3. `dim_state[state_code]` 1->* `fact_city_findings[state_code]`
4. `dim_state[state_code]` 1->* `fact_cluster_state_mix[state_code]`
5. `dim_cluster[cluster_id]` 1->* `fact_city_findings[cluster]`
6. `dim_cluster[cluster_id]` 1->* `fact_cluster_state_mix[cluster_id]`

Optional:

- Keep `fact_city_peer_benchmark` disconnected for drillthrough pages,
  or connect by municipality code using a dedicated municipality dimension if desired.
"""


def _build_powerbi_storyboard_md(
    n_states: int,
    n_cities: int,
    selected_k: int,
    selected_k_silhouette: float,
    city_ols_r2: float,
    city_ml_best_model: str,
    city_ml_best_r2: float,
) -> str:
    return f"""# Power BI Thesis Dashboard Storyboard

## Page 1 - Executive Summary

Cards:
- Total States: {n_states}
- Total Cities: {n_cities}
- City OLS R2: {city_ols_r2:.3f}
- Best City ML Model: {city_ml_best_model} (R2={city_ml_best_r2:.3f})
- Selected City Cluster K: {selected_k} (silhouette={selected_k_silhouette:.3f})

Visuals:
- Filled map by `sanctions_per_100k_state`
- Bar chart: top/bottom states by `sanctions_per_100k_state`
- Scatter: `avg_city_income_real_2022_brl` vs `avg_city_sanctions_per_100k`

## Page 2 - State Findings

Visuals:
- Matrix by state with:
  - `sanctions_per_100k_state`
  - `avg_city_sanctions_per_100k`
  - `high_sanctions_city_share`
  - `dominant_cluster`
  - `dominant_cluster_share_pct`
- Cluster-share stacked bar by state (`fact_cluster_state_mix`)

## Page 3 - City Findings

Visuals:
- Scatter:
  - X: `avg_income_real_2022_2022_brl`
  - Y: `sanctions_per_100k`
  - Legend: `cluster`
  - Size: `population_2022`
- Distribution plot (histogram/box) of city sanctions by cluster

## Page 4 - Cluster Outcomes

Visuals:
- Cluster-level bars:
  - avg sanctions per 100k
  - avg transfers per capita
- Slicer by region/state for contextual segmentation

## Page 5 - Peer Benchmark (Optional Drillthrough)

Visuals:
- Table from `fact_city_peer_benchmark` with:
  - source city
  - peer city
  - peer rank
  - distance
  - similarity score
"""


def _build_powerbi_theme_json() -> str:
    theme = {
        "name": "Thesis Compliance Theme",
        "dataColors": [
            "#0B4F6C",
            "#01A7C2",
            "#7D8597",
            "#F4A259",
            "#EF5B5B",
            "#2E8B57",
            "#6C5CE7",
            "#B08968",
        ],
        "background": "#F7F9FB",
        "foreground": "#202124",
        "tableAccent": "#0B4F6C",
    }
    return json.dumps(theme, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build QGIS + Power BI presentation assets from current thesis analysis datasets."
    )
    parser.add_argument(
        "--bucket-name",
        default="enok-mba-thesis-datalake",
        help="S3 data lake bucket name.",
    )
    parser.add_argument(
        "--aws-profile",
        default="",
        help="AWS profile used by boto3 session (leave empty to use default credentials chain).",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/thesis_presentation_assets",
        help="Output directory for generated presentation assets.",
    )
    parser.add_argument(
        "--min-k",
        type=int,
        default=2,
        help="Minimum K for city clustering diagnostics.",
    )
    parser.add_argument(
        "--max-k",
        type=int,
        default=10,
        help="Maximum K for city clustering diagnostics.",
    )
    parser.add_argument(
        "--peer-top-n",
        type=int,
        default=3,
        help="Nearest peers to keep per city in same-cluster benchmark table.",
    )
    parser.add_argument(
        "--download-municipality-boundaries",
        action="store_true",
        help="Also download BR_Municipios_2022.zip (larger file) for direct municipal polygon mapping in QGIS.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    base_output_dir = Path(args.output_dir)
    qgis_dir = base_output_dir / "qgis"
    powerbi_dir = base_output_dir / "powerbi"
    powerbi_tables_dir = powerbi_dir / "tables"
    qgis_dir.mkdir(parents=True, exist_ok=True)
    powerbi_tables_dir.mkdir(parents=True, exist_ok=True)

    loader = GoldDataLoader(bucket_name=args.bucket_name, aws_profile=args.aws_profile)
    state_df = loader.load_dataset("analysis_compliance")
    city_df = loader.load_dataset("analysis_compliance_municipality")
    cluster_base_df = loader.load_dataset("consolidated_clustering")

    if state_df is None:
        raise RuntimeError(
            "Could not load 'analysis_compliance'. Run Gold transformation first."
        )
    if city_df is None:
        raise RuntimeError(
            "Could not load 'analysis_compliance_municipality'. Run Gold transformation first."
        )
    if cluster_base_df is None:
        raise RuntimeError(
            "Could not load 'consolidated_clustering'. Run Gold transformation first."
        )

    city_cluster_result = cluster_cities(
        cluster_base_df,
        min_k=args.min_k,
        max_k=args.max_k,
    )
    cluster_assignments = city_cluster_result.clustered_df[["municipality_code", "cluster"]]

    city_with_cluster = city_df.merge(cluster_assignments, on="municipality_code", how="left")
    state_findings, cluster_state_mix = build_state_final_findings(
        state_df=state_df,
        city_df=city_df,
        cluster_assignments=cluster_assignments,
    )

    city_cluster_outcome_summary = summarize_city_clusters_for_outcomes(city_with_cluster)
    city_peer_benchmark = build_same_cluster_peer_table(
        city_cluster_result.clustered_df, top_n=args.peer_top_n
    )

    statistical_result = run_city_statistical_analysis(city_df)
    ml_result = run_city_ml_analysis(city_df)

    powerbi_bundle = build_powerbi_tables(
        state_findings=state_findings,
        city_findings=city_with_cluster,
        cluster_state_mix=cluster_state_mix,
        city_peer_benchmark=city_peer_benchmark,
        statistical_result=statistical_result,
        ml_result=ml_result,
    )

    # QGIS assets
    state_zip_path = download_ibge_boundary_zip("BR_UF_2022.zip", output_dir=qgis_dir)
    state_geojson_path = build_state_geojson_from_shapefile(
        state_zip_path=state_zip_path,
        state_findings=state_findings,
        output_geojson_path=qgis_dir / "brazil_states_final_findings.geojson",
    )

    qgis_written = [
        state_zip_path,
        state_geojson_path,
    ]
    if args.download_municipality_boundaries:
        municipality_zip_path = download_ibge_boundary_zip(
            "BR_Municipios_2022.zip", output_dir=qgis_dir
        )
        qgis_written.append(municipality_zip_path)

    state_findings_path = qgis_dir / "state_findings_table.csv"
    city_with_cluster_path = qgis_dir / "city_findings_with_cluster.csv"
    cluster_diag_path = qgis_dir / "city_cluster_k_diagnostics.csv"
    cluster_summary_path = qgis_dir / "city_cluster_outcome_summary.csv"
    peer_table_path = qgis_dir / "city_same_cluster_peers.csv"

    state_findings.to_csv(state_findings_path, index=False)
    city_with_cluster.to_csv(city_with_cluster_path, index=False)
    city_cluster_result.diagnostics.to_csv(cluster_diag_path, index=False)
    city_cluster_outcome_summary.to_csv(cluster_summary_path, index=False)
    city_peer_benchmark.to_csv(peer_table_path, index=False)
    write_text_files({qgis_dir / "README_qgis_map.md": _build_qgis_readme()})
    qgis_written.extend(
        [
            state_findings_path,
            city_with_cluster_path,
            cluster_diag_path,
            cluster_summary_path,
            peer_table_path,
            qgis_dir / "README_qgis_map.md",
        ]
    )

    # Power BI assets
    powerbi_written_map = write_powerbi_table_bundle(
        bundle=powerbi_bundle,
        output_tables_dir=powerbi_tables_dir,
    )
    dax_measures_path = powerbi_dir / "powerbi_measures.dax"
    relationships_path = powerbi_dir / "powerbi_relationships.md"
    storyboard_path = powerbi_dir / "powerbi_dashboard_storyboard.md"
    theme_path = powerbi_dir / "thesis_theme.json"

    best_model_row = ml_result.model_scores.iloc[0]
    write_text_files(
        {
            dax_measures_path: build_powerbi_measures_dax(),
            relationships_path: _build_powerbi_relationships_md(),
            storyboard_path: _build_powerbi_storyboard_md(
                n_states=len(state_findings),
                n_cities=len(city_with_cluster),
                selected_k=city_cluster_result.selected_k,
                selected_k_silhouette=city_cluster_result.silhouette,
                city_ols_r2=statistical_result.r_squared,
                city_ml_best_model=str(best_model_row["model"]),
                city_ml_best_r2=float(best_model_row["test_r2"]),
            ),
            theme_path: _build_powerbi_theme_json(),
        }
    )

    powerbi_written = list(powerbi_written_map.values()) + [
        dax_measures_path,
        relationships_path,
        storyboard_path,
        theme_path,
    ]

    print("QGIS package")
    print(summarize_file_paths(qgis_written))
    print()
    print("Power BI package")
    print(summarize_file_paths(powerbi_written))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
