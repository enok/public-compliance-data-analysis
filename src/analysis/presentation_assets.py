"""
Helpers to build thesis presentation assets for QGIS and Power BI.

This module centralizes:
- state-level and city-level table preparation for presentation
- cluster mix aggregation by state
- Power BI table bundle generation
- lightweight GIS export helpers for QGIS-ready state GeoJSON
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import tempfile
from typing import Dict, Iterable, Mapping, Optional
import zipfile

import numpy as np
import pandas as pd
import requests

from .city_full_analysis import CityMLAnalysisResult, CityStatisticalAnalysisResult


IBGE_MALHAS_BASE_URL = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/"
)

UF_ABBREV_TO_CODE = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}


@dataclass
class PresentationTablesBundle:
    """Container for Power BI presentation tables."""

    dim_region: pd.DataFrame
    dim_state: pd.DataFrame
    dim_cluster: pd.DataFrame
    fact_state_findings: pd.DataFrame
    fact_city_findings: pd.DataFrame
    fact_cluster_state_mix: pd.DataFrame
    fact_city_peer_benchmark: pd.DataFrame
    fact_model_benchmark: pd.DataFrame


def _format_state_code(value: object) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    as_str = str(value).strip()
    if not as_str:
        return None
    if as_str.isdigit():
        return as_str.zfill(2)
    return UF_ABBREV_TO_CODE.get(as_str.upper(), as_str)


def _clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _json_safe(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (float, int, str, bool)):
        if isinstance(value, float) and np.isnan(value):
            return None
        return value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return str(value)


def build_state_final_findings(
    state_df: pd.DataFrame,
    city_df: pd.DataFrame,
    cluster_assignments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build state-level findings enriched with city-level and cluster metrics.

    Returns:
    - state_findings: one row per state with merged state + city aggregate metrics
    - cluster_state_mix: one row per (state, cluster) with city count and share
    """
    required_state = {"state_code", "state_name", "region_code", "region_name", "sanctions_per_100k"}
    missing_state = sorted(required_state - set(state_df.columns))
    if missing_state:
        raise ValueError(f"Missing required state columns: {missing_state}")

    required_city = {
        "municipality_code",
        "state_code",
        "sanctions_per_100k",
        "avg_income_real_2022_2022_brl",
        "avg_transfer_per_capita",
    }
    missing_city = sorted(required_city - set(city_df.columns))
    if missing_city:
        raise ValueError(f"Missing required city columns: {missing_city}")

    if "municipality_code" not in cluster_assignments.columns or "cluster" not in cluster_assignments.columns:
        raise ValueError("cluster_assignments must include municipality_code and cluster columns.")

    states = state_df.copy()
    states["state_code"] = states["state_code"].apply(_format_state_code)
    states["sanctions_per_100k_state"] = _clean_numeric(states["sanctions_per_100k"])
    states = states.drop(columns=["sanctions_per_100k"])

    cities = city_df.copy()
    cities["municipality_code"] = cities["municipality_code"].astype(str)
    cities["state_code"] = cities["state_code"].apply(_format_state_code)
    cities["sanctions_per_100k"] = _clean_numeric(cities["sanctions_per_100k"])
    cities["avg_income_real_2022_2022_brl"] = _clean_numeric(cities["avg_income_real_2022_2022_brl"])
    cities["avg_transfer_per_capita"] = _clean_numeric(cities["avg_transfer_per_capita"])
    cities["n_sanctions"] = _clean_numeric(cities.get("n_sanctions", 0)).fillna(0)

    assignments = cluster_assignments.copy()
    assignments["municipality_code"] = assignments["municipality_code"].astype(str)
    assignments["cluster"] = _clean_numeric(assignments["cluster"]).round().astype("Int64")

    city_enriched = cities.merge(assignments, on="municipality_code", how="left")

    global_city_sanctions_p75 = city_enriched["sanctions_per_100k"].quantile(0.75)
    city_enriched["is_high_sanctions_city"] = (
        city_enriched["sanctions_per_100k"] >= global_city_sanctions_p75
    ).astype(int)

    state_city_agg = (
        city_enriched.groupby("state_code", dropna=False)
        .agg(
            n_cities=("municipality_code", "nunique"),
            n_cities_clustered=("cluster", lambda s: int(pd.Series(s).notna().sum())),
            avg_city_sanctions_per_100k=("sanctions_per_100k", "mean"),
            median_city_sanctions_per_100k=("sanctions_per_100k", "median"),
            p90_city_sanctions_per_100k=("sanctions_per_100k", lambda s: float(np.nanpercentile(s, 90))),
            high_sanctions_city_share=("is_high_sanctions_city", "mean"),
            avg_city_income_real_2022_brl=("avg_income_real_2022_2022_brl", "mean"),
            avg_city_transfer_per_capita=("avg_transfer_per_capita", "mean"),
            total_city_sanctions=("n_sanctions", "sum"),
        )
        .reset_index()
    )
    state_city_agg["high_sanctions_city_share"] = state_city_agg["high_sanctions_city_share"] * 100.0

    cluster_city = city_enriched[city_enriched["cluster"].notna()].copy()
    cluster_city["cluster"] = cluster_city["cluster"].astype(int)

    cluster_counts = (
        cluster_city.groupby(["state_code", "cluster"], dropna=False)
        .agg(cities_in_cluster=("municipality_code", "nunique"))
        .reset_index()
    )

    total_clustered_per_state = (
        cluster_counts.groupby("state_code")["cities_in_cluster"].sum().rename("clustered_total")
    )
    cluster_state_mix = cluster_counts.join(total_clustered_per_state, on="state_code")
    cluster_state_mix["cluster_share_pct"] = np.where(
        cluster_state_mix["clustered_total"] > 0,
        (cluster_state_mix["cities_in_cluster"] / cluster_state_mix["clustered_total"]) * 100.0,
        np.nan,
    )
    cluster_state_mix = cluster_state_mix.drop(columns=["clustered_total"]).sort_values(
        ["state_code", "cluster"]
    )

    cluster_share_wide = (
        cluster_state_mix.pivot(index="state_code", columns="cluster", values="cluster_share_pct")
        .add_prefix("cluster_")
        .add_suffix("_share_pct")
        .reset_index()
    )

    dominant_cluster = (
        cluster_state_mix.sort_values(["state_code", "cluster_share_pct", "cluster"], ascending=[True, False, True])
        .groupby("state_code")
        .head(1)
        .rename(
            columns={
                "cluster": "dominant_cluster",
                "cluster_share_pct": "dominant_cluster_share_pct",
            }
        )[["state_code", "dominant_cluster", "dominant_cluster_share_pct"]]
        .reset_index(drop=True)
    )

    state_findings = (
        states.merge(state_city_agg, on="state_code", how="left")
        .merge(dominant_cluster, on="state_code", how="left")
        .merge(cluster_share_wide, on="state_code", how="left")
        .sort_values("state_code")
        .reset_index(drop=True)
    )

    return state_findings, cluster_state_mix.reset_index(drop=True)


def build_powerbi_tables(
    state_findings: pd.DataFrame,
    city_findings: pd.DataFrame,
    cluster_state_mix: pd.DataFrame,
    city_peer_benchmark: pd.DataFrame,
    statistical_result: CityStatisticalAnalysisResult,
    ml_result: CityMLAnalysisResult,
) -> PresentationTablesBundle:
    """Build Power BI star-schema-like tables from analysis outputs."""
    required_state_cols = {"state_code", "state_name", "region_code", "region_name"}
    if missing := sorted(required_state_cols - set(state_findings.columns)):
        raise ValueError(f"state_findings is missing required columns: {missing}")

    if "cluster" not in city_findings.columns:
        raise ValueError("city_findings must include cluster column.")

    dim_region = (
        state_findings[["region_code", "region_name"]]
        .drop_duplicates()
        .sort_values(["region_code", "region_name"])
        .reset_index(drop=True)
    )

    state_optional_cols = [column for column in ["n_municipalities", "population"] if column in state_findings.columns]
    dim_state = (
        state_findings[["state_code", "state_name", "region_code", *state_optional_cols]]
        .drop_duplicates()
        .sort_values("state_code")
        .reset_index(drop=True)
    )

    cluster_ids = sorted([int(value) for value in city_findings["cluster"].dropna().unique().tolist()])
    dim_cluster = pd.DataFrame(
        {
            "cluster_id": cluster_ids,
            "cluster_label": [f"Cluster {cluster_id}" for cluster_id in cluster_ids],
        }
    )

    fact_state_findings = state_findings.copy()

    city_powerbi_cols = [
        "municipality_code",
        "municipality_name",
        "state_code",
        "state_name",
        "region_code",
        "region_name",
        "cluster",
        "population_2022",
        "sanctions_per_100k",
        "n_sanctions",
        "avg_income_real_2022_2022_brl",
        "avg_transfer_per_capita",
        "total_transfers",
        "sanctions_per_million_brl_transfers",
    ]
    existing_city_cols = [column for column in city_powerbi_cols if column in city_findings.columns]
    fact_city_findings = city_findings[existing_city_cols].copy()
    fact_city_findings["cluster"] = pd.to_numeric(fact_city_findings["cluster"], errors="coerce").astype("Int64")

    fact_cluster_state_mix = cluster_state_mix.copy().rename(columns={"cluster": "cluster_id"})
    fact_cluster_state_mix["cluster_id"] = pd.to_numeric(
        fact_cluster_state_mix["cluster_id"], errors="coerce"
    ).astype("Int64")

    fact_city_peer_benchmark = city_peer_benchmark.copy()

    top_corr = statistical_result.correlations.head(1).copy()
    top_ml = ml_result.model_scores.head(1).copy()

    fact_model_benchmark = pd.DataFrame(
        [
            {
                "metric_group": "City OLS",
                "metric_name": "R2",
                "metric_value": float(statistical_result.r_squared),
            },
            {
                "metric_group": "City OLS",
                "metric_name": "Adj_R2",
                "metric_value": float(statistical_result.adj_r_squared),
            },
            {
                "metric_group": "City OLS",
                "metric_name": "Top correlation",
                "metric_value": float(top_corr.iloc[0]["correlation_with_target"]),
            },
            {
                "metric_group": "City OLS",
                "metric_name": "Top correlated feature",
                "metric_value": None,
                "metric_text": str(top_corr.iloc[0]["feature"]),
            },
            {
                "metric_group": "City ML",
                "metric_name": "Best model",
                "metric_value": None,
                "metric_text": str(top_ml.iloc[0]["model"]),
            },
            {
                "metric_group": "City ML",
                "metric_name": "Best test R2",
                "metric_value": float(top_ml.iloc[0]["test_r2"]),
            },
            {
                "metric_group": "City Modeling",
                "metric_name": "N observations",
                "metric_value": float(statistical_result.n_obs),
            },
        ]
    )
    if "metric_text" not in fact_model_benchmark.columns:
        fact_model_benchmark["metric_text"] = None

    return PresentationTablesBundle(
        dim_region=dim_region,
        dim_state=dim_state,
        dim_cluster=dim_cluster,
        fact_state_findings=fact_state_findings,
        fact_city_findings=fact_city_findings,
        fact_cluster_state_mix=fact_cluster_state_mix,
        fact_city_peer_benchmark=fact_city_peer_benchmark,
        fact_model_benchmark=fact_model_benchmark,
    )


def build_powerbi_measures_dax() -> str:
    """Return a practical DAX starter pack for the thesis dashboard."""
    return """-- Thesis dashboard measure pack
Total States = DISTINCTCOUNT('dim_state'[state_code])

Total Cities = DISTINCTCOUNT('fact_city_findings'[municipality_code])

Total Sanctions (State) = SUM('fact_state_findings'[n_sanctions])

Average State Sanctions per 100k = AVERAGE('fact_state_findings'[sanctions_per_100k_state])

Average City Sanctions per 100k = AVERAGE('fact_city_findings'[sanctions_per_100k])

Median City Sanctions per 100k = MEDIAN('fact_city_findings'[sanctions_per_100k])

Average City Real Income (2022 BRL) = AVERAGE('fact_city_findings'[avg_income_real_2022_2022_brl])

Average Transfer per Capita = AVERAGE('fact_city_findings'[avg_transfer_per_capita])

Dominant Cluster Share (Avg %) = AVERAGE('fact_state_findings'[dominant_cluster_share_pct])

High-Sanctions Cities Share (Avg %) = AVERAGE('fact_state_findings'[high_sanctions_city_share])

Best City ML Test R2 =
MAXX(
    FILTER(
        'fact_model_benchmark',
        'fact_model_benchmark'[metric_group] = "City ML"
            && 'fact_model_benchmark'[metric_name] = "Best test R2"
    ),
    'fact_model_benchmark'[metric_value]
)

City OLS R2 =
MAXX(
    FILTER(
        'fact_model_benchmark',
        'fact_model_benchmark'[metric_group] = "City OLS"
            && 'fact_model_benchmark'[metric_name] = "R2"
    ),
    'fact_model_benchmark'[metric_value]
)
"""


def download_ibge_boundary_zip(filename: str, output_dir: Path, timeout_seconds: int = 120) -> Path:
    """
    Download IBGE boundary ZIP by filename from official geoftp endpoint.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / filename
    url = f"{IBGE_MALHAS_BASE_URL}{filename}"

    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def _extract_state_code_from_properties(properties: Mapping[str, object]) -> Optional[str]:
    code_fields = ["state_code", "CD_UF", "cd_uf", "CD_GEOCUF", "cd_geocuf"]
    for field in code_fields:
        if field in properties and properties[field] is not None:
            code = _format_state_code(properties[field])
            if code:
                return code

    for field in ["SIGLA_UF", "sigla_uf"]:
        if field in properties and properties[field] is not None:
            abbrev_code = _format_state_code(properties[field])
            if abbrev_code and abbrev_code.isdigit():
                return abbrev_code

    return None


def build_state_geojson_from_shapefile(
    state_zip_path: Path,
    state_findings: pd.DataFrame,
    output_geojson_path: Path,
) -> Path:
    """
    Build a GeoJSON state layer enriched with thesis findings.

    Requires `pyshp` (package `shapefile`) available in the environment.
    """
    try:
        import shapefile  # type: ignore
    except ImportError as exc:  # pragma: no cover - tested by integration use
        raise ImportError(
            "Missing dependency `pyshp`. Install with `pip install pyshp>=2.3.1`."
        ) from exc

    state_findings_work = state_findings.copy()
    state_findings_work["state_code"] = state_findings_work["state_code"].apply(_format_state_code)
    metrics_map = {
        row["state_code"]: {k: _json_safe(v) for k, v in row.items() if k != "state_code"}
        for row in state_findings_work.to_dict(orient="records")
    }

    with tempfile.TemporaryDirectory(prefix="ibge_state_shape_") as tmpdir:
        with zipfile.ZipFile(state_zip_path) as archive:
            archive.extractall(tmpdir)

        shp_candidates = sorted(Path(tmpdir).glob("*.shp"))
        if not shp_candidates:
            raise ValueError(f"No .shp file found in {state_zip_path}")

        reader = shapefile.Reader(str(shp_candidates[0]))
        fields = [field[0] for field in reader.fields[1:]]

        features = []
        for shape_record in reader.shapeRecords():
            properties = {
                field_name: _json_safe(value)
                for field_name, value in zip(fields, shape_record.record)
            }

            state_code = _extract_state_code_from_properties(properties)
            if state_code:
                properties["state_code"] = state_code
                properties.update(metrics_map.get(state_code, {}))

            feature = {
                "type": "Feature",
                "properties": properties,
                "geometry": shape_record.shape.__geo_interface__,
            }
            features.append(feature)
        
        reader.close()  # Close before temp directory cleanup (Windows file lock fix)

    output_geojson_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"type": "FeatureCollection", "features": features}
    output_geojson_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return output_geojson_path


def write_powerbi_table_bundle(bundle: PresentationTablesBundle, output_tables_dir: Path) -> Dict[str, Path]:
    """Write Power BI tables to CSV files and return written file map."""
    output_tables_dir.mkdir(parents=True, exist_ok=True)

    table_map = {
        "dim_region": bundle.dim_region,
        "dim_state": bundle.dim_state,
        "dim_cluster": bundle.dim_cluster,
        "fact_state_findings": bundle.fact_state_findings,
        "fact_city_findings": bundle.fact_city_findings,
        "fact_cluster_state_mix": bundle.fact_cluster_state_mix,
        "fact_city_peer_benchmark": bundle.fact_city_peer_benchmark,
        "fact_model_benchmark": bundle.fact_model_benchmark,
    }

    written_files: Dict[str, Path] = {}
    for table_name, dataframe in table_map.items():
        path = output_tables_dir / f"{table_name}.csv"
        dataframe.to_csv(path, index=False)
        written_files[table_name] = path

    return written_files


def write_text_files(files_to_content: Mapping[Path, str]) -> None:
    """Write a set of UTF-8 text files."""
    for path, content in files_to_content.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def summarize_file_paths(paths: Iterable[Path]) -> str:
    """Build a line-oriented summary string for CLI logs."""
    sorted_paths = sorted({str(path) for path in paths})
    lines = ["Generated files:"]
    lines.extend([f"  - {path}" for path in sorted_paths])
    return "\n".join(lines)


__all__ = [
    "IBGE_MALHAS_BASE_URL",
    "PresentationTablesBundle",
    "build_state_final_findings",
    "build_powerbi_tables",
    "build_powerbi_measures_dax",
    "download_ibge_boundary_zip",
    "build_state_geojson_from_shapefile",
    "write_powerbi_table_bundle",
    "write_text_files",
    "summarize_file_paths",
]
