"""
Analysis module for statistical analysis and machine learning.
"""

from .data_loader import GoldDataLoader
from .city_clustering import (
    CityClusteringResult,
    DEFAULT_CLUSTERING_FEATURES,
    build_same_cluster_peer_table,
    cluster_cities,
    compare_cities_in_same_cluster,
    evaluate_k_range,
)
from .city_full_analysis import (
    CityMLAnalysisResult,
    CityStatisticalAnalysisResult,
    DEFAULT_CITY_FEATURE_COLUMNS,
    DEFAULT_CITY_TARGET_COLUMN,
    build_city_thesis_conclusion_markdown,
    prepare_city_modeling_dataframe,
    run_city_ml_analysis,
    run_city_statistical_analysis,
    summarize_city_clusters_for_outcomes,
)
from .presentation_assets import (
    IBGE_MALHAS_BASE_URL,
    PresentationTablesBundle,
    build_powerbi_measures_dax,
    build_powerbi_tables,
    build_state_final_findings,
    build_state_geojson_from_shapefile,
    download_ibge_boundary_zip,
    summarize_file_paths,
    write_powerbi_table_bundle,
    write_text_files,
)

__all__ = [
    "GoldDataLoader",
    "CityClusteringResult",
    "DEFAULT_CLUSTERING_FEATURES",
    "build_same_cluster_peer_table",
    "build_city_thesis_conclusion_markdown",
    "cluster_cities",
    "compare_cities_in_same_cluster",
    "DEFAULT_CITY_FEATURE_COLUMNS",
    "DEFAULT_CITY_TARGET_COLUMN",
    "CityMLAnalysisResult",
    "CityStatisticalAnalysisResult",
    "evaluate_k_range",
    "prepare_city_modeling_dataframe",
    "run_city_ml_analysis",
    "run_city_statistical_analysis",
    "summarize_city_clusters_for_outcomes",
    "IBGE_MALHAS_BASE_URL",
    "PresentationTablesBundle",
    "build_powerbi_measures_dax",
    "build_powerbi_tables",
    "build_state_final_findings",
    "build_state_geojson_from_shapefile",
    "download_ibge_boundary_zip",
    "summarize_file_paths",
    "write_powerbi_table_bundle",
    "write_text_files",
]
