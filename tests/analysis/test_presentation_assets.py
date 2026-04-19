"""
Unit tests for thesis presentation asset helpers.
"""

import pandas as pd

from src.analysis.city_full_analysis import CityMLAnalysisResult, CityStatisticalAnalysisResult
from src.analysis.presentation_assets import (
    build_powerbi_measures_dax,
    build_powerbi_tables,
    build_state_final_findings,
)


def _sample_state_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "state_code": "11",
                "state_name": "Rondonia",
                "region_code": "1",
                "region_name": "Norte",
                "n_municipalities": 52,
                "population": 1_815_278,
                "n_sanctions": 110,
                "sanctions_per_100k": 6.06,
            },
            {
                "state_code": "35",
                "state_name": "Sao Paulo",
                "region_code": "3",
                "region_name": "Sudeste",
                "n_municipalities": 645,
                "population": 44_420_459,
                "n_sanctions": 1200,
                "sanctions_per_100k": 2.70,
            },
        ]
    )


def _sample_city_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "municipality_code": "1100015",
                "municipality_name": "Alta Floresta D'Oeste",
                "state_code": "11",
                "state_name": "Rondonia",
                "region_code": "1",
                "region_name": "Norte",
                "population_2022": 24_392,
                "sanctions_per_100k": 8.0,
                "n_sanctions": 2,
                "avg_income_real_2022_2022_brl": 1450.0,
                "avg_transfer_per_capita": 220.0,
                "total_transfers": 5_366_240.0,
                "sanctions_per_million_brl_transfers": 0.3728,
            },
            {
                "municipality_code": "1100023",
                "municipality_name": "Ariquemes",
                "state_code": "11",
                "state_name": "Rondonia",
                "region_code": "1",
                "region_name": "Norte",
                "population_2022": 107_863,
                "sanctions_per_100k": 3.5,
                "n_sanctions": 4,
                "avg_income_real_2022_2022_brl": 1900.0,
                "avg_transfer_per_capita": 180.0,
                "total_transfers": 19_415_340.0,
                "sanctions_per_million_brl_transfers": 0.2060,
            },
            {
                "municipality_code": "3550308",
                "municipality_name": "Sao Paulo",
                "state_code": "35",
                "state_name": "Sao Paulo",
                "region_code": "3",
                "region_name": "Sudeste",
                "population_2022": 11_451_245,
                "sanctions_per_100k": 1.2,
                "n_sanctions": 137,
                "avg_income_real_2022_2022_brl": 3200.0,
                "avg_transfer_per_capita": 44.0,
                "total_transfers": 503_854_780.0,
                "sanctions_per_million_brl_transfers": 0.2719,
            },
            {
                "municipality_code": "3509502",
                "municipality_name": "Campinas",
                "state_code": "35",
                "state_name": "Sao Paulo",
                "region_code": "3",
                "region_name": "Sudeste",
                "population_2022": 1_139_047,
                "sanctions_per_100k": 1.8,
                "n_sanctions": 20,
                "avg_income_real_2022_2022_brl": 2900.0,
                "avg_transfer_per_capita": 60.0,
                "total_transfers": 68_342_820.0,
                "sanctions_per_million_brl_transfers": 0.2926,
            },
        ]
    )


def _sample_cluster_assignments() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"municipality_code": "1100015", "cluster": 2},
            {"municipality_code": "1100023", "cluster": 2},
            {"municipality_code": "3550308", "cluster": 0},
            {"municipality_code": "3509502", "cluster": 1},
        ]
    )


def _sample_statistical_result() -> CityStatisticalAnalysisResult:
    return CityStatisticalAnalysisResult(
        n_obs=4,
        feature_columns=["log_population", "log_income"],
        target_column="sanctions_per_100k",
        correlations=pd.DataFrame(
            [
                {
                    "feature": "log_income",
                    "correlation_with_target": 0.12,
                    "abs_correlation": 0.12,
                }
            ]
        ),
        coefficients=pd.DataFrame(
            [
                {
                    "term": "const",
                    "coefficient": 0.5,
                    "std_error": 0.1,
                    "t_stat": 5.0,
                    "p_value": 0.01,
                }
            ]
        ),
        r_squared=0.24,
        adj_r_squared=0.21,
        f_pvalue=0.03,
    )


def _sample_ml_result() -> CityMLAnalysisResult:
    return CityMLAnalysisResult(
        n_obs=4,
        feature_columns=["log_population", "log_income"],
        target_column="sanctions_per_100k",
        model_scores=pd.DataFrame(
            [
                {"model": "ElasticNet", "test_mae": 0.4, "test_rmse": 0.7, "test_r2": 0.31},
                {"model": "RandomForest", "test_mae": 0.5, "test_rmse": 0.9, "test_r2": 0.20},
            ]
        ),
        feature_importance=pd.DataFrame(
            [
                {"feature": "log_income", "importance": 0.6, "normalized_importance": 0.6},
                {"feature": "log_population", "importance": 0.4, "normalized_importance": 0.4},
            ]
        ),
        best_model_name="ElasticNet",
    )


def test_build_state_final_findings_merges_city_and_cluster_aggregates():
    state_findings, cluster_state_mix = build_state_final_findings(
        state_df=_sample_state_df(),
        city_df=_sample_city_df(),
        cluster_assignments=_sample_cluster_assignments(),
    )

    assert len(state_findings) == 2
    assert set(state_findings["state_code"]) == {"11", "35"}
    assert "sanctions_per_100k_state" in state_findings.columns
    assert "avg_city_sanctions_per_100k" in state_findings.columns
    assert "dominant_cluster" in state_findings.columns
    assert "dominant_cluster_share_pct" in state_findings.columns
    assert state_findings["n_cities"].sum() == 4

    ro = state_findings[state_findings["state_code"] == "11"].iloc[0]
    assert int(ro["dominant_cluster"]) == 2
    assert ro["dominant_cluster_share_pct"] == 100.0

    assert len(cluster_state_mix) == 3
    assert set(cluster_state_mix["state_code"]) == {"11", "35"}


def test_build_powerbi_tables_returns_expected_dimensions_and_facts():
    state_findings, cluster_state_mix = build_state_final_findings(
        state_df=_sample_state_df(),
        city_df=_sample_city_df(),
        cluster_assignments=_sample_cluster_assignments(),
    )
    city_with_cluster = _sample_city_df().merge(
        _sample_cluster_assignments(), on="municipality_code", how="left"
    )
    peer_table = pd.DataFrame(
        [
            {
                "cluster": 2,
                "source_municipality_code": "1100015",
                "source_municipality_name": "Alta Floresta D'Oeste",
                "source_state_code": "11",
                "source_state_name": "Rondonia",
                "peer_rank": 1,
                "peer_municipality_code": "1100023",
                "peer_municipality_name": "Ariquemes",
                "peer_state_code": "11",
                "peer_state_name": "Rondonia",
                "distance": 0.22,
                "similarity_score": 0.82,
            }
        ]
    )

    bundle = build_powerbi_tables(
        state_findings=state_findings,
        city_findings=city_with_cluster,
        cluster_state_mix=cluster_state_mix,
        city_peer_benchmark=peer_table,
        statistical_result=_sample_statistical_result(),
        ml_result=_sample_ml_result(),
    )

    assert len(bundle.dim_region) == 2
    assert len(bundle.dim_state) == 2
    assert set(bundle.dim_cluster["cluster_id"]) == {0, 1, 2}
    assert len(bundle.fact_city_findings) == 4
    assert len(bundle.fact_cluster_state_mix) == 3
    assert len(bundle.fact_model_benchmark) >= 6


def test_build_powerbi_measures_dax_contains_key_measures():
    dax = build_powerbi_measures_dax()
    assert "Total States" in dax
    assert "Average City Sanctions per 100k" in dax
    assert "Best City ML Test R2" in dax
