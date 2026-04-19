"""
Unit tests for end-to-end city full analysis helpers.
"""

import numpy as np
import pandas as pd

from src.analysis.city_full_analysis import (
    build_city_thesis_conclusion_markdown,
    prepare_city_modeling_dataframe,
    run_city_ml_analysis,
    run_city_statistical_analysis,
    summarize_city_clusters_for_outcomes,
)


def _synthetic_city_df(n_rows: int = 120, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    population = rng.integers(5_000, 800_000, size=n_rows)
    literacy = np.clip(rng.normal(90, 4, size=n_rows), 65, 100)
    income_real = np.clip(rng.normal(2_400, 700, size=n_rows), 500, None)
    transfers = rng.uniform(100_000, 30_000_000, size=n_rows)
    transfer_pc = transfers / population

    log_population = np.log(population)
    log_income = np.log(income_real)
    log_transfers = np.log(transfers)

    sanctions_per_100k = (
        0.9 * log_population
        + 1.7 * log_income
        + 0.03 * literacy
        + 0.5 * np.log1p(transfer_pc)
        + rng.normal(0, 0.35, size=n_rows)
    )
    sanctions_per_100k = np.clip(sanctions_per_100k, 0.05, None)

    n_sanctions = np.maximum(np.round(sanctions_per_100k * population / 100_000), 0).astype(int)

    return pd.DataFrame(
        {
            "municipality_code": [f"{i+1:07d}" for i in range(n_rows)],
            "municipality_name": [f"City {i+1}" for i in range(n_rows)],
            "state_code": ["35"] * n_rows,
            "state_name": ["Sao Paulo"] * n_rows,
            "region_code": ["3"] * n_rows,
            "region_name": ["Sudeste"] * n_rows,
            "population_2022": population,
            "literacy_rate_2022": literacy,
            "avg_income_real_2022_2022_brl": income_real,
            "total_transfers": transfers,
            "avg_transfer_per_capita": transfer_pc,
            "log_population": log_population,
            "log_income": log_income,
            "log_total_transfers": log_transfers,
            "n_sanctions": n_sanctions,
            "sanctions_per_100k": sanctions_per_100k,
        }
    )


def test_prepare_city_modeling_dataframe_creates_missing_logs():
    df = _synthetic_city_df()
    df = df.drop(columns=["log_population", "log_income", "log_total_transfers", "sanctions_per_100k"])

    prepared = prepare_city_modeling_dataframe(df)

    assert len(prepared) > 0
    assert prepared["log_population"].notna().all()
    assert prepared["log_income"].notna().all()
    assert prepared["log_total_transfers"].notna().all()
    assert prepared["sanctions_per_100k"].notna().all()


def test_run_city_statistical_analysis_returns_coefficients_and_metrics():
    df = _synthetic_city_df()
    result = run_city_statistical_analysis(df)

    assert result.n_obs == len(df)
    assert result.r_squared > 0
    assert "const" in set(result.coefficients["term"])
    assert len(result.correlations) > 0


def test_run_city_ml_analysis_benchmarks_models():
    df = _synthetic_city_df()
    result = run_city_ml_analysis(df)

    assert result.n_obs == len(df)
    assert set(result.model_scores["model"]) == {
        "ElasticNet",
        "RandomForest",
        "GradientBoosting",
    }
    assert result.model_scores["test_r2"].max() > 0
    assert len(result.feature_importance) > 0


def test_build_city_thesis_conclusion_markdown_includes_cluster_summary():
    df = _synthetic_city_df()
    df["cluster"] = np.where(df["log_income"] > df["log_income"].median(), 1, 0)

    stat_result = run_city_statistical_analysis(df)
    ml_result = run_city_ml_analysis(df)
    cluster_summary = summarize_city_clusters_for_outcomes(df, cluster_column="cluster")

    text = build_city_thesis_conclusion_markdown(stat_result, ml_result, cluster_summary)

    assert "City-Level Full Analysis Conclusion Addendum" in text
    assert "Cluster Outcome Contrast" in text
