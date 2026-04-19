"""
End-to-end municipality analysis helpers for thesis-ready outputs.

This module mirrors the state-level analysis flow at city level:
- data preparation
- statistical inference (correlations + OLS)
- ML benchmarking
- conclusion text generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import ElasticNetCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


DEFAULT_CITY_FEATURE_COLUMNS = [
    "log_population",
    "literacy_rate_2022",
    "log_income",
    "log_total_transfers",
    "avg_transfer_per_capita",
]

DEFAULT_CITY_TARGET_COLUMN = "sanctions_per_100k"


@dataclass
class CityStatisticalAnalysisResult:
    """Result container for city-level statistical analysis."""

    n_obs: int
    feature_columns: List[str]
    target_column: str
    correlations: pd.DataFrame
    coefficients: pd.DataFrame
    r_squared: float
    adj_r_squared: float
    f_pvalue: float
    model_name: str = "OLS (HC3)"


@dataclass
class CityMLAnalysisResult:
    """Result container for city-level ML benchmark analysis."""

    n_obs: int
    feature_columns: List[str]
    target_column: str
    model_scores: pd.DataFrame
    feature_importance: pd.DataFrame
    best_model_name: str


def _resolve_columns(
    feature_columns: Optional[Sequence[str]] = None,
    target_column: str = DEFAULT_CITY_TARGET_COLUMN,
) -> tuple[List[str], str]:
    """Resolve model feature and target columns."""
    features = list(feature_columns) if feature_columns is not None else list(DEFAULT_CITY_FEATURE_COLUMNS)
    return features, target_column


def _ensure_required_columns(df: pd.DataFrame, required: Sequence[str], context: str) -> None:
    """Validate required columns for a given context."""
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for {context}: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def prepare_city_modeling_dataframe(
    df: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    target_column: str = DEFAULT_CITY_TARGET_COLUMN,
) -> pd.DataFrame:
    """
    Prepare municipality-level dataframe for statistical and ML analysis.

    The function computes target/log variables when possible and removes rows
    with missing modeling fields.
    """
    features, target = _resolve_columns(feature_columns, target_column)
    required_base = ["municipality_code", "population_2022", "n_sanctions"]
    _ensure_required_columns(df, required_base, "city modeling preparation")

    working = df.copy()

    if "total_transfers" in working.columns:
        working["total_transfers"] = pd.to_numeric(
            working["total_transfers"], errors="coerce"
        ).fillna(0.0).clip(lower=0.0)

    if "avg_transfer_per_capita" in features:
        if "avg_transfer_per_capita" not in working.columns and "total_transfers" in working.columns:
            working["avg_transfer_per_capita"] = working.apply(
                lambda row: (row["total_transfers"] / row["population_2022"])
                if pd.notna(row.get("population_2022")) and row.get("population_2022", 0) > 0
                else None,
                axis=1,
            )
        elif "avg_transfer_per_capita" in working.columns:
            working["avg_transfer_per_capita"] = pd.to_numeric(
                working["avg_transfer_per_capita"], errors="coerce"
            ).fillna(0.0).clip(lower=0.0)

    if target not in working.columns:
        working[target] = working.apply(
            lambda row: (row["n_sanctions"] / row["population_2022"]) * 100000
            if pd.notna(row.get("population_2022")) and row.get("population_2022", 0) > 0
            else None,
            axis=1,
        )

    if "log_population" in features and "log_population" not in working.columns:
        working["log_population"] = working["population_2022"].apply(
            lambda x: np.log(x) if pd.notna(x) and x > 0 else None
        )

    if "log_income" in features and "log_income" not in working.columns:
        income_col = (
            "avg_income_real_2022_2022_brl"
            if "avg_income_real_2022_2022_brl" in working.columns
            else "avg_income_2022"
        )
        _ensure_required_columns(working, [income_col], "log_income creation")
        working["log_income"] = working[income_col].apply(
            lambda x: np.log(x) if pd.notna(x) and x > 0 else None
        )

    if "log_total_transfers" in features and "log_total_transfers" not in working.columns:
        _ensure_required_columns(working, ["total_transfers"], "log_total_transfers creation")
        working["log_total_transfers"] = working["total_transfers"].apply(
            lambda x: np.log1p(x) if pd.notna(x) and x >= 0 else None
        )
    elif "log_total_transfers" in features and "total_transfers" in working.columns:
        transfers = pd.to_numeric(working["total_transfers"], errors="coerce").fillna(0.0).clip(lower=0.0)
        existing_log = pd.to_numeric(working["log_total_transfers"], errors="coerce")
        working["log_total_transfers"] = existing_log.where(existing_log.notna(), np.log1p(transfers))

    modeling_cols = [*features, target]
    _ensure_required_columns(working, modeling_cols, "city modeling dataframe")
    cleaned = working.dropna(subset=modeling_cols).copy()

    return cleaned.reset_index(drop=True)


def run_city_statistical_analysis(
    df: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    target_column: str = DEFAULT_CITY_TARGET_COLUMN,
) -> CityStatisticalAnalysisResult:
    """Run city-level correlations and OLS with robust (HC3) errors."""
    features, target = _resolve_columns(feature_columns, target_column)
    modeling_df = prepare_city_modeling_dataframe(df, features, target)

    if len(modeling_df) < len(features) + 2:
        raise ValueError(
            "Not enough rows for OLS after cleaning. "
            f"Rows: {len(modeling_df)}, features: {len(features)}"
        )

    # Correlations with target
    correlation_rows = []
    for feature in features:
        sample = modeling_df[[feature, target]].dropna()
        corr_value = float(sample[feature].corr(sample[target])) if len(sample) >= 3 else float("nan")
        correlation_rows.append(
            {
                "feature": feature,
                "correlation_with_target": corr_value,
                "abs_correlation": abs(corr_value) if pd.notna(corr_value) else np.nan,
            }
        )
    correlations = pd.DataFrame(correlation_rows).sort_values(
        "abs_correlation", ascending=False
    ).reset_index(drop=True)

    X = sm.add_constant(modeling_df[features], has_constant="add")
    y = modeling_df[target]

    ols_model = sm.OLS(y, X).fit(cov_type="HC3")
    coefficients = pd.DataFrame(
        {
            "term": ols_model.params.index,
            "coefficient": ols_model.params.values,
            "std_error": ols_model.bse.values,
            "t_stat": ols_model.tvalues.values,
            "p_value": ols_model.pvalues.values,
        }
    )

    return CityStatisticalAnalysisResult(
        n_obs=int(len(modeling_df)),
        feature_columns=features,
        target_column=target,
        correlations=correlations,
        coefficients=coefficients,
        r_squared=float(ols_model.rsquared),
        adj_r_squared=float(ols_model.rsquared_adj),
        f_pvalue=float(ols_model.f_pvalue) if ols_model.f_pvalue is not None else float("nan"),
    )


def _get_model_feature_importance(model_name: str, model, feature_columns: Sequence[str]) -> pd.DataFrame:
    """Extract model-specific feature-importance representation."""
    if model_name == "ElasticNet":
        coefficients = model.named_steps["model"].coef_
        importance = np.abs(coefficients)
    elif hasattr(model, "feature_importances_"):
        importance = model.feature_importances_
    else:
        importance = np.zeros(len(feature_columns))

    total = float(np.sum(importance))
    normalized = importance / total if total > 0 else importance

    return pd.DataFrame(
        {
            "feature": list(feature_columns),
            "importance": importance,
            "normalized_importance": normalized,
        }
    ).sort_values("importance", ascending=False).reset_index(drop=True)


def run_city_ml_analysis(
    df: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    target_column: str = DEFAULT_CITY_TARGET_COLUMN,
    test_size: float = 0.2,
    random_state: int = 42,
) -> CityMLAnalysisResult:
    """Benchmark supervised regressors for city-level sanctions prediction."""
    if test_size <= 0 or test_size >= 1:
        raise ValueError(f"test_size must be in (0, 1). Received: {test_size}")

    features, target = _resolve_columns(feature_columns, target_column)
    modeling_df = prepare_city_modeling_dataframe(df, features, target)

    if len(modeling_df) < 30:
        raise ValueError(
            "ML analysis needs at least 30 municipality rows after cleaning. "
            f"Received: {len(modeling_df)}"
        )

    X = modeling_df[features]
    y = modeling_df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    models = {
        "ElasticNet": Pipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "model",
                    ElasticNetCV(
                        l1_ratio=[0.1, 0.3, 0.5, 0.7, 0.9],
                        cv=5,
                        random_state=random_state,
                        max_iter=10_000,
                    ),
                ),
            ]
        ),
        "RandomForest": RandomForestRegressor(
            n_estimators=400,
            max_depth=None,
            random_state=random_state,
            n_jobs=-1,
        ),
        "GradientBoosting": GradientBoostingRegressor(
            random_state=random_state,
            n_estimators=350,
            learning_rate=0.03,
            max_depth=3,
        ),
    }

    score_rows = []
    fitted_models: Dict[str, object] = {}

    for name, model in models.items():
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        fitted_models[name] = model
        score_rows.append(
            {
                "model": name,
                "test_mae": float(mean_absolute_error(y_test, preds)),
                "test_rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
                "test_r2": float(r2_score(y_test, preds)),
            }
        )

    model_scores = pd.DataFrame(score_rows).sort_values("test_r2", ascending=False).reset_index(drop=True)
    best_model_name = str(model_scores.iloc[0]["model"])
    best_model = fitted_models[best_model_name]
    feature_importance = _get_model_feature_importance(best_model_name, best_model, features)

    return CityMLAnalysisResult(
        n_obs=int(len(modeling_df)),
        feature_columns=features,
        target_column=target,
        model_scores=model_scores,
        feature_importance=feature_importance,
        best_model_name=best_model_name,
    )


def summarize_city_clusters_for_outcomes(
    df_city: pd.DataFrame,
    cluster_column: str = "cluster",
    target_column: str = DEFAULT_CITY_TARGET_COLUMN,
) -> pd.DataFrame:
    """Summarize sanctions and transfer outcomes by city cluster."""
    required = [cluster_column, target_column, "n_sanctions", "total_transfers"]
    _ensure_required_columns(df_city, required, "cluster outcome summary")

    working = df_city.copy()
    working["total_transfers"] = pd.to_numeric(
        working["total_transfers"], errors="coerce"
    ).fillna(0.0).clip(lower=0.0)

    summary = (
        working.groupby(cluster_column)
        .agg(
            n_cities=("municipality_code", "nunique"),
            avg_sanctions_per_100k=(target_column, "mean"),
            avg_n_sanctions=("n_sanctions", "mean"),
            avg_total_transfers=("total_transfers", "mean"),
        )
        .reset_index()
        .sort_values(cluster_column)
        .reset_index(drop=True)
    )
    return summary


def build_city_thesis_conclusion_markdown(
    statistical_result: CityStatisticalAnalysisResult,
    ml_result: CityMLAnalysisResult,
    cluster_summary: Optional[pd.DataFrame] = None,
) -> str:
    """Generate a concise thesis-conclusion section for city-level analysis."""
    top_corr = statistical_result.correlations.iloc[0]
    top_feature = str(top_corr["feature"])
    top_corr_value = float(top_corr["correlation_with_target"])

    top_ml = ml_result.model_scores.iloc[0]
    best_model = str(top_ml["model"])
    best_r2 = float(top_ml["test_r2"])

    conclusion = f"""# City-Level Full Analysis Conclusion Addendum

## Evidence Snapshot

- Municipality sample used in modeling: **{statistical_result.n_obs:,}** rows
- Strongest linear association with `{statistical_result.target_column}`: **{top_feature}** (r = {top_corr_value:.3f})
- OLS goodness of fit: **R² = {statistical_result.r_squared:.3f}**, adjusted **R² = {statistical_result.adj_r_squared:.3f}**
- Best ML regressor: **{best_model}** with test **R² = {best_r2:.3f}**

## Interpretation

The municipality-level results reinforce the thesis claim that sanctions intensity is associated with structural capacity proxies (income, population, and transfer-related scale variables), not only with presumed misconduct prevalence. Compared with state-level models, city-level analysis improves sample size and enables more granular benchmarking among municipalities.
"""

    if cluster_summary is not None and not cluster_summary.empty:
        cluster_lines = []
        for _, row in cluster_summary.iterrows():
            cluster_lines.append(
                f"- Cluster {int(row.iloc[0])}: n={int(row['n_cities'])}, "
                f"avg sanctions/100k={float(row['avg_sanctions_per_100k']):.3f}, "
                f"avg transfers={float(row['avg_total_transfers']):.2f}"
            )
        cluster_block = "\n".join(cluster_lines)
        conclusion += f"""

## Cluster Outcome Contrast

{cluster_block}
"""

    conclusion += """

## Method Caveat

This remains associational evidence in a cross-sectional setup. Interpretation should continue to emphasize detection-capacity mechanisms and reporting bias, with causal claims reserved for future identification strategies.
"""
    return conclusion.strip() + "\n"


__all__ = [
    "CityMLAnalysisResult",
    "CityStatisticalAnalysisResult",
    "DEFAULT_CITY_FEATURE_COLUMNS",
    "DEFAULT_CITY_TARGET_COLUMN",
    "build_city_thesis_conclusion_markdown",
    "prepare_city_modeling_dataframe",
    "run_city_ml_analysis",
    "run_city_statistical_analysis",
    "summarize_city_clusters_for_outcomes",
]
