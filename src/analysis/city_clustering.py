"""
City-level clustering and within-cluster peer comparison helpers.

This module is intended for notebook and report workflows that need:
1) K-means clustering at municipality level
2) direct comparison of similar cities inside the same cluster
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.neighbors import NearestNeighbors


DEFAULT_CLUSTERING_FEATURES = [
    "population_2022_norm",
    "literacy_rate_2022_norm",
    "avg_income_real_2022_2022_brl_norm",
    "households_2022_norm",
    "population_change_pct_norm",
    "literacy_change_pp_norm",
    "income_change_real_pct_norm",
    "households_change_pct_norm",
]


@dataclass
class CityClusteringResult:
    """Structured output for city clustering runs."""

    clustered_df: pd.DataFrame
    model: KMeans
    feature_columns: List[str]
    selected_k: int
    silhouette: float
    inertia: float
    diagnostics: pd.DataFrame


def _resolve_feature_columns(feature_columns: Optional[Sequence[str]]) -> List[str]:
    """Resolve feature columns, using the module defaults when omitted."""
    if feature_columns is None:
        return list(DEFAULT_CLUSTERING_FEATURES)
    return [str(column) for column in feature_columns]


def _validate_columns(df: pd.DataFrame, required_columns: Sequence[str], context: str) -> None:
    """Raise a descriptive error when required columns are missing."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for {context}: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def evaluate_k_range(
    df: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    min_k: int = 2,
    max_k: int = 10,
    random_state: int = 42,
    n_init: int = 10,
) -> pd.DataFrame:
    """
    Evaluate K values with inertia and silhouette score.

    Returns one row per tested K.
    """
    if min_k < 2:
        raise ValueError(f"min_k must be >= 2. Received: {min_k}")
    if max_k < min_k:
        raise ValueError(f"max_k ({max_k}) must be >= min_k ({min_k}).")

    resolved_features = _resolve_feature_columns(feature_columns)
    _validate_columns(df, resolved_features, "K range evaluation")

    n_samples = len(df)
    if n_samples < 3:
        raise ValueError(
            f"At least 3 rows are required to evaluate clustering. Received: {n_samples}"
        )

    max_feasible_k = min(max_k, n_samples - 1)
    if min_k > max_feasible_k:
        raise ValueError(
            "No feasible K to evaluate. "
            f"Dataset has {n_samples} rows; allowed K range is 2..{max_feasible_k}."
        )

    X = df[resolved_features].to_numpy(dtype=float)
    rows = []

    for k in range(min_k, max_feasible_k + 1):
        model = KMeans(n_clusters=k, random_state=random_state, n_init=n_init)
        labels = model.fit_predict(X)

        try:
            silhouette = float(silhouette_score(X, labels))
        except ValueError:
            silhouette = float("nan")

        rows.append(
            {
                "k": int(k),
                "inertia": float(model.inertia_),
                "silhouette_score": silhouette,
            }
        )

    return pd.DataFrame(rows)


def cluster_cities(
    df: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    n_clusters: Optional[int] = None,
    min_k: int = 2,
    max_k: int = 10,
    random_state: int = 42,
    n_init: int = 10,
    cluster_column: str = "cluster",
) -> CityClusteringResult:
    """
    Cluster municipalities and return labels plus diagnostics.

    If `n_clusters` is omitted, selects K using the highest silhouette score.
    """
    resolved_features = _resolve_feature_columns(feature_columns)
    _validate_columns(df, resolved_features, "city clustering")

    diagnostics = evaluate_k_range(
        df=df,
        feature_columns=resolved_features,
        min_k=min_k,
        max_k=max_k,
        random_state=random_state,
        n_init=n_init,
    )

    if n_clusters is None:
        valid_scores = diagnostics.dropna(subset=["silhouette_score"])
        if valid_scores.empty:
            raise ValueError(
                "Could not select K automatically because all silhouette scores are NaN."
            )
        n_clusters = int(
            valid_scores.sort_values(
                by=["silhouette_score", "k"], ascending=[False, True]
            ).iloc[0]["k"]
        )

    if n_clusters < 2:
        raise ValueError(f"n_clusters must be >= 2. Received: {n_clusters}")

    X = df[resolved_features].to_numpy(dtype=float)
    model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    labels = model.fit_predict(X)

    clustered = df.copy()
    clustered[cluster_column] = labels

    try:
        final_silhouette = float(silhouette_score(X, labels))
    except ValueError:
        final_silhouette = float("nan")

    return CityClusteringResult(
        clustered_df=clustered,
        model=model,
        feature_columns=resolved_features,
        selected_k=int(n_clusters),
        silhouette=final_silhouette,
        inertia=float(model.inertia_),
        diagnostics=diagnostics,
    )


def _resolve_target_city(
    df_clustered: pd.DataFrame,
    municipality_code: Optional[str] = None,
    city_name: Optional[str] = None,
    state_code: Optional[str] = None,
) -> pd.Series:
    """Resolve and validate target city row for peer comparison."""
    if municipality_code:
        mask = df_clustered["municipality_code"].astype(str) == str(municipality_code)
        matches = df_clustered.loc[mask]
    elif city_name:
        name_mask = (
            df_clustered["municipality_name"].astype(str).str.casefold()
            == str(city_name).casefold()
        )
        matches = df_clustered.loc[name_mask]
        if state_code is not None:
            state_mask = matches["state_code"].astype(str) == str(state_code)
            matches = matches.loc[state_mask]
    else:
        raise ValueError("Provide either municipality_code or city_name.")

    if matches.empty:
        raise ValueError(
            "Target city not found. Check municipality_code/city_name/state_code inputs."
        )
    if len(matches) > 1:
        sample_matches = matches[
            ["municipality_code", "municipality_name", "state_code"]
        ].head(5)
        raise ValueError(
            "Target city is ambiguous. Provide municipality_code or city_name + state_code. "
            f"Example matches:\n{sample_matches.to_string(index=False)}"
        )

    return matches.iloc[0]


def compare_cities_in_same_cluster(
    df_clustered: pd.DataFrame,
    municipality_code: Optional[str] = None,
    city_name: Optional[str] = None,
    state_code: Optional[str] = None,
    feature_columns: Optional[Sequence[str]] = None,
    top_n: int = 5,
    cluster_column: str = "cluster",
) -> pd.DataFrame:
    """
    Compare one city with its nearest peers in the same cluster.

    Returns up to `top_n` nearest municipalities based on Euclidean distance
    across clustering features.
    """
    if top_n < 1:
        raise ValueError(f"top_n must be >= 1. Received: {top_n}")

    resolved_features = _resolve_feature_columns(feature_columns)
    required = ["municipality_code", "municipality_name", cluster_column, *resolved_features]
    _validate_columns(df_clustered, required, "same-cluster city comparison")

    target = _resolve_target_city(
        df_clustered=df_clustered,
        municipality_code=municipality_code,
        city_name=city_name,
        state_code=state_code,
    )

    cluster_id = target[cluster_column]
    cluster_df = df_clustered[df_clustered[cluster_column] == cluster_id].copy()
    cluster_df = cluster_df[
        cluster_df["municipality_code"].astype(str) != str(target["municipality_code"])
    ]

    if cluster_df.empty:
        return pd.DataFrame(
            columns=[
                "cluster",
                "target_municipality_code",
                "target_municipality_name",
                "target_state_code",
                "target_state_name",
                "peer_rank",
                "peer_municipality_code",
                "peer_municipality_name",
                "peer_state_code",
                "peer_state_name",
                "distance",
                "similarity_score",
            ]
        )

    target_vector = target[resolved_features].to_numpy(dtype=float)
    peer_vectors = cluster_df[resolved_features].to_numpy(dtype=float)
    distances = np.linalg.norm(peer_vectors - target_vector, axis=1)

    cluster_df = cluster_df.assign(distance=distances)
    cluster_df["similarity_score"] = 1.0 / (1.0 + cluster_df["distance"])
    cluster_df = cluster_df.sort_values("distance", ascending=True).head(top_n).reset_index(
        drop=True
    )
    cluster_df["peer_rank"] = np.arange(1, len(cluster_df) + 1, dtype=int)

    result = pd.DataFrame(
        {
            "cluster": [cluster_id] * len(cluster_df),
            "target_municipality_code": [target["municipality_code"]] * len(cluster_df),
            "target_municipality_name": [target["municipality_name"]] * len(cluster_df),
            "target_state_code": [target.get("state_code")] * len(cluster_df),
            "target_state_name": [target.get("state_name")] * len(cluster_df),
            "peer_rank": cluster_df["peer_rank"],
            "peer_municipality_code": cluster_df["municipality_code"],
            "peer_municipality_name": cluster_df["municipality_name"],
            "peer_state_code": cluster_df.get("state_code"),
            "peer_state_name": cluster_df.get("state_name"),
            "distance": cluster_df["distance"].round(6),
            "similarity_score": cluster_df["similarity_score"].round(6),
        }
    )
    return result


def build_same_cluster_peer_table(
    df_clustered: pd.DataFrame,
    feature_columns: Optional[Sequence[str]] = None,
    top_n: int = 3,
    cluster_column: str = "cluster",
) -> pd.DataFrame:
    """
    Build same-cluster nearest-peer comparisons for all cities.

    Returns one row per (source city, peer rank).
    """
    if top_n < 1:
        raise ValueError(f"top_n must be >= 1. Received: {top_n}")

    resolved_features = _resolve_feature_columns(feature_columns)
    required = ["municipality_code", "municipality_name", cluster_column, *resolved_features]
    _validate_columns(df_clustered, required, "bulk same-cluster peer table")

    records = []

    for cluster_id, cluster_slice in df_clustered.groupby(cluster_column):
        cluster_work = cluster_slice.reset_index(drop=True).copy()
        cluster_size = len(cluster_work)
        if cluster_size < 2:
            continue

        n_neighbors = min(top_n + 1, cluster_size)
        X = cluster_work[resolved_features].to_numpy(dtype=float)

        nn = NearestNeighbors(metric="euclidean", n_neighbors=n_neighbors)
        nn.fit(X)
        distances, indices = nn.kneighbors(X)

        for src_idx, src_row in cluster_work.iterrows():
            peer_positions = zip(distances[src_idx][1:], indices[src_idx][1:])
            for rank, (distance, peer_idx) in enumerate(peer_positions, start=1):
                peer_row = cluster_work.iloc[int(peer_idx)]
                records.append(
                    {
                        "cluster": cluster_id,
                        "source_municipality_code": src_row["municipality_code"],
                        "source_municipality_name": src_row["municipality_name"],
                        "source_state_code": src_row.get("state_code"),
                        "source_state_name": src_row.get("state_name"),
                        "peer_rank": rank,
                        "peer_municipality_code": peer_row["municipality_code"],
                        "peer_municipality_name": peer_row["municipality_name"],
                        "peer_state_code": peer_row.get("state_code"),
                        "peer_state_name": peer_row.get("state_name"),
                        "distance": round(float(distance), 6),
                        "similarity_score": round(1.0 / (1.0 + float(distance)), 6),
                    }
                )

    if not records:
        return pd.DataFrame(
            columns=[
                "cluster",
                "source_municipality_code",
                "source_municipality_name",
                "source_state_code",
                "source_state_name",
                "peer_rank",
                "peer_municipality_code",
                "peer_municipality_name",
                "peer_state_code",
                "peer_state_name",
                "distance",
                "similarity_score",
            ]
        )

    return (
        pd.DataFrame.from_records(records)
        .sort_values(["cluster", "source_municipality_code", "peer_rank"])
        .reset_index(drop=True)
    )


__all__ = [
    "CityClusteringResult",
    "DEFAULT_CLUSTERING_FEATURES",
    "build_same_cluster_peer_table",
    "cluster_cities",
    "compare_cities_in_same_cluster",
    "evaluate_k_range",
]
