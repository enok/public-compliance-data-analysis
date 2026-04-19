#!/usr/bin/env python3
"""
Run full municipality-level analysis and produce thesis-ready artifacts.

Outputs:
- correlations, OLS coefficients, ML scores
- cluster outcome summary
- same-cluster peer tables
- thesis conclusion addendum (EN + pt-BR)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

# Ensure `src` imports work when running as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.city_clustering import build_same_cluster_peer_table, cluster_cities
from src.analysis.city_full_analysis import (
    build_city_thesis_conclusion_markdown,
    run_city_ml_analysis,
    run_city_statistical_analysis,
    summarize_city_clusters_for_outcomes,
)
from src.analysis.data_loader import GoldDataLoader


def _build_city_conclusion_markdown_pt_br(
    n_obs: int,
    top_feature: str,
    top_corr: float,
    r2: float,
    adj_r2: float,
    best_model: str,
    best_r2: float,
) -> str:
    return f"""# Adendo de Conclusão — Análise Municipal Completa

## Evidências Principais

- Amostra municipal usada na modelagem: **{n_obs:,}** linhas
- Associação linear mais forte com `sanctions_per_100k`: **{top_feature}** (r = {top_corr:.3f})
- Qualidade do ajuste OLS: **R² = {r2:.3f}**, **R² ajustado = {adj_r2:.3f}**
- Melhor modelo de ML: **{best_model}** com **R² de teste = {best_r2:.3f}**

## Interpretação

Os resultados em nível municipal reforçam a tese de que a intensidade de sanções está associada a proxies de capacidade estrutural (renda, escala populacional e escala de transferências), e não apenas à prevalência presumida de irregularidades.

Em comparação ao nível estadual, a análise municipal amplia substancialmente o tamanho amostral e permite benchmarking entre municípios comparáveis.

## Limitação Metodológica

A evidência continua associativa em desenho transversal. A interpretação deve manter o foco em mecanismos de capacidade de detecção e viés de reporte, deixando alegações causais para estratégias de identificação futuras.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full city-level thesis analysis.")
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
        default="docs/city_full_analysis",
        help="Directory where analysis artifacts are written.",
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
        help="How many nearest peers to keep per city inside each cluster.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    loader = GoldDataLoader(bucket_name=args.bucket_name, aws_profile=args.aws_profile)
    city_df = loader.load_dataset("analysis_compliance_municipality")
    cluster_base_df = loader.load_dataset("consolidated_clustering")

    if city_df is None:
        raise RuntimeError(
            "Could not load 'analysis_compliance_municipality'. "
            "Run Gold transformation first."
        )
    if cluster_base_df is None:
        raise RuntimeError(
            "Could not load 'consolidated_clustering'. "
            "Run Gold transformation first."
        )

    statistical_result = run_city_statistical_analysis(city_df)
    ml_result = run_city_ml_analysis(city_df)

    city_cluster_result = cluster_cities(
        cluster_base_df,
        min_k=args.min_k,
        max_k=args.max_k,
    )
    cluster_assignments = city_cluster_result.clustered_df[["municipality_code", "cluster"]]

    city_with_cluster = city_df.merge(cluster_assignments, on="municipality_code", how="left")
    cluster_outcome_summary = summarize_city_clusters_for_outcomes(city_with_cluster)
    same_cluster_peers = build_same_cluster_peer_table(
        city_cluster_result.clustered_df, top_n=args.peer_top_n
    )

    conclusion_md = build_city_thesis_conclusion_markdown(
        statistical_result=statistical_result,
        ml_result=ml_result,
        cluster_summary=cluster_outcome_summary,
    )

    top_corr_row = statistical_result.correlations.iloc[0]
    top_model_row = ml_result.model_scores.iloc[0]
    conclusion_md_pt_br = _build_city_conclusion_markdown_pt_br(
        n_obs=statistical_result.n_obs,
        top_feature=str(top_corr_row["feature"]),
        top_corr=float(top_corr_row["correlation_with_target"]),
        r2=statistical_result.r_squared,
        adj_r2=statistical_result.adj_r_squared,
        best_model=str(top_model_row["model"]),
        best_r2=float(top_model_row["test_r2"]),
    )

    statistical_result.correlations.to_csv(output_dir / "city_correlations.csv", index=False)
    statistical_result.coefficients.to_csv(output_dir / "city_ols_coefficients.csv", index=False)
    ml_result.model_scores.to_csv(output_dir / "city_ml_scores.csv", index=False)
    ml_result.feature_importance.to_csv(output_dir / "city_ml_feature_importance.csv", index=False)
    city_cluster_result.diagnostics.to_csv(output_dir / "city_cluster_k_diagnostics.csv", index=False)
    cluster_outcome_summary.to_csv(output_dir / "city_cluster_outcome_summary.csv", index=False)
    same_cluster_peers.to_csv(output_dir / "city_same_cluster_peers.csv", index=False)

    (output_dir / "city_thesis_conclusion_addendum.md").write_text(
        conclusion_md, encoding="utf-8"
    )
    (output_dir / "city_thesis_conclusion_addendum.pt-BR.md").write_text(
        conclusion_md_pt_br, encoding="utf-8"
    )

    print("City full analysis artifacts generated:")
    for file_path in sorted(output_dir.glob("*")):
        print(f"  - {file_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
