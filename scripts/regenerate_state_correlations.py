"""Regenerate the state-level Pearson correlation heatmap used as Figura C in
the thesis appendix.

The previous `docs/thesis_presentation_assets/correlations_state.png` showed
only `num_sancoes` x `sancoes_por_100k` — a degenerate 2x2 matrix that
did not include any of the socioeconomic predictors discussed in the
thesis body. This script rebuilds the figure from the Gold
`analysis_compliance` state-level table (n = 27 UFs) with the predictors
actually used in the OLS model plus the dependent variable.

Output: docs/thesis_presentation_assets/correlations_state.png (overwrites).
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

REPO = Path(
    r"C:\google-drive\cursos\usp\mba\data-science\tcc\code"
    r"\public-compliance-data-analysis"
)
SOURCE = REPO / "data" / "gold" / "analysis_compliance" / "data.parquet"
OUTPUT = REPO / "docs" / "thesis_presentation_assets" / "correlations_state.png"

# Variables included in the state-level correlation matrix.
# Keep log transforms consistent with the OLS specification used in the body.
VARIABLES = {
    "sanctions_per_100k": "Sanções / 100 mil hab.",
    "log_income": "log(renda média real)",
    "log_population": "log(população)",
    "avg_literacy_rate": "Taxa de alfabetização",
    "n_sanctions": "Sanções (total)",
}


def main() -> None:
    df = pd.read_parquet(SOURCE)
    missing = [c for c in VARIABLES if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"analysis_compliance is missing expected columns: {missing}. "
            f"Available: {list(df.columns)}"
        )

    corr = df[list(VARIABLES)].corr(method="pearson")
    corr.columns = [VARIABLES[c] for c in corr.columns]
    corr.index = [VARIABLES[c] for c in corr.index]

    fig, ax = plt.subplots(figsize=(8.0, 6.5), dpi=150)
    sns.heatmap(
        corr,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        center=0.0,
        vmin=-1.0,
        vmax=1.0,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.8},
        annot_kws={"size": 10},
        ax=ax,
    )
    ax.set_title(
        f"Correlações estaduais / State-level correlations (n = {len(df)})",
        fontsize=11,
    )
    plt.setp(ax.get_xticklabels(), rotation=35, ha="right", fontsize=9)
    plt.setp(ax.get_yticklabels(), rotation=0, fontsize=9)
    fig.tight_layout()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {OUTPUT}  (n={len(df)}, {len(VARIABLES)} variables)")


if __name__ == "__main__":
    main()
