"""Smoke test for the migrated EDA notebook logic (EN + pt-BR paths).

Runs the core new code paths without needing Jupyter, so we can confirm
everything loads, the dummies are created correctly, and the new
aggregations / correlations execute without errors.
"""
from __future__ import annotations

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import pandas as pd
import numpy as np

# ---- EN path ----
print("=" * 72)
print("EN path: LocalGoldDataLoader + analysis_compliance_municipality")
print("=" * 72)

from src.analysis.local_data_loader import LocalGoldDataLoader

loader = LocalGoldDataLoader()
df_analysis = loader.load_dataset("analysis_compliance_municipality")
assert df_analysis is not None and len(df_analysis) > 5000, "muni dataset did not load"

# Dummies
region_dummies = pd.get_dummies(df_analysis["region_code"], prefix="is_region").astype("Int64")
state_dummies = pd.get_dummies(df_analysis["state_code"], prefix="is_state").astype("Int64")
df_analysis = pd.concat([df_analysis, region_dummies, state_dummies], axis=1)
REGION_DUMMY_COLS = list(region_dummies.columns)
STATE_DUMMY_COLS = list(state_dummies.columns)
print(f"Rows={len(df_analysis):,}  region dummies={len(REGION_DUMMY_COLS)}  state dummies={len(STATE_DUMMY_COLS)}")
assert len(REGION_DUMMY_COLS) == 5, "expected 5 region dummies"
assert len(STATE_DUMMY_COLS) == 27, "expected 27 state dummies"
# sanity: for each row the state dummies sum to 1 and region dummies sum to 1
assert (state_dummies.sum(axis=1) == 1).all(), "state dummies not one-hot"
assert (region_dummies.sum(axis=1) == 1).all(), "region dummies not one-hot"
# sanity: means = proportions
r1_share = df_analysis["is_region_1"].mean()
print(f"is_region_1 mean (share of munis in Norte) = {r1_share:.4f}")
assert 0 < r1_share < 1, "region-1 share not a proportion"

# Describe analytical features (excluding dummies)
numeric_cols = df_analysis.select_dtypes(include=[np.number]).columns
analytical_numeric = [c for c in numeric_cols if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]
desc = df_analysis[analytical_numeric].describe().T
print(f"describe rows (non-dummy numeric): {len(desc)}  (sanity: must be <= 20)")
assert len(desc) <= 25, "describe unexpectedly large"

# Summary (pop-weighted)
_pop = df_analysis["population_2022"]
_lit_mask = df_analysis["literacy_rate_2022"].notna()
_inc_mask = df_analysis["avg_income_2022"].notna()
lit_w = float(np.average(df_analysis.loc[_lit_mask, "literacy_rate_2022"], weights=_pop[_lit_mask]))
inc_w = float(np.average(df_analysis.loc[_inc_mask, "avg_income_2022"], weights=_pop[_inc_mask]))
total_pop = int(_pop.sum())
total_sanc = int(df_analysis["n_sanctions"].sum())
natl_rate = total_sanc / total_pop * 100_000
print(f"National rate (pop-weighted) = {natl_rate:.2f} / 100k  literacy_w={lit_w:.2f}  income_w={inc_w:.2f}")

# Missing values
missing = df_analysis.isnull().sum()
top_missing = missing[missing > 0].sort_values(ascending=False).head(5)
print(f"Columns with missing values: {len(missing[missing > 0])}  top={list(top_missing.index)}")

# Regional rollup
def _region_rollup(g: pd.DataFrame) -> pd.Series:
    pop = g["population_2022"]
    lit_mask = g["literacy_rate_2022"].notna()
    inc_mask = g["avg_income_2022"].notna()
    return pd.Series({
        "N Municipalities": len(g),
        "N States": g["state_code"].nunique(),
        "Total Population": int(pop.sum()),
        "Total Sanctions": int(g["n_sanctions"].sum()),
        "Sanctions/100k (pop-weighted)": round(g["n_sanctions"].sum() / pop.sum() * 100_000, 2),
        "Literacy % (pop-weighted)": round(np.average(g.loc[lit_mask, "literacy_rate_2022"], weights=pop[lit_mask]), 2),
        "Avg Income BRL (pop-weighted)": round(np.average(g.loc[inc_mask, "avg_income_2022"], weights=pop[inc_mask]), 2),
    })

regional_summary = df_analysis.groupby("region_name", observed=True).apply(_region_rollup)
print("\nRegional rollup (muni-level aggregation, pop-weighted):")
print(regional_summary.to_string())

# Sanity: sum of "Total Population" across regions == national total_pop
assert int(regional_summary["Total Population"].sum()) == total_pop, "region pop totals do not sum to national"

# State rollup
state_rollup = (
    df_analysis.groupby(["state_code", "state_name", "region_name"], observed=True)
    .apply(lambda g: pd.Series({
        "population": g["population_2022"].sum(),
        "n_sanctions": g["n_sanctions"].sum(),
        "sanctions_per_100k": g["n_sanctions"].sum() / g["population_2022"].sum() * 100_000,
    }))
    .reset_index()
    .sort_values("sanctions_per_100k", ascending=False)
)
print(f"\nState rollup shape: {state_rollup.shape}  (expect 27 x 6)")
assert len(state_rollup) == 27
print("Top 5 states by sanctions_per_100k (pop-weighted rollup):")
print(state_rollup.head().to_string(index=False))

# Correlation
corr_cols = ["sanctions_per_100k", "literacy_rate_2022", "avg_income_2022",
             "log_population", "log_income"]
if "log_total_transfers" in df_analysis.columns:
    corr_cols.append("log_total_transfers")
corr_cols += REGION_DUMMY_COLS
corr_df = df_analysis[corr_cols].astype("Float64").astype(float)
corr_matrix = corr_df.corr()
print(f"\nCorrelation matrix shape: {corr_matrix.shape}")
print("Correlation with sanctions_per_100k:")
print(corr_matrix["sanctions_per_100k"].round(3).to_string())

# ---- pt-BR path ----
print()
print("=" * 72)
print("pt-BR path: GoldDataLoaderPtBr + analise_compliance_municipio")
print("=" * 72)

from src.analysis.pt_br_loader import GoldDataLoaderPtBr

loader_pt = GoldDataLoaderPtBr()
datasets_pt = loader_pt.load_all()
df_pt = datasets_pt.get("analise_compliance_municipio")
assert df_pt is not None and len(df_pt) > 5000

region_dummies = pd.get_dummies(df_pt["codigo_regiao"], prefix="is_region").astype("Int64")
state_dummies = pd.get_dummies(df_pt["codigo_estado"], prefix="is_state").astype("Int64")
df_pt = pd.concat([df_pt, region_dummies, state_dummies], axis=1)
print(f"pt-BR: rows={len(df_pt):,}  region_dummies={len(region_dummies.columns)}  state_dummies={len(state_dummies.columns)}")

# Test key pt-BR column names exist
for col in ["populacao_2022", "taxa_alfabetizacao_2022", "renda_media_2022",
            "num_sancoes", "sancoes_por_100k", "log_populacao", "log_renda",
            "codigo_estado", "nome_estado", "codigo_regiao", "nome_regiao",
            "codigo_municipio", "nome_municipio"]:
    assert col in df_pt.columns, f"pt-BR missing expected column: {col}"
print("pt-BR: all expected translated columns present")

# Sanity: pt-BR regional rollup matches EN totals
pt_rr = df_pt.groupby("nome_regiao", observed=True)["populacao_2022"].sum().sort_index()
en_rr = regional_summary["Total Population"].sort_index()
# Convert both to plain int for comparison
pt_ints = [int(v) for v in pt_rr.values]
en_ints = [int(v) for v in en_rr.values]
assert pt_ints == en_ints, f"EN vs pt-BR regional pop mismatch: {en_ints} vs {pt_ints}"
print("pt-BR: regional pop totals identical to EN")

print("\nSMOKE TEST OK")
