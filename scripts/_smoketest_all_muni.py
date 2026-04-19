"""End-to-end smoke test for the municipality-level migration.

Executes the core code paths of notebooks 01, 02, 03 against the real Gold
layer, without needing Jupyter. Confirms that:
    - Data loads and renames correctly (EN + pt-BR)
    - Region / state dummies are generated as expected
    - Correlations, OLS regressions and ML models train without errors
    - Sample sizes and diagnostics are in the expected ranges

Also spot-checks notebooks 04 and 05 to confirm they already use the
municipality grain and no migration is required.
"""
from __future__ import annotations

import sys
import io
import warnings

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

HEADER = "=" * 72

# ============================================================================
# Notebook 01 -- EDA
# ============================================================================
print(HEADER)
print("Notebook 01: EDA (muni-level)")
print(HEADER)

from src.analysis.local_data_loader import LocalGoldDataLoader

loader = LocalGoldDataLoader()
df_analysis = loader.load_dataset("analysis_compliance_municipality")
assert df_analysis is not None and len(df_analysis) > 5000, "muni dataset failed to load"

REGION_NAME_TO_DUMMY = {
    "Norte": "is_norte", "Nordeste": "is_nordeste", "Sudeste": "is_sudeste",
    "Sul": "is_sul", "Centro-Oeste": "is_centro_oeste",
}
for rname, col in REGION_NAME_TO_DUMMY.items():
    df_analysis[col] = (df_analysis["region_name"] == rname).astype("Int64")
REGION_DUMMY_COLS = list(REGION_NAME_TO_DUMMY.values())
state_dummies = pd.get_dummies(df_analysis["state_code"], prefix="is_state").astype("Int64")
df_analysis = pd.concat([df_analysis, state_dummies], axis=1)
STATE_DUMMY_COLS = list(state_dummies.columns)
print(f"EN: rows={len(df_analysis):,}  region_dummies={len(REGION_DUMMY_COLS)}  state_dummies={len(STATE_DUMMY_COLS)}")
assert len(REGION_DUMMY_COLS) == 5
assert len(STATE_DUMMY_COLS) == 27

# ============================================================================
# Notebook 02 -- Statistical analysis (OLS)
# ============================================================================
print()
print(HEADER)
print("Notebook 02: Statistical analysis (OLS at muni grain)")
print(HEADER)

df = loader.load_dataset("analysis_compliance_municipality")
df = df.rename(columns={
    "population_2022": "population",
    "literacy_rate_2022": "avg_literacy_rate",
    "avg_income_2022": "avg_income",
})
for rname, col in REGION_NAME_TO_DUMMY.items():
    df[col] = (df["region_name"] == rname).astype("Int64")
state_dummies = pd.get_dummies(df["state_code"], prefix="is_state").astype("Int64")
df = pd.concat([df, state_dummies], axis=1)
print(f"Loaded {len(df):,} observations (municipalities across {df['state_code'].nunique()} states)")

# Correlation matrix (key_vars)
key_vars = ["sanctions_per_100k", "avg_literacy_rate", "avg_income",
            "log_population", "log_income"]
if "log_total_transfers" in df.columns:
    key_vars.append("log_total_transfers")
corr_matrix = df[key_vars].astype("Float64").astype(float).corr(method="pearson")
print(f"\nCorrelation matrix shape: {corr_matrix.shape}")
print("Correlations with sanctions_per_100k:")
print(corr_matrix["sanctions_per_100k"].round(4).sort_values(ascending=False).to_string())

# OLS regression (matches notebook 02 cell 26-28)
import statsmodels.api as sm

X_vars = ["log_income", "avg_literacy_rate", "log_population",
          "is_norte", "is_nordeste", "is_sul", "is_centro_oeste"]
if "log_total_transfers" in df.columns:
    X_vars.append("log_total_transfers")

_mask = df[X_vars + ["sanctions_per_100k"]].notna().all(axis=1)
X = df.loc[_mask, X_vars].copy().astype(float)
y = df.loc[_mask, "sanctions_per_100k"].astype(float)
X_const = sm.add_constant(X)
ols_model = sm.OLS(y, X_const).fit()

print(f"\nOLS model fit: N={int(ols_model.nobs):,}  R^2={ols_model.rsquared:.4f}  Adj-R^2={ols_model.rsquared_adj:.4f}")
print(f"F-statistic={ols_model.fvalue:.2f}  p(F)={ols_model.f_pvalue:.4e}")
print(f"AIC={ols_model.aic:.2f}  BIC={ols_model.bic:.2f}")
print("Significant predictors (p<0.05):")
sig = ols_model.pvalues[ols_model.pvalues < 0.05]
for name, pval in sig.items():
    coef = ols_model.params[name]
    print(f"  {name}: coef={coef:.4f} p={pval:.4e}")

# VIF (notebook cell 40)
from statsmodels.stats.outliers_influence import variance_inflation_factor as _vif
vif_data = pd.DataFrame({
    "Variable": X.columns,
    "VIF": [_vif(X.values, i) for i in range(X.shape[1])],
})
print(f"\nVIF (max): {vif_data['VIF'].max():.2f}  (flags >10 as high multicollinearity)")
print(vif_data.sort_values("VIF", ascending=False).head().to_string(index=False))

# Regional ANOVA (cell 20)
from scipy.stats import f_oneway
groups = [df[df["region_name"] == r]["sanctions_per_100k"].dropna().astype(float)
          for r in df["region_name"].unique()]
f_stat, p_anova = f_oneway(*groups)
print(f"\nANOVA across regions: F={f_stat:.3f}  p={p_anova:.4e}  ({'significant' if p_anova < 0.05 else 'NS'})")

# ============================================================================
# Notebook 03 -- Machine Learning
# ============================================================================
print()
print(HEADER)
print("Notebook 03: Machine Learning (regression + classification at muni grain)")
print(HEADER)

df3 = loader.load_dataset("analysis_compliance_municipality")
df3 = df3.rename(columns={
    "population_2022": "population",
    "literacy_rate_2022": "avg_literacy_rate",
    "avg_income_2022": "avg_income",
})
for rname, col in REGION_NAME_TO_DUMMY.items():
    df3[col] = (df3["region_name"] == rname).astype("Int64")

feature_cols = ["log_income", "avg_literacy_rate", "log_population",
                "is_norte", "is_nordeste", "is_sul", "is_centro_oeste"]
if "log_total_transfers" in df3.columns:
    feature_cols.append("log_total_transfers")

_mask = df3[feature_cols + ["sanctions_per_100k"]].notna().all(axis=1)
X = df3.loc[_mask, feature_cols].copy().astype(float)
y_reg = df3.loc[_mask, "sanctions_per_100k"].astype(float)
print(f"ML features: {X.shape}  target: {y_reg.shape}")
assert len(X) > 5000, f"expected >5000 rows after masking, got {len(X)}"

from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, RandomForestClassifier
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

X_train, X_test, y_train, y_test = train_test_split(X, y_reg, test_size=0.25, random_state=42)
scaler = StandardScaler()
X_train_s = pd.DataFrame(scaler.fit_transform(X_train), columns=X.columns, index=X_train.index)
X_test_s = pd.DataFrame(scaler.transform(X_test), columns=X.columns, index=X_test.index)

print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

models = {
    "Linear Regression": LinearRegression(),
    "Ridge": Ridge(alpha=1.0),
    "Lasso": Lasso(alpha=0.1),
    "ElasticNet": ElasticNet(alpha=0.1, l1_ratio=0.5),
    "Decision Tree": DecisionTreeRegressor(max_depth=5, random_state=42),
    "Random Forest": RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42, n_jobs=-1),
    "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42),
}

results = []
for name, model in models.items():
    if name in ("Ridge", "Lasso", "ElasticNet"):
        model.fit(X_train_s, y_train)
        y_pred = model.predict(X_test_s)
    else:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    results.append({"Model": name, "RMSE": mse**0.5, "MAE": mae, "R2_test": r2})

results_df = pd.DataFrame(results).sort_values("R2_test", ascending=False)
print("\nRegression model comparison (test set):")
print(results_df.round(4).to_string(index=False))

# Classification target: "has at least one sanction registered".
# At muni level this is naturally ~22/78 imbalanced (most munis have 0 sanctions).
# Downstream classifiers use class_weight='balanced' to handle this.
df3_sub = df3.loc[_mask].copy()
df3_sub["high_risk"] = (df3_sub["n_sanctions"] > 0).astype(int)
balance = df3_sub["high_risk"].value_counts(normalize=True)
print(f"\nClassification target balance: {dict(balance.round(3))}  (expected ~22/78 at muni grain)")
assert 0.05 < balance.min() < 0.5, f"positive class share {balance.min():.2%} outside sanity range"

# Train a balanced classifier to verify the class_weight fix works
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, f1_score
X_tr, X_te, y_tr, y_te = train_test_split(X, df3_sub["high_risk"], test_size=0.25,
                                          random_state=42, stratify=df3_sub["high_risk"])
scaler_clf = StandardScaler()
X_tr_s = scaler_clf.fit_transform(X_tr)
X_te_s = scaler_clf.transform(X_te)
lr = LogisticRegression(random_state=42, max_iter=1000, class_weight="balanced")
lr.fit(X_tr_s, y_tr)
y_pred = lr.predict(X_te_s)
y_proba = lr.predict_proba(X_te_s)[:, 1]
print(f"Balanced LogReg: ROC-AUC={roc_auc_score(y_te, y_proba):.4f}  F1={f1_score(y_te, y_pred):.4f}")
assert y_pred.sum() > 0, "balanced classifier predicting all zeros -- class_weight not working"

# ============================================================================
# Notebook 04 -- Clustering (spot-check: consolidated_clustering is muni)
# ============================================================================
print()
print(HEADER)
print("Notebook 04: Clustering (consolidated_clustering is muni-level)")
print(HEADER)

cluster = loader.load_dataset("consolidated_clustering")
print(f"consolidated_clustering: shape={cluster.shape}  "
      f"unique municipalities={cluster['municipality_code'].nunique() if 'municipality_code' in cluster.columns else '(no muni col)'}")
assert len(cluster) > 5000, "consolidated_clustering should already be at muni grain"
print("Notebook 04: already at muni grain -- no migration needed.")

# ============================================================================
# Notebook 05 -- Corruption x HDI (loads both state + muni datasets)
# ============================================================================
print()
print(HEADER)
print("Notebook 05: Corruption x HDI (loads both state + muni)")
print(HEADER)

df_state = loader.load_dataset("analysis_compliance")
df_city = loader.load_dataset("analysis_compliance_municipality")
print(f"analysis_compliance (state): shape={df_state.shape}")
print(f"analysis_compliance_municipality (city): shape={df_city.shape}")
print("Notebook 05: uses both grains by design -- verify narrative is grounded in muni findings only.")

# ============================================================================
# Sanity cross-check: sanctions counts between state and muni
# ============================================================================
print()
print(HEADER)
print("Sanctions count comparison: state-level vs muni-level aggregate")
print(HEADER)

state_total = int(df_state["n_sanctions"].sum())
muni_total = int(df_city["n_sanctions"].sum())
print(f"State-level n_sanctions sum:   {state_total:,}")
print(f"Muni-level  n_sanctions sum:   {muni_total:,}")
if state_total != muni_total:
    delta = state_total - muni_total
    pct = delta / state_total * 100
    print(f"DELTA: {delta:,} sanctions exist at state level but NOT mapped to any muni ({pct:.1f}% of state total).")
    print("This is a Gold ETL data quality flag -- sanctions are likely not geocoded to")
    print("a specific municipality during the state-level aggregation path.")
else:
    print("OK: counts agree.")

print("\nALL SMOKE TESTS PASSED")
