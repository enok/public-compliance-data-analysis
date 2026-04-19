"""
Migrate downstream notebooks (02 statistical, 03 ML, 06 complete pipeline, EN + pt-BR)
from state-level (N=27) to municipality-level (N~=5,570) analysis.

Strategy: the existing cell logic is mostly correct -- we just need to (a) load
the municipality-level dataset, (b) rename its columns to the old state-level
column names so downstream cells keep working with minimal changes, and
(c) recreate the old is_norte / is_nordeste / ... region dummies with the same
names so feature specifications don't break.

For notebook 03 (ML) we also drop `n_municipalities` from feature_cols because
at the municipality grain it is always 1 (a zero-variance feature).

Idempotent: re-running re-applies the same cell contents, clearing outputs so
the notebook renders cleanly when re-executed.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NB_02_EN = REPO / "notebooks" / "02_statistical_analysis.ipynb"
NB_02_PT = REPO / "notebooks" / "02_statistical_analysis.pt-BR.ipynb"
NB_03_EN = REPO / "notebooks" / "03_machine_learning.ipynb"
NB_03_PT = REPO / "notebooks" / "03_machine_learning.pt-BR.ipynb"
NB_06_EN = REPO / "notebooks" / "06_complete_thesis_pipeline.ipynb"
NB_06_PT = REPO / "notebooks" / "06_complete_thesis_pipeline.pt-BR.ipynb"


def _lines(src: str) -> list[str]:
    parts = src.split("\n")
    return [p + "\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def _set_code(nb: dict, idx: int, content: str) -> None:
    cell = nb["cells"][idx]
    assert cell["cell_type"] == "code", f"cell {idx} is not code"
    cell["source"] = _lines(content)
    cell["outputs"] = []
    cell["execution_count"] = None


def _save(nb_path: Path, nb: dict) -> None:
    with nb_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)
        f.write("\n")


# ---------------------------------------------------------------------------
# Shared "load muni dataset + alias columns to old state-level names" snippet.
# This is embedded directly into each notebook's data-loading cell so the
# downstream cells keep working unchanged.
# ---------------------------------------------------------------------------

# EN: load via GoldDataLoader, rename muni-level columns to the old state-level
# column names that existed in `analysis_compliance`, and regenerate the
# human-readable region dummies.
MUNI_LOAD_SNIPPET_EN = """# --- Municipality-level analysis (N ~= 5,570) -----------------------------
# We now load the municipality-level `analysis_compliance_municipality` dataset
# instead of the state-level `analysis_compliance` (N=27). The municipality
# grain gives proper statistical power for correlation, OLS and ML work below.
#
# For backward compatibility with the rest of the notebook we:
#   1. Rename muni-level columns to the old state-level names (e.g.
#      `population_2022` -> `population`) so downstream cells work unchanged.
#   2. Regenerate the human-readable region dummies (is_norte, is_nordeste,
#      is_sudeste, is_sul, is_centro_oeste) with the same names they had in
#      the state-level dataset.
#   3. Add state dummies (is_state_<IBGE-2digit-code>) as extra features.
df = loader.load_dataset('analysis_compliance_municipality')
df = df.rename(columns={
    'population_2022': 'population',
    'literacy_rate_2022': 'avg_literacy_rate',
    'avg_income_2022': 'avg_income',
})

REGION_NAME_TO_DUMMY = {
    'Norte': 'is_norte',
    'Nordeste': 'is_nordeste',
    'Sudeste': 'is_sudeste',
    'Sul': 'is_sul',
    'Centro-Oeste': 'is_centro_oeste',
}
for _rname, _col in REGION_NAME_TO_DUMMY.items():
    df[_col] = (df['region_name'] == _rname).astype('Int64')
REGION_DUMMY_COLS = list(REGION_NAME_TO_DUMMY.values())

state_dummies = pd.get_dummies(df['state_code'], prefix='is_state').astype('Int64')
df = pd.concat([df, state_dummies], axis=1)
STATE_DUMMY_COLS = list(state_dummies.columns)

print(f"Loaded {len(df):,} observations (municipalities across {df['state_code'].nunique()} states)")
print(f"Region dummies: {REGION_DUMMY_COLS}")
print(f"State dummies: {len(STATE_DUMMY_COLS)} columns (first: {STATE_DUMMY_COLS[0]}, last: {STATE_DUMMY_COLS[-1]})")
df.head()"""

MUNI_LOAD_SNIPPET_PT = """# --- Análise em nível municipal (N ~= 5.570) -----------------------------
# Agora carregamos o dataset municipal `analise_compliance_municipio` em vez
# do dataset estadual `analise_compliance` (N=27). A granularidade municipal
# garante poder estatístico adequado para correlação, OLS e ML abaixo.
#
# Para manter compatibilidade com o restante do notebook:
#   1. Renomeamos colunas do nível municipal para os antigos nomes estaduais
#      (ex.: `populacao_2022` -> `populacao`), preservando células a jusante.
#   2. Recriamos as dummies regionais humanamente legíveis (is_norte, is_nordeste,
#      is_sudeste, is_sul, is_centro_oeste) com os mesmos nomes do dataset
#      estadual.
#   3. Adicionamos dummies de estado (is_state_<código-IBGE>) como features extras.
df = loader.load_dataset('analise_compliance_municipio')
df = df.rename(columns={
    'populacao_2022': 'populacao',
    'taxa_alfabetizacao_2022': 'taxa_alfabetizacao_media',
    'renda_media_2022': 'renda_media',
})

REGION_NAME_TO_DUMMY = {
    'Norte': 'is_norte',
    'Nordeste': 'is_nordeste',
    'Sudeste': 'is_sudeste',
    'Sul': 'is_sul',
    'Centro-Oeste': 'is_centro_oeste',
}
for _rname, _col in REGION_NAME_TO_DUMMY.items():
    df[_col] = (df['nome_regiao'] == _rname).astype('Int64')
REGION_DUMMY_COLS = list(REGION_NAME_TO_DUMMY.values())

state_dummies = pd.get_dummies(df['codigo_estado'], prefix='is_state').astype('Int64')
df = pd.concat([df, state_dummies], axis=1)
STATE_DUMMY_COLS = list(state_dummies.columns)

print(f"Carregadas {len(df):,} observações (municípios em {df['codigo_estado'].nunique()} estados)")
print(f"Dummies regionais: {REGION_DUMMY_COLS}")
print(f"Dummies de estado: {len(STATE_DUMMY_COLS)} colunas (primeira: {STATE_DUMMY_COLS[0]}, última: {STATE_DUMMY_COLS[-1]})")
df.head()"""


# ---------------------------------------------------------------------------
# Notebook 02: statistical analysis
# ---------------------------------------------------------------------------
# Structure check done in inspection: the only cells that need changes are:
#   [10] load data                         -> swap to muni-level + dummies
#   [13] key_vars correlation matrix       -> drop `n_municipalities` (no variance)
#
# The rest of the cells (correlations, scatter plots, ANOVA, Tukey, OLS, VIF,
# diagnostics, model comparison) work as-is with the renamed muni-level df.


def _nb02_key_vars_cell(lang: str) -> str:
    if lang == "en":
        return """# Key analytical variables (municipality level). Dropped `n_municipalities`
# because it is constant at this grain. Added `log_total_transfers` where
# available to expose the transfer-side signal (central to the thesis question).
key_vars = ['sanctions_per_100k', 'avg_literacy_rate', 'avg_income',
            'log_population', 'log_income']
if 'log_total_transfers' in df.columns:
    key_vars.append('log_total_transfers')

# Cast to plain float -- Int64/Float64 nullable types with NaN can trip up
# seaborn heatmap rendering.
corr_matrix = df[key_vars].astype('Float64').astype(float).corr(method='pearson')

plt.figure(figsize=(12, 10))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.3f', cmap='RdBu_r',
            center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8})
plt.title('Pearson Correlation Matrix (municipality-level, N=5,570)',
          fontsize=16, fontweight='bold', pad=20)
plt.tight_layout()
plt.show()

print("\\nCorrelations with Sanctions per 100k:")
print("=" * 60)
sanctions_corr = corr_matrix['sanctions_per_100k'].sort_values(ascending=False)
print(sanctions_corr)"""
    else:  # pt
        return """# Variáveis analíticas principais (nível municipal). Removemos
# `num_municipios` pois é constante nessa granularidade. Adicionamos
# `log_total_transferencias` quando disponível para expor o sinal do lado
# das transferências (central para a pergunta do TCC).
key_vars = ['sancoes_por_100k', 'taxa_alfabetizacao_media', 'renda_media',
            'log_populacao', 'log_renda']
if 'log_total_transferencias' in df.columns:
    key_vars.append('log_total_transferencias')

# Cast para float puro -- tipos Int64/Float64 nullable com NaN podem dar
# problema no heatmap do seaborn.
corr_matrix = df[key_vars].astype('Float64').astype(float).corr(method='pearson')

plt.figure(figsize=(12, 10))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.3f', cmap='RdBu_r',
            center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8})
plt.title('Matriz de Correlação de Pearson (nível municipal, N=5.570)',
          fontsize=16, fontweight='bold', pad=20)
plt.tight_layout()
plt.show()

print("\\nCorrelações com Sanções por 100k:")
print("=" * 60)
sanctions_corr = corr_matrix['sancoes_por_100k'].sort_values(ascending=False)
print(sanctions_corr)"""


def _nb02_pearsonr_cell(lang: str) -> str:
    if lang == "en":
        return """from scipy.stats import pearsonr

def correlation_test(x, y, var_names):
    \"\"\"Compute Pearson correlation with p-value.\"\"\"
    clean_data = pd.DataFrame({'x': x, 'y': y}).dropna()
    if len(clean_data) < 3:
        return None, None
    r, p = pearsonr(clean_data['x'].astype(float), clean_data['y'].astype(float))
    return r, p

results = []
target = df['sanctions_per_100k']

_test_vars = ['avg_literacy_rate', 'avg_income', 'log_income', 'log_population']
if 'log_total_transfers' in df.columns:
    _test_vars.append('log_total_transfers')

for var in _test_vars:
    r, p = correlation_test(target, df[var], (var, 'sanctions_per_100k'))
    if r is not None:
        sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else 'ns'
        results.append({
            'Variable': var,
            'Correlation (r)': f"{r:.4f}",
            'P-value': f"{p:.4f}",
            'Significance': sig,
        })

print("Pearson correlation tests vs. sanctions_per_100k (N=5,570)")
print("=" * 60)
print(pd.DataFrame(results).to_string(index=False))"""
    else:
        return """from scipy.stats import pearsonr

def correlation_test(x, y, var_names):
    \"\"\"Calcula correlação de Pearson com valor-p.\"\"\"
    clean_data = pd.DataFrame({'x': x, 'y': y}).dropna()
    if len(clean_data) < 3:
        return None, None
    r, p = pearsonr(clean_data['x'].astype(float), clean_data['y'].astype(float))
    return r, p

results = []
target = df['sancoes_por_100k']

_test_vars = ['taxa_alfabetizacao_media', 'renda_media', 'log_renda', 'log_populacao']
if 'log_total_transferencias' in df.columns:
    _test_vars.append('log_total_transferencias')

for var in _test_vars:
    r, p = correlation_test(target, df[var], (var, 'sancoes_por_100k'))
    if r is not None:
        sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else 'ns'
        results.append({
            'Variável': var,
            'Correlação (r)': f"{r:.4f}",
            'P-valor': f"{p:.4f}",
            'Significância': sig,
        })

print("Testes de correlação de Pearson vs. sancoes_por_100k (N=5.570)")
print("=" * 60)
print(pd.DataFrame(results).to_string(index=False))"""


def _nb02_ols_xvars_cell(lang: str) -> str:
    # For OLS at muni-level we include region dummies (minus one for base)
    # and the transfer signal. Keep the same output structure as the original.
    if lang == "en":
        return """y = df['sanctions_per_100k']

# Drop `is_sudeste` as the reference region to avoid the dummy trap.
X_vars = ['log_income', 'avg_literacy_rate', 'log_population',
          'is_norte', 'is_nordeste', 'is_sul', 'is_centro_oeste']
if 'log_total_transfers' in df.columns:
    X_vars.append('log_total_transfers')

# Drop rows with any NaN in the selected features (muni-level has some
# nullable columns) and convert to plain float for statsmodels.
_mask = df[X_vars + ['sanctions_per_100k']].notna().all(axis=1)
X = df.loc[_mask, X_vars].copy().astype(float)
y = df.loc[_mask, 'sanctions_per_100k'].astype(float)
X = sm.add_constant(X)

print("Model Specification:")
print("=" * 70)
print(f"Dependent Variable: sanctions_per_100k (municipality level)")
print(f"Independent Variables: {X_vars}")
print(f"Reference region (omitted to avoid dummy trap): Sudeste")
print(f"\\nSample size: {len(X):,} municipalities (rows with any NaN dropped)")
print(f"Number of predictors: {len(X_vars)}")"""
    else:
        return """y = df['sancoes_por_100k']

# Removemos `is_sudeste` como região de referência para evitar a armadilha
# das dummies (dummy variable trap).
X_vars = ['log_renda', 'taxa_alfabetizacao_media', 'log_populacao',
          'is_norte', 'is_nordeste', 'is_sul', 'is_centro_oeste']
if 'log_total_transferencias' in df.columns:
    X_vars.append('log_total_transferencias')

# Descarta linhas com NaN em qualquer feature (o nível municipal tem algumas
# colunas nullable) e converte para float puro para o statsmodels.
_mask = df[X_vars + ['sancoes_por_100k']].notna().all(axis=1)
X = df.loc[_mask, X_vars].copy().astype(float)
y = df.loc[_mask, 'sancoes_por_100k'].astype(float)
X = sm.add_constant(X)

print("Especificação do Modelo:")
print("=" * 70)
print(f"Variável Dependente: sancoes_por_100k (nível municipal)")
print(f"Variáveis Independentes: {X_vars}")
print(f"Região de referência (omitida para evitar armadilha das dummies): Sudeste")
print(f"\\nTamanho da amostra: {len(X):,} municípios (linhas com NaN descartadas)")
print(f"Número de preditores: {len(X_vars)}")"""


def _nb02_simple_compare_cell(lang: str) -> str:
    if lang == "en":
        return """# Two-variable baseline for comparison (same sample size as the full model).
_simple_vars = ['log_income', 'avg_literacy_rate']
X_simple = sm.add_constant(df.loc[_mask, _simple_vars].astype(float))
model_simple = sm.OLS(y, X_simple).fit()

model_full = model

comparison = pd.DataFrame({
    'Model': [f'Simple ({len(_simple_vars)} vars)', f'Full ({len(X_vars)} vars inc. region dummies)'],
    'N': [int(model_simple.nobs), int(model_full.nobs)],
    'R-squared': [model_simple.rsquared, model_full.rsquared],
    'Adj. R-squared': [model_simple.rsquared_adj, model_full.rsquared_adj],
    'AIC': [model_simple.aic, model_full.aic],
    'BIC': [model_simple.bic, model_full.bic],
    'F-statistic': [model_simple.fvalue, model_full.fvalue],
    'Prob (F)': [model_simple.f_pvalue, model_full.f_pvalue],
})

print("Model Comparison (municipality level)")
print("=" * 80)
display(comparison.round(4))

print("\\nNote: Lower AIC/BIC indicates better model fit")"""
    else:
        return """# Baseline de duas variáveis para comparação (mesmo tamanho de amostra).
_simple_vars = ['log_renda', 'taxa_alfabetizacao_media']
X_simple = sm.add_constant(df.loc[_mask, _simple_vars].astype(float))
model_simple = sm.OLS(y, X_simple).fit()

model_full = model

comparison = pd.DataFrame({
    'Modelo': [f'Simples ({len(_simple_vars)} vars)', f'Completo ({len(X_vars)} vars + dummies de região)'],
    'N': [int(model_simple.nobs), int(model_full.nobs)],
    'R-quadrado': [model_simple.rsquared, model_full.rsquared],
    'R-quadrado ajustado': [model_simple.rsquared_adj, model_full.rsquared_adj],
    'AIC': [model_simple.aic, model_full.aic],
    'BIC': [model_simple.bic, model_full.bic],
    'F': [model_simple.fvalue, model_full.fvalue],
    'Prob (F)': [model_simple.f_pvalue, model_full.f_pvalue],
})

print("Comparação de Modelos (nível municipal)")
print("=" * 80)
display(comparison.round(4))

print("\\nObs.: AIC/BIC menor indica melhor ajuste")"""


def _migrate_notebook_02(nb_path: Path, lang: str) -> None:
    with nb_path.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    # Sanity guard
    expected_first_tokens = {
        "en": "Statistical Analysis",
        "pt": "Análise Estatística",
    }
    first_src = "".join(nb["cells"][0].get("source", []))
    if expected_first_tokens[lang] not in first_src and expected_first_tokens[lang].lower() not in first_src.lower():
        # Don't abort; just warn and proceed -- structure may have been touched.
        print(f"  [warn] {nb_path.name}: cell[0] does not clearly match the expected title; proceeding anyway.")

    # Cell 10: load (wrap the snippet in the cell -- loader instance is created inline).
    # In the original cell this was `loader = GoldDataLoader()` + `df = loader.load_dataset(...)`
    # We keep the loader instantiation and add our muni snippet after it.
    if lang == "en":
        load_cell = (
            "# Try local loader first, fallback to S3 if available\n"
            "from src.analysis.local_data_loader import LocalGoldDataLoader as GoldDataLoader\n"
            "loader = GoldDataLoader()\n\n"
            + MUNI_LOAD_SNIPPET_EN
        )
    else:
        load_cell = (
            "from src.analysis.pt_br_loader import GoldDataLoaderPtBr as GoldDataLoader\n"
            "loader = GoldDataLoader()\n\n"
            + MUNI_LOAD_SNIPPET_PT
        )
    _set_code(nb, 10, load_cell)

    # Cell 13: Pearson correlation matrix (drop n_municipalities, add transfer var)
    _set_code(nb, 13, _nb02_key_vars_cell(lang))

    # Cell 15: Per-variable pearsonr test (same change)
    _set_code(nb, 15, _nb02_pearsonr_cell(lang))

    # Cell 26: OLS X_vars -- drop Sudeste as reference + add transfer var
    _set_code(nb, 26, _nb02_ols_xvars_cell(lang))

    # Cell 44: Model comparison -- use the same _mask so sample sizes align
    _set_code(nb, 44, _nb02_simple_compare_cell(lang))

    _save(nb_path, nb)
    print(f"[ok] Patched {nb_path.name}")


# ---------------------------------------------------------------------------
# Notebook 03: machine learning
# ---------------------------------------------------------------------------
# Changes:
#   [10] load data                           -> swap to muni-level + dummies
#   [11] feature_cols                        -> drop n_municipalities, handle NaN
# Downstream ML cells (train_test_split, model zoo, RF importance, ROC, etc.)
# work unchanged with ~5,570 observations instead of 27.


def _nb03_features_cell(lang: str) -> str:
    if lang == "en":
        return """# Feature columns at MUNICIPALITY level.
# - dropped `n_municipalities` (constant=1 at this grain)
# - dropped `is_sudeste` as the implicit base region (dummy trap)
# - added `log_total_transfers` if present -- central to the thesis question
feature_cols = ['log_income', 'avg_literacy_rate', 'log_population',
                'is_norte', 'is_nordeste', 'is_sul', 'is_centro_oeste']
if 'log_total_transfers' in df.columns:
    feature_cols.append('log_total_transfers')

# Drop rows with NaN in features or target (muni-level has some nullable cols).
_mask = df[feature_cols + ['sanctions_per_100k']].notna().all(axis=1)
X = df.loc[_mask, feature_cols].copy().astype(float)
y_regression = df.loc[_mask, 'sanctions_per_100k'].astype(float)

print(f"Features shape: {X.shape}  (N={len(X):,} municipalities after dropping NaN)")
print(f"Target shape: {y_regression.shape}")
print(f"\\nFeatures: {list(X.columns)}")"""
    else:
        return """# Colunas de features em nível MUNICIPAL.
# - removido `num_municipios` (constante = 1 nessa granularidade)
# - removido `is_sudeste` como região-base implícita (dummy trap)
# - adicionado `log_total_transferencias` se presente -- central para a pergunta do TCC
feature_cols = ['log_renda', 'taxa_alfabetizacao_media', 'log_populacao',
                'is_norte', 'is_nordeste', 'is_sul', 'is_centro_oeste']
if 'log_total_transferencias' in df.columns:
    feature_cols.append('log_total_transferencias')

# Remove linhas com NaN em features ou alvo (nível municipal tem colunas nullable).
_mask = df[feature_cols + ['sancoes_por_100k']].notna().all(axis=1)
X = df.loc[_mask, feature_cols].copy().astype(float)
y_regression = df.loc[_mask, 'sancoes_por_100k'].astype(float)

print(f"Formato de features: {X.shape}  (N={len(X):,} municípios após remover NaN)")
print(f"Formato do alvo: {y_regression.shape}")
print(f"\\nFeatures: {list(X.columns)}")"""


def _nb03_binary_target_cell(lang: str) -> str:
    if lang == "en":
        return """# Binary target definition (municipality-level).
#
# DATA CAVEAT: in the current Gold snapshot ~78% of municipalities have
# `n_sanctions == 0`, so the median of `sanctions_per_100k` is 0. Using
# `> median` as the boundary collapses to "has any sanction at all", which
# IS actually a meaningful binary target (did this muni ever show up in
# CEIS / CNEP / CEPIM?). We therefore define:
#     high_risk == 1  iff  the muni has at least one sanction registered.
# Class balance is ~22/78, so downstream classifiers below use
# class_weight='balanced' (and we track ROC-AUC / F1, not just accuracy).
df_sub = df.loc[_mask].copy()
df_sub['high_risk'] = (df_sub['n_sanctions'] > 0).astype(int)

print("Binary target: has at least one sanction registered")
print(f"Class distribution:")
print(df_sub['high_risk'].value_counts())
print(f"\\nClass balance:")
print(df_sub['high_risk'].value_counts(normalize=True).round(3))

y_classification = df_sub['high_risk'].copy()"""
    else:
        return """# Definição do alvo binário (nível municipal).
#
# RESSALVA SOBRE OS DADOS: no snapshot Gold atual ~78% dos municípios têm
# `num_sancoes == 0`, então a mediana de `sancoes_por_100k` é 0. Usar
# `> mediana` como fronteira colapsa para "teve ao menos uma sanção", que
# É um alvo binário significativo (o município apareceu alguma vez em
# CEIS / CNEP / CEPIM?). Definimos então:
#     high_risk == 1  se  o município registra ao menos uma sanção.
# O balanceamento fica ~22/78, por isso os classificadores abaixo usam
# class_weight='balanced' (e acompanhamos ROC-AUC / F1, não só acurácia).
df_sub = df.loc[_mask].copy()
df_sub['high_risk'] = (df_sub['num_sancoes'] > 0).astype(int)

print("Alvo binário: tem ao menos uma sanção registrada")
print(f"Distribuição de classes:")
print(df_sub['high_risk'].value_counts())
print(f"\\nBalanceamento de classes:")
print(df_sub['high_risk'].value_counts(normalize=True).round(3))

y_classification = df_sub['high_risk'].copy()"""


def _nb03_classifiers_cell(lang: str) -> str:
    """Classifier training cell with class_weight='balanced' added to handle
    the ~78/22 class imbalance at muni level."""
    if lang == "en":
        return """# class_weight='balanced' compensates for the ~78/22 imbalance (only 22%
# of munis have a sanction registered). Without it, classifiers would trivially
# predict 0 everywhere and get 78% accuracy while missing every positive case.
clf_models = {
    'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000,
                                              class_weight='balanced'),
    'Random Forest': RandomForestClassifier(n_estimators=100, max_depth=5,
                                            random_state=42, n_jobs=-1,
                                            class_weight='balanced'),
    'Gradient Boosting': GradientBoostingClassifier(n_estimators=100,
                                                    max_depth=3, random_state=42),
}

clf_results = []

for name, model in clf_models.items():
    print(f"\\nTraining {name}...")

    if name == 'Logistic Regression':
        model.fit(X_train_clf_scaled, y_train_clf)
        y_pred = model.predict(X_test_clf_scaled)
        y_pred_proba = model.predict_proba(X_test_clf_scaled)[:, 1]
    else:
        model.fit(X_train_clf, y_train_clf)
        y_pred = model.predict(X_test_clf)
        y_pred_proba = model.predict_proba(X_test_clf)[:, 1]

    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    accuracy = accuracy_score(y_test_clf, y_pred)
    precision = precision_score(y_test_clf, y_pred, zero_division=0)
    recall = recall_score(y_test_clf, y_pred, zero_division=0)
    f1 = f1_score(y_test_clf, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test_clf, y_pred_proba)

    clf_results.append({
        'Model': name,
        'Accuracy': accuracy,
        'Precision': precision,
        'Recall': recall,
        'F1-Score': f1,
        'ROC-AUC': roc_auc,
    })

    print(f"\\n{name} - Classification Report:")
    print(classification_report(y_test_clf, y_pred,
                                target_names=['No sanctions', 'Has sanctions'],
                                zero_division=0))

clf_results_df = pd.DataFrame(clf_results).sort_values('ROC-AUC', ascending=False)

print("\\n" + "=" * 80)
print("CLASSIFICATION MODEL COMPARISON (imbalanced, class_weight='balanced')")
print("=" * 80)
display(clf_results_df)"""
    else:
        return """# class_weight='balanced' compensa o desbalanceamento ~78/22 (apenas 22%
# dos municípios têm sanção registrada). Sem isso, os classificadores preveriam
# trivialmente 0 em tudo e teriam 78% de acurácia errando todos os positivos.
clf_models = {
    'Regressão Logística': LogisticRegression(random_state=42, max_iter=1000,
                                              class_weight='balanced'),
    'Random Forest': RandomForestClassifier(n_estimators=100, max_depth=5,
                                            random_state=42, n_jobs=-1,
                                            class_weight='balanced'),
    'Gradient Boosting': GradientBoostingClassifier(n_estimators=100,
                                                    max_depth=3, random_state=42),
}

clf_results = []

for name, model in clf_models.items():
    print(f"\\nTreinando {name}...")

    if name == 'Regressão Logística':
        model.fit(X_train_clf_scaled, y_train_clf)
        y_pred = model.predict(X_test_clf_scaled)
        y_pred_proba = model.predict_proba(X_test_clf_scaled)[:, 1]
    else:
        model.fit(X_train_clf, y_train_clf)
        y_pred = model.predict(X_test_clf)
        y_pred_proba = model.predict_proba(X_test_clf)[:, 1]

    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    accuracy = accuracy_score(y_test_clf, y_pred)
    precision = precision_score(y_test_clf, y_pred, zero_division=0)
    recall = recall_score(y_test_clf, y_pred, zero_division=0)
    f1 = f1_score(y_test_clf, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test_clf, y_pred_proba)

    clf_results.append({
        'Modelo': name,
        'Acurácia': accuracy,
        'Precisão': precision,
        'Recall': recall,
        'F1': f1,
        'ROC-AUC': roc_auc,
    })

    print(f"\\n{name} - Relatório de Classificação:")
    print(classification_report(y_test_clf, y_pred,
                                target_names=['Sem sanções', 'Com sanções'],
                                zero_division=0))

clf_results_df = pd.DataFrame(clf_results).sort_values('ROC-AUC', ascending=False)

print("\\n" + "=" * 80)
print("COMPARAÇÃO DE MODELOS DE CLASSIFICAÇÃO (desbalanceado, class_weight='balanced')")
print("=" * 80)
display(clf_results_df)"""


def _migrate_notebook_03(nb_path: Path, lang: str) -> None:
    with nb_path.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    # Cell 10: load
    if lang == "en":
        load_cell = (
            "from src.analysis.local_data_loader import LocalGoldDataLoader as GoldDataLoader\n"
            "loader = GoldDataLoader()\n\n"
            + MUNI_LOAD_SNIPPET_EN
        )
    else:
        load_cell = (
            "from src.analysis.pt_br_loader import GoldDataLoaderPtBr as GoldDataLoader\n"
            "loader = GoldDataLoader()\n\n"
            + MUNI_LOAD_SNIPPET_PT
        )
    _set_code(nb, 10, load_cell)

    # Cell 11: feature_cols
    _set_code(nb, 11, _nb03_features_cell(lang))

    # Cell 26: binary target (uses the _mask defined in cell 11)
    _set_code(nb, 26, _nb03_binary_target_cell(lang))

    # Cell 29: classifiers with class_weight='balanced'
    _set_code(nb, 29, _nb03_classifiers_cell(lang))

    _save(nb_path, nb)
    print(f"[ok] Patched {nb_path.name}")


# ---------------------------------------------------------------------------
# Notebook 06: complete thesis pipeline
# ---------------------------------------------------------------------------
# Notebook 06 already loads BOTH `df_state` and `df_city` (analysis_compliance
# + analysis_compliance_municipality). It runs a "STATE-LEVEL DESCRIPTIVES" and
# "STATE-LEVEL CORRELATIONS" section on the 27-state dataset. Rather than
# rewriting the whole notebook, we add a header note at the top telling the
# reader that the primary analytical grain is the municipality and that the
# state-level blocks are rollups for presentation.


def _migrate_notebook_06(nb_path: Path, lang: str) -> None:
    with nb_path.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    # Insert / update a prominent note at the top.
    # We identify our note by a marker in its source. If found, we replace it;
    # otherwise we insert it as the second cell (right after the title markdown).
    if lang == "en":
        note_marker = "Thesis grain note (auto-inserted by migration):"
        note_src = (
            "> **Thesis grain note (auto-inserted by migration):** the primary\n"
            "> analytical grain for the thesis is now the **municipality**\n"
            "> (`analysis_compliance_municipality`, N ~= 5,570). The state-level\n"
            "> blocks below (`STATE-LEVEL DESCRIPTIVES`, `STATE-LEVEL CORRELATIONS`)\n"
            "> are kept as presentation rollups (N=27) but are not the basis of\n"
            "> the thesis's statistical claims anymore. See notebooks 01-03 for\n"
            "> muni-level descriptives, OLS and ML. See notebook 04 for\n"
            "> muni-level clustering and notebook 05 for the corruption x HDI\n"
            "> analysis that already operates on munis."
        )
    else:
        note_marker = "Nota sobre granularidade da tese (inserida automaticamente pela migração):"
        note_src = (
            "> **Nota sobre granularidade da tese (inserida automaticamente pela migração):**\n"
            "> a granularidade analítica principal da tese agora é o **município**\n"
            "> (`analise_compliance_municipio`, N ~= 5.570). Os blocos em nível\n"
            "> estadual abaixo (`DESCRITIVAS EM NÍVEL ESTADUAL`,\n"
            "> `CORRELAÇÕES EM NÍVEL ESTADUAL`) são mantidos como agregações\n"
            "> de apresentação (N=27), mas não são a base das afirmações\n"
            "> estatísticas da tese. Ver notebooks 01-03 para descritivas,\n"
            "> OLS e ML em nível municipal. Ver notebook 04 para clustering\n"
            "> em nível municipal e notebook 05 para a análise corrupção x IDH\n"
            "> que já opera em municípios."
        )

    # Look for existing note
    existing_idx = None
    for i, c in enumerate(nb["cells"]):
        if c["cell_type"] == "markdown" and note_marker in "".join(c.get("source", [])):
            existing_idx = i
            break

    if existing_idx is not None:
        nb["cells"][existing_idx]["source"] = _lines(note_src)
    else:
        new_cell = {"cell_type": "markdown", "metadata": {}, "source": _lines(note_src)}
        nb["cells"].insert(1, new_cell)

    _save(nb_path, nb)
    print(f"[ok] Patched {nb_path.name}")


def main() -> None:
    print("--- Notebook 02 ---")
    _migrate_notebook_02(NB_02_EN, "en")
    _migrate_notebook_02(NB_02_PT, "pt")
    print("--- Notebook 03 ---")
    _migrate_notebook_03(NB_03_EN, "en")
    _migrate_notebook_03(NB_03_PT, "pt")
    print("--- Notebook 06 ---")
    _migrate_notebook_06(NB_06_EN, "en")
    _migrate_notebook_06(NB_06_PT, "pt")
    print("\nDone.")


if __name__ == "__main__":
    main()
