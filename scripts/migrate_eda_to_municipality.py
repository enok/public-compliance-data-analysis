"""
Migrate notebook 01_exploratory_data_analysis (EN + pt-BR) from state-level
(analysis_compliance, 27 rows) to municipality-level (analysis_compliance_municipality,
5,570 rows), adding one-hot region AND state dummies in-notebook (no Gold ETL change).

Rationale (user request):
    "you are handling data by state, this is not good, because we end up with few
    records. I want the records by city, you can handle the state as a column
    pointing what state that city belongs to, you can handle these state columns
    the same way you do with: is_norte, is_nordeste, etc, a dummy/boolean value"

Cells 0-10 (intro, setup, imports, reproducibility, runtime config, loader init)
are left untouched. Cells 11-38 are rewritten to use the municipality dataset.

Idempotent: re-running the script will re-apply the same cell contents, wiping
execution outputs so the notebook renders cleanly when re-executed.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NB_EN = REPO / "notebooks" / "01_exploratory_data_analysis.ipynb"
NB_PT = REPO / "notebooks" / "01_exploratory_data_analysis.pt-BR.ipynb"


# ---------------------------------------------------------------------------
# Cell content definitions (EN)
# ---------------------------------------------------------------------------

INTRO_MD_EN = """# Exploratory Data Analysis (EDA)
## Public Compliance Data Analysis - MBA Thesis

**Objective:** Explore the Gold layer datasets at **municipality level** (5,570 records, 1 row per Brazilian municipality) to understand:
- Data distributions and summary statistics
- Missing values and data quality
- Initial patterns and relationships
- Regional variations in compliance and socioeconomic indicators

**Grain note.** The main analytical dataset used here is `analysis_compliance_municipality` (one row per municipality, N=5,570). The previous state-level rollup (`analysis_compliance`, N=27) had too few observations for meaningful correlation/regression analysis. To preserve geographic context, `state_code` / `state_name` / `region_code` / `region_name` are kept as identifier columns and one-hot encoded into dummy variables (`is_region_*`, `is_state_*`) the same way the state-level dataset encoded region."""

LOAD_CELL_EN = """datasets = loader.load_all()

df_muni = datasets.get('municipality_socioeconomic')
df_state = datasets.get('state_summary')
df_sanctions = datasets.get('sanctions_summary')

# Primary analytical dataset: one row per municipality (N ~= 5,570).
df_analysis = datasets.get('analysis_compliance_municipality')

# --- Add one-hot region and state dummies (not present in the muni dataset). ---
# These mirror the is_norte / is_nordeste / ... columns that existed on the
# state-level dataset -- keeping the same human-readable names so any downstream
# notebook that referenced those columns continues to work.
# Kept as Int64 (0/1) for consistency with the pre-existing is_* convention and
# for direct use as regression features in notebooks 02 / 03.
REGION_NAME_TO_DUMMY = {
    'Norte': 'is_norte',
    'Nordeste': 'is_nordeste',
    'Sudeste': 'is_sudeste',
    'Sul': 'is_sul',
    'Centro-Oeste': 'is_centro_oeste',
}
for rname, col in REGION_NAME_TO_DUMMY.items():
    df_analysis[col] = (df_analysis['region_name'] == rname).astype('Int64')
REGION_DUMMY_COLS = list(REGION_NAME_TO_DUMMY.values())

# State dummies: one per state, named by 2-digit IBGE state code.
state_dummies = pd.get_dummies(df_analysis['state_code'], prefix='is_state').astype('Int64')
df_analysis = pd.concat([df_analysis, state_dummies], axis=1)
STATE_DUMMY_COLS = list(state_dummies.columns)

print(f"\\n\u2705 Loaded {len(datasets)} datasets")
print(f"   Main analytical dataset: analysis_compliance_municipality")
print(f"   Rows: {len(df_analysis):,} municipalities, {df_analysis['state_code'].nunique()} states, {df_analysis['region_code'].nunique()} regions")
print(f"   Region dummies added ({len(REGION_DUMMY_COLS)}): {REGION_DUMMY_COLS}")
print(f"   State dummies added ({len(STATE_DUMMY_COLS)}): first 3 = {STATE_DUMMY_COLS[:3]} ... last = {STATE_DUMMY_COLS[-1]}")"""

OVERVIEW_PRINT_EN = """print("=" * 80)
print("ANALYSIS COMPLIANCE MUNICIPALITY DATASET")
print("=" * 80)
print(f"Shape: {df_analysis.shape}  (rows = municipalities, cols = features + dummies)")
print(f"\\nDtypes (non-dummy columns only):")
print(df_analysis.drop(columns=REGION_DUMMY_COLS + STATE_DUMMY_COLS).dtypes)
print(f"\\nMemory Usage: {df_analysis.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")"""

HEAD_CELL_EN = """# Preview a handful of columns (hiding the 32 region+state dummies).
preview_cols = [c for c in df_analysis.columns if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]
df_analysis[preview_cols].head(10)"""

INFO_CELL_EN = """df_analysis.info(verbose=False)"""

DESCRIBE_CELL_EN = """# Describe only the analytical features, excluding the 32 one-hot dummies
# (dummies are summarized separately below).
numeric_cols = df_analysis.select_dtypes(include=[np.number]).columns
analytical_numeric = [c for c in numeric_cols if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]
df_analysis[analytical_numeric].describe().T"""

DUMMY_SUMMARY_MD_EN = """**Dummy variable summary.** For the one-hot region and state dummies the mean equals the proportion of municipalities in that region / state (i.e. `df['is_region_1'].mean()` is the share of munis in the Norte region). A short summary is shown below so we do not flood the main `.describe()` table with 32 extra rows."""

DUMMY_SUMMARY_CELL_EN = """dummy_stats = pd.DataFrame({
    'Count (=1)': df_analysis[REGION_DUMMY_COLS + STATE_DUMMY_COLS].sum(),
    'Share of munis %': (df_analysis[REGION_DUMMY_COLS + STATE_DUMMY_COLS].mean() * 100).round(2),
}).sort_values('Count (=1)', ascending=False)
print(f"Total dummies: {len(dummy_stats)} (5 regions + {len(STATE_DUMMY_COLS)} states)")
dummy_stats.head(10)"""

# Summary of key metrics -- use population-weighted averages for rates.
SUMMARY_CELL_EN = """# Population-weighted averages for rates. A straight mean across 5,570
# municipalities would treat a 500-inhabitant town and Sao Paulo city equally,
# which is statistically misleading for rate and average indicators.
_pop = df_analysis['population_2022']
_lit_mask = df_analysis['literacy_rate_2022'].notna()
_inc_mask = df_analysis['avg_income_2022'].notna()

total_pop = int(_pop.sum())
total_sanc = int(df_analysis['n_sanctions'].sum())

summary = pd.DataFrame({
    'Total Municipalities': [len(df_analysis)],
    'Total States': [int(df_analysis['state_code'].nunique())],
    'Total Regions': [int(df_analysis['region_code'].nunique())],
    'Total Population (2022)': [total_pop],
    'Total Sanctions': [total_sanc],
    'Sanctions/100k (national, pop-weighted)': [round(total_sanc / total_pop * 100_000, 2)],
    'Literacy % (pop-weighted)': [round(np.average(df_analysis.loc[_lit_mask, 'literacy_rate_2022'], weights=_pop[_lit_mask]), 2)],
    'Avg Income BRL (pop-weighted)': [round(np.average(df_analysis.loc[_inc_mask, 'avg_income_2022'], weights=_pop[_inc_mask]), 2)],
})

summary.T"""

MISSING_CELL_EN = """missing = df_analysis.isnull().sum()
missing_pct = (missing / len(df_analysis)) * 100

missing_df = pd.DataFrame({
    'Missing Count': missing,
    'Missing %': missing_pct.round(2),
}).sort_values('Missing Count', ascending=False)

missing_df[missing_df['Missing Count'] > 0]"""

SOCIO_HIST_EN = """fig, axes = plt.subplots(2, 2, figsize=(16, 12))

axes[0, 0].hist(df_analysis['literacy_rate_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='skyblue')
axes[0, 0].set_title('Literacy Rate 2022 Distribution (by municipality)', fontsize=12, fontweight='bold')
axes[0, 0].set_xlabel('Literacy Rate (%)')
axes[0, 0].set_ylabel('Frequency')

axes[0, 1].hist(df_analysis['avg_income_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='lightgreen')
axes[0, 1].set_title('Average Income 2022 Distribution (by municipality)', fontsize=12, fontweight='bold')
axes[0, 1].set_xlabel('Average Income (BRL)')
axes[0, 1].set_ylabel('Frequency')

axes[1, 0].hist(df_analysis['population_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='salmon')
axes[1, 0].set_title('Population 2022 Distribution (by municipality)', fontsize=12, fontweight='bold')
axes[1, 0].set_xlabel('Population')
axes[1, 0].set_ylabel('Frequency')

axes[1, 1].hist(df_analysis['log_population'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='plum')
axes[1, 1].set_title('Log(Population) Distribution (by municipality)', fontsize=12, fontweight='bold')
axes[1, 1].set_xlabel('Log(Population)')
axes[1, 1].set_ylabel('Frequency')

plt.tight_layout()
plt.show()"""

REGIONAL_SUMMARY_EN = """# Regional aggregation from MUNICIPALITY-level data, using POPULATION-WEIGHTED
# statistics. A simple groupby('region_name').agg('mean') would treat every
# municipality equally, which is statistically misleading: tiny towns would
# dominate the average for rates and monetary values.
# For rates and averages across a region we therefore aggregate from totals
# and weight by municipal population.

def _region_rollup(g: pd.DataFrame) -> pd.Series:
    pop = g['population_2022']
    lit_mask = g['literacy_rate_2022'].notna()
    inc_mask = g['avg_income_2022'].notna()
    return pd.Series({
        'N Municipalities': len(g),
        'N States': g['state_code'].nunique(),
        'Total Population': int(pop.sum()),
        'Total Sanctions': int(g['n_sanctions'].sum()),
        'Sanctions/100k (pop-weighted)': round(g['n_sanctions'].sum() / pop.sum() * 100_000, 2),
        'Literacy % (pop-weighted)': round(np.average(g.loc[lit_mask, 'literacy_rate_2022'], weights=pop[lit_mask]), 2),
        'Avg Income BRL (pop-weighted)': round(np.average(g.loc[inc_mask, 'avg_income_2022'], weights=pop[inc_mask]), 2),
    })

regional_summary = (
    df_analysis.groupby('region_name', observed=True)
    .apply(_region_rollup)
)

regional_summary"""

REGIONAL_DASHBOARD_EN = """# Regional dashboard built from municipality-level data (pop-weighted rates).
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Sanctions per 100k by Region (pop-weighted)',
                    'Literacy Rate by Region (pop-weighted)',
                    'Average Income by Region (pop-weighted)',
                    'Total Sanctions by Region'),
    specs=[[{'type': 'bar'}, {'type': 'bar'}],
           [{'type': 'bar'}, {'type': 'bar'}]]
)

regions = regional_summary.reset_index()

fig.add_trace(go.Bar(x=regions['region_name'], y=regions['Sanctions/100k (pop-weighted)'],
                     name='Sanctions/100k', marker_color='indianred'), row=1, col=1)
fig.add_trace(go.Bar(x=regions['region_name'], y=regions['Literacy % (pop-weighted)'],
                     name='Literacy %', marker_color='lightseagreen'), row=1, col=2)
fig.add_trace(go.Bar(x=regions['region_name'], y=regions['Avg Income BRL (pop-weighted)'],
                     name='Income', marker_color='lightsalmon'), row=2, col=1)
fig.add_trace(go.Bar(x=regions['region_name'], y=regions['Total Sanctions'],
                     name='Total Sanctions', marker_color='mediumpurple'), row=2, col=2)

fig.update_layout(height=800, showlegend=False, title_text="Regional Comparison Dashboard (built from 5,570 municipalities)")
fig.show()"""

STATE_HEADER_MD_EN = """## 7. State-Level Rollup and Municipality Extremes

We look at both ends of the granularity spectrum:

1. **State-level rollup** (population-weighted from the 5,570 municipalities) for a stable, policy-relevant bar chart.
2. **Top / bottom municipalities** by `sanctions_per_100k`, which can surface individual outliers that the state rollup hides."""

STATE_TOP_MUNI_EN = """# Top 10 and bottom 10 MUNICIPALITIES by sanctions per 100k.
# NOTE: per-capita rates for very small municipalities can be unstable
# (denominator effect) -- use with care.
cols = ['municipality_code', 'municipality_name', 'state_name', 'region_name',
        'population_2022', 'n_sanctions', 'sanctions_per_100k']

top_10_munis = df_analysis.nlargest(10, 'sanctions_per_100k')[cols]
bottom_10_munis = df_analysis.nsmallest(10, 'sanctions_per_100k')[cols]

print("TOP 10 MUNICIPALITIES - Highest Sanctions per 100k")
print("=" * 90)
print(top_10_munis.to_string(index=False))

print("\\n\\nBOTTOM 10 MUNICIPALITIES - Lowest Sanctions per 100k (among munis with sanctions > 0)")
print("=" * 90)
with_sanctions = df_analysis[df_analysis['n_sanctions'] > 0]
print(with_sanctions.nsmallest(10, 'sanctions_per_100k')[cols].to_string(index=False))"""

STATE_ROLLUP_BAR_EN = """# State-level rollup from municipality data (population-weighted).
# state_name is used on the x-axis (qualitative label); state_code is just an id.
state_rollup = (
    df_analysis.groupby(['state_code', 'state_name', 'region_name'], observed=True)
    .apply(lambda g: pd.Series({
        'population': g['population_2022'].sum(),
        'n_sanctions': g['n_sanctions'].sum(),
        'sanctions_per_100k': g['n_sanctions'].sum() / g['population_2022'].sum() * 100_000,
    }))
    .reset_index()
    .sort_values('sanctions_per_100k', ascending=False)
)

fig = px.bar(state_rollup,
             x='state_name', y='sanctions_per_100k',
             color='region_name',
             title='Sanctions per 100k Population by State (rolled up from 5,570 municipalities)',
             labels={'sanctions_per_100k': 'Sanctions per 100k', 'state_name': 'State'},
             height=500)
fig.update_xaxes(tickangle=-45)
fig.show()"""

CORR_CELL_EN = """# Correlation matrix of key analytical features at municipality level
# (5,570 observations instead of 27 states -- much more statistical power).
# We include the region dummies but NOT the 27 state dummies (would make the
# heatmap unreadable). Transfer-side features are also included since the
# thesis question links federal transfers to compliance outcomes.

corr_cols = ['sanctions_per_100k', 'literacy_rate_2022', 'avg_income_2022',
             'log_population', 'log_income']
# Include log_total_transfers if present (new in muni dataset)
if 'log_total_transfers' in df_analysis.columns:
    corr_cols.append('log_total_transfers')
corr_cols += REGION_DUMMY_COLS

# Cast to float64 (numpy) so np.corrcoef / seaborn are happy with pandas
# nullable Int64/Float64 + rows that contain NaN in any included column.
corr_df = df_analysis[corr_cols].astype('Float64').astype(float)
corr_matrix = corr_df.corr()

plt.figure(figsize=(12, 10))
sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', center=0,
            square=True, linewidths=1, cbar_kws={"shrink": 0.8})
plt.title('Correlation Matrix: Key Variables (municipality-level, N=5,570)',
          fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()"""

FINDINGS_EN = """## 10. Key Findings Summary

### 10.1 Data Quality
- **Grain:** this EDA runs at **municipality level** (5,570 rows, 1 per Brazilian municipality), up from the previous state-level view (27 rows). This gives ~200x more statistical power for correlation and regression work downstream.
- **Complete geographic coverage**: all 27 states and all 5 regions are represented.
- **Sanctions data is dense**: `n_sanctions` is non-null for all 5,570 municipalities (zeros are real, not missing).
- **Transfer-rate feature is sparse**: `sanctions_per_million_brl_transfers` is null for ~91.5% of municipalities (most have no federal transfer records in the current Gold cut). Use with care in any modeling that depends on it.

### 10.2 Regional Patterns (population-weighted)
- Regional numbers are computed as `sum(sanctions) / sum(population) * 100_000` across each region's municipalities -- not as an unweighted mean of per-muni rates -- so they are not dominated by tiny municipalities.
- Once weighted correctly, the ordering of regions by sanctions/100k is flatter than the old state-level unweighted view suggested; see the regional summary table above for the actual values on the current Gold snapshot.

### 10.3 State and Municipality Extremes
- The state-level bar chart is now computed as a **rollup from municipality data** (population-weighted), so every state's number is consistent with the regional totals.
- The top-10 municipalities by `sanctions_per_100k` surface individual outliers that the state rollup hides. Rates for very small municipalities can be unstable (denominator effect) and should be interpreted alongside absolute `n_sanctions` and `population_2022`.

### 10.4 Dummy variables
- Region and state are preserved as identifier columns (`state_code`, `state_name`, `region_code`, `region_name`) AND as one-hot dummies (`is_region_*`, `is_state_*`) for use as regression features.
- The mean of a dummy equals the proportion of municipalities in that category (e.g. `df['is_region_1'].mean()` == share of munis in the Norte region). See the dummy summary table for the actual shares.

### 10.5 Correlations (municipality-level)
- With N=5,570 the correlation coefficients in the heatmap are far more reliable than at the 27-state grain. Inspect the `sanctions_per_100k` row / column in the heatmap above for the strongest bivariate signals; the thesis question about federal transfers vs. compliance outcomes can now be tested at the unit of observation where policy actually lands (the municipality)."""


# ---------------------------------------------------------------------------
# Cell content definitions (pt-BR)
# ---------------------------------------------------------------------------
# The pt-BR notebook uses the GoldDataLoaderPtBr wrapper, which renames columns.
# Municipality-level column names after translation:
#   municipality_code        -> codigo_municipio
#   municipality_name        -> nome_municipio
#   state_code               -> codigo_estado
#   state_name               -> nome_estado
#   region_code              -> codigo_regiao
#   region_name              -> nome_regiao
#   population_2022          -> populacao_2022
#   literacy_rate_2022       -> taxa_alfabetizacao_2022
#   avg_income_2022          -> renda_media_2022
#   n_sanctions              -> num_sancoes
#   sanctions_per_100k       -> sancoes_por_100k
#   log_population           -> log_populacao
#   log_income               -> log_renda
#   log_total_transfers      -> log_total_transferencias

INTRO_MD_PT = """# Análise Exploratória de Dados (EDA)
## Análise de Compliance no Setor Público - TCC MBA

**Objetivo:** Explorar os datasets da camada Gold em **nível municipal** (5.570 registros, 1 linha por município brasileiro) para entender:
- Distribuições e estatísticas descritivas dos dados
- Valores ausentes e qualidade dos dados
- Padrões e relações iniciais
- Variações regionais em indicadores de compliance e socioeconômicos

**Nota sobre granularidade.** O principal dataset analítico usado aqui é `analise_compliance_municipio` (uma linha por município, N=5.570). A antiga agregação em nível estadual (`analise_compliance`, N=27) tinha poucas observações para análise de correlação/regressão significativa. Para preservar o contexto geográfico, `codigo_estado` / `nome_estado` / `codigo_regiao` / `nome_regiao` são mantidos como colunas identificadoras e também codificados em variáveis dummy (`is_region_*`, `is_state_*`), do mesmo jeito que o dataset estadual já codificava as regiões."""

LOAD_CELL_PT = """datasets = loader.load_all()

df_muni = datasets.get('municipio_socioeconomico')
df_state = datasets.get('resumo_estado')
df_sanctions = datasets.get('resumo_sancoes')

# Dataset analítico principal: uma linha por município (N ~= 5.570).
df_analysis = datasets.get('analise_compliance_municipio')

# --- Adiciona dummies one-hot de região e de estado (não estão no dataset muni). ---
# Espelham as colunas is_norte / is_nordeste / ... que existiam no dataset
# estadual -- usando os mesmos nomes humanamente legíveis para que qualquer
# notebook a jusante que referenciasse essas colunas continue funcionando.
# Mantidas como Int64 (0/1) para consistência com a convenção is_* existente
# e para uso direto como features de regressão nos notebooks 02 / 03.
REGION_NAME_TO_DUMMY = {
    'Norte': 'is_norte',
    'Nordeste': 'is_nordeste',
    'Sudeste': 'is_sudeste',
    'Sul': 'is_sul',
    'Centro-Oeste': 'is_centro_oeste',
}
for rname, col in REGION_NAME_TO_DUMMY.items():
    df_analysis[col] = (df_analysis['nome_regiao'] == rname).astype('Int64')
REGION_DUMMY_COLS = list(REGION_NAME_TO_DUMMY.values())

# Dummies de estado: uma por estado, nomeada pelo código IBGE de 2 dígitos.
state_dummies = pd.get_dummies(df_analysis['codigo_estado'], prefix='is_state').astype('Int64')
df_analysis = pd.concat([df_analysis, state_dummies], axis=1)
STATE_DUMMY_COLS = list(state_dummies.columns)

print(f"\\n\u2705 Carregados {len(datasets)} datasets")
print(f"   Dataset analítico principal: analise_compliance_municipio")
print(f"   Linhas: {len(df_analysis):,} municípios, {df_analysis['codigo_estado'].nunique()} estados, {df_analysis['codigo_regiao'].nunique()} regiões")
print(f"   Dummies de região adicionadas ({len(REGION_DUMMY_COLS)}): {REGION_DUMMY_COLS}")
print(f"   Dummies de estado adicionadas ({len(STATE_DUMMY_COLS)}): primeiras 3 = {STATE_DUMMY_COLS[:3]} ... última = {STATE_DUMMY_COLS[-1]}")"""

OVERVIEW_PRINT_PT = """print("=" * 80)
print("DATASET DE ANÁLISE DE COMPLIANCE (NÍVEL MUNICIPAL)")
print("=" * 80)
print(f"Formato: {df_analysis.shape}  (linhas = municípios, colunas = features + dummies)")
print(f"\\nTipos de dados (colunas não-dummy):")
print(df_analysis.drop(columns=REGION_DUMMY_COLS + STATE_DUMMY_COLS).dtypes)
print(f"\\nUso de Memória: {df_analysis.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")"""

HEAD_CELL_PT = """# Pré-visualização de algumas colunas (escondendo as 32 dummies de região+estado).
preview_cols = [c for c in df_analysis.columns if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]
df_analysis[preview_cols].head(10)"""

INFO_CELL_PT = """df_analysis.info(verbose=False)"""

DESCRIBE_CELL_PT = """# Descreve apenas as features analíticas, excluindo as 32 dummies one-hot
# (as dummies são resumidas separadamente abaixo).
numeric_cols = df_analysis.select_dtypes(include=[np.number]).columns
analytical_numeric = [c for c in numeric_cols if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]
df_analysis[analytical_numeric].describe().T"""

DUMMY_SUMMARY_MD_PT = """**Resumo das variáveis dummy.** Para as dummies one-hot de região e estado, a média equivale à proporção de municípios naquela região / estado (ex.: `df['is_region_1'].mean()` é a fração de municípios na região Norte). Um resumo curto é mostrado abaixo para não poluir a tabela principal do `.describe()` com 32 linhas extras."""

DUMMY_SUMMARY_CELL_PT = """dummy_stats = pd.DataFrame({
    'Contagem (=1)': df_analysis[REGION_DUMMY_COLS + STATE_DUMMY_COLS].sum(),
    'Fração de municípios %': (df_analysis[REGION_DUMMY_COLS + STATE_DUMMY_COLS].mean() * 100).round(2),
}).sort_values('Contagem (=1)', ascending=False)
print(f"Total de dummies: {len(dummy_stats)} (5 regiões + {len(STATE_DUMMY_COLS)} estados)")
dummy_stats.head(10)"""

SUMMARY_CELL_PT = """# Médias ponderadas pela população para taxas. Uma média simples entre 5.570
# municípios trataria uma cidade de 500 habitantes e São Paulo da mesma forma,
# o que é estatisticamente enganoso para indicadores de taxa e valor médio.
_pop = df_analysis['populacao_2022']
_lit_mask = df_analysis['taxa_alfabetizacao_2022'].notna()
_inc_mask = df_analysis['renda_media_2022'].notna()

total_pop = int(_pop.sum())
total_sanc = int(df_analysis['num_sancoes'].sum())

summary = pd.DataFrame({
    'Total de Municípios': [len(df_analysis)],
    'Total de Estados': [int(df_analysis['codigo_estado'].nunique())],
    'Total de Regiões': [int(df_analysis['codigo_regiao'].nunique())],
    'População Total (2022)': [total_pop],
    'Total de Sanções': [total_sanc],
    'Sanções/100k (nacional, ponderada)': [round(total_sanc / total_pop * 100_000, 2)],
    'Alfabetização % (ponderada)': [round(np.average(df_analysis.loc[_lit_mask, 'taxa_alfabetizacao_2022'], weights=_pop[_lit_mask]), 2)],
    'Renda Média BRL (ponderada)': [round(np.average(df_analysis.loc[_inc_mask, 'renda_media_2022'], weights=_pop[_inc_mask]), 2)],
})

summary.T"""

MISSING_CELL_PT = """missing = df_analysis.isnull().sum()
missing_pct = (missing / len(df_analysis)) * 100

missing_df = pd.DataFrame({
    'Qtde Ausente': missing,
    'Ausente %': missing_pct.round(2),
}).sort_values('Qtde Ausente', ascending=False)

missing_df[missing_df['Qtde Ausente'] > 0]"""

SOCIO_HIST_PT = """fig, axes = plt.subplots(2, 2, figsize=(16, 12))

axes[0, 0].hist(df_analysis['taxa_alfabetizacao_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='skyblue')
axes[0, 0].set_title('Distribuição da Taxa de Alfabetização 2022 (por município)', fontsize=12, fontweight='bold')
axes[0, 0].set_xlabel('Taxa de Alfabetização (%)')
axes[0, 0].set_ylabel('Frequência')

axes[0, 1].hist(df_analysis['renda_media_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='lightgreen')
axes[0, 1].set_title('Distribuição da Renda Média 2022 (por município)', fontsize=12, fontweight='bold')
axes[0, 1].set_xlabel('Renda Média (BRL)')
axes[0, 1].set_ylabel('Frequência')

axes[1, 0].hist(df_analysis['populacao_2022'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='salmon')
axes[1, 0].set_title('Distribuição da População 2022 (por município)', fontsize=12, fontweight='bold')
axes[1, 0].set_xlabel('População')
axes[1, 0].set_ylabel('Frequência')

axes[1, 1].hist(df_analysis['log_populacao'].dropna(), bins=40, edgecolor='black', alpha=0.7, color='plum')
axes[1, 1].set_title('Distribuição do Log(População) (por município)', fontsize=12, fontweight='bold')
axes[1, 1].set_xlabel('Log(População)')
axes[1, 1].set_ylabel('Frequência')

plt.tight_layout()
plt.show()"""

REGIONAL_SUMMARY_PT = """# Agregação regional a partir de dados em NÍVEL MUNICIPAL, usando
# estatísticas PONDERADAS PELA POPULAÇÃO. Um groupby('nome_regiao').agg('mean')
# simples trataria cada município igualmente, o que é estatisticamente
# enganoso: cidades pequenas dominariam a média para taxas e valores monetários.
# Para taxas e médias regionais, agregamos a partir dos totais e ponderamos
# pela população municipal.

def _region_rollup(g: pd.DataFrame) -> pd.Series:
    pop = g['populacao_2022']
    lit_mask = g['taxa_alfabetizacao_2022'].notna()
    inc_mask = g['renda_media_2022'].notna()
    return pd.Series({
        'N Municípios': len(g),
        'N Estados': g['codigo_estado'].nunique(),
        'População Total': int(pop.sum()),
        'Total de Sanções': int(g['num_sancoes'].sum()),
        'Sanções/100k (ponderada)': round(g['num_sancoes'].sum() / pop.sum() * 100_000, 2),
        'Alfabetização % (ponderada)': round(np.average(g.loc[lit_mask, 'taxa_alfabetizacao_2022'], weights=pop[lit_mask]), 2),
        'Renda BRL (ponderada)': round(np.average(g.loc[inc_mask, 'renda_media_2022'], weights=pop[inc_mask]), 2),
    })

regional_summary = (
    df_analysis.groupby('nome_regiao', observed=True)
    .apply(_region_rollup)
)

regional_summary"""

REGIONAL_DASHBOARD_PT = """# Painel regional construído a partir dos dados municipais (taxas ponderadas pela população).
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Sanções por 100 mil por Região (ponderada)',
                    'Taxa de Alfabetização por Região (ponderada)',
                    'Renda Média por Região (ponderada)',
                    'Total de Sanções por Região'),
    specs=[[{'type': 'bar'}, {'type': 'bar'}],
           [{'type': 'bar'}, {'type': 'bar'}]]
)

regions = regional_summary.reset_index()

fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['Sanções/100k (ponderada)'],
                     name='Sanções/100k', marker_color='indianred'), row=1, col=1)
fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['Alfabetização % (ponderada)'],
                     name='Alfabetização %', marker_color='lightseagreen'), row=1, col=2)
fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['Renda BRL (ponderada)'],
                     name='Renda', marker_color='lightsalmon'), row=2, col=1)
fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['Total de Sanções'],
                     name='Total de Sanções', marker_color='mediumpurple'), row=2, col=2)

fig.update_layout(height=800, showlegend=False, title_text="Painel Regional (construído a partir de 5.570 municípios)")
fig.show()"""

STATE_HEADER_MD_PT = """## 7. Agregação por Estado e Extremos Municipais

Analisamos os dois extremos do espectro de granularidade:

1. **Agregação por estado** (ponderada pela população, a partir dos 5.570 municípios) para um gráfico de barras estável e relevante para políticas públicas.
2. **Top / bottom municípios** por `sancoes_por_100k`, capaz de revelar outliers individuais que a agregação por estado esconde."""

STATE_TOP_MUNI_PT = """# Top 10 e bottom 10 MUNICÍPIOS por sanções por 100 mil hab.
# OBS: taxas per capita para municípios muito pequenos podem ser instáveis
# (efeito denominador) -- usar com cautela.
cols = ['codigo_municipio', 'nome_municipio', 'nome_estado', 'nome_regiao',
        'populacao_2022', 'num_sancoes', 'sancoes_por_100k']

top_10_munis = df_analysis.nlargest(10, 'sancoes_por_100k')[cols]

print("TOP 10 MUNICÍPIOS - Maiores Sanções por 100 mil")
print("=" * 90)
print(top_10_munis.to_string(index=False))

print("\\n\\nBOTTOM 10 MUNICÍPIOS - Menores Sanções por 100 mil (entre os com sanções > 0)")
print("=" * 90)
with_sanctions = df_analysis[df_analysis['num_sancoes'] > 0]
print(with_sanctions.nsmallest(10, 'sancoes_por_100k')[cols].to_string(index=False))"""

STATE_ROLLUP_BAR_PT = """# Agregação por estado a partir dos dados municipais (ponderada pela população).
# nome_estado é usado no eixo x (rótulo qualitativo); codigo_estado é apenas um id.
state_rollup = (
    df_analysis.groupby(['codigo_estado', 'nome_estado', 'nome_regiao'], observed=True)
    .apply(lambda g: pd.Series({
        'populacao': g['populacao_2022'].sum(),
        'num_sancoes': g['num_sancoes'].sum(),
        'sancoes_por_100k': g['num_sancoes'].sum() / g['populacao_2022'].sum() * 100_000,
    }))
    .reset_index()
    .sort_values('sancoes_por_100k', ascending=False)
)

fig = px.bar(state_rollup,
             x='nome_estado', y='sancoes_por_100k',
             color='nome_regiao',
             title='Sanções por 100 mil habitantes por Estado (agregado a partir de 5.570 municípios)',
             labels={'sancoes_por_100k': 'Sanções por 100 mil', 'nome_estado': 'Estado'},
             height=500)
fig.update_xaxes(tickangle=-45)
fig.show()"""

CORR_CELL_PT = """# Matriz de correlação das principais features analíticas em nível municipal
# (5.570 observações em vez de 27 estados -- muito mais poder estatístico).
# Incluímos as dummies de região, mas NÃO as 27 dummies de estado (deixariam
# o heatmap ilegível). Features do lado das transferências também são
# incluídas, já que a pergunta do TCC liga transferências federais a
# resultados de compliance.

corr_cols = ['sancoes_por_100k', 'taxa_alfabetizacao_2022', 'renda_media_2022',
             'log_populacao', 'log_renda']
if 'log_total_transferencias' in df_analysis.columns:
    corr_cols.append('log_total_transferencias')
corr_cols += REGION_DUMMY_COLS

# Cast para float64 (numpy) para np.corrcoef / seaborn aceitarem tipos
# Int64/Float64 nullable do pandas junto com linhas que contenham NaN.
corr_df = df_analysis[corr_cols].astype('Float64').astype(float)
corr_matrix = corr_df.corr()

plt.figure(figsize=(12, 10))
sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', center=0,
            square=True, linewidths=1, cbar_kws={"shrink": 0.8})
plt.title('Matriz de Correlação: Variáveis Principais (nível municipal, N=5.570)',
          fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()"""

FINDINGS_PT = """## 10. Resumo das Principais Descobertas

### 10.1 Qualidade dos Dados
- **Granularidade:** esta EDA roda em **nível municipal** (5.570 linhas, 1 por município brasileiro), ao invés da antiga visão estadual (27 linhas). Isso dá ~200x mais poder estatístico para o trabalho de correlação e regressão a jusante.
- **Cobertura geográfica completa**: todos os 27 estados e todas as 5 regiões estão representados.
- **Dados de sanções são densos**: `num_sancoes` é não-nulo para todos os 5.570 municípios (zeros são reais, não ausentes).
- **Feature de taxa de sanções por transferência é esparsa**: `sancoes_por_milhao_brl_transferencias` é nulo para ~91,5% dos municípios (a maioria não tem registros de transferência federal no corte Gold atual). Usar com cautela em qualquer modelo que dependa dela.

### 10.2 Padrões Regionais (ponderados pela população)
- Os números regionais são calculados como `sum(sancoes) / sum(populacao) * 100_000` entre os municípios de cada região -- não como média simples das taxas municipais -- então não são dominados por municípios pequenos.
- Depois da ponderação correta, a ordenação das regiões por sanções/100k é mais achatada do que a antiga visão estadual sem ponderação sugeria; ver a tabela de resumo regional acima para os valores exatos no snapshot Gold atual.

### 10.3 Extremos Estaduais e Municipais
- O gráfico de barras por estado agora é calculado como uma **agregação a partir dos dados municipais** (ponderada pela população), então o número de cada estado fica consistente com os totais regionais.
- Os top-10 municípios por `sancoes_por_100k` revelam outliers individuais que a agregação estadual esconde. Taxas de municípios muito pequenos podem ser instáveis (efeito denominador) e devem ser interpretadas junto com `num_sancoes` absoluto e `populacao_2022`.

### 10.4 Variáveis dummy
- Região e estado são preservados como colunas identificadoras (`codigo_estado`, `nome_estado`, `codigo_regiao`, `nome_regiao`) E como dummies one-hot (`is_region_*`, `is_state_*`) para uso como features de regressão.
- A média de uma dummy equivale à proporção de municípios naquela categoria (ex.: `df['is_region_1'].mean()` == fração de municípios na região Norte). Ver a tabela de resumo das dummies para as frações exatas.

### 10.5 Correlações (nível municipal)
- Com N=5.570 os coeficientes de correlação no heatmap são muito mais confiáveis do que no grão estadual de 27 linhas. Inspecionar a linha / coluna de `sancoes_por_100k` no heatmap acima para os sinais bivariados mais fortes; a pergunta do TCC sobre transferências federais vs. resultados de compliance agora pode ser testada na unidade de observação onde a política realmente aterriza (o município)."""


# ---------------------------------------------------------------------------
# Cell patching
# ---------------------------------------------------------------------------

def _lines(src: str) -> list[str]:
    """Split into notebook-style list of lines, preserving trailing newlines except on last line."""
    parts = src.split("\n")
    return [p + "\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def _set_markdown(nb: dict, idx: int, content: str) -> None:
    cell = nb["cells"][idx]
    assert cell["cell_type"] == "markdown", f"cell {idx} is not markdown"
    cell["source"] = _lines(content)


def _set_code(nb: dict, idx: int, content: str) -> None:
    cell = nb["cells"][idx]
    assert cell["cell_type"] == "code", f"cell {idx} is not code"
    cell["source"] = _lines(content)
    cell["outputs"] = []
    cell["execution_count"] = None


def _insert_markdown_after(nb: dict, idx: int, content: str) -> None:
    nb["cells"].insert(idx + 1, {
        "cell_type": "markdown",
        "metadata": {},
        "source": _lines(content),
    })


def _insert_code_after(nb: dict, idx: int, content: str) -> None:
    nb["cells"].insert(idx + 1, {
        "cell_type": "code",
        "metadata": {},
        "source": _lines(content),
        "outputs": [],
        "execution_count": None,
    })


def _patch_notebook(nb_path: Path, lang: str) -> None:
    with nb_path.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    cells = nb["cells"]

    # Guard: confirm structure matches what the script was written for.
    expected_firsts = {
        "en": "# Exploratory Data Analysis (EDA)",
        "pt": "# Análise Exploratória de Dados (EDA)",
    }
    first_src = "".join(cells[0].get("source", []))
    if not first_src.strip().startswith(expected_firsts[lang]):
        raise SystemExit(
            f"[abort] {nb_path.name}: unexpected cell[0] content -- notebook structure changed? "
            f"First line: {first_src[:80]!r}"
        )
    if cells[38]["cell_type"] != "markdown":
        raise SystemExit(
            f"[abort] {nb_path.name}: expected cell[38] to be the 'Key Findings' markdown, "
            f"got {cells[38]['cell_type']!r}"
        )

    # Drop any cells we inserted in a previous run of this migration (idempotency).
    # Our markers: cells whose source starts with "# --- Add one-hot region and state dummies"
    # are re-inserted by this script -- remove them and also remove the dummy-summary
    # markdown/code we add.
    cleaned = []
    skip_next_code = False
    for c in cells:
        src = "".join(c.get("source", []))
        if skip_next_code and c["cell_type"] == "code":
            skip_next_code = False
            continue
        if c["cell_type"] == "markdown" and (
            "dummy variable summary" in src.lower()
            or "resumo das variáveis dummy" in src.lower()
        ):
            skip_next_code = True
            continue
        cleaned.append(c)
    nb["cells"] = cleaned
    cells = nb["cells"]

    # Pick content bank based on language.
    if lang == "en":
        bank = {
            "intro": INTRO_MD_EN,
            "load": LOAD_CELL_EN,
            "overview_print": OVERVIEW_PRINT_EN,
            "head": HEAD_CELL_EN,
            "info": INFO_CELL_EN,
            "describe": DESCRIBE_CELL_EN,
            "dummy_md": DUMMY_SUMMARY_MD_EN,
            "dummy_cell": DUMMY_SUMMARY_CELL_EN,
            "summary": SUMMARY_CELL_EN,
            "missing": MISSING_CELL_EN,
            "socio_hist": SOCIO_HIST_EN,
            "regional_summary": REGIONAL_SUMMARY_EN,
            "regional_dashboard": REGIONAL_DASHBOARD_EN,
            "state_header": STATE_HEADER_MD_EN,
            "state_top_muni": STATE_TOP_MUNI_EN,
            "state_rollup": STATE_ROLLUP_BAR_EN,
            "corr": CORR_CELL_EN,
            "findings": FINDINGS_EN,
        }
    else:
        bank = {
            "intro": INTRO_MD_PT,
            "load": LOAD_CELL_PT,
            "overview_print": OVERVIEW_PRINT_PT,
            "head": HEAD_CELL_PT,
            "info": INFO_CELL_PT,
            "describe": DESCRIBE_CELL_PT,
            "dummy_md": DUMMY_SUMMARY_MD_PT,
            "dummy_cell": DUMMY_SUMMARY_CELL_PT,
            "summary": SUMMARY_CELL_PT,
            "missing": MISSING_CELL_PT,
            "socio_hist": SOCIO_HIST_PT,
            "regional_summary": REGIONAL_SUMMARY_PT,
            "regional_dashboard": REGIONAL_DASHBOARD_PT,
            "state_header": STATE_HEADER_MD_PT,
            "state_top_muni": STATE_TOP_MUNI_PT,
            "state_rollup": STATE_ROLLUP_BAR_PT,
            "corr": CORR_CELL_PT,
            "findings": FINDINGS_PT,
        }

    # --- Replace cells in place ----------------------------------------------
    # Expected original indices (after dummy-cell cleanup, before insertions):
    # 0  markdown  intro
    # 1  code      AUTO-GENERATED DEPENDENCY INSTALL -- keep
    # 2  markdown  Step-by-step -- keep
    # 3  markdown  Packages -- keep
    # 4  code      imports -- keep
    # 5  code      matplotlib rcParams -- keep
    # 6  markdown  Reproducibility -- keep
    # 7  code      SEED -- keep
    # 8  code      runtime config -- keep
    # 9  markdown  ## 1. Load Data -- keep
    # 10 code      loader init -- keep
    # 11 code      datasets = loader.load_all() + df_analysis  -- REPLACE
    # 12 markdown  ## 2. Dataset Overview -- keep
    # 13 code      overview print -- REPLACE
    # 14 code      df_analysis.head(10) -- REPLACE
    # 15 code      df_analysis.info() -- REPLACE
    # 16 markdown  ## 3. Descriptive Statistics -- keep
    # 17 code      describe -- REPLACE
    # 18 markdown  ### Key Metrics Summary -- keep
    # 19 code      summary -- REPLACE
    # 20 markdown  ## 4. Missing Values -- keep
    # 21 code      missing -- REPLACE
    # 22 markdown  ## 5. Distribution -- keep
    # 23 markdown  ### 5.1 Target Variable -- keep
    # 24 code      sanctions_per_100k hist/box/qq -- keep (column name unchanged)
    # 25 markdown  ### 5.2 Socioeconomic -- keep
    # 26 markdown  log transformation note -- keep
    # 27 code      socio 2x2 hist -- REPLACE
    # 28 markdown  ## 6. Regional Analysis -- keep
    # 29 code      regional summary -- REPLACE
    # 30 code      regional dashboard -- REPLACE
    # 31 markdown  ## 7. State-Level Analysis -- REPLACE HEADER
    # 32 code      top/bottom 10 states -- REPLACE (top munis)
    # 33 code      per-state bar -- REPLACE (state rollup bar)
    # 34 markdown  ## 8. Sanctions Registry -- keep
    # 35 code      sanctions pie -- keep (uses df_sanctions)
    # 36 markdown  ## 9. Correlation -- keep
    # 37 code      corr matrix -- REPLACE
    # 38 markdown  ## 10. Key Findings -- REPLACE

    _set_markdown(nb, 0, bank["intro"])
    _set_code(nb, 11, bank["load"])
    _set_code(nb, 13, bank["overview_print"])
    _set_code(nb, 14, bank["head"])
    _set_code(nb, 15, bank["info"])
    _set_code(nb, 17, bank["describe"])
    _set_code(nb, 19, bank["summary"])
    _set_code(nb, 21, bank["missing"])
    _set_code(nb, 27, bank["socio_hist"])
    _set_code(nb, 29, bank["regional_summary"])
    _set_code(nb, 30, bank["regional_dashboard"])
    _set_markdown(nb, 31, bank["state_header"])
    _set_code(nb, 32, bank["state_top_muni"])
    _set_code(nb, 33, bank["state_rollup"])
    _set_code(nb, 37, bank["corr"])
    _set_markdown(nb, 38, bank["findings"])

    # --- Insert dummy summary (markdown + code) right after Key Metrics block -
    # We insert AFTER cell 19 (summary) -- which means the inserted cells land
    # at indices 20 and 21, pushing the old 20 (Missing heading) to 22, etc.
    # Do this AFTER all in-place replacements so indices above stay stable.
    _insert_code_after(nb, 19, bank["dummy_cell"])
    _insert_markdown_after(nb, 19, bank["dummy_md"])

    # --- Write back ----------------------------------------------------------
    with nb_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)
        f.write("\n")

    print(f"[ok] Patched {nb_path.name}: {len(nb['cells'])} cells total")


def main() -> None:
    for nb_path, lang in [(NB_EN, "en"), (NB_PT, "pt")]:
        print(f"\nPatching {nb_path.relative_to(REPO)} (lang={lang}) ...")
        _patch_notebook(nb_path, lang)
    print("\nDone.")


if __name__ == "__main__":
    main()
