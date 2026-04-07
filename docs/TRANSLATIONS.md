# Translations Guide (pt-BR)

This document describes how English Gold dataset/column names are translated to Portuguese for notebooks and reports.

Primary implementation files:
- `src/config/pt_br_translations.py`
- `src/analysis/pt_br_loader.py`

---

## Dataset Name Mappings

| English | Portuguese (code) | Portuguese (display) |
|---|---|---|
| `municipality_socioeconomic` | `municipio_socioeconomico` | Município - Socioeconômico |
| `state_summary` | `resumo_estado` | Resumo por Estado |
| `sanctions_summary` | `resumo_sancoes` | Resumo de Sanções |
| `analysis_compliance` | `analise_compliance` | Análise de Compliance |
| `consolidated_clustering` | `clustering_consolidado` | Clustering Consolidado |

---

## Usage Patterns

### 1) Load translated datasets directly

```python
from src.analysis.pt_br_loader import GoldDataLoaderPtBr

loader = GoldDataLoaderPtBr(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
    use_display_names=False,
)

datasets = loader.load_all()
df = datasets["analise_compliance"]
```

### 2) Use display names for charts/tables

```python
loader = GoldDataLoaderPtBr(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
    use_display_names=True,
)

datasets = loader.load_all()
print(datasets["analise_compliance"].columns)
```

### 3) Translate an already loaded DataFrame

```python
from src.analysis.data_loader import GoldDataLoader
from src.config.pt_br_translations import translate_dataframe_columns

base_loader = GoldDataLoader("enok-mba-thesis-datalake", aws_profile="mba-thesis")
df_en = base_loader.load_dataset("analysis_compliance")

df_pt = translate_dataframe_columns(df_en, display=False)
df_display = translate_dataframe_columns(df_en, display=True)
```

---

## Maintenance

When adding new Gold columns or datasets:
1. update `DATASET_TRANSLATIONS` if dataset keys change
2. update `COLUMN_TRANSLATIONS` for code-friendly Portuguese names
3. update `DISPLAY_NAME_TRANSLATIONS` for report/chart labels
4. keep this file synchronized with the new mappings
