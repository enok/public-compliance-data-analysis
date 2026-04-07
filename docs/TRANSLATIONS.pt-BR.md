# Guia de Traduções (pt-BR)

Este documento descreve como nomes de datasets e colunas da Gold (em inglês) são traduzidos para português em notebooks e relatórios.

Arquivos principais:
- `src/config/pt_br_translations.py`
- `src/analysis/pt_br_loader.py`

---

## Mapeamento de Datasets

| Inglês | Português (código) | Português (exibição) |
|---|---|---|
| `municipality_socioeconomic` | `municipio_socioeconomico` | Município - Socioeconômico |
| `state_summary` | `resumo_estado` | Resumo por Estado |
| `sanctions_summary` | `resumo_sancoes` | Resumo de Sanções |
| `analysis_compliance` | `analise_compliance` | Análise de Compliance |
| `consolidated_clustering` | `clustering_consolidado` | Clustering Consolidado |

---

## Formas de Uso

### 1) Carregar datasets já traduzidos

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

### 2) Usar nomes de exibição para gráficos e tabelas

```python
loader = GoldDataLoaderPtBr(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
    use_display_names=True,
)

datasets = loader.load_all()
print(datasets["analise_compliance"].columns)
```

### 3) Traduzir um DataFrame já carregado

```python
from src.analysis.data_loader import GoldDataLoader
from src.config.pt_br_translations import translate_dataframe_columns

base_loader = GoldDataLoader("enok-mba-thesis-datalake", aws_profile="mba-thesis")
df_en = base_loader.load_dataset("analysis_compliance")

df_pt = translate_dataframe_columns(df_en, display=False)
df_display = translate_dataframe_columns(df_en, display=True)
```

---

## Manutenção

Quando adicionar novas colunas ou datasets da Gold:
1. atualize `DATASET_TRANSLATIONS` se houver alteração de chaves
2. atualize `COLUMN_TRANSLATIONS` para nomes de código em português
3. atualize `DISPLAY_NAME_TRANSLATIONS` para rótulos de exibição
4. sincronize este documento com os novos mapeamentos
