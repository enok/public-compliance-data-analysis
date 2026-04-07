# Notebooks de Análise

Este diretório contém os notebooks analíticos bilíngues da tese.

Versão em inglês: [README.md](README.md)

---

## Notebooks

1. `01_exploratory_data_analysis.ipynb`
- exploração dos datasets Gold, distribuições, missing e padrões regionais.

2. `02_statistical_analysis.ipynb`
- testes de correlação, testes de hipótese, diagnósticos OLS e interpretação.

3. `03_machine_learning.ipynb`
- baselines de regressão/classificação, validação cruzada e importância de features.

4. `04_clustering_analysis.ipynb`
- PCA e K-means usando o dataset consolidado de clustering municipal.

---

## Pré-requisitos

1. Instalar dependências:

```bash
pip install -r ../requirements.txt
```

2. Garantir que a Gold foi gerada:

```bash
./scripts/03_gold_transformation.sh
```

3. Configurar credenciais/perfil AWS para leitura no S3.

---

## Execução

```bash
cd notebooks
jupyter notebook
# ou
jupyter lab
```

---

## Carregador de Dados

Todos os notebooks podem usar `GoldDataLoader`:

```python
from src.analysis.data_loader import GoldDataLoader

loader = GoldDataLoader(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
)

df = loader.load_dataset("analysis_compliance")
datasets = loader.load_all()
```

Carregamento traduzido para português:

```python
from src.analysis.pt_br_loader import GoldDataLoaderPtBr

loader_pt = GoldDataLoaderPtBr(
    bucket_name="enok-mba-thesis-datalake",
    aws_profile="mba-thesis",
    use_display_names=False,
)
```

---

## Chaves de Datasets Gold Disponíveis

- `analysis_compliance`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

---

## Solução de Problemas Comum

### `Dataset not found in S3`
Execute Silver e Gold:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### `AWS credentials not found`
Configure o perfil e exporte/use no ambiente.

### `Module not found`
Instale dependências via `../requirements.txt`.
