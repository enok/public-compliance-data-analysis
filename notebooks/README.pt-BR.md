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

### Estilo de Organização dos Notebooks

Os notebooks finais mantêm o padrão inspirado nas aulas em um único arquivo canônico por tema.

Cada notebook inclui seções explícitas de:
- `# Pacotes`
- `# Reprodutibilidade` (`SEED = 42`)
- carregamento de dados, blocos de análise e resumo

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

Clustering por cidade e comparação de cidades dentro do mesmo cluster:

```python
from src.analysis.city_clustering import (
    build_same_cluster_peer_table,
    cluster_cities,
    compare_cities_in_same_cluster,
)

df_cluster = loader.load_dataset("consolidated_clustering")
result = cluster_cities(df_cluster, n_clusters=None, min_k=2, max_k=10)
df_clustered = result.clustered_df

# Compara uma cidade-alvo com as mais próximas dentro do mesmo cluster
city_peers = compare_cities_in_same_cluster(
    df_clustered,
    municipality_code="3550308",  # Sao Paulo
    top_n=5,
)

# Gera tabela completa de pares cidade-a-cidade por cluster
all_peers = build_same_cluster_peer_table(df_clustered, top_n=3)
```

---

## Chaves de Datasets Gold Disponíveis

- `analysis_compliance`
- `analysis_compliance_municipality`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

---

## Workflow de Análise Municipal Completa

Para executar uma análise municipal completa no estilo da análise estadual (estatística + ML + pares por cluster + adendo de conclusão):

```bash
python ../scripts/run_city_full_analysis.py
```

Os artefatos são gravados por padrão em `docs/city_full_analysis/`.

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
