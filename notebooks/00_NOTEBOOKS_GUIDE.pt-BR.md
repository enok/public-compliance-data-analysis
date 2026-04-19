# Guia dos Notebooks de Análise

Este guia resume o fluxo recomendado dos notebooks e os datasets esperados.

---

## Categorias de Notebooks

### Categoria 1: Pipeline End-to-End (Fluxo Completo)

Execute estes notebooks para uma análise completa desde a ingestão até os resultados finais:

| Notebook | Idioma | Propósito |
|----------|----------|---------|
| `05_end_to_end_pipeline.ipynb` | Inglês | ETL Completo → Análise → Resultados |
| `05_end_to_end_pipeline.pt-BR.ipynb` | Português | ETL Completo → Análise → Resultados |

**Caso de Uso**: Execute quando quiser executar todo o pipeline em um notebook:
1. Ingestão de Dados (camada Bronze das APIs)
2. Processamento de Dados (normalização camada Silver)
3. Engenharia de Features (camada Gold)
4. Análise Exploratória de Dados
5. Análise Estatística (regressão OLS)
6. Machine Learning (ElasticNet, Random Forest)
7. Análise de Clustering (K-means + PCA)
8. Exportação de Resultados

**Modos de Armazenamento**: Estes notebooks suportam os modos `local-only`, `s3-only`, ou `both`.

### Categoria 2: Notebooks de Análise Individual (Modular)

Execute estes para análises focadas em tópicos específicos:

| Ordem | Notebook | Propósito |
|-------|----------|---------|
| 1 | `01_exploratory_data_analysis.ipynb` | Perfil dos dados, distribuições, quality checks |
| 2 | `02_statistical_analysis.ipynb` | Correlação, regressão OLS, testes de hipótese |
| 3 | `03_machine_learning.ipynb` | Modelos supervisionados e avaliação |
| 4 | `04_clustering_analysis.ipynb` | Segmentação não-supervisionada, visualização PCA |

**Caso de Uso**: Use estes quando:
- Já executou o pipeline e tem dados Gold prontos
- Quer explorar análises específicas em profundidade
- Precisa iterar em uma metodologia específica

---

## Ordem Recomendada

### Opção A: End-to-End Rápido (Recomendado para Resultados)

Execute **apenas** o notebook de pipeline end-to-end:

```bash
# Para Inglês
jupyter notebook notebooks/05_end_to_end_pipeline.ipynb

# Para Português
jupyter notebook notebooks/05_end_to_end_pipeline.pt-BR.ipynb
```

Este único notebook irá:
- Ingerir todos os dados das APIs (ou usar dados existentes)
- Processar através das camadas Bronze → Silver → Gold
- Executar todas as análises (EDA, Estatística, ML, Clustering)
- Exportar resultados e figuras

### Opção B: Análise Modular (Para Desenvolvimento)

Execute os notebooks 01-04 em sequência após os dados estarem prontos:

1. `01_exploratory_data_analysis.ipynb`
2. `02_statistical_analysis.ipynb`
3. `03_machine_learning.ipynb`
4. `04_clustering_analysis.ipynb`

Os notebooks finais mantêm a estrutura no estilo aula diretamente em cada arquivo canônico.

---

## Objetivos dos Notebooks

### 1) EDA
- avaliar qualidade dos dados
- inspecionar distribuições e missing
- comparar regiões/estados

### 2) Análise estatística
- correlação e testes de hipótese
- modelagem OLS e diagnósticos
- interpretação de regressão

### 3) Machine learning
- baselines supervisionados de regressão/classificação
- comparação de modelos e relevância de features

### 4) Clustering
- redução de dimensionalidade com PCA
- segmentação K-means com features municipais consolidadas

---

## Configuração de Ambiente

```bash
pip install -r ../requirements.txt
./scripts/03_gold_transformation.sh
cd notebooks
jupyter notebook
```

---

## Padrão de Acesso aos Dados

```python
from src.analysis.data_loader import GoldDataLoader

loader = GoldDataLoader("enok-mba-thesis-datalake", aws_profile="mba-thesis")
datasets = loader.load_all()
```

Chaves de datasets disponíveis:
- `analysis_compliance`
- `municipality_socioeconomic`
- `state_summary`
- `sanctions_summary`
- `consolidated_clustering`

---

## Padrão de Estrutura dos Notebooks

Todos os notebooks seguem a mesma estrutura inicial:
- `# Pacotes`
- `# Reprodutibilidade` com `SEED = 42`
- bloco de carregamento de dados
- seções de análise
- resumo de descobertas

---

## Problemas Comuns

### Dataset ausente no S3
Execute:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### Problemas de autenticação AWS
Verifique perfil e credenciais ativos antes de abrir os notebooks.
