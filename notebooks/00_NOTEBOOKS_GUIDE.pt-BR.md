# Guia dos Notebooks de Análise

Este guia resume o fluxo recomendado dos notebooks e os datasets esperados.

---

## Ordem Recomendada

1. `01_exploratory_data_analysis.ipynb`
2. `02_statistical_analysis.ipynb`
3. `03_machine_learning.ipynb`
4. `04_clustering_analysis.ipynb`

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

## Problemas Comuns

### Dataset ausente no S3
Execute:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### Problemas de autenticação AWS
Verifique perfil e credenciais ativos antes de abrir os notebooks.
