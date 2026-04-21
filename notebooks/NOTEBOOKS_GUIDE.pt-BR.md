# Guia dos Notebooks de Análise

Este guia resume o fluxo recomendado dos notebooks e os datasets esperados.

---

## Sequência dos Notebooks (Ordem Recomendada)

Os notebooks estão organizados em sequência lógica: engenharia de dados → análises → compilação da tese:

| Ordem | Notebook | Idioma | Propósito |
|-------|----------|--------|-----------|
| 0 | `00_etl_pipeline.ipynb` | Inglês | **Pipeline ETL**: Execução Bronze → Silver → Gold |
| 0 | `00_etl_pipeline.pt-BR.ipynb` | Português | **Pipeline ETL**: Execução completa Bronze → Silver → Gold |
| 1 | `01_exploratory_data_analysis.ipynb` | Inglês | **EDA**: Perfil de dados, distribuições, quality checks |
| 1 | `01_exploratory_data_analysis.pt-BR.ipynb` | Português | **EDA**: Perfil de dados, distribuições, quality checks |
| 2 | `02_statistical_analysis.ipynb` | Inglês | **Estatística**: Correlação, regressão OLS, testes de hipótese |
| 2 | `02_statistical_analysis.pt-BR.ipynb` | Português | **Estatística**: Correlação, regressão OLS, testes de hipótese |
| 3 | `03_machine_learning.ipynb` | Inglês | **ML**: Modelos supervisionados (ElasticNet, Random Forest) |
| 3 | `03_machine_learning.pt-BR.ipynb` | Português | **ML**: Modelos de aprendizado supervisionado |
| 4 | `04_clustering_analysis.ipynb` | Inglês | **Clustering**: Segmentação K-means, visualização PCA |
| 4 | `04_clustering_analysis.pt-BR.ipynb` | Português | **Clusterização**: Segmentação K-means, visualização PCA |
| 5 | `05_corruption_hdi_clusters.ipynb` | Inglês | **Corrupção vs IDH**: Análise de correlação estratificada |
| 5 | `05_corruption_hdi_clusters_pt-BR.ipynb` | Português | **Corrupção vs IDH**: Análise de correlação estratificada |
| 6 | `06_complete_thesis_pipeline.ipynb` | Inglês | **Notebook Master**: ETL + análises + QGIS + Dashboard |
| 6 | `06_complete_thesis_pipeline.pt-BR.ipynb` | Português | **Notebook Master**: ETL completo + análises + QGIS + Dashboard |

---

## Categorias de Notebooks

### Categoria 0: Engenharia de Dados (ETL)

**Notebooks**: `00_etl_pipeline.ipynb` (EN), `00_etl_pipeline.pt-BR.ipynb` (PT)

Execute primeiro para materializar as camadas de dados a partir das fontes brutas:
1. **Camada Bronze**: Ingestão bruta de APIs (IBGE, Portal da Transparência)
2. **Camada Silver**: Normalização, deflação, star schema
3. **Camada Gold**: Agregações prontas para análise

**Pré-requisitos**: Credenciais AWS, chaves de API no `.env` (para Bronze)

**Caso de Uso**: Execute quando:
- Começando do zero (sem dados locais)
- Precisa atualizar dados das APIs
- Configurando um novo ambiente

---

### Categoria 1-5: Notebooks de Análise Individual (Modular)

**Notebooks**: `01` a `05` (ambos idiomas)

Execute estes para análises focadas em tópicos específicos após o ETL estar completo:

| Ordem | Notebook | Propósito |
|-------|----------|---------|
| 1 | `01_exploratory_data_analysis.ipynb` | Perfil dos dados, distribuições, quality checks |
| 2 | `02_statistical_analysis.ipynb` | Correlação, regressão OLS, testes de hipótese |
| 3 | `03_machine_learning.ipynb` | Modelos supervisionados e avaliação |
| 4 | `04_clustering_analysis.ipynb` | Segmentação não-supervisionada, visualização PCA |
| 5 | `05_corruption_hdi_clusters.ipynb` | Proxy de corrupção vs IDH por cluster |

**Caso de Uso**: Use estes quando:
- Já executou `00_etl_pipeline` e tem dados Gold prontos
- Quer explorar análises específicas em profundidade
- Precisa iterar em uma metodologia específica

---

### Categoria 6: Notebook Master da Tese (Fluxo Completo)

**Notebooks**: `06_complete_thesis_pipeline.ipynb` (EN), `06_complete_thesis_pipeline.pt-BR.ipynb` (PT)

Execute para uma análise completa da tese (dados até conclusões) em um único notebook:
1. ETL (Bronze → Silver → Gold) — opcional, pode usar dados existentes
2. Análise Exploratória de Dados
3. Análise Estatística
4. Machine Learning
5. Análise de Clustering
6. Análise Corrupção vs IDH
7. Geração de Mapas QGIS
8. Exportação para Dashboard
9. Conclusões e Referências

**Caso de Uso**: Execute quando:
- Quer o fluxo completo da tese em um documento
- Gerando evidências finais da tese
- Precisa de um único artefato reprodutível

---

## Caminhos de Execução Recomendados

### Caminho A: ETL Modular + Análises Individuais (Recomendado para Desenvolvimento)

Execute os notebooks em sequência para execução passo a passo:

```bash
# Passo 0: Engenharia de Dados (execute uma vez para materializar dados)
jupyter notebook notebooks/00_etl_pipeline.pt-BR.ipynb

# Passos 1-5: Análises individuais (podem rodar independentemente após ETL)
jupyter notebook notebooks/01_exploratory_data_analysis.pt-BR.ipynb
jupyter notebook notebooks/02_statistical_analysis.pt-BR.ipynb
jupyter notebook notebooks/03_machine_learning.pt-BR.ipynb
jupyter notebook notebooks/04_clustering_analysis.pt-BR.ipynb
jupyter notebook notebooks/05_corruption_hdi_clusters_pt-BR.ipynb
```

Este caminho permite:
- Executar e verificar cada camada (Bronze, Silver, Gold)
- Iterar nas análises individuais sem re-executar ETL
- Debugar problemas em estágios específicos

### Caminho B: Notebook Master da Tese (Recomendado para Resultados Finais)

Execute o pipeline completo da tese em um notebook:

```bash
# Para versão em inglês
jupyter notebook notebooks/06_complete_thesis_pipeline.ipynb

# Para versão em português
jupyter notebook notebooks/06_complete_thesis_pipeline.pt-BR.ipynb
```

Este único notebook irá:
- Executar ETL (ou usar dados em cache existentes)
- Rodar todas as análises (EDA, Estatística, ML, Clustering, Corrupção vs IDH)
- Gerar mapas QGIS e exports para Dashboard
- Produzir conclusões finais com bibliografia completa

**Melhor para**: Gerar o documento de evidências completo da tese.

---

## Objetivos dos Notebooks

### 0) Pipeline ETL
- Ingerir dados brutos das APIs (IBGE, Portal da Transparência)
- Normalizar e transformar (Bronze → Silver → Gold)
- Materializar datasets prontos para análise

### 1) EDA
- Perfil da qualidade dos dados
- Inspecionar distribuições e missingness
- Comparar regiões/estados

### 2) Análise Estatística
- Correlação e testes de hipótese
- Modelagem OLS e diagnósticos
- Interpretação de regressão

### 3) Machine Learning
- Baselines supervisionados para regressão/classificação
- Comparação de modelos e relevância de features

### 4) Clustering
- Redução de dimensionalidade PCA
- Segmentação K-means em features municipais consolidadas

### 5) Corrupção vs IDH
- Análise de correlação estratificada por cluster
- Construção do índice de vulnerabilidade
- Implicações de política pública

### 6) Notebook Master
- Fluxo completo da tese em um documento
- Todas as referências bibliográficas (formato ABNT)
- Reprodutível de dados brutos a conclusões

---

## Configuração de Ambiente

```bash
pip install -r ../requirements.txt

# Execute o ETL primeiro (ou via notebook 00)
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# Ou tudo de uma vez
./scripts/run_pipeline.sh

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

### Dados faltando (sem Bronze/Silver/Gold local)
Execute o pipeline ETL primeiro:

```bash
# Opção 1: Via notebook
jupyter notebook notebooks/00_etl_pipeline.pt-BR.ipynb

# Opção 2: Via scripts shell
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# Ou tudo de uma vez
./scripts/run_pipeline.sh
```

### Problemas de autenticação AWS
Verifique o perfil ativo e credenciais antes de abrir os notebooks:

```bash
aws configure --profile mba-thesis
aws sts get-caller-identity --profile mba-thesis
```

### Dependências faltando
Todos os notebooks incluem uma célula de auto-instalação no topo. Se pulou, execute manualmente:

```bash
pip install -r requirements.txt
```
