# Insumos da Tese — Referência Completa

> **Título do TCC:** Compliance Público Baseado em Dados: Correlação entre Repasses Federais e Indicadores Socioeconômicos Municipais
>
> **Autor:** Enok Antônio de Jesus | **Orientador:** Prof. Dr. Carlos Nabil Ghobril
>
> **Instituição:** USP / ESALQ — MBA em Data Science & Analytics
>
> **Data:** 19/04/2026

---

## Pergunta de Pesquisa

A **corrupção** (ou **mau uso do dinheiro público**) contribui para um **menor IDH** em municípios brasileiros, impactando a condição socioeconômica da sociedade como um todo?

---

## Estrutura da Tese

```
┌──────────────────────────────────────────────────────────────────────┐
│  CAPÍTULO 1: Introdução                                              │
│    - Contexto do problema (corrupção no gasto público brasileiro)    │
│    - Objetivos da pesquisa                                           │
│    - Relevância e justificativa                                      │
├──────────────────────────────────────────────────────────────────────┤
│  CAPÍTULO 2: Material e Métodos                                      │
│    - Fontes de dados (IBGE, Portal da Transparência, IPCA)           │
│    - Pipeline ETL (Bronze → Silver → Gold)                           │
│    - Métodos analíticos (EDA, OLS, ML, K-means, PCA)                 │
├──────────────────────────────────────────────────────────────────────┤
│  CAPÍTULO 3: Resultados e Discussão                                  │
│    - Análise exploratória (NB01)                                     │
│    - Inferência estatística (NB02)                                   │
│    - Aprendizado de máquina (NB03)                                   │
│    - Análise de agrupamento (NB04)                                   │
│    - Corrupção vs IDH por cluster (NB06) ← NOVO                      │
├──────────────────────────────────────────────────────────────────────┤
│  CAPÍTULO 4: Conclusão                                               │
│    - Principais achados                                              │
│    - Limitações                                                      │
│    - Implicações de política pública                                 │
│    - Trabalhos futuros                                               │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Parte 1: Pipeline ETL (Material e Métodos — Seção 2.2)

O pipeline de engenharia de dados é **uma contribuição metodológica central** desta tese.

### Arquitetura: Medalhão (Bronze/Silver/Gold)

```
    ┌─────────┐      ┌─────────┐      ┌─────────┐
    │ BRONZE  │ ───► │ SILVER  │ ───► │  GOLD   │
    │ (bruto) │      │(normal.)│      │(analít.)│
    └─────────┘      └─────────┘      └─────────┘
        │                │                │
        ▼                ▼                ▼
    S3 JSON          S3 Parquet      S3 Parquet
    Respostas API    Dim/Fato        Agregações
```

### Camada Bronze — Ingestão Bruta

| Fonte | Dataset | Período | Registros |
|-------|---------|---------|-----------|
| **API IBGE SIDRA** | Censo 2010 (população, alfabetização, renda, saneamento) | 2010 | 5.565 municípios |
| **API IBGE SIDRA** | Censo 2022 (população, alfabetização, renda, saneamento) | 2022 | 5.570 municípios |
| **Portal da Transparência** | Transferências Federais (Mensal) | 2013-12 a 2022-12 | ~109 arquivos mensais |
| **Portal da Transparência** | CEIS (Sanções - Inidôneas) | Cumulativo | 22.545 registros |
| **Portal da Transparência** | CNEP (Sanções - Entes Privados) | Cumulativo | 1.625 registros |
| **Portal da Transparência** | CEPIM (Sanções - Impedidas) | Cumulativo | 3.579 registros |
| **BCB SGS 433** | IPCA (Inflação) Índice Mensal | 1980-2026 | 47 linhas (anuais) |

**Evidência Chave:** `2010-01` a `2013-11` revalidados como fonte-vazia sob atualização forçada.

**Localização:** `bronze/` no bucket S3 `enok-mba-thesis-datalake`
**Cache Local:** `data/bronze/` (disponível quando necessário)
**Scripts:** `scripts/01_bronze_ingestion.sh`

### Camada Silver — Normalização

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `dim_municipalities` | 5.570 | Lookup municipal com estado/região |
| `dim_municipality_lookup` | 5.570 | Lookup normalizado de nomes para correspondência |
| `dim_inflation_index` | 47 | Deflator IPCA (base R$ 2022) |
| `fact_population` | 11.135 | 2010 (5.565) + 2022 (5.570) |
| `fact_literacy` | 11.135 | Taxa de alfabetização por ano |
| `fact_income` | 11.135 | Renda real + nominal |
| `fact_sanitation` | 11.135 | Domicílios por ano |
| `fact_sanctions` | 27.749 | Todas sanções (CEIS + CNEP + CEPIM) |
| `fact_federal_transfers` | 2.065 | Transferências mensais agregadas |

**Localização:** `silver/` no S3 + `data/silver/` local
**Script:** `scripts/02_silver_transformation.sh`

### Camada Gold — Datasets Prontos para Análise

| Tabela | Registros | Cols | Propósito |
|--------|-----------|------|-----------|
| `agg_municipality_socioeconomic` | 5.570 | 21 | Features socioeconômicas municipais |
| `agg_state_summary` | 27 | 18 | Agregações estaduais |
| `agg_sanctions_summary` | 3 | 8 | Resumo por tipo de sanção |
| `analysis_compliance` | 27 | 20 | ML-ready estadual |
| `analysis_compliance_municipality` | 5.570 | 22 | ML-ready municipal |
| `consolidated_clustering` | 5.565 | 37 | Features para clustering (12 normalizadas) |

**Localização:** `gold/` no S3 + `data/gold/` local (2,0MB)
**Script:** `scripts/03_gold_transformation.sh`

### Código do Pipeline de Dados

| Componente | Arquivo | Propósito |
|------------|---------|-----------|
| Ingestores Bronze | `src/ingestion/` | Clientes API para IBGE, Transparência, BCB |
| Transformador Silver | `src/processing/silver_transformer.py` | Validação de schema, deflação |
| Transformador Gold | `src/processing/gold_transformer.py` | Agregações, engenharia de features |
| Helpers de Análise | `src/analysis/` | Clustering, ML, apresentação |

---

## Parte 2: Notebooks Analíticos (Resultados — Capítulo 3)

Todos os notebooks são **bilíngues (EN + pt-BR)**.

| Notebook | Propósito | Evidência para Tese |
|----------|-----------|---------------------|
| `01_exploratory_data_analysis` | Estatísticas descritivas + visualizações | Capítulo 3.1 |
| `02_statistical_analysis` | Regressão OLS, testes de hipótese | Capítulo 3.2 |
| `03_machine_learning` | ElasticNet, Random Forest, Logístico | Capítulo 3.3 |
| `04_clustering_analysis` | K-means (K=4), PCA, mapas do Brasil | Capítulo 3.4 |
| `05_corruption_hdi_clusters` | Corrupção vs IDH por cluster | Capítulo 3.5 |
| `06_complete_thesis_pipeline` | **MASTER:** ETL completo + Análise + QGIS + Dashboard | Capítulo 2.2 + 3 + 4 |

### Principais Achados Estatísticos

| Achado | Evidência | Onde |
|--------|-----------|------|
| **Renda → Sanções**: r = 0,74 (p < 0,001) | Nível estadual (n=27) | NB01, NB02 |
| **R² OLS = 0,835** | Renda + dummies regionais | NB02 |
| **Efeito Norte/Nordeste** | β = +22 após controlar renda | NB02 |
| **Distrito Federal outlier** | 65,45 sanções/100k (2x próximo estado) | NB02 |
| **K-means K=4**, silhueta=0,288 | Clustering municipal | NB04 |
| **PCA 3 PCs = 84,4% variância** | Redução de dimensionalidade | NB04 |
| **r municipal = 0,149** (mais fraca) | n=5.570 cidades | NB03 estendido |

---

## Parte 3: Nova Análise - Corrupção vs IDH (NB05)

### Hipótese

**H1:** Municípios com mais sanções por real transferido (proxy de corrupção/ineficiência) têm menor IDH dentro do mesmo cluster.

### Variáveis

**Proxy de Corrupção (X):**
- `sanctions_per_million_brl_transfers`: Número de sanções por milhão de reais transferidos

**Componentes do IDH (Y):**
- `avg_income_real_2022_2022_brl` (renda ajustada IPCA)
- `literacy_rate_2022` (proxy educação)
- `avg_income_2022` (renda nominal)

**Controle:**
- `cluster` (K-means, K=4)

### Execução

```bash
# 1. Baixar dados localmente (já feito)
jupyter nbconvert --to notebook --execute notebooks/06_complete_thesis_pipeline.ipynb

# 2. Rodar análise
python3 scripts/run_corruption_hdi_analysis.py

# 3. Gerar mapa QGIS
python3 scripts/generate_map_geojson.py
```

### Resultados

**Analisados:** 5.570 municípios totais, **469 com dados de sanções**, 4 clusters

**Correlação por Cluster:** Nenhuma correlação estatisticamente significativa (p < 0,05) detectada dentro dos clusters. Isso sugere:
1. O efeito corrupção→IDH é **heterogêneo** entre municípios
2. A métrica de sanções tem **viés de detecção** (capacidade institucional afeta relato)
3. Tamanho de efeito é **baixo** no nível municipal ao controlar similaridade socioeconômica

**Distribuição dos Clusters:**
| Cluster | N | Índice de Vulnerabilidade Médio |
|---------|---|----------------------------------|
| 0 | 233 | +1,70 (alta vulnerabilidade) |
| 1 | 119 | +0,59 |
| 2 | 115 | -0,85 (melhor gestão) |
| 3 | 2 | -2,48 (outliers megacidades) |

**Top 10 Estados por Vulnerabilidade (maior índice médio):**
1. Acre (3,25)
2. Santa Catarina (3,10)
3. Rondônia (2,97)
4. Paraná (2,25)
5. Espírito Santo (1,90)
6. Roraima (1,62)
7. Minas Gerais (1,59)
8. São Paulo (1,46)
9. Mato Grosso (1,26)
10. Rio Grande do Sul (1,15)

---

## Parte 4: Entregáveis da Tese

### Saídas Quantitativas (CSV/JSON)

| Arquivo | Descrição | Uso |
|---------|-----------|-----|
| `docs/thesis_presentation_assets/correlation_by_cluster.csv` | Pearson r por cluster/var IDH | Tabelas Capítulo 3.5 |
| `docs/thesis_presentation_assets/cluster_summary.csv` | Estatísticas de cluster | Tabela Capítulo 3.4 |
| `docs/thesis_presentation_assets/representative_sample_cities.csv` | 34 cidades amostra (EN) | Apêndice / Apresentação |
| `docs/thesis_presentation_assets/amostra_representativa_cidades.csv` | 34 cidades amostra (pt-BR) | Apêndice / Apresentação |
| `docs/thesis_presentation_assets/representative_sample.json` | Amostra em formato JSON | Dashboard |

### Saídas Geoespaciais (GeoJSON)

| Arquivo | Tamanho | Descrição |
|---------|---------|-----------|
| `docs/thesis_presentation_assets/qgis/brazil_municipalities_vulnerability_index.geojson` | 3,6MB | **Mapa municipal principal (5.570 cidades)** - vermelho=alta vuln, azul=boa gestão |
| `docs/thesis_presentation_assets/qgis/brazil_states_final_findings.geojson` | 39MB | Achados estaduais |
| `docs/thesis_presentation_assets/qgis/BR_Municipios_2022.zip` | 195MB | Limites municipais oficiais IBGE |
| `docs/thesis_presentation_assets/qgis/BR_UF_2022.zip` | 14MB | Limites estaduais oficiais IBGE |

### Saídas Visuais (da execução dos notebooks)

Todos os gráficos/figuras são gerados quando notebooks são executados com acesso aos dados:
- Histogramas de distribuição
- Heatmaps de correlação
- Scatter plots PCA (2D e 3D)
- Visualizações de clusters K-means
- Box plots por cluster
- Análise de silhueta
- Gráficos do método do cotovelo
- Mapas coropléticos brasileiros
- Gráficos radar para perfis de cluster

---

## Parte 5: Recomendações para Dashboard

### Integração Power BI / Tableau / QGIS

**Fontes de Dados Principais:**
- `correlation_by_cluster.csv` → Tabela de correlação
- `cluster_summary.csv` → Perfis de cluster
- `representative_sample_cities.csv` → Exploração da amostra
- `brazil_municipalities_vulnerability_index.geojson` → Camada de mapa

**Layout Sugerido do Dashboard:**

```
┌────────────────────────────────────────────────────────────────┐
│  DASHBOARD: Compliance Público em Municípios Brasileiros       │
├────────────────────────────────────────────────────────────────┤
│  [MAPA]                         [Cards KPI]                    │
│  Vulnerabilidade Municipal BR    - Total de municípios: 5.570  │
│  (Gradiente Vermelho/Azul)       - Com sanções: 469            │
│                                  - Total sanções: 27.749       │
│                                                                │
├────────────────────────────────────────────────────────────────┤
│  [Perfil dos Clusters]           [Top 10 Estados Vulneráveis]  │
│  - Cluster 0: 233 cidades        - Gráfico de barras           │
│  - Cluster 1: 119 cidades                                      │
│  - Cluster 2: 115 cidades                                      │
│  - Cluster 3: 2 cidades (outliers)                             │
├────────────────────────────────────────────────────────────────┤
│  [Tabela de Amostra Representativa]                            │
│  - 17 melhor gestão + 17 mais vulneráveis                      │
│  - Filtrável por cluster/estado                                │
└────────────────────────────────────────────────────────────────┘
```

### Tutorial Mapa QGIS

1. Abrir o QGIS
2. Camada → Adicionar Camada → Adicionar Camada Vetorial
3. Selecionar `docs/thesis_presentation_assets/qgis/brazil_municipalities_vulnerability_index.geojson`
4. Clique direito na camada → Propriedades → Simbologia
5. Alterar para **Graduado**
6. Coluna: `vulnerability_index`
7. Rampa de cores: **RdYlBu** invertida (vermelho = alto, azul = baixo)
8. Classificação: Quantil, 5 classes
9. Aplicar

---

## Parte 6: Estrutura do Projeto para Escrita da Tese

```
public-compliance-data-analysis/
├── config/                        # Contratos de dados (metadados)
├── data/                          # Cache Gold local (2MB)
│   ├── silver/                    # Tabelas normalizadas (1,7MB)
│   └── gold/                      # Prontas para análise (2,0MB)
├── docs/                          # Documentação + entregáveis
│   ├── 01_BRONZE_LAYER.md         # Doc camada Bronze (EN + pt-BR)
│   ├── 02_SILVER_LAYER.md         # Doc camada Silver (EN + pt-BR)
│   ├── 03_GOLD_LAYER.md           # Doc camada Gold (EN + pt-BR)
│   ├── thesis_conclusion.md       # Resumo dos achados (EN + pt-BR)
│   ├── THESIS_INPUTS.md           # ESTE ARQUIVO (EN + pt-BR)
│   └── thesis_presentation_assets/
│       ├── qgis/                  # Arquivos de mapa
│       └── *.csv / *.json         # Dados para dashboard
├── infra/                         # IaC Terraform
├── notebooks/                     # Bilíngue (6 notebooks × 2)
├── scripts/                       # Scripts ETL + análise
├── src/                           # Código-fonte Python
└── tests/                         # Suite de testes (125 testes)
```

---

## Parte 7: Reprodutibilidade

**Para reproduzir todos os resultados do zero:**

```bash
# 1. Clonar e configurar ambiente
git clone <repo>
cd public-compliance-data-analysis
python3 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 2. Configurar perfil AWS
aws configure --profile mba-thesis
# Setar credenciais para acesso leitura s3://enok-mba-thesis-datalake

# 3. Baixar dados Gold localmente
jupyter nbconvert --to notebook --execute notebooks/06_complete_thesis_pipeline.ipynb

# 4. Rodar análise
python3 scripts/run_corruption_hdi_analysis.py

# 5. Gerar mapa QGIS
python3 scripts/generate_map_geojson.py

# 6. Rodar todos os testes
python3 -m pytest tests/ -v

# 7. Executar notebooks (requer jupyter)
jupyter notebook notebooks/
```

**Regenerar pipeline inteiro desde Bronze (requer credenciais de API):**

```bash
# 1. Configurar chaves de API em .env
cp .env.example .env
# Editar .env com TRANSPARENCY_API_KEY

# 2. Rodar ingestão Bronze
./scripts/01_bronze_ingestion.sh

# 3. Rodar transformação Silver
./scripts/02_silver_transformation.sh

# 4. Rodar transformação Gold
./scripts/03_gold_transformation.sh
```

---

## Parte 8: Status de Validação

| Verificação | Status | Quantidade |
|-------------|--------|------------|
| Scan de segurança (sem secrets) | PASS | 0 falhas |
| Suíte pytest | PASS | 137 aprovados, 3 skipados |
| Notebooks bilíngues | PASS | 6 pares sincronizados |
| Dados Silver locais | PASS | 9 tabelas |
| Dados Gold locais | PASS | 6 tabelas |
| GeoJSON gerado | PASS | 5.570 features |
| Exportações CSV | PASS | 5 arquivos |

---

## Citação

Para referências da tese, citar este toolkit como:

> DE JESUS, E. A. *Compliance Público Baseado em Dados: Correlação entre Repasses Federais e Indicadores Socioeconômicos Municipais*. Trabalho de Conclusão de Curso (MBA em Data Science & Analytics) — USP/ESALQ, Piracicaba, 2026.

---

**Arquivo atualizado:** 19/04/2026

**Contato:** Ver README.md do repositório
