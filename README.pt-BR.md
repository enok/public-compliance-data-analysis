## Visão Geral do Projeto

Este repositório contém a implementação da tese de MBA em Data Science & Analytics (USP/Esalq), com foco em análise de compliance público e eficiência do gasto.

O projeto integra:
- Indicadores censitários do IBGE (2010 e 2022)
- Transferências federais do Portal da Transparência (mensal, 01/2010 a 12/2022)
- Registros de sanções da CGU (CEIS, CNEP, CEPIM)
- Série IPCA do Banco Central para ajuste de renda real em BRL de 2022

Versão em inglês: [README.md](README.md)

## Status Atual

- Ingestão Bronze I/II/III: completa e operacional (`scripts/01_bronze_ingestion.sh`)
  - Bronze I: datasets censitários do IBGE
  - Bronze II: transferências federais + sanções do Portal da Transparência
  - Bronze III: inflação IPCA mensal do BCB
- Transformação Silver: completa e operacional (`scripts/02_silver_transformation.sh`)
- Transformação Gold: completa e operacional (`scripts/03_gold_transformation.sh`)
- Notebooks de análise (EN + pt-BR): disponíveis
- Testes automatizados: `130` testes coletados (`pytest --collect-only -q tests`, 2026-04-09)

## Enquadramento da Pesquisa

Objetivo: identificar anomalias socioeconômicas e de compliance associadas a transferências públicas.

Desenho temporal:

```text
Censo 2010 (baseline) -> Transferências federais 2010-2022 (janela de tratamento) -> Censo 2022 (resultado)
```

## Início Rápido

```bash
git clone <repository-url>
cd public-compliance-data-analysis

python -m venv .venv
source .venv/bin/activate            # Linux/Mac
# .\\.venv\\Scripts\\Activate.ps1      # Windows PowerShell

pip install -r requirements.txt
cp .env.example .env
```

Defina variáveis obrigatórias (ou use defaults de `config/runtime_config.json`):

```bash
AWS_PROFILE=mba-thesis
S3_BUCKET_NAME=enok-mba-thesis-datalake
TRANSPARENCY_API_KEY=<sua-chave-api>
```

## Execução do Pipeline

### Scripts por etapa (fonte de verdade)

```bash
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

### Opções úteis da Bronze

```bash
./scripts/01_bronze_ingestion.sh --only-ibge
./scripts/01_bronze_ingestion.sh --only-inflation
./scripts/01_bronze_ingestion.sh --only-transparency
```

Rechecagem direcionada da Transparência:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2022 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

## Estrutura do Repositório

```text
config/      Metadados de ingestão, schemas e contratos de runtime
src/         Código reutilizável de ingestão, processamento, análise e config
scripts/     Entrypoints principais de orquestração (Bronze/Silver/Gold)
notebooks/   Notebooks e guias bilíngues
tests/       Suítes pytest de ingestão, processamento e análise
docs/        Documentação por camada, relatórios e guia de tradução
infra/       Terraform da fundação do data lake em S3
```

## Arquitetura de Dados

### Bronze (bruta)
- `bronze/ibge/*`
- `bronze/economic/ipca_monthly.json`
- `bronze/transparency/federal_transfers_YYYY_MM.json`
- `bronze/transparency/{ceis,cnep,cepim}_compliance.json`
- checkpoints em `bronze/transparency/.metadata/`

### Silver (normalizada)
- `dim_municipalities`
- `dim_municipality_lookup`
- `fact_population`
- `fact_sanitation`
- `fact_literacy`
- `fact_income`
- `dim_inflation_index`
- `fact_federal_transfers`
- `fact_sanctions`

### Gold (pronta para análise)
- `agg_municipality_socioeconomic`
- `agg_state_summary`
- `agg_sanctions_summary`
- `analysis_compliance`
- `analysis_compliance_municipality`
- `consolidated_clustering`

## Mapa de Documentação

- Bronze: [docs/01_BRONZE_LAYER.md](docs/01_BRONZE_LAYER.md) | [pt-BR](docs/01_BRONZE_LAYER.pt-BR.md)
- Silver: [docs/02_SILVER_LAYER.md](docs/02_SILVER_LAYER.md) | [pt-BR](docs/02_SILVER_LAYER.pt-BR.md)
- Gold: [docs/03_GOLD_LAYER.md](docs/03_GOLD_LAYER.md) | [pt-BR](docs/03_GOLD_LAYER.pt-BR.md)
- Workflow de análise municipal completa: [docs/city_full_analysis_workflow.md](docs/city_full_analysis_workflow.md) | [pt-BR](docs/city_full_analysis_workflow.pt-BR.md)
- Adendo municipal da tese: [docs/city_thesis_conclusion_addendum.md](docs/city_thesis_conclusion_addendum.md) | [pt-BR](docs/city_thesis_conclusion_addendum.pt-BR.md)
- Ativos de apresentação da tese (QGIS + Power BI): [docs/thesis_presentation_assets.md](docs/thesis_presentation_assets.md) | [pt-BR](docs/thesis_presentation_assets.pt-BR.md)
- Notebooks: [notebooks/README.md](notebooks/README.md) | [pt-BR](notebooks/README.pt-BR.md)
- Testes: [tests/README.md](tests/README.md) | [pt-BR](tests/README.pt-BR.md)
- Traduções: [docs/TRANSLATIONS.md](docs/TRANSLATIONS.md) | [pt-BR](docs/TRANSLATIONS.pt-BR.md)

## Testes

```bash
pytest tests/ -v
```

Suítes específicas:

```bash
pytest tests/ingestion/ -v
pytest tests/processing/ -v
pytest tests/analysis/ -v
```

## Segurança

Execute a verificação obrigatória antes de encerrar alterações:

```bash
./scripts/security_check_this_repo.sh
```

## Referências

- Bibliografia completa (EN): [BIBLIOGRAPHY.md](BIBLIOGRAPHY.md)
- Bibliografia completa (pt-BR): [BIBLIOGRAPHY.pt-BR.md](BIBLIOGRAPHY.pt-BR.md)
