# Bronze Expansion Plan

## Objective

Close the current Bronze backlog first, then add one new Bronze source at a time only when it materially improves the MBA thesis argument about municipal public spending efficiency, compliance risk, and outcome heterogeneity.

This plan is intentionally conservative:

- finish the current `federal_transfers` backfill and confirm Silver/Gold outputs
- add the highest-value municipal control source next
- defer lower-signal or higher-friction sources until the thesis narrative proves they are needed

## Current Recommendation

### Priority 0: Close the existing Bronze scope

Before adding any new source:

1. Let the current Transparency backfill finish for `2010-01` through `2022-12`
2. Recheck the suspicious early `no_data` checkpoints with:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2013 \
AWS_SDK_LOAD_CONFIG=1 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

3. Run:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

4. Confirm the following real outputs exist:
- `silver/dim_inflation_index`
- `silver/fact_income.avg_income_real_2022_brl`
- Gold outputs with `income_change_real_pct`
- monthly `federal_transfers_YYYY_MM.json` coverage for the full intercensal window

Only after that should Bronze scope expand.

## Source Prioritization

### 1. FINBRA / Siconfi

Status: `Recommended next source`

Why this is first:

- gives fiscal capacity and budget structure controls at municipal level
- improves the thesis identification strategy more directly than most sectoral datasets
- helps separate "high transfers because of fiscal weakness" from "high transfers plus anomalous outcomes"
- supports features such as own-revenue share, transfer dependency, personnel spending intensity, investment mix, and budget balance proxies

Official sources:

- Tesouro Transparente Siconfi API: https://www.tesourotransparente.gov.br/consultas/consultas-siconfi/siconfi-api-de-dados-abertos
- Siconfi API docs landing page referenced by Tesouro: http://apidatalake.tesouro.gov.br/docs/siconfi/
- Tabela dos Entes da Federação: https://www.tesourotransparente.gov.br/ckan/dataset/api-tabela-entes

Expected thesis value:

- strongest candidate for municipal fiscal controls
- improves causal and descriptive interpretation
- likely the best next Bronze addition if only one more source is added

Suggested Bronze datasets:

- `siconfi_entes`
- `siconfi_municipal_revenue`
- `siconfi_municipal_expense`
- `siconfi_municipal_fiscal_balance`

Suggested Bronze paths:

- `bronze/fiscal/siconfi_entes.json`
- `bronze/fiscal/siconfi_municipal_revenue_YYYY.json`
- `bronze/fiscal/siconfi_municipal_expense_YYYY.json`
- `bronze/fiscal/siconfi_municipal_fiscal_balance_YYYY.json`

Silver outcome:

- one normalized municipal fiscal fact table by `municipality_code` and `year`
- one optional entity dimension for municipality and state codes

### 2. CNES

Status: `Second-best option`

Why it helps:

- adds municipal health-capacity controls
- can explain differences in execution and local absorptive capacity
- useful if the thesis narrative links transfers to local state capability

Official source:

- Portal de Dados Abertos do SUS CNES dataset: https://dadosabertos.saude.gov.br/dataset/cnes-cadastro-nacional-de-estabelecimentos-de-saude

Expected thesis value:

- strong control variable source
- especially useful if the thesis discusses institutional capacity or health system structure

Suggested Bronze datasets:

- `cnes_establishments`
- optionally derived later into counts by municipality and establishment type

Suggested Bronze paths:

- `bronze/health/cnes_establishments_YYYY_MM.parquet` or source-native monthly files if the portal delivers them that way

Silver outcome:

- `dim_health_capacity` by municipality and reference month/year

### 3. IDEB / Inep

Status: `Best outcome variable addition, but lower priority than FINBRA`

Why it helps:

- provides a municipal outcome metric instead of only input-side controls
- useful if the thesis wants to frame efficiency in terms of social performance, not just budget execution

Official sources:

- Inep data portal: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos
- IDEB overview: https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb

Expected thesis value:

- strong if the empirical chapter compares public resource flows with measured educational performance
- weaker if the main dependent variable remains compliance-risk only

Suggested Bronze datasets:

- `ideb_municipal`

Suggested Bronze path:

- `bronze/education/ideb_municipal_YEAR.csv`

Silver outcome:

- `fact_education_outcomes`

### 4. SIOPS

Status: `Useful, but more operationally expensive`

Why it helps:

- measures actual health spending by municipalities
- allows comparison between transfers received and health expenditure executed

Official sources:

- SIOPS portal: https://portalfns.saude.gov.br/siops/
- current downloads area references and structure releases are maintained by FNS

Expected thesis value:

- high analytical value if health spending is central
- higher ingestion and standardization friction than FINBRA

Suggested Bronze datasets:

- `siops_municipal_health_spending`

### 5. SNIS

Status: `Deferred unless the thesis needs service-quality evolution beyond census years`

Why it helps:

- gives intercensal service indicators for sanitation and utilities
- useful only if the thesis needs annual service delivery proxies between 2010 and 2022

Official source:

- SNIS portal: https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/snis

## Recommended Implementation Order

### Phase 1

Implement `FINBRA / Siconfi` first.

Acceptance criteria:

- municipal fiscal raw data lands in Bronze with documented provenance
- fiscal metrics can be keyed to `municipality_code` and `year`
- Silver produces one canonical fiscal table
- tests validate schema, naming, and key coverage

### Phase 2

Implement either `CNES` or `IDEB`, not both immediately.

Choose `CNES` if:

- the thesis needs state-capacity controls
- the analysis chapter remains focused on absorptive capacity, governance, and execution context

Choose `IDEB` if:

- the thesis needs an interpretable public-service outcome
- the analysis chapter compares spending and measured educational performance

## Repo Changes To Make Next

When Phase 1 starts, the expected repo touchpoints are:

- new config file: `config/fiscal_metadata.json`
- new Bronze ingestor: `src/ingestion/fiscal_client.py`
- runner update: `scripts/01_bronze_ingestion.sh`
- Silver normalization: `src/processing/fiscal_transformer.py` or a targeted extension of the current processing modules
- schema contract update: `config/silver_schemas.json`
- tests:
  - `tests/ingestion/test_fiscal_client.py`
  - `tests/ingestion/test_fiscal_config_validation.py`
  - `tests/processing/test_fiscal_transformer.py`
- docs:
  - `docs/01_BRONZE_LAYER.md`
  - `docs/01_BRONZE_LAYER.pt-BR.md`
  - `docs/02_SILVER_LAYER.md`
  - `docs/02_SILVER_LAYER.pt-BR.md`
  - coverage reports if the source is fully operationalized

## Minimal Feature Set For FINBRA / Siconfi

To avoid scope creep, Phase 1 should target only the variables that improve the thesis most:

- total current revenue
- own-source revenue
- intergovernmental transfers received
- total expenditure
- personnel expenditure
- investment expenditure

Everything else should be explicitly deferred.

## Decision Rule

If only one more Bronze source can be added before thesis writing intensifies, add `FINBRA / Siconfi`.

If a second source still fits the schedule:

- add `CNES` for capacity controls
- or add `IDEB` for outcome measurement

Do not add all candidates at once.

