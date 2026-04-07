# ETAPA 1: CAMADA BRONZE - Ingestão de Dados

**Última Atualização:** 2026-04-07 (UTC-03:00)  
**Projeto:** Public Compliance Data Analysis (TCC MBA)  
**Entrypoint Principal:** `scripts/01_bronze_ingestion.sh`

---

## Visão Geral

A camada Bronze ingere dados públicos brutos para o S3 com auditabilidade de origem e comportamento incremental com checkpoints.

Escopo atual da Bronze:
- Bronze I: datasets censitários do IBGE (2010 e 2022)
- Bronze II: transferências federais mensais do Portal da Transparência (`01/2010` a `12/2022`) + sanções (CEIS, CNEP, CEPIM)
- Bronze III: série mensal do IPCA do Banco Central (`SGS 433`)

---

## Fontes de Dados

### Bronze I - Censo IBGE

Implementação:
- `src/ingestion/ibge_client.py`
- `config/ibge_metadata.json`

Datasets:
1. `pop_2010`
2. `pop_2022`
3. `sanitation_2010`
4. `sanitation_2022`
5. `literacy_2010`
6. `literacy_2022`
7. `income_2010`
8. `income_2022`

Destaques de comportamento:
- fast-skip no S3 quando o arquivo já existe (`IBGE_FAST_SKIP_IF_EXISTS=1` por padrão)
- metadata SHA-256 no objeto (`content-sha256`) para comparação de integridade
- log de fontes em `docs/data_sources.log`

### Bronze II - Portal da Transparência

Implementação:
- `src/ingestion/transparency_client.py`
- `config/transparency_metadata.json`

Datasets:
- `federal_transfers` (autoexpansão de uma única entrada para arquivos mensais)
- `ceis_sanctions`
- `cnep_sanctions`
- `cepim_sanctions`

Cobertura alvo:
- arquivos mensais de transferências federais de `federal_transfers_2010_01.json` até `federal_transfers_2022_12.json`
- total de 156 meses

Destaques de comportamento:
- checkpoints de paginação com retomada
- estado `no_data` para evitar chamadas repetidas sem dados
- fetch incremental quando surgem novas páginas
- sobrescrita opcional de intervalo:
  - `TRANSPARENCY_START_MA=MM/YYYY`
  - `TRANSPARENCY_END_MA=MM/YYYY`
  - `TRANSPARENCY_FORCE_REFRESH=1` para revalidar meses checkpointados

### Bronze III - Inflação (BCB IPCA)

Implementação:
- `src/ingestion/ibge_client.py`
- `config/ibge_metadata.json` (`ipca_monthly`)

Dataset:
- `ipca_monthly` da série `SGS 433` do Banco Central

Modo de execução:
- incluído na execução completa da Bronze
- pode rodar isolado com `./scripts/01_bronze_ingestion.sh --only-inflation`

---

## Estrutura no S3

```text
s3://enok-mba-thesis-datalake/bronze/
├── ibge/
│   ├── census_2010_pop.json
│   ├── census_2022_pop.json
│   ├── census_2010_sanitation.json
│   ├── census_2022_sanitation.json
│   ├── census_2010_literacy.json
│   ├── census_2022_literacy.json
│   ├── census_2010_income.json
│   └── census_2022_income.json
├── economic/
│   └── ipca_monthly.json
└── transparency/
    ├── federal_transfers_YYYY_MM.json
    ├── ceis_compliance.json
    ├── cnep_compliance.json
    ├── cepim_compliance.json
    └── .metadata/
        └── *.meta.json
```

---

## Execução

### Execução completa da Bronze

```bash
./scripts/01_bronze_ingestion.sh
```

### Execução seletiva

```bash
./scripts/01_bronze_ingestion.sh --only-ibge
./scripts/01_bronze_ingestion.sh --only-inflation
./scripts/01_bronze_ingestion.sh --only-transparency
```

### Flags de skip

```bash
./scripts/01_bronze_ingestion.sh --skip-ibge --skip-inflation
```

### Configuração obrigatória em runtime

- credenciais/perfil AWS com acesso ao S3
- `S3_BUCKET_NAME` (default via runtime config / `enok-mba-thesis-datalake`)
- `TRANSPARENCY_API_KEY` para ingestão de Transparência

---

## Integridade, Idempotência e Auditoria

- reexecução idempotente: objetos sem alteração são ignorados
- objetos Bronze com metadata SHA-256 para comparação determinística
- ingestão da Transparência grava checkpoints (`last_page`, `status`, contadores)
- logs de fonte e status em `docs/data_sources.log`

---

## Solução de Problemas

### `TRANSPARENCY_API_KEY` ausente
Defina no `.env` ou no ambiente antes da ingestão da Transparência.

### Erros 401/403
Verifique validade da chave e uso do header `chave-api-dados`.

### Cobertura vazia/parcial de Transparência
Faça rechecagem direcionada com refresh forçado:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2022 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

### Falhas de acesso ao S3
Valide bucket, permissões IAM e perfil AWS ativo.

---

## Validação

```bash
pytest tests/ingestion/ -v
pytest tests/processing/test_silver_config_validation.py -v
```

---

## Links Downstream

- Camada Silver: [02_SILVER_LAYER.pt-BR.md](02_SILVER_LAYER.pt-BR.md)
- Camada Gold: [03_GOLD_LAYER.pt-BR.md](03_GOLD_LAYER.pt-BR.md)
- Guia de notebooks: [../notebooks/README.pt-BR.md](../notebooks/README.pt-BR.md)
