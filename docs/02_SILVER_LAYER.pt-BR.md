# ETAPA 2: CAMADA SILVER - Transformação de Dados

**Última Atualização:** 2026-04-07 (UTC-03:00)  
**Projeto:** Public Compliance Data Analysis (TCC MBA)  
**Entrypoint Principal:** `scripts/02_silver_transformation.sh`

---

## Visão Geral

A camada Silver normaliza JSON da Bronze para tabelas analíticas validadas por schema em Parquet/JSON.

Objetivos principais:
- padronizar chaves de município/estado/região
- aplicar contratos de schema em `config/silver_schemas.json`
- produzir fatos e dimensões reutilizáveis para a Gold
- manter idempotência com lógica de skip baseada em metadata

---

## Entradas e Saídas

### Entradas Bronze

- `bronze/ibge/*.json`
- `bronze/economic/ipca_monthly.json`
- `bronze/transparency/federal_transfers_YYYY_MM.json`
- `bronze/transparency/{ceis,cnep,cepim}_compliance.json`

### Saídas Silver

1. `silver/dim_municipalities`
2. `silver/dim_municipality_lookup`
3. `silver/fact_population`
4. `silver/fact_sanitation`
5. `silver/fact_literacy`
6. `silver/fact_income`
7. `silver/dim_inflation_index`
8. `silver/fact_federal_transfers`
9. `silver/fact_sanctions`

Cada caminho de saída armazena:
- `data.parquet`
- `data.json`
- `_metadata.json` (rastreamento de digest de origem)

---

## Implementação

Módulos principais:
- `src/processing/base_transformer.py`
- `src/processing/ibge_transformer.py`
- `src/processing/transparency_transformer.py`

### `IBGETransformer`

Constrói:
- dimensões de município (`dim_municipalities`, `dim_municipality_lookup`)
- fatos censitários (`fact_population`, `fact_sanitation`, `fact_literacy`, `fact_income`)
- dimensão anual de inflação (`dim_inflation_index`) a partir do IPCA mensal

### `TransparencyTransformer`

Constrói:
- `fact_federal_transfers` a partir de arquivos mensais descobertos dinamicamente
- `fact_sanctions` de CEIS/CNEP/CEPIM
- vínculo de município via lookup normalizado quando a fonte traz nome de município + UF
- mascaramento de CPF/CNPJ

---

## Cache Inteligente e Processamento Incremental

A lógica de skip da Silver compara digests atuais com `_metadata.json`.

Estratégia de digest (`BaseTransformer._get_object_digest`):
1. prioriza metadata S3 `content-sha256` (gravada na Bronze)
2. usa fallback para `ETag` quando a metadata customizada não existe

O processamento só é pulado quando:
- a saída existe
- nenhum digest de origem rastreado mudou

Isso vale para Bronze->Silver e para encadeamento Silver->Gold.

---

## Schemas Silver (Atual)

### `dim_municipalities`
- `municipality_code`, `municipality_name`, `state_code`, `state_abbrev`, `state_name`, `region_code`, `region_name`

### `dim_municipality_lookup`
- `municipality_code`, `municipality_name_normalized`, `state_code`, `state_abbrev`

### `fact_population`
- `municipality_code`, `year`, `total_population`

### `fact_sanitation`
- `municipality_code`, `year`, `total_households`

### `fact_literacy`
- `municipality_code`, `year`, `literacy_rate`

### `fact_income`
- `municipality_code`, `year`, `avg_income`
- `annual_ipca_rate_pct`, `ipca_index_avg`, `deflator_to_2022`, `reference_base_year`
- `avg_income_real_2022_brl`

### `dim_inflation_index`
- `year`, `annual_ipca_rate_pct`, `ipca_index_avg`, `ipca_index_dec`
- `deflator_to_2022`, `reference_base_year`, `last_reference_date`

### `fact_federal_transfers`
- `municipality_code` (nullable), `year`, `month`, `transfer_amount`, `transfer_type`, `source_agency`

### `fact_sanctions`
- `sanction_id`, `registry_type`, `sanctioned_entity`, `entity_type`, `cpf_cnpj`
- `sanction_type`, `sanction_start_date`, `sanction_end_date`
- `sanctioning_agency`, `state_code`, `municipality_code`

---

## Execução

### Execução completa da Silver

```bash
./scripts/02_silver_transformation.sh
```

### Execução segmentada

```bash
./scripts/02_silver_transformation.sh --only-ibge
./scripts/02_silver_transformation.sh --only-transparency
```

### Execução direta dos módulos

```bash
python -m src.processing.ibge_transformer
python -m src.processing.transparency_transformer
```

---

## Qualidade de Dados e Privacidade

Controles de validação:
- validação de código de município de 7 dígitos
- enforcement de schema por tabela
- remoção de duplicatas por chave natural
- parsing seguro de números e datas

Controles de privacidade:
- CPF/CNPJ mascarados na saída de sanções
- sem documentos em texto puro nos campos de entidade sancionada

Log operacional:
- auditoria de transformação em `docs/processing.log`

---

## Restrições Conhecidas

- o vínculo municipal em transferências federais não é garantido para todos os registros brutos
- campos de localização em sanções continuam esparsos na fonte
- indicadores censitários de resultado continuam disponíveis apenas para 2010 e 2022

---

## Testes

```bash
pytest tests/processing/ -v
pytest tests/processing/test_transformers.py -v
pytest tests/processing/test_smart_caching.py -v
pytest tests/processing/test_silver_integration.py -v
```

Total atual do repositório: `117` testes coletados (`pytest --collect-only -q tests`, 2026-04-07).

---

## Links Downstream

- Camada Gold: [03_GOLD_LAYER.pt-BR.md](03_GOLD_LAYER.pt-BR.md)
- Guia de notebooks: [../notebooks/README.pt-BR.md](../notebooks/README.pt-BR.md)
