# ETAPA 3: CAMADA GOLD - Analytics e Agregações

**Última Atualização:** 2026-04-08 (UTC-03:00)  
**Projeto:** Public Compliance Data Analysis (TCC MBA)  
**Entrypoint Principal:** `scripts/03_gold_transformation.sh`

---

## Visão Geral

A camada Gold transforma tabelas Silver em datasets prontos para análise estatística, machine learning e clustering.

Saídas Gold atuais:
1. `gold/agg_municipality_socioeconomic`
2. `gold/agg_state_summary`
3. `gold/agg_sanctions_summary`
4. `gold/analysis_compliance`
5. `gold/analysis_compliance_municipality`
6. `gold/consolidated_clustering`

Todas as saídas incluem:
- `data.parquet`
- `data.json`
- `_metadata.json`

---

## Tabelas de Origem (Silver)

- `silver/dim_municipalities/data.parquet`
- `silver/fact_population/data.parquet`
- `silver/fact_sanitation/data.parquet`
- `silver/fact_literacy/data.parquet`
- `silver/fact_income/data.parquet`
- `silver/fact_federal_transfers/data.parquet`
- `silver/fact_sanctions/data.parquet`

---

## Datasets Gold

### 1) `agg_municipality_socioeconomic`
Painel socioeconômico municipal 2010->2022 com métricas de variação.

Campos principais:
- chaves geográficas (`municipality_code`, `state_code`, `region_code`)
- níveis populacionais e variação `%`
- níveis de alfabetização e variação em `p.p.`
- níveis de renda nominal + real e variação
- níveis de domicílios e variação `%`

### 2) `agg_state_summary`
Agregação estadual de municípios com densidade de sanções.

Campos principais:
- chaves estadual/regional
- contagem de municípios
- resumos de população e renda
- totais de sanções por tipo de entidade (`PF`, `PJ`)
- `sanctions_per_100k`

### 3) `agg_sanctions_summary`
Agregação de sanções por registro (`CEIS`, `CNEP`, `CEPIM`).

Campos principais:
- total de sanções
- divisão `PF`/`PJ`
- `pj_ratio_pct`
- intervalo temporal (`earliest_sanction`, `latest_sanction`)
- total de órgãos sancionadores únicos

### 4) `analysis_compliance`
Dataset estadual para modelagem de regressão/correlação.

Campos principais:
- controles socioeconômicos (`population`, `avg_literacy_rate`, `avg_income`)
- targets de sanções (`n_sanctions`, quebra por registro, `sanctions_per_100k`)
- features de engenharia (`log_population`, `log_income`, dummies regionais)

### 5) `analysis_compliance_municipality`
Dataset municipal para modelagem alinhado ao `analysis_compliance` estadual.

Campos principais:
- chaves de município/estado/região
- controles socioeconômicos (`population_2022`, `literacy_rate_2022`, `avg_income_2022`, `avg_income_real_2022_2022_brl`)
- targets de sanções (`n_sanctions`, quebra por registro, `sanctions_per_100k`)
- controles de transferências (`total_transfers`, `n_transfer_records`, `avg_transfer_per_capita`)
- features de engenharia (`log_population`, `log_income`, `log_total_transfers`, `sanctions_per_million_brl_transfers`)

### 6) `consolidated_clustering`
Dataset municipal para análise não supervisionada.

Regras de construção:
- uma linha por município
- linhas com ausência de features de clustering são removidas
- campos normalizados por z-score (`*_norm`)
- features log para métricas com assimetria:
  - `log_population_2022`
  - `log_avg_income_2022`
  - `log_households_2022`

---

## Implementação

Código principal:
- `src/processing/gold_transformer.py`

Métodos principais:
- `_transform_municipality_socioeconomic()`
- `_transform_state_summary()`
- `_transform_sanctions_summary()`
- `_transform_analysis_compliance()`
- `_transform_analysis_compliance_municipality()`
- `_transform_clustering_dataset()`

Os contratos de schema são validados contra entradas Gold em `config/silver_schemas.json`.

---

## Execução

### Execução completa da Gold

```bash
./scripts/03_gold_transformation.sh
```

### Execução direta do módulo

```bash
python -m src.processing.gold_transformer
```

### Pré-requisito

```bash
./scripts/02_silver_transformation.sh
```

---

## Cache Inteligente

A Gold reutiliza a mesma estratégia de skip por metadata:
- rastreamento de digest em `_metadata.json`
- comparação de fontes via SHA-256 com fallback para ETag
- reprocessamento automático apenas quando fontes Silver mudam

---

## Qualidade de Dados e Tratamento de Bordas

- validação de schema e enforcement de tipos em cada saída
- métricas de variação com proteção contra divisão por zero
- tabelas de origem ausentes tratadas com warnings
- saída de clustering registra contagem de municípios mantidos vs removidos

---

## Restrições Conhecidas

- cobertura geográfica de sanções ainda é incompleta na fonte
- features de desfecho censitário continuam restritas à comparação 2010 e 2022
- cobertura municipal de sanções ainda depende da presença de `municipality_code` na fonte

---

## Testes

```bash
pytest tests/processing/test_gold_transformer.py -v
pytest tests/processing/ -v
```

Total atual do repositório: `127` testes coletados (`pytest --collect-only -q tests`, 2026-04-08).

---

## Links Downstream

- Guia de notebooks: [../notebooks/README.pt-BR.md](../notebooks/README.pt-BR.md)
- Guia de traduções: [TRANSLATIONS.pt-BR.md](TRANSLATIONS.pt-BR.md)
