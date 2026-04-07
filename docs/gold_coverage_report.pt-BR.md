# Relatório de Cobertura da Camada Gold

**Gerado:** 2026-02-19 (UTC-03:00)  
**Projeto:** Public Compliance Data Analysis (TCC MBA)  
**Bucket:** `enok-mba-thesis-datalake`

---

## Resumo Executivo

✅ **Todas as 5 transformações da camada Gold foram concluídas com sucesso**

| Dataset | Status | Registros | Descrição |
|---------|--------|----------|----------|
| agg_municipality_socioeconomic | ✅ SUCCESS | 5.570 | Métricas socioeconômicas por município com indicadores de variação |
| agg_state_summary | ✅ SUCCESS | 27 | Agregações por estado com sanções per capita |
| agg_sanctions_summary | ✅ SUCCESS | 3 | Agregações de sanções por registro |
| analysis_compliance | ✅ SUCCESS | 27 | Dataset pronto para regressão/correlação |
| consolidated_clustering | ✅ SUCCESS | 5.565 | Dataset consolidado por município para clustering (limpo + normalizado) |

**Total de registros:** 11.192  
**Taxa de sucesso:** 100%

---

## Resumo do Log de Execução

### Última execução: 2026-02-19 18:51

Todas as transformações concluídas com sucesso.

1. **gold_municipality_socioeconomic** (01:27:01)
   - Entrada: 5.570 municípios
   - Saída: 5.570 registros
   - Fontes: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanitation
   - **Cobertura alfabetização 2010: 99,9%** (5.565/5.570)

2. **gold_state_summary** (01:05:10)
   - Entrada: 5.570 municípios
   - Saída: 27 estados
   - Fontes: dim_municipalities, fact_population, fact_income, fact_sanctions

3. **gold_sanctions_summary** (01:05:13)
   - Entrada: 32 sanções
   - Saída: 3 tipos de registro (CEIS, CNEP, CEPIM)
   - Fonte: fact_sanctions

4. **gold_analysis_compliance** (01:27:14)
   - Entrada: agregada de múltiplas fontes
   - Saída: 27 estados
   - Fontes: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanctions
   - **Todas as colunas 100% preenchidas**

5. **gold_consolidated_clustering** (02:15:43)
   - Entrada: 5.570 municípios
   - Saída: 5.565 registros
   - Removidos: 5 (valores ausentes)
   - Fontes: dim_municipalities, fact_population, fact_literacy, fact_income, fact_sanitation

---

## Detalhes dos Datasets

### 1. agg_municipality_socioeconomic

**Finalidade:** agregação socioeconômica por município com métricas de variação (2010→2022)

**Cobertura:**
- ✅ Todos os 5.570 municípios
- ✅ 27 estados
- ✅ 5 regiões

**Casos de uso:**
- Tendências em nível municipal
- Identificar municípios com maior/menor crescimento
- Base para estudos de correlação de compliance

---

### 2. agg_state_summary

**Finalidade:** resumos por estado para análise regional

**Cobertura:**
- ✅ 27 estados
- ✅ 5 regiões
- ✅ Integração de sanções (quando disponível)

---

### 3. agg_sanctions_summary

**Finalidade:** agregações por tipo de registro (CEIS, CNEP, CEPIM)

---

### 4. analysis_compliance

**Finalidade:** dataset pronto para análise em nível de estado (regressão/correlação)

---

**Fim do relatório**
