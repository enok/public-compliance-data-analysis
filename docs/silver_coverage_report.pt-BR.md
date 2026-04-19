# Relatório de Auditoria de Qualidade - Camada Silver

**Gerado:** 2026-02-19

---

## Resumo Executivo

- **Total de tabelas:** 7
- **Tabelas existentes:** 7 / 7
- **Cobertura:** 100,0%
- **Total de registros:** 63.018
- **Tamanho total:** 1,24 MB
- **Tabelas com metadata:** 7
- **Problemas encontrados:** 0

---

## Detalhes por Tabela

### dim_municipalities

✅ **Status:** Existe

- **Registros:** 5.570
- **Tamanho:** 0,10 MB
- **Colunas:** 7
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Municípios únicos: 5.570 (sem duplicatas)
- Estados: 27
- Regiões: 5

---

### fact_population

✅ **Status:** Existe

- **Registros:** 11.135
- **Tamanho:** 0,10 MB
- **Colunas:** 3
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Anos: 2010, 2022
- Cobertura de código de município: 100,0%

---

### fact_sanitation

✅ **Status:** Existe

- **Registros:** 11.135
- **Tamanho:** 0,09 MB
- **Colunas:** 3
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Anos: 2010, 2022
- Cobertura de código de município: 100,0%

---

### fact_literacy

✅ **Status:** Existe

- **Registros:** 11.135
- **Tamanho:** 0,07 MB
- **Colunas:** 3
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Anos: 2010, 2022
- Cobertura de código de município: 100,0%

---

### fact_income

✅ **Status:** Existe

- **Registros:** 11.135
- **Tamanho:** 0,11 MB
- **Colunas:** 3
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Anos: 2010, 2022
- Cobertura de código de município: 100,0%

---

### fact_federal_transfers

✅ **Status:** Existe

- **Registros:** 1.881
- **Tamanho:** 0,02 MB
- **Colunas:** 6
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Anos: 2010-2022 (cobertura intercensitária completa na Bronze)
- Cobertura de código de município: ~97%

---

### fact_sanctions

✅ **Status:** Existe

- **Registros:** 32
- **Tamanho:** 0,01 MB
- **Colunas:** 11
- **Metadata:** ✅ Sim

**Checagens de qualidade (resumo):**
- Tipos de registro: CEIS (15), CEPIM (15), CNEP (2)
- Cobertura de município: 40,62% (muitas sanções sem município)

---

## Problemas e Avisos

✅ Nenhum problema encontrado.

---

**Fim do relatório**
