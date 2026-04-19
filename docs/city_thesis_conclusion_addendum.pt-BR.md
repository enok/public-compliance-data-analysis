# Adendo da Conclusão da Tese (Análise Supervisionada em Nível Municipal)

**Data da execução:** 8 de abril de 2026  
**Comando:** `python scripts/run_city_full_analysis.py --aws-profile '' --output-dir /tmp/city_full_analysis_run`  
**Dataset:** `gold/analysis_compliance_municipality`

## Resumo das Evidências

- Amostra municipal usada na modelagem: **5.570**
- Associação linear mais forte com `sanctions_per_100k`: **`log_income`** (`r = 0,149`)
- Ajuste OLS (erros robustos HC3): **R² = 0,024**, **R² ajustado = 0,023**
- Melhor modelo de benchmark de ML: **ElasticNet**, **R² de teste = 0,030**

## Contraste de Resultados por Cluster

- Cluster 0: `n=821`, média de sanções/100k `= 5,514`, média de transferências `= 114.117,40`
- Cluster 1: `n=2.158`, média de sanções/100k `= 1,845`, média de transferências `= 2.263,60`
- Cluster 2: `n=2.584`, média de sanções/100k `= 7,583`, média de transferências `= 1.172,09`
- Cluster 3: `n=2`, média de sanções/100k `= 0,367`, média de transferências `= 4.298.406,99`

## Interpretação

A extensão municipal preserva a direção da narrativa central da tese (proxies de renda/capacidade seguem relevantes), porém com baixo poder explicativo no nível municipal. Isso reforça uma leitura cautelosa, centrada em heterogeneidade, cobertura geográfica parcial das sanções e necessidade de preditores mais ricos e desenho temporal.
