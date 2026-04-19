# Ativos de Apresentacao da Tese (QGIS + Power BI)

Este fluxo gera dois pacotes prontos para apresentacao com base nos datasets Gold atuais e na extensao municipal da tese:

- pacote QGIS para narrativa geoespacial do Brasil (estados + contexto de clusters municipais)
- pacote Power BI para narrativa em dashboard (tabelas, relacionamentos e medidas DAX iniciais)

## Comando

```bash
python scripts/build_thesis_presentation_assets.py \
  --aws-profile '' \
  --output-dir docs/thesis_presentation_assets \
  --min-k 2 \
  --max-k 10 \
  --peer-top-n 3 \
  --download-municipality-boundaries
```

Observacoes:

- `--download-municipality-boundaries` e opcional e baixa um ZIP maior oficial do IBGE (`BR_Municipios_2022.zip`).
- O limite de estados (`BR_UF_2022.zip`) sempre e baixado e convertido para GeoJSON enriquecido com metricas da tese.

## Saida do Pacote QGIS

Gerado em `docs/thesis_presentation_assets/qgis/`:

- `brazil_states_final_findings.geojson`
- `state_findings_table.csv`
- `city_findings_with_cluster.csv`
- `city_cluster_k_diagnostics.csv`
- `city_cluster_outcome_summary.csv`
- `city_same_cluster_peers.csv`
- `BR_UF_2022.zip` (fonte IBGE)
- `BR_Municipios_2022.zip` (fonte IBGE opcional)
- `README_qgis_map.md` (instrucoes de mapa)

Mapas recomendados para a defesa:

1. Coropletico estadual por `sanctions_per_100k_state`.
2. Mapa categorial estadual por `dominant_cluster`.
3. Mapa municipal com join por `cluster` ou `sanctions_per_100k` (opcional, com ZIP municipal carregado).

## Saida do Pacote Power BI

Gerado em `docs/thesis_presentation_assets/powerbi/`:

- `tables/dim_region.csv`
- `tables/dim_state.csv`
- `tables/dim_cluster.csv`
- `tables/fact_state_findings.csv`
- `tables/fact_city_findings.csv`
- `tables/fact_cluster_state_mix.csv`
- `tables/fact_city_peer_benchmark.csv`
- `tables/fact_model_benchmark.csv`
- `powerbi_measures.dax`
- `powerbi_relationships.md`
- `powerbi_dashboard_storyboard.md`
- `thesis_theme.json`

### Paginas sugeridas do Dashboard

1. Resumo executivo (cards + mapa + ranking)
2. Achados por estado (matriz + mix de cluster por estado)
3. Achados por municipio (dispersao e distribuicoes por cluster)
4. Contraste de clusters (sancoes/transferencias)
5. Drillthrough de benchmark (pares mais proximos no mesmo cluster)

## Fontes de Dados Utilizadas

- `gold/analysis_compliance`
- `gold/analysis_compliance_municipality`
- `gold/consolidated_clustering`
- limites oficiais do IBGE (municipio 2022, Brasil/BR)

## Caveat para Narrativa da Defesa

Esses visuais sustentam analise associativa. Mantenha o enquadramento da tese explicito:

- indicadores de sancoes nao sao acusacoes diretas
- relacoes observadas nao implicam causalidade
- heterogeneidade municipal e vies de deteccao/reporte continuam como limitacoes centrais
