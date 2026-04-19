# Conclusão da Tese — Análise de Dados de Compliance Público

> **Tese de MBA**: Eficiência do Gasto Público, Risco de Compliance e Detecção de Anomalias no Brasil
>
> **Escopo dos dados**: 5.570 municípios, 27 estados, Censo 2010/2022, sanções do Portal da Transparência (CEIS, CNEP, CEPIM), renda ajustada pela inflação (IPCA, base BRL 2022)

## 1. Resumo das Descobertas

Este estudo investigou a relação entre indicadores socioeconômicos e sanções de compliance público em estados e municípios brasileiros. Quatro abordagens analíticas complementares foram aplicadas: análise exploratória de dados, inferência estatística, aprendizado de máquina supervisionado e clustering não supervisionado. Juntas, convergem em um conjunto consistente de achados.

### 1.1 Renda É o Preditor Dominante das Taxas de Sanções

Em todos os métodos, a **renda média** emergiu como o preditor individual mais forte de sanções por 100 mil habitantes:

- Correlação bivariada: r = 0,74 (p < 0,001)
- Regressão OLS: coeficiente log da renda β = 49,75 (p < 0,001), R² = 0,835
- Aprendizado de máquina: ElasticNet e Random Forest classificam features relacionadas à renda como as mais importantes
- Clustering: renda é um diferenciador primário entre os quatro clusters de municípios

Esta descoberta é robusta à especificação do modelo e à escolha do método. Entretanto, a direção da relação é contraintuitiva: **estados com maior renda têm mais sanções per capita**, não menos.

### 1.2 Sanções Refletem Capacidade de Detecção, Não Níveis de Irregularidade

A explicação mais parcimoniosa para a associação renda–sanções é a **capacidade institucional de detecção**. Estados e municípios com maior renda tendem a ter:

- Instituições de auditoria e fiscalização mais fortes
- Mais servidores públicos capacitados em procedimentos de compliance
- Maior digitalização e infraestrutura de reporte
- Maior visibilidade das transferências federais sujeitas a monitoramento

Esta interpretação está alinhada com a literatura de administração pública sobre "viés de reporte" — jurisdições com mais capacidade de detectar e registrar violações produzem mais registros de sanções, independentemente dos níveis reais de irregularidade.

### 1.3 Efeitos Regionais Persistem Após Controle pela Renda

A regressão OLS com dummies regionais revela que **Norte** (β = 20,94; p = 0,003) e **Nordeste** (β = 22,97; p = 0,009) têm taxas de sanções significativamente mais altas do que o esperado dado seus níveis de renda. Isso sugere que fatores de governança, institucionais ou estruturais além do desenvolvimento socioeconômico contribuem para o risco de compliance nessas regiões.

Em contraste, a região **Sul** mostra efeito negativo marginalmente significativo (β = −9,72; p = 0,050), indicando taxas de sanções abaixo do esperado em relação ao seu nível de renda.

### 1.4 O Distrito Federal É um Outlier Estrutural

Com 65,45 sanções por 100 mil habitantes — mais que o dobro do próximo estado (Rondônia com 31,56) — o Distrito Federal é a observação mais influente em todos os modelos. Como sede do governo federal com infraestrutura de fiscalização concentrada, representa um artefato estrutural da concentração institucional, não um padrão de compliance generalizável. A análise da distância de Cook confirma-o como o ponto mais influente na regressão.

### 1.5 Os Municípios Brasileiros Exibem uma Estrutura Socioeconômica Dual

Clustering K-means (K = 4, silhouette = 0,288) sobre 12 features derivadas do censo em 5.565 municípios revela uma clara estrutura dual:

- **Cluster desenvolvido** (Clusters 0, 2, 3 — 61,2%): Maior renda, maior alfabetização, concentrados no Sudeste e Sul
- **Cluster menos desenvolvido** (Cluster 1 — 38,8%): Menor renda, menor alfabetização, concentrados no Nordeste e Norte

O PCA confirma esta estrutura: três componentes principais explicam 84,4% da variância total, com o primeiro componente (39,2%) carregando em desenvolvimento socioeconômico geral.

Esta estrutura dual contextualiza por que os padrões de compliance diferem regionalmente — o gradiente socioeconômico subjacente molda tanto a geração de transferências públicas quanto a capacidade institucional de monitorá-las.

### 1.6 Extensão Supervisionada em Nível Municipal Confirma a Direção, com Menor Poder Explicativo

Em **8 de abril de 2026**, o workflow supervisionado municipal (`scripts/run_city_full_analysis.py`) foi executado sobre `gold/analysis_compliance_municipality`, com **n = 5.570 municípios**:

- correlação linear mais forte com taxa de sanções: `log_income` (**r = 0,149**)
- modelo OLS (erros robustos HC3): **R² = 0,024**, **R² ajustado = 0,023**
- melhor modelo de ML: ElasticNet com **R² de teste = 0,030**

A extensão municipal mantém a mesma interpretação direcional (renda e proxies de capacidade estrutural seguem relevantes), porém com efeitos mais fracos e baixa variância explicada. Isso indica heterogeneidade municipal elevada e ruído de mensuração nos registros de sanções geolocalizados.

### 1.7 Camada de Apresentação Consolida a Evidência Final da Tese

Em **9 de abril de 2026**, o workflow de ativos de apresentação (`scripts/build_thesis_presentation_assets.py`) foi executado para operacionalizar a evidência analítica final para banca e comunicação com stakeholders:

- **Pacote QGIS**: limites estaduais oficiais do IBGE (`BR_UF_2022.zip`) e GeoJSON enriquecido (`brazil_states_final_findings.geojson`) com métricas estaduais de sanções e indicadores de composição de clusters municipais.
- **Pacote Power BI**: tabelas CSV em formato tipo estrela, medidas DAX iniciais, guia de relacionamentos e storyboard alinhado aos achados estaduais/municipais e aos outputs de benchmark por cluster.

Essa camada de apresentação não adiciona novas alegações inferenciais; ela consolida a comunicação reprodutível dos resultados já estabelecidos e melhora a consistência entre evidência técnica e narrativa da tese.

## 2. Limitações

### 2.1 Tamanho da Amostra

A análise em nível estadual opera com apenas **n = 27 observações**, limitando severamente o poder estatístico e o número de preditores que podem ser estimados de forma confiável. Modelos de classificação de aprendizado de máquina tiveram desempenho fraco (melhor F1 = 0,51), e os resultados devem ser tratados como exploratórios neste nível de agregação.

### 2.2 Desenho Transversal

Todas as análises usam um único recorte temporal. **Causalidade não pode ser estabelecida** — a associação renda–sanções pode refletir causalidade reversa (investimento institucional impulsionado por sanções), confundimento (terceiras variáveis impulsionando ambas) ou o mecanismo de capacidade de detecção hipotetizado acima.

### 2.3 Viés de Detecção

Registros de sanções medem **violações detectadas e registradas**, não irregularidades reais. Jurisdições com fiscalização mais fraca podem ter menos sanções registradas, mas igual ou maior volume real de falhas de compliance. Esta limitação fundamental de mensuração aplica-se a todos os achados.

### 2.4 Nível de Agregação

Modelos em nível estadual mascaram variação intra-estadual. O clustering em nível municipal revela heterogeneidade substancial dentro dos estados, e os modelos supervisionados municipais ainda apresentam baixo poder explicativo, sugerindo que conclusões apenas estaduais simplificam o panorama enquanto o nível municipal exige preditores mais ricos.

### 2.5 Deflação da Renda

Colunas de renda ajustada pela inflação usam deflação pelo IPCA para BRL de 2022. Os resultados dependem da série de deflator escolhida. Deflatores alternativos (IGP-M, IPCs regionais) poderiam alterar a magnitude dos coeficientes relacionados à renda.

## 3. Implicações para Políticas Públicas

### 3.1 Investir em Capacidade de Fiscalização, Não em Medidas Punitivas

O achado de que maior renda prediz mais sanções — provavelmente via capacidade de detecção — implica que **fortalecer a infraestrutura de auditoria em regiões de menor renda** melhoraria a visibilidade de compliance sem exigir mudanças punitivas de política.

### 3.2 Focalização Regional

Os efeitos regionais do Norte e Nordeste sugerem que essas regiões podem se beneficiar de programas de compliance direcionados que considerem seus desafios específicos de governança, além do que modelos baseados em renda preveem.

### 3.3 Desenho de Políticas Consciente de Outliers

A posição extrema do Distrito Federal alerta contra o uso de médias nacionais ou rankings para comparar estados. Benchmarks de compliance devem considerar diferenças estruturais na arquitetura de governança.

## 4. Trabalhos Futuros

1. **Análise temporal**: Incorporar dados de sanções de múltiplos anos para testar se as relações renda–sanções são estáveis ao longo do tempo ou impulsionadas por períodos específicos de política.
2. **Regressão em nível municipal (baseline já implementada)**: Usar `gold/analysis_compliance_municipality` junto com `scripts/run_city_full_analysis.py` para executar correlação, OLS, benchmark de ML e geração de adendo de conclusão; estender esse baseline com métodos de econometria espacial.
3. **Econometria espacial**: Aplicar modelos de lag espacial e erro espacial para testar se sanções em municípios vizinhos são correlacionadas (autocorrelação espacial).
4. **Clustering alternativo**: Testar métodos baseados em densidade (DBSCAN, HDBSCAN) que não assumem clusters esféricos e podem capturar melhor as distribuições assimétricas observadas nos dados.
5. **Análise textual e jurídica**: Incorporar descrições de sanções e categorias legais para distinguir entre tipos de falhas de compliance (fiscal, contratual, administrativa).
6. **Identificação causal**: Desenhar abordagens de diferenças-em-diferenças ou variáveis instrumentais para avançar além de evidências associativas em direção a alegações causais sobre capacidade institucional.
7. **Disseminação interativa da evidência**: Evoluir os ativos atuais de QGIS e Power BI para um dashboard companheiro versionado e atualizado periodicamente, ligado diretamente aos outputs do pipeline medallion.

---

*Base de evidências: 4 notebooks executados (NB01–NB04), execução do workflow municipal via `scripts/run_city_full_analysis.py` em 8 de abril de 2026 e execução do workflow de ativos de apresentação via `scripts/build_thesis_presentation_assets.py` em 9 de abril de 2026; datasets Gold do pipeline medallion de análise de compliance público; Censo 2010/2022; registros de sanções do Portal da Transparência (CEIS, CNEP, CEPIM); limites territoriais oficiais do IBGE (2022).*
