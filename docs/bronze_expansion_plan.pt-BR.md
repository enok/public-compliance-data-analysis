# Plano de Expansao da Bronze

## Objetivo

Fechar primeiro o backlog atual da Bronze e depois adicionar uma nova fonte por vez, somente quando ela melhorar de forma material o argumento do TCC sobre eficiencia do gasto publico municipal, risco de compliance e heterogeneidade de resultados.

Este plano e deliberadamente conservador:

- concluir o backfill atual de `federal_transfers` e confirmar as saidas de Silver e Gold
- adicionar em seguida a fonte municipal de maior valor analitico
- adiar fontes de menor sinal ou maior atrito ate que o texto do TCC mostre que elas sao realmente necessarias

## Recomendacao Atual

### Prioridade 0: Fechar o escopo Bronze existente

Antes de adicionar qualquer nova fonte:

1. Deixar terminar o backfill atual da Transparencia para `2010-01` ate `2022-12`
2. Revalidar os checkpoints suspeitos de `no_data` do inicio da serie com:

```bash
TRANSPARENCY_FORCE_REFRESH=1 \
TRANSPARENCY_START_MA=01/2010 \
TRANSPARENCY_END_MA=12/2013 \
AWS_SDK_LOAD_CONFIG=1 \
./scripts/01_bronze_ingestion.sh --only-transparency
```

3. Executar:

```bash
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh
```

4. Confirmar que as seguintes saidas reais existem:
- `silver/dim_inflation_index`
- `silver/fact_income.avg_income_real_2022_brl`
- saidas Gold com `income_change_real_pct`
- cobertura mensal `federal_transfers_YYYY_MM.json` para toda a janela intercensitaria

So depois disso faz sentido expandir a Bronze.

## Priorizacao de Fontes

### 1. FINBRA / Siconfi

Status: `Proxima fonte recomendada`

Por que vem primeiro:

- entrega controles de capacidade fiscal e estrutura orcamentaria municipal
- melhora a estrategia empirica do TCC de forma mais direta do que a maioria das fontes setoriais
- ajuda a separar "mais transferencias por fragilidade fiscal" de "mais transferencias com resultados anomicos"
- suporta features como participacao de receita propria, dependencia de transferencias, intensidade de gasto com pessoal, mix de investimento e proxies de equilibrio fiscal

Fontes oficiais:

- API Siconfi do Tesouro Transparente: https://www.tesourotransparente.gov.br/consultas/consultas-siconfi/siconfi-api-de-dados-abertos
- landing page da documentacao da API citada pelo Tesouro: http://apidatalake.tesouro.gov.br/docs/siconfi/
- Tabela dos Entes da Federacao: https://www.tesourotransparente.gov.br/ckan/dataset/api-tabela-entes

Valor esperado para o TCC:

- melhor candidata para controles fiscais municipais
- melhora a interpretacao causal e descritiva
- provavelmente e a melhor proxima adicao se so mais uma fonte puder entrar

Datasets sugeridos na Bronze:

- `siconfi_entes`
- `siconfi_municipal_revenue`
- `siconfi_municipal_expense`
- `siconfi_municipal_fiscal_balance`

Caminhos sugeridos na Bronze:

- `bronze/fiscal/siconfi_entes.json`
- `bronze/fiscal/siconfi_municipal_revenue_YYYY.json`
- `bronze/fiscal/siconfi_municipal_expense_YYYY.json`
- `bronze/fiscal/siconfi_municipal_fiscal_balance_YYYY.json`

Resultado esperado na Silver:

- uma tabela fiscal municipal normalizada por `municipality_code` e `year`
- opcionalmente uma dimensao de entes para codigos de municipio e UF

### 2. CNES

Status: `Segunda melhor opcao`

Por que ajuda:

- adiciona controles de capacidade em saude no nivel municipal
- pode explicar diferencas de execucao e capacidade local de absorcao
- e util se a narrativa do TCC ligar transferencias a capacidade estatal local

Fonte oficial:

- portal de dados abertos do SUS para o CNES: https://dadosabertos.saude.gov.br/dataset/cnes-cadastro-nacional-de-estabelecimentos-de-saude

Valor esperado para o TCC:

- fonte forte para controles
- especialmente util se o TCC discutir capacidade institucional ou estrutura do sistema de saude

Datasets sugeridos na Bronze:

- `cnes_establishments`

### 3. IDEB / Inep

Status: `Melhor adicao de outcome, mas abaixo de FINBRA na fila`

Por que ajuda:

- adiciona um indicador de resultado municipal em vez de apenas controles de insumo
- e util se o TCC quiser falar de eficiencia em termos de desempenho social e nao apenas execucao orcamentaria

Fontes oficiais:

- portal de dados abertos do Inep: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos
- pagina do IDEB: https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb

Valor esperado para o TCC:

- alto se o capitulo empirico comparar fluxos de recursos publicos com desempenho educacional medido
- menor se a variavel dependente principal continuar sendo apenas risco de compliance

Dataset sugerido na Bronze:

- `ideb_municipal`

### 4. SIOPS

Status: `Util, mas mais caro operacionalmente`

Por que ajuda:

- mede gasto efetivo em saude pelos municipios
- permite comparar transferencias recebidas com despesa em saude executada

Fontes oficiais:

- portal SIOPS: https://portalfns.saude.gov.br/siops/

Valor esperado para o TCC:

- alto se gasto em saude for eixo central
- maior atrito de ingestao e padronizacao do que FINBRA

### 5. SNIS

Status: `Adiar, a menos que o TCC precise de indicadores anuais de servico alem do censo`

Por que ajuda:

- traz indicadores intercensitarios de saneamento e servicos
- so vale a pena se o TCC precisar de proxies anuais de entrega de servico entre 2010 e 2022

Fonte oficial:

- SNIS: https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/snis

## Ordem Recomendada De Implementacao

### Fase 1

Implementar `FINBRA / Siconfi` primeiro.

Criterios de aceite:

- dados fiscais municipais brutos chegam na Bronze com proveniencia documentada
- metricas fiscais podem ser ligadas a `municipality_code` e `year`
- a Silver produz uma tabela fiscal canonica
- testes validam schema, padrao de nome e cobertura de chaves

### Fase 2

Implementar `CNES` ou `IDEB`, nao os dois imediatamente.

Escolher `CNES` se:

- o TCC precisa de controles de capacidade estatal
- o capitulo empirico continua focado em capacidade de absorcao, governanca e contexto de execucao

Escolher `IDEB` se:

- o TCC precisa de um outcome de servico publico mais interpretavel
- o capitulo empirico comparar gasto e desempenho educacional medido

## Mudancas De Repositorio Esperadas

Quando a Fase 1 comecar, os pontos de toque esperados sao:

- novo arquivo de config: `config/fiscal_metadata.json`
- novo ingestor Bronze: `src/ingestion/fiscal_client.py`
- atualizacao do runner: `scripts/01_bronze_ingestion.sh`
- normalizacao Silver: `src/processing/fiscal_transformer.py` ou extensao direcionada dos modulos atuais
- atualizacao do contrato em `config/silver_schemas.json`
- testes:
  - `tests/ingestion/test_fiscal_client.py`
  - `tests/ingestion/test_fiscal_config_validation.py`
  - `tests/processing/test_fiscal_transformer.py`
- docs:
  - `docs/01_BRONZE_LAYER.md`
  - `docs/01_BRONZE_LAYER.pt-BR.md`
  - `docs/02_SILVER_LAYER.md`
  - `docs/02_SILVER_LAYER.pt-BR.md`
  - relatorios de cobertura quando a fonte estiver operacional

## Escopo Minimo Para FINBRA / Siconfi

Para evitar crescimento descontrolado, a Fase 1 deve mirar apenas nas variaveis que mais ajudam o TCC:

- receita corrente total
- receita propria
- transferencias intergovernamentais recebidas
- despesa total
- despesa com pessoal
- despesa de investimento

Todo o resto deve ser explicitamente adiado.

## Regra De Decisao

Se so mais uma fonte Bronze puder entrar antes de a escrita do TCC ganhar prioridade, adicione `FINBRA / Siconfi`.

Se ainda houver espaco para uma segunda fonte:

- adicione `CNES` para controles de capacidade
- ou adicione `IDEB` para mensurar outcome

Nao adicione todas as candidatas de uma vez.

