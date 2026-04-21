# Política de sincronização TCC ↔ Notebook

> **A fonte da verdade para cada estatística, tabela e figura do TCC é o
> notebook Jupyter que a produz. O texto do TCC é um consumidor a jusante
> — nunca uma fonte independente.**

A banca examinadora pode abrir qualquer notebook, re-executá-lo de ponta
a ponta e verificar se o valor impresso pelo notebook corresponde ao valor
que aparece na página correspondente do TCC. Se divergirem, o TCC está
errado — não o notebook.

Esta política torna esse invariante auditável.

---

## 1. Mapa de propriedade

| Artefato no TCC | Notebook autoritativo | O que o notebook exporta |
|---|---|---|
| `TABELA_1_ROWS` (fontes de dados) | `notebooks/00_etl_pipeline.ipynb` | Contagens de registros de cada ingestão Bronze |
| `TABELA_2_ROWS` (Pearson, municipal) | `notebooks/01_exploratory_data_analysis.ipynb` | `r`, `p-valor`, significância |
| `TABELA_3_ROWS` (coeficientes OLS) | `notebooks/02_statistical_analysis.ipynb` | coef, erro-padrão (HC3), p-valor |
| `TABELA_4_ROWS` (métricas ML) | `notebooks/03_machine_learning.ipynb` | ROC-AUC, F1, R² teste |
| `TABELA_5_ROWS` (perfis de cluster) | `notebooks/04_clustering.ipynb` | Média dos centroides por cluster |
| `EDA_MAP_IMAGE_PATH`, `brazil_map_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Coroplético → `generated/` |
| `EDA_STATE_RANKING_IMAGE_PATH`, `state_ranking_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Barras ordenadas → `generated/` |
| `EDA_REGIONAL_IMAGE_PATH`, `regional_summary_*.png` | `notebooks/01_exploratory_data_analysis.ipynb` | Pequenos múltiplos → `generated/` |
| `OLS_DIAGNOSTICS_IMAGE_PATH`, `ols_diagnostics_*.png` | `notebooks/02_statistical_analysis.ipynb` | Gráficos de resíduos → `generated/` |
| `ML_PERFORMANCE_IMAGE_PATH`, `ml_performance_*.png` | `notebooks/03_machine_learning.ipynb` | ROC + matriz de confusão → `generated/` |
| `CLUSTER_ELBOW_IMAGE_PATH`, `elbow_silhouette_*.png` | `notebooks/04_clustering.ipynb` | Seleção de K → `generated/` |
| `CLUSTER_PROFILE_IMAGE_PATH`, `cluster_profile_*.png` | `notebooks/04_clustering.ipynb` | Perfil de clusters → `generated/` |
| `CLUSTER_CORR_IMAGE_PATH`, `intra_cluster_corr_*.png` | `notebooks/04_clustering.ipynb` | Correlação intra-cluster → `generated/` |
| `correlations_state.png` (Apêndice Figura C) | `scripts/regenerate_state_correlations.py` (replica lógica do notebook sobre o Gold) | Heatmap 5×5 de `analysis_compliance` |
| Afirmações no texto: `r = 0,74` (estadual), `R² = 0,024`, `F = 18,5`, `ROC-AUC = 0,83`, `silhueta = 0,288`, `K = 4`, `n = 5.570`, etc. | O notebook que computa cada valor | Saída impressa da célula |

Ativos manuais que **não** vêm dos notebooks:

| Artefato | Origem |
|---|---|
| `MEDALLION_IMAGE_PATH` (Figura 1) | `_build/medallion-architecture.png` / `.pt-BR.png` — peça de design, não computada |
| `EQUATION_1_IMAGE_PATH` | `generated/equation_*.png` — render LaTeX, regenerado manualmente |

---

## 2. Fluxo direcional

```
Parquet Gold (data/gold/*)
        │
        ▼
Notebook Jupyter roda a análise, imprime valores, salva PNGs em
docs/thesis_presentation_assets/generated/
        │
        │  (transcrição manual — valores vindos da saída impressa)
        ▼
tcc/final/_build/content_*.py  (TABELA_*_ROWS, números no texto, IMAGE_PATHs)
        │
        ▼
build_tcc.py gera o DOCX
```

**Proibido:** editar um número em `content_*.py` que não tenha sido
previamente verificado contra a saída do notebook. Mesmo que pareça
"obviamente correto" — ele não é a fonte da verdade.

**Proibido:** editar manualmente um PNG em `generated/` (ex.: em um editor
de imagens). Sempre re-executar a célula do notebook que o produz.

---

## 3. Aplicação: `scripts/verify_thesis_notebook_sync.py`

Rodar antes de cada commit que altere notebooks ou `content_*.py`:

```powershell
.venv\Scripts\python.exe scripts\verify_thesis_notebook_sync.py
```

O que o script verifica:

1. **Proveniência de figuras** — cada `*_IMAGE_PATH` referenciado em
   `content_pt[2-3].py` / `content_en[2-3].py` existe em disco e está em
   `generated/` (ou `_build/` para peças de design), além de não ser mais
   antigo que os parquets do Gold.

2. **Pearson (municipal)** — recomputa as quatro correlações de
   `TABELA_2_ROWS` diretamente de
   `data/gold/analysis_compliance_municipality/data.parquet` e sinaliza
   qualquer desvio maior que ±0,01.

3. **Pearson (estadual)** — verifica a afirmação `r = 0,74` entre renda
   média estadual e sanções por 100 mil habitantes.

4. **OLS (municipal)** — re-ajusta a especificação OLS com erros robustos
   HC3 em statsmodels e sinaliza desvio em R², R² ajustado, estatística F
   e cada coeficiente de `TABELA_3_ROWS` (tolerância 0,05 em coef, 0,005
   em R²).

Código de saída `0` se tudo casa; `1` se há qualquer desvio.

Métricas de ML e k-means **não** são re-verificadas automaticamente porque
envolvem estocasticidade (seed do train/test split, inicialização). Para
essas, a saída impressa do notebook é o valor autoritativo e o operador
precisa confirmar manualmente `TABELA_4_ROWS` e o texto do K-means contra
a célula após cada re-execução do notebook.

---

## 4. Recuperação: quando a auditoria falha

Rode a auditoria. Para cada `FAIL`:

1. Abra o notebook responsável segundo a tabela acima.
2. Execute a célula que produz a métrica. Leia o valor impresso.
3. Compare com o valor em `content_*.py`:

   - Se o valor do notebook mudou → **atualize `content_*.py`** em
     ambos PT e EN para casar com o notebook. Reconstrua o DOCX.
   - Se o valor do TCC estava obviamente correto e o notebook regrediu
     → investigue por quê. Não edite `content_*.py` para esconder um
     notebook quebrado.

4. Rode a auditoria novamente. Confirme zero FAILs.
5. Commit as mudanças do notebook, as edições de `content_*.py` e o DOCX
   regenerado em um único commit, para os três permanecerem sincronizados
   atomicamente.

---

## 5. Checklist de commit (copie para a descrição do PR)

- [ ] Notebook executa de ponta a ponta limpo a partir de um kernel novo.
- [ ] Todas as figuras referenciadas pelo TCC existem em `generated/` e
      foram regeneradas pelo notebook nesta sessão.
- [ ] `python scripts/verify_thesis_notebook_sync.py` termina com código 0.
- [ ] A reconstrução do DOCX ocorre sem erro para PT e EN.
- [ ] Verifiquei na mão ao menos 2 números do TCC contra as saídas do
      notebook (idealmente um de cada: Pearson, OLS, ML, cluster).
- [ ] TCC em PT e EN contêm os **mesmos** valores numéricos (diferença
      apenas em formatação: `0,149` vs `0.149`).
