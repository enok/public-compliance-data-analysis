# Workflow de Análise Completa Municipal (Espelho do Fluxo Estadual)

Este workflow executa um caminho analítico completo em nível municipal, alinhado ao fluxo estadual da tese:
- análise estatística (correlação + OLS)
- benchmark de aprendizado de máquina
- contraste de desfechos por cluster
- comparação de municípios pares dentro do mesmo cluster
- geração de adendo de conclusão da tese

## Comando

```bash
python scripts/run_city_full_analysis.py
```

Parâmetros opcionais:

```bash
python scripts/run_city_full_analysis.py \
  --bucket-name enok-mba-thesis-datalake \
  --aws-profile mba-thesis \
  --output-dir docs/city_full_analysis \
  --min-k 2 \
  --max-k 10 \
  --peer-top-n 3
```

## Artefatos Gerados

- `docs/city_full_analysis/city_correlations.csv`
- `docs/city_full_analysis/city_ols_coefficients.csv`
- `docs/city_full_analysis/city_ml_scores.csv`
- `docs/city_full_analysis/city_ml_feature_importance.csv`
- `docs/city_full_analysis/city_cluster_k_diagnostics.csv`
- `docs/city_full_analysis/city_cluster_outcome_summary.csv`
- `docs/city_full_analysis/city_same_cluster_peers.csv`
- `docs/city_full_analysis/city_thesis_conclusion_addendum.md`
- `docs/city_full_analysis/city_thesis_conclusion_addendum.pt-BR.md`

## Integração na Tese

Após executar o comando, incorpore o adendo gerado em:
- `docs/thesis_conclusion.md`
- `docs/thesis_conclusion.pt-BR.md`

Isso fecha a lacuna entre inferência apenas estadual e evidência em nível municipal.
