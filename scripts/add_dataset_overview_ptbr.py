#!/usr/bin/env python3
"""
Add comprehensive Gold dataset overview section to EDA notebook (Portuguese version).
"""

import json
from pathlib import Path

NOTEBOOK_PATH = Path("notebooks/01_exploratory_data_analysis.pt-BR.ipynb")

def create_dataset_overview_cells():
    """Create the new cells for comprehensive dataset overview in Portuguese."""
    
    markdown_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 2.1 Inventario Completo dos Datasets da Camada Gold\n",
            "\n",
            "Esta secao fornece uma visao geral abrangente de **TODOS os 6 datasets da camada Gold**.\n",
            "\n",
            "| Dataset | Linhas | Granularidade | Proposito |\n",
            "|---------|--------|---------------|-----------|\n",
            "| `municipality_socioeconomic` | 5.570 | Municipio | Comparacao censos 2010→2022 com renda ajustada pela inflacao |\n",
            "| `state_summary` | 27 | Estado | Agregacoes estaduais (ponderadas por populacao) |\n",
            "| `sanctions_summary` | 3 | Tipo de registro | Sancoes por registros CEIS/CNEP/CEPIM |\n",
            "| `analysis_compliance` | 27 | Estado | Analise de compliance estadual (legado) |\n",
            "| `analysis_compliance_municipality` | 5.570 | Municipio | **Dataset primario de analise** - granularidade principal |\n",
            "| `consolidated_clustering` | 5.565 | Municipio | Pronto para ML com features normalizadas (z-score) |\n",
            "\n",
            "### Campos de Renda Explicados\n",
            "\n",
            "**Colunas de renda ajustadas pela inflacao (IPCA ajustado para BRL 2022):**\n",
            "- `avg_income_real_2022_2022_brl` - Renda 2022 em BRL 2022 (ano base)\n",
            "- `avg_income_real_2010_2022_brl` - Renda 2010 reajustada para BRL 2022\n",
            "- `income_change_real_pct` - Variacao real da renda % (2010→2022), ajustada pela inflacao\n",
            "\n",
            "**Colunas com transformacao logaritmica (para analise de regressao):**\n",
            "- `log_population` - Log natural da populacao (lida com assimetria)\n",
            "- `log_income` - Log natural da renda real (base: avg_income_real_2022_2022_brl)\n",
            "- `log_total_transfers` - Log1p das transferencias federais (lida com zeros)\n"
        ]
    }
    
    code_cell_1 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.000000Z", "start_time": "2026-04-19T23:07:29.800000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 1: municipality_socioeconomic\n",
            "# Granularidade: 1 linha por municipio (5.570 linhas)\n",
            "# Contem: AMBOS os anos censitarios 2010 E 2022 com metricas de mudanca\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 1: municipality_socioeconomic (Ambos os Anos Censitarios)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_muni.shape}\")\n",
            "print(f\"\nColunas ({len(df_muni.columns)} total):\")\n",
            "print(df_muni.dtypes.to_string())\n",
            "print(f\"\\n--- Colunas de renda (ajustadas pela inflacao) ---\")\n",
            "income_cols = [c for c in df_muni.columns if 'income' in c.lower()]\n",
            "for col in income_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Amostra de dados (primeiras 5 linhas, colunas principais) ---\")\n",
            "key_cols = ['municipality_code', 'municipality_name', 'population_2010', 'population_2022', \n",
            "            'avg_income_2010', 'avg_income_2022', 'avg_income_real_2010_2022_brl', \n",
            "            'avg_income_real_2022_2022_brl', 'income_change_real_pct']\n",
            "display(df_muni[key_cols].head())"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    code_cell_2 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.200000Z", "start_time": "2026-04-19T23:07:30.000000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 2: state_summary\n",
            "# Granularidade: 1 linha por estado (27 linhas = 26 estados + DF)\n",
            "# Contem: Agregacoes estaduais ponderadas por populacao\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 2: state_summary (Agregacoes Estaduais)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_state.shape}\")\n",
            "print(f\"\nColunas ({len(df_state.columns)} total):\")\n",
            "print(df_state.dtypes.to_string())\n",
            "print(f\"\\n--- Colunas de renda (ambos os anos censitarios) ---\")\n",
            "income_cols = [c for c in df_state.columns if 'income' in c.lower()]\n",
            "for col in income_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Amostra de dados (primeiros 5 estados) ---\")\n",
            "display(df_state.head())"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    code_cell_3 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.400000Z", "start_time": "2026-04-19T23:07:30.200000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 3: sanctions_summary\n",
            "# Granularidade: 1 linha por tipo de registro (3 linhas: CEIS, CNEP, CEPIM)\n",
            "# Contem: Contagens de sancoes por registro com detalhamento PJ/PF\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 3: sanctions_summary (Por Tipo de Registro)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_sanctions.shape}\")\n",
            "print(f\"\nTodas as colunas:\")\n",
            "print(df_sanctions.dtypes.to_string())\n",
            "print(f\"\\n--- Dataset completo (todas as 3 linhas) ---\")\n",
            "display(df_sanctions)"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    code_cell_4 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.600000Z", "start_time": "2026-04-19T23:07:30.400000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 4: analysis_compliance (Nivel Estadual)\n",
            "# Granularidade: 1 linha por estado (27 linhas)\n",
            "# Contem: Analise estadual legado com variaveis log-transformadas\n",
            "# Nota: Usa apenas dados de 2022 (unico ano censitario)\n",
            "# ============================================================\n",
            "df_analysis_state = datasets.get('analysis_compliance')\n",
            "\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 4: analysis_compliance (Nivel Estadual, Legado)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_analysis_state.shape}\")\n",
            "print(f\"\nColunas ({len(df_analysis_state.columns)} total):\")\n",
            "print(df_analysis_state.dtypes.to_string())\n",
            "print(f\"\\n--- Colunas log-transformadas ---\")\n",
            "log_cols = [c for c in df_analysis_state.columns if c.startswith('log_')]\n",
            "for col in log_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Amostra de dados (primeiros 5 estados) ---\")\n",
            "display(df_analysis_state.head())"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    code_cell_5 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.800000Z", "start_time": "2026-04-19T23:07:30.600000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 5: analysis_compliance_municipality\n",
            "# Granularidade: 1 linha por municipio (5.570 linhas)\n",
            "# Contem: DATASET PRIMARIO DE ANALISE (granularidade principal)\n",
            "# Nota: Usa apenas dados de 2022 + transferencias federais\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 5: analysis_compliance_municipality (PRIMARIO)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_analysis.shape} (inclui {len(REGION_DUMMY_COLS)} region + {len(STATE_DUMMY_COLS)} state dummies)\")\n",
            "\n",
            "# Show base columns (excluding dummies)\n",
            "base_cols = [c for c in df_analysis.columns if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]\n",
            "print(f\"\\nColunas base ({len(base_cols)} total, excluindo dummies):\")\n",
            "for col in base_cols:\n",
            "    print(f\"  • {col} ({df_analysis[col].dtype})\")\n",
            "\n",
            "print(f\"\\n--- Colunas log-transformadas ---\")\n",
            "for col in ['log_population', 'log_income', 'log_total_transfers']:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Coluna de renda ajustada pelo IPCA ---\")\n",
            "print(f\"  • avg_income_real_2022_2022_brl (ajustada pela inflacao para BRL 2022)\")\n",
            "print(f\"\\n--- Amostra de dados (primeiros 5 municipios, colunas base) ---\")\n",
            "display(df_analysis[base_cols].head())"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    code_cell_6 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:31.000000Z", "start_time": "2026-04-19T23:07:30.800000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 6: consolidated_clustering\n",
            "# Granularidade: 1 linha por municipio (5.565 linhas, 5 a menos devido a dados faltantes)\n",
            "# Contem: Dados prontos para ML com AMBOS 2010+2022 + features normalizadas z-score\n",
            "# ============================================================\n",
            "df_clustering = datasets.get('consolidated_clustering')\n",
            "\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 6: consolidated_clustering (Pronto para ML)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Dimensao: {df_clustering.shape}\")\n",
            "\n",
            "print(f\"\\n--- Colunas de valores brutos (ambos os anos censitarios) ---\")\n",
            "raw_cols = [c for c in df_clustering.columns if not c.endswith('_norm') and \n",
            "            c not in ['municipality_code', 'municipality_name', 'state_code', 'state_abbrev', \n",
            "                     'state_name', 'region_code', 'region_name']]\n",
            "for col in raw_cols:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Colunas normalizadas (z-score, para ML) ---\")\n",
            "norm_cols = [c for c in df_clustering.columns if c.endswith('_norm')]\n",
            "for col in norm_cols:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Colunas log (para regressao) ---\")\n",
            "log_cols_cluster = [c for c in df_clustering.columns if c.startswith('log_')]\n",
            "for col in log_cols_cluster:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Amostra de dados: comparacao 2010 vs 2022 ---\")\n",
            "compare_cols = ['municipality_name', 'state_abbrev', 'population_2010', 'population_2022',\n",
            "                'avg_income_real_2010_2022_brl', 'avg_income_real_2022_2022_brl',\n",
            "                'income_change_real_pct']\n",
            "display(df_clustering[compare_cols].head(10))"
        ],
        "outputs": [],
        "execution_count": None
    }
    
    summary_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### Resumo dos Datasets: Qual Dataset Usar Quando?\n",
            "\n",
            "| Objetivo da Analise | Dataset Recomendado | Por que? |\n",
            "|---------------------|---------------------|----------|\n",
            "| **Comparar mudanca 2010↔2022** | `municipality_socioeconomic` ou `consolidated_clustering` | Ambos contem ambos os anos censitarios com ajuste inflacionario |\n",
            "| **Analise de politica estadual** | `state_summary` | Agregados ponderados por populacao, estaveis |\n",
            "| **Analise de registros de sancoes** | `sanctions_summary` | Detalhamento CEIS/CNEP/CEPIM |\n",
            "| **Analise principal da tese** | `analysis_compliance_municipality` | **Granularidade primaria** - 5.570 municipios com transferencias + sancoes |\n",
            "| **Modelagem de regressao/ML** | `consolidated_clustering` | Limpo, normalizado, sem valores faltantes |\n",
            "| **Tendencias de compliance ao longo do tempo** | `analysis_compliance` (estado) ou versao municipio | Nivel estadual para analise com N pequeno |\n",
            "\n",
            "### Referencia de Renda e Inflacao\n",
            "\n",
            "**Convencao de nomenclatura:** `avg_income_real_{ano_valor}_{ano_base}_brl`\n",
            "- `ano_valor`: O ano censitario em que a renda foi medida\n",
            "- `ano_base`: O ano base da inflacao (2022 = ano de referencia do IPCA)\n",
            "\n",
            "**Exemplos:**\n",
            "- `avg_income_real_2022_2022_brl` = Renda 2022 em BRL 2022 (nominal = real, ano base)\n",
            "- `avg_income_real_2010_2022_brl` = Renda 2010 reajustada para BRL 2022 (ajustada pela inflacao)\n",
            "- `income_change_real_pct` = Mudanca real % entre os dois (comparacao ajustada pela inflacao)"
        ]
    }
    
    return [
        markdown_cell,
        code_cell_1,
        code_cell_2,
        code_cell_3,
        code_cell_4,
        code_cell_5,
        code_cell_6,
        summary_cell
    ]

def insert_cells_after_section_2():
    """Insert the new cells after section 2 in the notebook."""
    
    with open(NOTEBOOK_PATH, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    # Find the index of the cell containing "## 2. Visao Geral"
    insert_index = None
    for i, cell in enumerate(notebook['cells']):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if cell['cell_type'] == 'markdown' and ('## 2.' in source or 'Visao Geral' in source or 'Dataset Overview' in source):
            # Find the end of section 2 (after df_analysis[preview_cols].head() cell)
            insert_index = i + 3  # After section 2 markdown + 2 code cells
            break
    
    if insert_index is None:
        print("Could not find section 2 cell")
        return False
    
    # Create new cells
    new_cells = create_dataset_overview_cells()
    
    # Insert new cells
    notebook['cells'] = (
        notebook['cells'][:insert_index] + 
        new_cells + 
        notebook['cells'][insert_index:]
    )
    
    # Save updated notebook
    with open(NOTEBOOK_PATH, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)
    
    print(f"Successfully inserted {len(new_cells)} new cells after section 2 (pt-BR)")
    print(f"Notebook now has {len(notebook['cells'])} total cells")
    return True

if __name__ == "__main__":
    success = insert_cells_after_section_2()
    exit(0 if success else 1)
