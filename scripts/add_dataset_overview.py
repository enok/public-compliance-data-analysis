#!/usr/bin/env python3
"""
Add comprehensive Gold dataset overview section to EDA notebook.
"""

import json
from pathlib import Path

NOTEBOOK_PATH = Path("notebooks/01_exploratory_data_analysis.ipynb")

def create_dataset_overview_cells():
    """Create the new cells for comprehensive dataset overview."""
    
    markdown_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 2.1 Complete Gold Layer Dataset Inventory\n",
            "\n",
            "This section provides a comprehensive overview of **ALL 6 Gold layer datasets**.\n",
            "\n",
            "| Dataset | Rows | Grain | Purpose |\n",
            "|---------|------|-------|---------|\n",
            "| `municipality_socioeconomic` | 5,570 | Municipality | 2010→2022 census comparison with inflation-adjusted income |\n",
            "| `state_summary` | 27 | State | State-level aggregations (pop-weighted) |\n",
            "| `sanctions_summary` | 3 | Registry type | Sanctions by CEIS/CNEP/CEPIM registries |\n",
            "| `analysis_compliance` | 27 | State | State-level compliance analysis (legacy) |\n",
            "| `analysis_compliance_municipality` | 5,570 | Municipality | **Primary analysis dataset** - main analytical grain |\n",
            "| `consolidated_clustering` | 5,565 | Municipality | ML-ready with z-score normalized features |\n",
            "\n",
            "### Key Income Fields Explained\n",
            "\n",
            "**Inflation-adjusted income columns (IPCA-adjusted to 2022 BRL):**\n",
            "- `avg_income_real_2022_2022_brl` - 2022 income in 2022 BRL (base year)\n",
            "- `avg_income_real_2010_2022_brl` - 2010 income restated to 2022 BRL\n",
            "- `income_change_real_pct` - Real income change % (2010→2022), inflation-adjusted\n",
            "\n",
            "**Log-transformed columns (for regression analysis):**\n",
            "- `log_population` - Natural log of population (handles skewness)\n",
            "- `log_income` - Natural log of real income (base: avg_income_real_2022_2022_brl)\n",
            "- `log_total_transfers` - Log1p of federal transfers (handles zeros)\n"
        ]
    }
    
    code_cell_1 = {
        "cell_type": "code",
        "metadata": {"ExecuteTime": {"end_time": "2026-04-19T23:07:30.000000Z", "start_time": "2026-04-19T23:07:29.800000Z"}},
        "source": [
            "# ============================================================\n",
            "# DATASET 1: municipality_socioeconomic\n",
            "# Grain: 1 row per municipality (5,570 rows)\n",
            "# Contains: BOTH 2010 AND 2022 census years with change metrics\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 1: municipality_socioeconomic (Both Census Years)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_muni.shape}\")\n",
            "print(f\"\nColumns ({len(df_muni.columns)} total):\")\n",
            "print(df_muni.dtypes.to_string())\n",
            "print(f\"\\n--- Income-related columns (inflation-adjusted) ---\")\n",
            "income_cols = [c for c in df_muni.columns if 'income' in c.lower()]\n",
            "for col in income_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Sample data (first 5 rows, key columns) ---\")\n",
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
            "# Grain: 1 row per state (27 rows = 26 states + DF)\n",
            "# Contains: Population-weighted state aggregations\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 2: state_summary (State-level Aggregations)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_state.shape}\")\n",
            "print(f\"\nColumns ({len(df_state.columns)} total):\")\n",
            "print(df_state.dtypes.to_string())\n",
            "print(f\"\\n--- Income columns (both census years) ---\")\n",
            "income_cols = [c for c in df_state.columns if 'income' in c.lower()]\n",
            "for col in income_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Sample data (first 5 states) ---\")\n",
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
            "# Grain: 1 row per registry type (3 rows: CEIS, CNEP, CEPIM)\n",
            "# Contains: Sanction counts by registry with PJ/PF breakdown\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 3: sanctions_summary (By Registry Type)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_sanctions.shape}\")\n",
            "print(f\"\nAll columns:\")\n",
            "print(df_sanctions.dtypes.to_string())\n",
            "print(f\"\\n--- Full dataset (all 3 rows) ---\")\n",
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
            "# DATASET 4: analysis_compliance (State-level)\n",
            "# Grain: 1 row per state (27 rows)\n",
            "# Contains: Legacy state-level analysis with log-transformed vars\n",
            "# Note: Uses 2022 data only (single census year)\n",
            "# ============================================================\n",
            "df_analysis_state = datasets.get('analysis_compliance')\n",
            "\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 4: analysis_compliance (State-level, Legacy)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_analysis_state.shape}\")\n",
            "print(f\"\nColumns ({len(df_analysis_state.columns)} total):\")\n",
            "print(df_analysis_state.dtypes.to_string())\n",
            "print(f\"\\n--- Log-transformed columns ---\")\n",
            "log_cols = [c for c in df_analysis_state.columns if c.startswith('log_')]\n",
            "for col in log_cols:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- Sample data (first 5 states) ---\")\n",
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
            "# Grain: 1 row per municipality (5,570 rows)\n",
            "# Contains: PRIMARY ANALYTICAL DATASET (main grain)\n",
            "# Note: Uses 2022 data only + federal transfers\n",
            "# ============================================================\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 5: analysis_compliance_municipality (PRIMARY)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_analysis.shape} (includes {len(REGION_DUMMY_COLS)} region + {len(STATE_DUMMY_COLS)} state dummies)\")\n",
            "\n",
            "# Show base columns (excluding dummies)\n",
            "base_cols = [c for c in df_analysis.columns if c not in REGION_DUMMY_COLS + STATE_DUMMY_COLS]\n",
            "print(f\"\\nBase columns ({len(base_cols)} total, excluding dummies):\")\n",
            "for col in base_cols:\n",
            "    print(f\"  • {col} ({df_analysis[col].dtype})\")\n",
            "\n",
            "print(f\"\\n--- Log-transformed columns ---\")\n",
            "for col in ['log_population', 'log_income', 'log_total_transfers']:\n",
            "    print(f\"  • {col}\")\n",
            "print(f\"\\n--- IPCA-adjusted income column ---\")\n",
            "print(f\"  • avg_income_real_2022_2022_brl (inflation-adjusted to 2022 BRL)\")\n",
            "print(f\"\\n--- Sample data (first 5 municipalities, base columns) ---\")\n",
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
            "# Grain: 1 row per municipality (5,565 rows, 5 fewer due to missing data)\n",
            "# Contains: ML-ready data with BOTH 2010+2022 + z-score normalized features\n",
            "# ============================================================\n",
            "df_clustering = datasets.get('consolidated_clustering')\n",
            "\n",
            "print(\"=\" * 80)\n",
            "print(\"DATASET 6: consolidated_clustering (ML-Ready)\")\n",
            "print(\"=\" * 80)\n",
            "print(f\"Shape: {df_clustering.shape}\")\n",
            "\n",
            "print(f\"\\n--- Raw value columns (both census years) ---\")\n",
            "raw_cols = [c for c in df_clustering.columns if not c.endswith('_norm') and \n",
            "            c not in ['municipality_code', 'municipality_name', 'state_code', 'state_abbrev', \n",
            "                     'state_name', 'region_code', 'region_name']]\n",
            "for col in raw_cols:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Normalized columns (z-score, for ML) ---\")\n",
            "norm_cols = [c for c in df_clustering.columns if c.endswith('_norm')]\n",
            "for col in norm_cols:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Log columns (for regression) ---\")\n",
            "log_cols_cluster = [c for c in df_clustering.columns if c.startswith('log_')]\n",
            "for col in log_cols_cluster:\n",
            "    print(f\"  • {col}\")\n",
            "\n",
            "print(f\"\\n--- Sample data: 2010 vs 2022 comparison ---\")\n",
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
            "### Dataset Summary: Which Dataset to Use When?\n",
            "\n",
            "| Analysis Goal | Recommended Dataset | Why |\n",
            "|--------------|---------------------|-----|\n",
            "| **Compare 2010↔2022 census change** | `municipality_socioeconomic` or `consolidated_clustering` | Both contain both census years with inflation adjustment |\n",
            "| **State-level policy analysis** | `state_summary` | Population-weighted, stable aggregates |\n",
            "| **Sanctions registry analysis** | `sanctions_summary` | CEIS/CNEP/CEPIM breakdown |\n",
            "| **Main thesis analysis** | `analysis_compliance_municipality` | **Primary grain** - 5,570 municipalities with transfers + sanctions |\n",
            "| **Regression/ML modeling** | `consolidated_clustering` | Clean, normalized, no missing values |\n",
            "| **Compliance trends over time** | `analysis_compliance` (state) or municipality version | State-level for small-N analysis |\n",
            "\n",
            "### Income & Inflation Reference\n",
            "\n",
            "**Column naming convention:** `avg_income_real_{value_year}_{base_year}_brl`\n",
            "- `value_year`: The census year the income was measured\n",
            "- `base_year`: The inflation base year (2022 = IPCA reference year)\n",
            "\n",
            "**Examples:**\n",
            "- `avg_income_real_2022_2022_brl` = 2022 income in 2022 BRL (nominal = real, base year)\n",
            "- `avg_income_real_2010_2022_brl` = 2010 income restated to 2022 BRL (inflation-adjusted)\n",
            "- `income_change_real_pct` = Real change % between the two (inflation-adjusted comparison)"
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
    
    # Find the index of the cell containing "## 2. Dataset Overview"
    insert_index = None
    for i, cell in enumerate(notebook['cells']):
        if cell['cell_type'] == 'markdown' and '## 2. Dataset Overview' in ''.join(cell['source']):
            # Find the end of section 2 (after df_analysis[preview_cols].head() cell)
            # We need to skip past: markdown + 2 code cells (shape/dtypes + preview)
            insert_index = i + 3  # After section 2 markdown + 2 code cells
            break
    
    if insert_index is None:
        print("Could not find '## 2. Dataset Overview' cell")
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
    
    print(f"✅ Successfully inserted {len(new_cells)} new cells after section 2")
    print(f"   Notebook now has {len(notebook['cells'])} total cells")
    return True

if __name__ == "__main__":
    success = insert_cells_after_section_2()
    exit(0 if success else 1)
