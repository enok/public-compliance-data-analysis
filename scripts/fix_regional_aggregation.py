"""Fix statistical and visualization issues in the EDA notebooks.

Issues fixed:
1. Regional aggregation used unweighted mean of state-level rates/averages,
   which gives misleading regional statistics (small states with extreme rates
   get equal weight to large states). Replaced with population-weighted
   aggregation and totals-based rate calculation.
2. 4-panel regional dashboard used the same misleading unweighted means.
3. Per-state bar chart used `state_code` ("11", "35", ...) as x-axis labels.
   Replaced with `state_name` (and for pt-BR, `nome_estado`) for readability.

Run once:
    python scripts/fix_regional_aggregation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"

EN_NOTEBOOK = NOTEBOOKS_DIR / "01_exploratory_data_analysis.ipynb"
PT_NOTEBOOK = NOTEBOOKS_DIR / "01_exploratory_data_analysis.pt-BR.ipynb"


# ---------------------------------------------------------------------------
# Replacement sources (each string is joined with "\n" in Jupyter's "source")
# ---------------------------------------------------------------------------

EN_REGIONAL_SUMMARY_OLD = [
    "regional_summary = df_analysis.groupby('region_name').agg({\n",
    "    'state_code': 'count',\n",
    "    'population': 'sum',\n",
    "    'n_sanctions': 'sum',\n",
    "    'sanctions_per_100k': 'mean',\n",
    "    'avg_literacy_rate': 'mean',\n",
    "    'avg_income': 'mean'\n",
    "}).round(2)\n",
    "\n",
    "regional_summary.columns = ['N States', 'Total Population', 'Total Sanctions', \n",
    "                            'Avg Sanctions/100k', 'Avg Literacy %', 'Avg Income (BRL)']\n",
    "\n",
    "regional_summary\n",
]

EN_REGIONAL_SUMMARY_NEW = [
    "# Regional aggregation using POPULATION-WEIGHTED statistics.\n",
    "# A simple groupby('region').agg('mean') would be statistically misleading\n",
    "# because it gives each state equal weight regardless of population size\n",
    "# (e.g. a 3M-inhabitant state would weigh the same as a 44M-inhabitant state).\n",
    "# For rates and averages across a region, we aggregate from totals / weight by population.\n",
    "\n",
    "_pop_totals = df_analysis.groupby('region_name')['population'].sum()\n",
    "_san_totals = df_analysis.groupby('region_name')['n_sanctions'].sum()\n",
    "\n",
    "regional_summary = pd.DataFrame({\n",
    "    'N States': df_analysis.groupby('region_name')['state_code'].count(),\n",
    "    'Total Population': _pop_totals,\n",
    "    'Total Sanctions': _san_totals,\n",
    "    # Rate computed from regional totals (naturally population-weighted):\n",
    "    'Sanctions/100k (pop-weighted)': (_san_totals / _pop_totals * 100_000).round(2),\n",
    "    # Literacy and income weighted by each state's population:\n",
    "    'Literacy % (pop-weighted)': (\n",
    "        df_analysis.groupby('region_name').apply(\n",
    "            lambda g: np.average(g['avg_literacy_rate'], weights=g['population'])\n",
    "        )\n",
    "    ).round(2),\n",
    "    'Avg Income BRL (pop-weighted)': (\n",
    "        df_analysis.groupby('region_name').apply(\n",
    "            lambda g: np.average(g['avg_income'], weights=g['population'])\n",
    "        )\n",
    "    ).round(2),\n",
    "})\n",
    "\n",
    "regional_summary\n",
]

EN_REGIONAL_DASHBOARD_OLD = [
    "fig = make_subplots(\n",
    "    rows=2, cols=2,\n",
    "    subplot_titles=('Sanctions per 100k by Region', 'Literacy Rate by Region',\n",
    "                   'Average Income by Region', 'Total Sanctions by Region'),\n",
    "    specs=[[{'type': 'bar'}, {'type': 'bar'}],\n",
    "           [{'type': 'bar'}, {'type': 'bar'}]]\n",
    ")\n",
    "\n",
    "regions = df_analysis.groupby('region_name').agg({\n",
    "    'sanctions_per_100k': 'mean',\n",
    "    'avg_literacy_rate': 'mean',\n",
    "    'avg_income': 'mean',\n",
    "    'n_sanctions': 'sum'\n",
    "}).reset_index()\n",
    "\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['sanctions_per_100k'], \n",
    "                     name='Sanctions/100k', marker_color='indianred'), row=1, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['avg_literacy_rate'], \n",
    "                     name='Literacy %', marker_color='lightseagreen'), row=1, col=2)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['avg_income'], \n",
    "                     name='Income', marker_color='lightsalmon'), row=2, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['n_sanctions'], \n",
    "                     name='Total Sanctions', marker_color='mediumpurple'), row=2, col=2)\n",
    "\n",
    "fig.update_layout(height=800, showlegend=False, title_text=\"Regional Comparison Dashboard\")\n",
    "fig.show()\n",
]

EN_REGIONAL_DASHBOARD_NEW = [
    "# Regional dashboard using population-weighted aggregation (see note above).\n",
    "fig = make_subplots(\n",
    "    rows=2, cols=2,\n",
    "    subplot_titles=('Sanctions per 100k by Region (pop-weighted)',\n",
    "                    'Literacy Rate by Region (pop-weighted)',\n",
    "                    'Average Income by Region (pop-weighted)',\n",
    "                    'Total Sanctions by Region'),\n",
    "    specs=[[{'type': 'bar'}, {'type': 'bar'}],\n",
    "           [{'type': 'bar'}, {'type': 'bar'}]]\n",
    ")\n",
    "\n",
    "regions = (\n",
    "    df_analysis.groupby('region_name')\n",
    "    .apply(lambda g: pd.Series({\n",
    "        'sanctions_per_100k_w': g['n_sanctions'].sum() / g['population'].sum() * 100_000,\n",
    "        'avg_literacy_rate_w': np.average(g['avg_literacy_rate'], weights=g['population']),\n",
    "        'avg_income_w': np.average(g['avg_income'], weights=g['population']),\n",
    "        'n_sanctions': g['n_sanctions'].sum(),\n",
    "    }))\n",
    "    .reset_index()\n",
    ")\n",
    "\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['sanctions_per_100k_w'],\n",
    "                     name='Sanctions/100k', marker_color='indianred'), row=1, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['avg_literacy_rate_w'],\n",
    "                     name='Literacy %', marker_color='lightseagreen'), row=1, col=2)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['avg_income_w'],\n",
    "                     name='Income', marker_color='lightsalmon'), row=2, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['region_name'], y=regions['n_sanctions'],\n",
    "                     name='Total Sanctions', marker_color='mediumpurple'), row=2, col=2)\n",
    "\n",
    "fig.update_layout(height=800, showlegend=False, title_text=\"Regional Comparison Dashboard\")\n",
    "fig.show()\n",
]

EN_STATE_BAR_OLD = [
    "fig = px.bar(df_analysis.sort_values('sanctions_per_100k', ascending=False),\n",
    "             x='state_code', y='sanctions_per_100k',\n",
    "             color='region_name',\n",
    "             title='Sanctions per 100k Population by State',\n",
    "             labels={'sanctions_per_100k': 'Sanctions per 100k', 'state_code': 'State'},\n",
    "             height=500)\n",
    "fig.show()\n",
]

EN_STATE_BAR_NEW = [
    "# Use state_name on the x-axis instead of the 2-digit state_code (qualitative id).\n",
    "# state_code is a label, not a quantity, and \"11\", \"35\", ... are not informative.\n",
    "fig = px.bar(df_analysis.sort_values('sanctions_per_100k', ascending=False),\n",
    "             x='state_name', y='sanctions_per_100k',\n",
    "             color='region_name',\n",
    "             title='Sanctions per 100k Population by State',\n",
    "             labels={'sanctions_per_100k': 'Sanctions per 100k', 'state_name': 'State'},\n",
    "             height=500)\n",
    "fig.update_xaxes(tickangle=-45)\n",
    "fig.show()\n",
]


# PT-BR replacements ---------------------------------------------------------

PT_REGIONAL_SUMMARY_OLD = [
    "regional_summary = df_analysis.groupby('nome_regiao').agg({\n",
    "    'codigo_estado': 'count',\n",
    "    'populacao': 'sum',\n",
    "    'num_sancoes': 'sum',\n",
    "    'sancoes_por_100k': 'mean',\n",
    "    'taxa_alfabetizacao_media': 'mean',\n",
    "    'renda_media': 'mean'\n",
    "}).round(2)\n",
    "\n",
    "regional_summary.columns = ['N Estados', 'População Total', 'Total de Sanções', \n",
    "                            'Média Sanções/100k', 'Média Alfabetização %', 'Renda Média (BRL)']\n",
    "\n",
    "regional_summary\n",
]

PT_REGIONAL_SUMMARY_NEW = [
    "# Agregação regional usando estatísticas PONDERADAS PELA POPULAÇÃO.\n",
    "# Um groupby('regiao').agg('mean') simples seria estatisticamente enganoso\n",
    "# pois atribui peso igual a cada estado, independentemente da população\n",
    "# (ex.: um estado de 3M de habitantes teria o mesmo peso que um de 44M).\n",
    "# Para taxas e médias regionais, agregamos a partir dos totais / ponderamos pela população.\n",
    "\n",
    "_pop_totais = df_analysis.groupby('nome_regiao')['populacao'].sum()\n",
    "_san_totais = df_analysis.groupby('nome_regiao')['num_sancoes'].sum()\n",
    "\n",
    "regional_summary = pd.DataFrame({\n",
    "    'N Estados': df_analysis.groupby('nome_regiao')['codigo_estado'].count(),\n",
    "    'População Total': _pop_totais,\n",
    "    'Total de Sanções': _san_totais,\n",
    "    # Taxa calculada a partir dos totais regionais (naturalmente ponderada pela população):\n",
    "    'Sanções/100k (ponderada)': (_san_totais / _pop_totais * 100_000).round(2),\n",
    "    # Alfabetização e renda ponderadas pela população de cada estado:\n",
    "    'Alfabetização % (ponderada)': (\n",
    "        df_analysis.groupby('nome_regiao').apply(\n",
    "            lambda g: np.average(g['taxa_alfabetizacao_media'], weights=g['populacao'])\n",
    "        )\n",
    "    ).round(2),\n",
    "    'Renda BRL (ponderada)': (\n",
    "        df_analysis.groupby('nome_regiao').apply(\n",
    "            lambda g: np.average(g['renda_media'], weights=g['populacao'])\n",
    "        )\n",
    "    ).round(2),\n",
    "})\n",
    "\n",
    "regional_summary\n",
]

PT_REGIONAL_DASHBOARD_OLD = [
    "fig = make_subplots(\n",
    "    rows=2, cols=2,\n",
    "    subplot_titles=('Sanções por 100 mil por Região', 'Taxa de Alfabetização por Região',\n",
    "                   'Renda Média por Região', 'Total de Sanções por Região'),\n",
    "    specs=[[{'type': 'bar'}, {'type': 'bar'}],\n",
    "           [{'type': 'bar'}, {'type': 'bar'}]]\n",
    ")\n",
    "\n",
    "regions = df_analysis.groupby('nome_regiao').agg({\n",
    "    'sancoes_por_100k': 'mean',\n",
    "    'taxa_alfabetizacao_media': 'mean',\n",
    "    'renda_media': 'mean',\n",
    "    'num_sancoes': 'sum'\n",
    "}).reset_index()\n",
    "\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['sancoes_por_100k'], \n",
    "                     name='Sanções/100k', marker_color='indianred'), row=1, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['taxa_alfabetizacao_media'], \n",
    "                     name='Alfabetização %', marker_color='lightseagreen'), row=1, col=2)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['renda_media'], \n",
    "                     name='Renda', marker_color='lightsalmon'), row=2, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['num_sancoes'], \n",
    "                     name='Total de Sanções', marker_color='mediumpurple'), row=2, col=2)\n",
    "\n",
    "fig.update_layout(height=800, showlegend=False, title_text=\"Painel de Comparação Regional\")\n",
    "fig.show()\n",
]

PT_REGIONAL_DASHBOARD_NEW = [
    "# Painel regional usando agregação ponderada pela população (ver nota acima).\n",
    "fig = make_subplots(\n",
    "    rows=2, cols=2,\n",
    "    subplot_titles=('Sanções por 100 mil por Região (ponderada)',\n",
    "                    'Taxa de Alfabetização por Região (ponderada)',\n",
    "                    'Renda Média por Região (ponderada)',\n",
    "                    'Total de Sanções por Região'),\n",
    "    specs=[[{'type': 'bar'}, {'type': 'bar'}],\n",
    "           [{'type': 'bar'}, {'type': 'bar'}]]\n",
    ")\n",
    "\n",
    "regions = (\n",
    "    df_analysis.groupby('nome_regiao')\n",
    "    .apply(lambda g: pd.Series({\n",
    "        'sancoes_por_100k_p': g['num_sancoes'].sum() / g['populacao'].sum() * 100_000,\n",
    "        'taxa_alfabetizacao_p': np.average(g['taxa_alfabetizacao_media'], weights=g['populacao']),\n",
    "        'renda_media_p': np.average(g['renda_media'], weights=g['populacao']),\n",
    "        'num_sancoes': g['num_sancoes'].sum(),\n",
    "    }))\n",
    "    .reset_index()\n",
    ")\n",
    "\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['sancoes_por_100k_p'],\n",
    "                     name='Sanções/100k', marker_color='indianred'), row=1, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['taxa_alfabetizacao_p'],\n",
    "                     name='Alfabetização %', marker_color='lightseagreen'), row=1, col=2)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['renda_media_p'],\n",
    "                     name='Renda', marker_color='lightsalmon'), row=2, col=1)\n",
    "fig.add_trace(go.Bar(x=regions['nome_regiao'], y=regions['num_sancoes'],\n",
    "                     name='Total de Sanções', marker_color='mediumpurple'), row=2, col=2)\n",
    "\n",
    "fig.update_layout(height=800, showlegend=False, title_text=\"Painel de Comparação Regional\")\n",
    "fig.show()\n",
]

PT_STATE_BAR_OLD = [
    "fig = px.bar(df_analysis.sort_values('sancoes_por_100k', ascending=False),\n",
    "             x='codigo_estado', y='sancoes_por_100k',\n",
    "             color='nome_regiao',\n",
    "             title='Sanções por 100 mil habitantes por Estado',\n",
    "             labels={'sancoes_por_100k': 'Sanções por 100 mil', 'codigo_estado': 'Estado'},\n",
    "             height=500)\n",
    "fig.show()\n",
]

PT_STATE_BAR_NEW = [
    "# Usamos nome_estado no eixo x, em vez do código de 2 dígitos (identificador qualitativo).\n",
    "# codigo_estado é um rótulo, não uma quantidade — \"11\", \"35\", ... não são informativos.\n",
    "fig = px.bar(df_analysis.sort_values('sancoes_por_100k', ascending=False),\n",
    "             x='nome_estado', y='sancoes_por_100k',\n",
    "             color='nome_regiao',\n",
    "             title='Sanções por 100 mil habitantes por Estado',\n",
    "             labels={'sancoes_por_100k': 'Sanções por 100 mil', 'nome_estado': 'Estado'},\n",
    "             height=500)\n",
    "fig.update_xaxes(tickangle=-45)\n",
    "fig.show()\n",
]


REPLACEMENTS = {
    EN_NOTEBOOK: [
        (EN_REGIONAL_SUMMARY_OLD, EN_REGIONAL_SUMMARY_NEW),
        (EN_REGIONAL_DASHBOARD_OLD, EN_REGIONAL_DASHBOARD_NEW),
        (EN_STATE_BAR_OLD, EN_STATE_BAR_NEW),
    ],
    PT_NOTEBOOK: [
        (PT_REGIONAL_SUMMARY_OLD, PT_REGIONAL_SUMMARY_NEW),
        (PT_REGIONAL_DASHBOARD_OLD, PT_REGIONAL_DASHBOARD_NEW),
        (PT_STATE_BAR_OLD, PT_STATE_BAR_NEW),
    ],
}


def apply_replacements(nb_path: Path, replacements: list) -> None:
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    cells = nb.get("cells", [])

    for old_src, new_src in replacements:
        replaced = False
        for cell in cells:
            if cell.get("cell_type") != "code":
                continue
            if cell.get("source") == old_src:
                cell["source"] = new_src
                # Outputs are stale after source change — clear to avoid confusion.
                cell["outputs"] = []
                cell["execution_count"] = None
                replaced = True
                break
        if not replaced:
            print(
                f"  [skip] No matching cell found in {nb_path.name} for cell starting with: "
                f"{old_src[0].strip()[:80]!r}"
            )
        else:
            print(f"  [ok]   Replaced cell in {nb_path.name}: {new_src[0].strip()[:80]!r}")

    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main() -> int:
    for nb_path, replacements in REPLACEMENTS.items():
        if not nb_path.exists():
            print(f"[miss] {nb_path}")
            continue
        print(f"\nPatching {nb_path.relative_to(REPO_ROOT)} ...")
        apply_replacements(nb_path, replacements)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
