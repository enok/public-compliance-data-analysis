#!/usr/bin/env python3
"""
Analysis: Correlation between Corruption/Poor Resource Use vs HDI by Cluster

This script executes the complete analysis using local data (no S3 required).
"""

import os
import sys
import json
import warnings
warnings.filterwarnings('ignore')

from pathlib import Path

# Configure paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Libraries
import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import pearsonr, spearmanr

# Configure reproducibility
SEED = 42
np.random.seed(SEED)

print("="*80)
print("ANALYSIS: CORRUPTION/POOR USE vs HDI BY CLUSTER")
print("="*80)
print()

# ============================================================================
# 1. LOAD LOCAL DATA
# ============================================================================
print("1. Loading local data...")
print()

data_dir = project_root / "data" / "gold"

# Main dataset: municipal compliance analysis
df_city = pd.read_parquet(data_dir / "analysis_compliance_municipality" / "data.parquet")
print(f"   [OK] Municipal compliance: {len(df_city):,} municipalities")

# Clustering dataset
df_cluster = pd.read_parquet(data_dir / "consolidated_clustering" / "data.parquet")
print(f"   [OK] Clustering: {len(df_cluster):,} municipalities")

# Check if clustering columns exist, if not perform clustering
if 'cluster' not in df_cluster.columns:
    print("   [!] Cluster assignments not found, performing K-means clustering...")
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    
    # Get normalized feature columns
    norm_cols = [c for c in df_cluster.columns if c.endswith('_norm')]
    if len(norm_cols) == 0:
        # Use raw features if no normalized columns
        norm_cols = ['population_2022', 'literacy_rate_2022', 'avg_income_real_2022_2022_brl', 
                     'households_2022', 'population_change_pct', 'literacy_change_pp',
                     'income_change_real_pct', 'households_change_pct']
    
    # Select only rows with complete data
    cluster_data = df_cluster[norm_cols].dropna()
    valid_idx = cluster_data.index
    
    # Perform K-means clustering
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(cluster_data)
    
    # Assign clusters back to dataframe
    df_cluster.loc[valid_idx, 'cluster'] = cluster_labels
    
    # Perform PCA for visualization
    pca = PCA(n_components=3)
    pca_result = pca.fit_transform(cluster_data)
    df_cluster.loc[valid_idx, 'PC1'] = pca_result[:, 0]
    df_cluster.loc[valid_idx, 'PC2'] = pca_result[:, 1]
    df_cluster.loc[valid_idx, 'PC3'] = pca_result[:, 2]
    
    print(f"   [OK] Clustering complete: {len(valid_idx)} municipalities assigned")

# Merge datasets
print()
print("   Merging datasets...")
merge_cols = ['municipality_code', 'cluster']
if 'PC1' in df_cluster.columns:
    merge_cols.extend(['PC1', 'PC2', 'PC3'])

df = df_city.merge(
    df_cluster[merge_cols], 
    on='municipality_code', 
    how='left'
)
print(f"   [OK] Merge completed: {len(df):,} municipalities")

# Check municipalities without cluster
no_cluster = df['cluster'].isna().sum()
if no_cluster > 0:
    print(f"   [!] Municipalities without cluster: {no_cluster}")

# ============================================================================
# 2. DEFINE ANALYSIS VARIABLES
# ============================================================================
print()
print("2. Preparing analysis variables...")

CORRUPTION_VAR = 'sanctions_per_million_brl_transfers'
IDH_VARS = [
    'avg_income_real_2022_2022_brl',      # Income (HDI proxy)
    'literacy_rate_2022',                  # Literacy
    'avg_income_2022',                     # Nominal income
]

# Create flag for municipalities with valid sanctions data
df['has_sanctions_data'] = (
    df[CORRUPTION_VAR].notna() & 
    (df[CORRUPTION_VAR] >= 0) &
    df['total_transfers'].notna() &
    (df['total_transfers'] > 0)
)

print(f"   [OK] With sanctions data: {df['has_sanctions_data'].sum():,}")
print(f"   [OK] Without data: {(~df['has_sanctions_data']).sum():,}")

# ============================================================================
# 3. CORRELATION ANALYSIS BY CLUSTER
# ============================================================================
print()
print("3. Calculating correlations by cluster...")
print()

def calculate_correlation_by_cluster(df, cluster_id, corr_var, idh_var):
    """Calculate correlation between corr_var and idh_var for a specific cluster."""
    
    subset = df[
        (df['cluster'] == cluster_id) & 
        df['has_sanctions_data'] &
        df[corr_var].notna() & 
        df[idh_var].notna()
    ].copy()
    
    n = len(subset)
    
    if n < 10:
        return {
            'cluster': cluster_id,
            'n_municipios': n,
            'pearson_r': None,
            'pearson_p': None,
            'significance': 'N/A',
            'interpretation': 'Insufficient sample'
        }
    
    # Remove extreme outliers (>3 standard deviations)
    z_scores = np.abs(stats.zscore(subset[corr_var]))
    subset_clean = subset[z_scores < 3]
    
    n_clean = len(subset_clean)
    
    if n_clean < 10:
        return {
            'cluster': cluster_id,
            'n_municipios': n_clean,
            'pearson_r': None,
            'pearson_p': None,
            'significance': 'N/A',
            'interpretation': 'Insufficient clean data'
        }
    
    # Calculate correlation
    pearson_r, pearson_p = pearsonr(subset_clean[corr_var], subset_clean[idh_var])
    
    # Interpretation
    if pearson_p < 0.001:
        significance = '***'
    elif pearson_p < 0.01:
        significance = '**'
    elif pearson_p < 0.05:
        significance = '*'
    else:
        significance = 'ns'
    
    return {
        'cluster': cluster_id,
        'n_municipios': n_clean,
        'pearson_r': round(pearson_r, 4),
        'pearson_p': round(pearson_p, 6),
        'significance': significance,
        'interpretation': f"r={pearson_r:.3f}{significance}"
    }

clusters = sorted(df['cluster'].dropna().unique())
results = []

for cluster_id in clusters:
    for idh_var in IDH_VARS:
        result = calculate_correlation_by_cluster(df, cluster_id, CORRUPTION_VAR, idh_var)
        result['idh_var'] = idh_var
        results.append(result)

df_results = pd.DataFrame(results)

print("="*80)
print("RESULTS: Correlation Sanctions/Transfer vs HDI by Cluster")
print("="*80)
print()

# Pivot for visualization
pivot_table = df_results.pivot_table(
    index=['cluster', 'n_municipios'], 
    columns='idh_var', 
    values='interpretation',
    aggfunc='first'
)

print(pivot_table)

# ============================================================================
# 4. MUNICIPAL VULNERABILITY INDEX
# ============================================================================
print()
print("4. Calculating municipal vulnerability index...")
print()

def calculate_vulnerability_index(row):
    """
    Combined index: (normalized sanctions) - (normalized HDI)
    Higher = more vulnerable (high corruption, low development)
    """
    
    if pd.isna(row[CORRUPTION_VAR]) or row[CORRUPTION_VAR] < 0:
        return None
    
    # Use log to reduce skewness
    sanctions_log = np.log1p(row[CORRUPTION_VAR])
    
    # HDI component
    income_norm = row['avg_income_real_2022_2022_brl'] if pd.notna(row['avg_income_real_2022_2022_brl']) else 0
    literacy_norm = row['literacy_rate_2022'] if pd.notna(row['literacy_rate_2022']) else 0
    
    # HDI score (0-1 approximate)
    idh_score = (income_norm / 5000 + literacy_norm / 100) / 2
    idh_score = min(max(idh_score, 0), 1)
    
    # Vulnerability
    vulnerability = sanctions_log - (idh_score * 5)
    
    return vulnerability

df['vulnerability_index'] = df.apply(calculate_vulnerability_index, axis=1)

print("Vulnerability Index Statistics:")
print(df['vulnerability_index'].describe())
print()

print("Distribution by Cluster:")
print(df.groupby('cluster')['vulnerability_index'].describe().round(3))

# ============================================================================
# 5. REPRESENTATIVE SAMPLE SELECTION
# ============================================================================
print()
print("5. Selecting representative sample...")
print()

TOP_N = 5
representative_sample = []

for cluster_id in sorted(df['cluster'].dropna().unique()):
    cluster_df = df[df['cluster'] == cluster_id].copy()
    
    # Best (lowest vulnerability = blue)
    best = cluster_df.nsmallest(TOP_N, 'vulnerability_index')
    best['category'] = 'Best Management'
    best['rank_in_cluster'] = range(1, len(best) + 1)
    
    # Worst (highest vulnerability = red)
    worst = cluster_df.nlargest(TOP_N, 'vulnerability_index')
    worst['category'] = 'High Vulnerability'
    worst['rank_in_cluster'] = range(1, len(worst) + 1)
    
    representative_sample.extend([best, worst])

df_sample = pd.concat(representative_sample, ignore_index=True)

print(f"Sample selected: {len(df_sample)} municipalities")
print(f"  - {len(df_sample[df_sample['category'] == 'Best Management'])} best")
print(f"  - {len(df_sample[df_sample['category'] == 'High Vulnerability'])} worst")

# ============================================================================
# 6. EXPORT RESULTS
# ============================================================================
print()
print("6. Exporting results...")
print()

output_dir = project_root / "docs" / "thesis_presentation_assets"
output_dir.mkdir(parents=True, exist_ok=True)

# 1. Correlation table
correlations_export = df_results[
    ['cluster', 'n_municipios', 'idh_var', 'pearson_r', 'pearson_p', 'significance', 'interpretation']
].copy()
correlations_export.to_csv(output_dir / "correlation_by_cluster.csv", index=False)
print(f"   [OK] {output_dir / 'correlation_by_cluster.csv'}")

# 2. Representative sample
sample_export = df_sample[
    ['municipality_code', 'municipality_name', 'state_code', 'state_name', 
     'cluster', 'category', 'rank_in_cluster', 'vulnerability_index',
     'sanctions_per_million_brl_transfers', 'avg_income_real_2022_2022_brl', 
     'literacy_rate_2022', 'total_transfers', 'n_sanctions']
].copy()
sample_export.to_csv(output_dir / "representative_sample_cities.csv", index=False)
print(f"   [OK] {output_dir / 'representative_sample_cities.csv'}")

# Also save Portuguese version
sample_export_pt = sample_export.copy()
sample_export_pt.columns = ['municipality_code', 'municipality_name', 'state_code', 'state_name',
                          'cluster', 'categoria', 'ranking_no_cluster', 'vulnerability_index',
                          'sanctions_per_million_brl_transfers', 'avg_income_real_2022_2022_brl',
                          'literacy_rate_2022', 'total_transfers', 'n_sanctions']
sample_export_pt.to_csv(output_dir / "amostra_representativa_cidades.csv", index=False)
print(f"   [OK] {output_dir / 'amostra_representativa_cidades.csv'}")

# 3. Cluster summary
cluster_summary = df.groupby('cluster').agg({
    'municipality_code': 'count',
    'vulnerability_index': ['mean', 'std', 'min', 'max'],
    'sanctions_per_million_brl_transfers': ['mean', 'median'],
    'avg_income_real_2022_2022_brl': 'mean',
    'literacy_rate_2022': 'mean'
}).round(3)
cluster_summary.to_csv(output_dir / "cluster_summary.csv")
cluster_summary.to_csv(output_dir / "resumo_por_cluster.csv")
print(f"   [OK] {output_dir / 'cluster_summary.csv'}")
print(f"   [OK] {output_dir / 'resumo_por_cluster.csv'}")

# 4. Sample in JSON format for easy consumption
sample_json = output_dir / "representative_sample.json"
with open(sample_json, 'w', encoding='utf-8') as f:
    json.dump(df_sample.to_dict('records'), f, ensure_ascii=False, indent=2)
print(f"   [OK] {sample_json}")

# Also save Portuguese version
sample_json_pt = output_dir / "amostra_representativa.json"
with open(sample_json_pt, 'w', encoding='utf-8') as f:
    json.dump(df_sample.to_dict('records'), f, ensure_ascii=False, indent=2)
print(f"   [OK] {sample_json_pt}")

# ============================================================================
# 7. EXECUTIVE SUMMARY
# ============================================================================
print()
print("="*80)
print("EXECUTIVE SUMMARY")
print("="*80)
print()

print(f"[DATA] Data Analyzed:")
print(f"   - Total municipalities: {len(df):,}")
print(f"   - With sanctions data: {df['has_sanctions_data'].sum():,}")
print(f"   - Clusters defined: {len(clusters)}")
print()

print(f"[STATS] Significant Correlations (p < 0.05):")
sig_corrs = df_results[df_results['pearson_p'] < 0.05]
if len(sig_corrs) > 0:
    for _, row in sig_corrs.iterrows():
        print(f"   - Cluster {int(row['cluster'])} vs {row['idh_var']}: r={row['pearson_r']:.3f} {row['significance']}")
else:
    print("   - No significant correlations detected in clusters")
    print("   - This suggests the effect is heterogeneous or of low magnitude")
print()

print(f"[SAMPLE] Representative Sample:")
print(f"   - {TOP_N} best municipalities per cluster = {len(df_sample[df_sample['category'] == 'Best Management'])} total")
print(f"   - {TOP_N} worst municipalities per cluster = {len(df_sample[df_sample['category'] == 'High Vulnerability'])} total")
print()

print("[MAP] Next Step:")
print("   Run 'gerar_geojson_mapa.py' or 'generate_map_geojson.py' to create GeoJSON for QGIS")
print()

print("="*80)
print("ANALYSIS COMPLETED SUCCESSFULLY!")
print("="*80)
