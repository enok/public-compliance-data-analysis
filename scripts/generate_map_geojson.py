#!/usr/bin/env python3
"""
Generate municipal GeoJSON with vulnerability index for QGIS mapping.

Input: Local Gold layer data
Output: GeoJSON ready for QGIS (red=high vulnerability, blue=good management)
"""

import os
import sys
import json
import zipfile
import tempfile
import shutil
import warnings
warnings.filterwarnings('ignore')

from pathlib import Path

import numpy as np
import pandas as pd

print("="*80)
print("GENERATING GEOJSON FOR QGIS")
print("="*80)
print()

# Configure paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ============================================================================
# 1. LOAD DATA
# ============================================================================
print("1. Loading data...")
print()

data_dir = project_root / "data" / "gold"
qgis_dir = project_root / "docs" / "thesis_presentation_assets" / "qgis"
qgis_dir.mkdir(parents=True, exist_ok=True)

# Analysis data
df_city = pd.read_parquet(data_dir / "analysis_compliance_municipality" / "data.parquet")
df_cluster = pd.read_parquet(data_dir / "consolidated_clustering" / "data.parquet")

# Check if clustering columns exist
if 'cluster' not in df_cluster.columns:
    print("   [!] Cluster assignments not found, performing K-means clustering...")
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    
    norm_cols = [c for c in df_cluster.columns if c.endswith('_norm')]
    if len(norm_cols) == 0:
        norm_cols = ['population_2022', 'literacy_rate_2022', 'avg_income_real_2022_2022_brl', 
                     'households_2022', 'population_change_pct', 'literacy_change_pp',
                     'income_change_real_pct', 'households_change_pct']
    
    cluster_data = df_cluster[norm_cols].dropna()
    valid_idx = cluster_data.index
    
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(cluster_data)
    df_cluster.loc[valid_idx, 'cluster'] = cluster_labels
    
    pca = PCA(n_components=3)
    pca_result = pca.fit_transform(cluster_data)
    df_cluster.loc[valid_idx, 'PC1'] = pca_result[:, 0]
    df_cluster.loc[valid_idx, 'PC2'] = pca_result[:, 1]
    df_cluster.loc[valid_idx, 'PC3'] = pca_result[:, 2]
    
    print(f"   [OK] Clustering complete: {len(valid_idx)} municipalities assigned")

merge_cols = ['municipality_code', 'cluster']
if 'PC1' in df_cluster.columns:
    merge_cols.extend(['PC1', 'PC2', 'PC3'])

df = df_city.merge(df_cluster[merge_cols], on='municipality_code', how='left')

print(f"   [OK] Data loaded: {len(df):,} municipalities")

# Calculate vulnerability index
CORRUPTION_VAR = 'sanctions_per_million_brl_transfers'

def calculate_vulnerability_index(row):
    if pd.isna(row[CORRUPTION_VAR]) or row[CORRUPTION_VAR] < 0:
        return None
    
    sanctions_log = np.log1p(row[CORRUPTION_VAR])
    income_norm = row['avg_income_real_2022_2022_brl'] if pd.notna(row['avg_income_real_2022_2022_brl']) else 0
    literacy_norm = row['literacy_rate_2022'] if pd.notna(row['literacy_rate_2022']) else 0
    
    idh_score = (income_norm / 5000 + literacy_norm / 100) / 2
    idh_score = min(max(idh_score, 0), 1)
    
    vulnerability = sanctions_log - (idh_score * 5)
    return vulnerability

df['vulnerability_index'] = df.apply(calculate_vulnerability_index, axis=1)

# ============================================================================
# 2. LOAD MUNICIPALITY CENTROIDS
# ============================================================================
print()
print("2. Loading municipality centroids...")
print()

try:
    import shapefile
except ImportError as exc:
    raise ImportError("pyshp is required. Install with: pip install pyshp>=2.3.1") from exc

municipality_zip_path = qgis_dir / "BR_Municipios_2022.zip"

if not municipality_zip_path.exists():
    print(f"   [ERROR] Shapefile not found: {municipality_zip_path}")
    print("   Download from: https://geoftp.ibge.gov.br/...")
    print("   Or run notebook 04_clustering_analysis.ipynb first")
    sys.exit(1)

print(f"   Reading: {municipality_zip_path}")

_tmp_dir = tempfile.mkdtemp(prefix="ibge_muni_shape_")
with zipfile.ZipFile(municipality_zip_path) as _zip_file:
    _zip_file.extractall(_tmp_dir)

_shp_path = next(Path(_tmp_dir).glob("*.shp"))
_reader = shapefile.Reader(str(_shp_path))
_fields = [field[0] for field in _reader.fields[1:]]
_code_idx = _fields.index("CD_MUN")

centroids = []
for _shape_record in _reader.iterShapeRecords():
    municipality_code = str(_shape_record.record[_code_idx]).zfill(7)
    xmin, ymin, xmax, ymax = _shape_record.shape.bbox
    centroids.append({
        "municipality_code": municipality_code,
        "lon": (xmin + xmax) / 2.0,
        "lat": (ymin + ymax) / 2.0,
    })

_reader.close()

df_centroids = pd.DataFrame(centroids)
print(f"   [OK] Centroids loaded: {len(df_centroids):,}")

df_geo = df.merge(df_centroids, on='municipality_code', how='inner')
print(f"   [OK] Municipalities with data + coordinates: {len(df_geo):,}")

# ============================================================================
# 3. CREATE VULNERABILITY CATEGORIES
# ============================================================================
print()
print("3. Categorizing vulnerability...")
print()

def categorize_vulnerability(v):
    if pd.isna(v):
        return 'No data'
    elif v < -3:
        return 'Very Low (Blue)'
    elif v < -1:
        return 'Low (Light Blue)'
    elif v < 1:
        return 'Neutral (Yellow)'
    elif v < 3:
        return 'High (Orange)'
    else:
        return 'Very High (Red)'

df_geo['vulnerability_category'] = df_geo['vulnerability_index'].apply(categorize_vulnerability)

print("Category distribution:")
for cat, count in df_geo['vulnerability_category'].value_counts().items():
    print(f"   - {cat}: {count} ({count/len(df_geo)*100:.1f}%)")

# ============================================================================
# 4. GENERATE GEOJSON
# ============================================================================
print()
print("4. Generating GeoJSON...")
print()

features = []

for _, row in df_geo.iterrows():
    if pd.isna(row['lon']) or pd.isna(row['lat']):
        continue
    
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(row['lon']), float(row['lat'])]
        },
        "properties": {
            "municipality_code": str(row['municipality_code']),
            "municipality_name": str(row['municipality_name']),
            "state_code": str(row['state_code']) if pd.notna(row['state_code']) else None,
            "state_name": str(row['state_name']) if pd.notna(row['state_name']) else None,
            "cluster": int(row['cluster']) if pd.notna(row['cluster']) else None,
            "vulnerability_index": float(row['vulnerability_index']) if pd.notna(row['vulnerability_index']) else None,
            "vulnerability_category": str(row['vulnerability_category']),
            "sanctions_per_million": float(row['sanctions_per_million_brl_transfers']) if pd.notna(row['sanctions_per_million_brl_transfers']) else 0,
            "avg_income_2022": float(row['avg_income_real_2022_2022_brl']) if pd.notna(row['avg_income_real_2022_2022_brl']) else None,
            "literacy_rate_2022": float(row['literacy_rate_2022']) if pd.notna(row['literacy_rate_2022']) else None,
            "total_transfers": float(row['total_transfers']) if pd.notna(row['total_transfers']) else 0,
            "n_sanctions": int(row['n_sanctions']) if pd.notna(row['n_sanctions']) else 0,
        }
    }
    features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "name": "Municipalities_Vulnerability_Index",
    "crs": {
        "type": "name",
        "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        }
    },
    "features": features
}

output_path = qgis_dir / "brazil_municipalities_vulnerability_index.geojson"
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print(f"   [OK] GeoJSON saved: {output_path}")
print(f"   [OK] Total features: {len(features)}")

# ============================================================================
# 5. STATE STATISTICS
# ============================================================================
print()
print("5. State statistics...")
print()

state_stats = df_geo.groupby('state_name').agg({
    'vulnerability_index': ['mean', 'count'],
    'sanctions_per_million_brl_transfers': 'mean',
    'avg_income_real_2022_2022_brl': 'mean'
}).round(3)

print("Top 10 states with highest average vulnerability:")
top_states = state_stats.sort_values(('vulnerability_index', 'mean'), ascending=False).head(10)
for state, row in top_states.iterrows():
    print(f"   - {state}: index={row[('vulnerability_index', 'mean')]:.3f}, n={int(row[('vulnerability_index', 'count')])}")

# ============================================================================
# 6. SUMMARY
# ============================================================================
print()
print("="*80)
print("SUMMARY")
print("="*80)
print()

print(f"[OK] GeoJSON generated with {len(features)} municipalities")
print(f"[OK] Colors for QGIS:")
print(f"  - Red (Very High): High relative corruption + Low HDI")
print(f"  - Orange (High): Moderate relative corruption")
print(f"  - Yellow (Neutral): Neutral")
print(f"  - Light Blue (Low): Good management")
print(f"  - Blue (Very Low): Excellent management + High HDI")
print()
print(f"[OK] File: {output_path}")
print()
print("="*80)
print("NEXT STEP: Open the file in QGIS and apply graduated symbology")
print("           by the 'vulnerability_index' field")
print("="*80)

# Cleanup temp directory
try:
    shutil.rmtree(_tmp_dir)
except:
    pass
