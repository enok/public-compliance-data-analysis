"""Build the ETL pipeline notebook (00) - Bronze → Silver → Gold.

Run from project root:
    python scripts/build_etl_notebook.py

Generates:
    notebooks/00_etl_pipeline.ipynb (English)
    notebooks/00_etl_pipeline.pt-BR.ipynb (Portuguese)
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_EN = ROOT / "notebooks" / "00_etl_pipeline.ipynb"
OUT_PT = ROOT / "notebooks" / "00_etl_pipeline.pt-BR.ipynb"


def md(src: str) -> dict:
    return {"cell_type": "markdown", "metadata": {},
            "source": src.lstrip("\n").splitlines(keepends=True)}


def code(src: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {},
            "outputs": [],
            "source": src.lstrip("\n").splitlines(keepends=True)}


# -----------------------------------------------------------------------------
# English Content
# -----------------------------------------------------------------------------

TITLE_MD = r"""
# 00 — Complete ETL Pipeline: Bronze → Silver → Gold

**Purpose**: Execute the full data engineering pipeline to materialize Bronze, Silver, and Gold layer datasets from raw sources.

**Prerequisites**:
- AWS credentials configured (`aws configure --profile mba-thesis`)
- API keys in `.env` (for Bronze ingestion from APIs)
- Python dependencies installed (`pip install -r requirements.txt`)

**Outputs**:
- Bronze layer: Raw API responses and CSV extracts
- Silver layer: Normalized, typed, deduplicated datasets
- Gold layer: Analysis-ready aggregations for downstream notebooks (01-06)

---

## Architecture Overview

This notebook implements the **Medallion Architecture** (Databricks, 2023):

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw)                                             │
│  • IBGE API responses (Censo 2010, 2022)                        │
│  • Portal da Transparência (transfers, sanctions)               │
│  • IPCA/BCB deflator series                                     │
├─────────────────────────────────────────────────────────────────┤
│  SILVER LAYER (Normalized)                                      │
│  • Star schema with dimension + fact tables                     │
│  • Type enforcement, deduplication, deflation                   │
│  • 7-digit IBGE municipal code standard                         │
├─────────────────────────────────────────────────────────────────┤
│  GOLD LAYER (Analysis-Ready)                                    │
│  • Consolidated municipal socioeconomic profiles                │
│  • State-level aggregations                                     │
│  • Clustering features (normalized)                             │
│  • Analysis datasets for ML and statistics                      │
└─────────────────────────────────────────────────────────────────┘
```

**Downstream Notebooks**: After running this ETL, proceed to:
- `01_exploratory_data_analysis.ipynb` — EDA on Gold datasets
- `02_statistical_analysis.ipynb` — OLS regression
- `03_machine_learning.ipynb` — Predictive modeling
- `04_clustering_analysis.ipynb` — K-means segmentation
- `05_corruption_hdi_clusters.ipynb` — Corruption vs HDI analysis
- `06_complete_thesis_pipeline.ipynb` — Master thesis notebook (reruns ETL + all analyses)
"""

SETUP_MD = r"""
---

## 1. Environment Setup

### 1.1 Install Dependencies (if needed)
"""

SETUP_CODE = r"""
# Auto-install dependencies on first run
import subprocess
import sys
from pathlib import Path

_req = Path.cwd().parent / "requirements.txt"
if not _req.exists():
    _req = Path.cwd() / "requirements.txt"

if _req.exists():
    print(f"Installing dependencies from {_req} ...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(_req)])
    print("Dependencies ready.")
else:
    print("requirements.txt not found. Install manually: pip install -r requirements.txt")
"""

IMPORTS_MD = r"""
### 1.2 Imports and Configuration
"""

IMPORTS_CODE = r"""
import os
import sys
import json
import warnings
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path.cwd().parent))

# Project modules
from src.ingestion.ibge_client import IBGEClient
from src.ingestion.transparency_client import TransparencyClient
from src.processing.ibge_transformer import IBGETransformer
from src.processing.transparency_transformer import TransparencyTransformer
from src.processing.gold_transformer import GoldTransformer
from src.config.runtime_config import RUNTIME_CONFIG

print(f"Python path: {sys.path[0]}")
print(f"Working directory: {Path.cwd()}")
print(f"Timestamp: {datetime.now().isoformat()}")
"""

REPRODUCIBILITY_MD = r"""
### 1.3 Reproducibility Settings
"""

REPRODUCIBILITY_CODE = r"""
# Fixed seed for reproducibility
SEED = 42
os.environ["PYTHONHASHSEED"] = str(SEED)

import random
random.seed(SEED)
np.random.seed(SEED)

print(f"Reproducibility seed fixed at {SEED}")
print(f"NumPy version: {np.__version__}")
print(f"Pandas version: {pd.__version__}")
"""

BRONZE_MD = r"""
---

## 2. Bronze Layer — Raw Data Ingestion

### 2.1 Methodology

The Bronze layer preserves raw data exactly as received from sources (Armbrust et al., 2020). This ensures:
- **Auditability**: Original data can be re-examined
- **Reproducibility**: Pipeline can be replayed from raw state
- **Legal compliance**: LAI (2011) and LGPD (2018) requirements for data lineage

**Data Sources**:
- IBGE SIDRA API: Municipal demographics (Censo 2010, 2022)
- Portal da Transparência: Federal transfers and sanctions
- BCB SGS: IPCA deflator series

### 2.2 Load Runtime Configuration
"""

BRONZE_CONFIG_CODE = r"""
# Load runtime configuration
config_path = Path("../config/runtime_config.json")
if config_path.exists():
    with open(config_path) as f:
        runtime_config = json.load(f)
    print(f"Loaded runtime config from {config_path}")
else:
    runtime_config = {}
    print(f"Runtime config not found at {config_path}, using defaults")

# Extract settings
aws_profile = runtime_config.get("aws", {}).get("profile", "mba-thesis")
s3_bucket = runtime_config.get("aws", {}).get("s3_bucket_name", "enok-mba-thesis-datalake")
use_local_cache = runtime_config.get("execution", {}).get("use_local_cache", True)

print(f"AWS Profile: {aws_profile}")
print(f"S3 Bucket: {s3_bucket}")
print(f"Use Local Cache: {use_local_cache}")
"""

BRONZE_INGEST_MD = r"""
### 2.3 Execute Bronze Ingestion

**Note**: Bronze ingestion from APIs requires:
- IBGE: No API key needed (public data)
- Portal da Transparência: `TRANSPARENCY_API_KEY` in `.env`

To skip API calls and use existing local data, set:
```python
SKIP_BRONZE_INGESTION = True
```
"""

BRONZE_INGEST_CODE = r"""
# Set to True to skip API ingestion (use existing local Bronze data)
SKIP_BRONZE_INGESTION = False

if SKIP_BRONZE_INGESTION:
    print("SKIPPING Bronze ingestion — using existing local data")
    print("Ensure Bronze data exists in data/bronze/")
else:
    print("Executing Bronze layer ingestion...")
    print("\n1. IBGE Censo 2010 + 2022")
    print("2. Portal da Transparência: Federal transfers")
    print("3. Portal da Transparência: Sanctions (CEIS, CNEP, CEPIM)")
    print("4. BCB IPCA deflator series")
    
    # Import ingestion utilities
    from src.ingestion.ingestion_utils import (
        ingest_ibge_censo_2010,
        ingest_ibge_censo_2022,
        ingest_transparency_transfers,
        ingest_transparency_sanctions,
        ingest_bcb_ipca
    )
    
    # Execute ingestion (this may take 5-10 minutes)
    # Uncomment to run:
    # ingest_ibge_censo_2010()
    # ingest_ibge_censo_2022()
    # ingest_transparency_transfers()
    # ingest_transparency_sanctions()
    # ingest_bcb_ipca()
    
    print("\n[NOTE] Bronze ingestion code is commented out to prevent accidental re-runs.")
    print("Uncomment the ingestion calls above to execute.")
    print("\nFor this notebook, we assume Bronze data exists at data/bronze/")
"""

BRONZE_VERIFY_CODE = r"""
# Verify Bronze data availability
bronze_dir = Path("../data/bronze")
if bronze_dir.exists():
    bronze_files = list(bronze_dir.rglob("*.parquet")) + list(bronze_dir.rglob("*.json"))
    print(f"Bronze directory: {bronze_dir}")
    print(f"Found {len(bronze_files)} files")
    for f in sorted(bronze_files)[:10]:  # Show first 10
        print(f"  • {f.relative_to(bronze_dir)}")
    if len(bronze_files) > 10:
        print(f"  ... and {len(bronze_files)-10} more")
else:
    print(f"WARNING: Bronze directory not found: {bronze_dir}")
    print("Run Bronze ingestion scripts first:")
    print("  ./scripts/01_bronze_ingestion.sh")
"""

SILVER_MD = r"""
---

## 3. Silver Layer — Normalization and Transformation

### 3.1 Methodology

Silver layer applies:
- **Schema enforcement**: Type validation per `config/silver_schemas.json`
- **Deflation**: Nominal → Real BRL using IPCA (base 2022)
- **Deduplication**: Remove duplicates from incremental loads
- **Star schema**: Dimension tables + fact tables (Kimball & Ross, 2013)

**Transformation Pipeline**:
1. Load Bronze raw data
2. Apply type mappings and validations
3. Deflate monetary values (IPCA)
4. Standardize municipal codes (7-digit IBGE)
5. Write Silver Parquet files
"""

SILVER_TRANSFORMS_MD = r"""
### 3.2 Execute Silver Transformations
"""

SILVER_TRANSFORMS_CODE = r"""
from src.processing.base_transformer import BaseTransformer

# Initialize transformers with S3 configuration
transformers = {
    "ibge": IBGETransformer(
        bucket_name=s3_bucket,
        aws_profile=aws_profile,
        use_local_cache=use_local_cache
    ),
    "transparency": TransparencyTransformer(
        bucket_name=s3_bucket,
        aws_profile=aws_profile,
        use_local_cache=use_local_cache
    )
}

print("Silver transformers initialized:")
for name, transformer in transformers.items():
    print(f"  • {name}: {type(transformer).__name__}")

# Execute transformations
# Uncomment to run (takes 2-5 minutes):
# transformers["ibge"].transform_censo_2010()
# transformers["ibge"].transform_censo_2022()
# transformers["transparency"].transform_transfers()
# transformers["transparency"].transform_sanctions()

print("\n[NOTE] Silver transformation code is commented out to prevent accidental re-runs.")
print("Uncomment the transform calls above to execute.")
print("\nAlternatively, run the shell scripts:")
print("  ./scripts/02_silver_transformation.sh")
"""

SILVER_VERIFY_CODE = r"""
# Verify Silver data availability
silver_dir = Path("../data/silver")
if silver_dir.exists():
    silver_files = list(silver_dir.rglob("*.parquet"))
    print(f"Silver directory: {silver_dir}")
    print(f"Found {len(silver_files)} Parquet files")
    
    # Group by dataset
    datasets = {}
    for f in silver_files:
        dataset = f.parent.name
        datasets.setdefault(dataset, []).append(f)
    
    print("\nDatasets:")
    for dataset, files in sorted(datasets.items()):
        print(f"  • {dataset}: {len(files)} file(s)")
else:
    print(f"WARNING: Silver directory not found: {silver_dir}")
    print("Run Silver transformation scripts first:")
    print("  ./scripts/02_silver_transformation.sh")
"""

GOLD_MD = r"""
---

## 4. Gold Layer — Analysis-Ready Aggregations

### 4.1 Methodology

Gold layer creates denormalized, analysis-optimized datasets (Kleppmann, 2017):
- **Municipal profiles**: Socioeconomic indicators + deltas (2010→2022)
- **State summaries**: Aggregated by UF (27 states)
- **Clustering features**: Normalized vectors for K-means
- **ML datasets**: Labelled datasets for supervised learning

**Key Datasets Produced**:
| Dataset | Records | Purpose |
|---------|---------|---------|
| `agg_municipality_socioeconomic` | ~5,570 | Municipal feature vectors |
| `agg_state_summary` | 27 | State-level aggregations |
| `analysis_compliance` | 27 | ML-ready state dataset |
| `analysis_compliance_municipality` | ~5,570 | ML-ready municipal dataset |
| `consolidated_clustering` | ~5,565 | Normalized clustering features |
"""

GOLD_TRANSFORMS_MD = r"""
### 4.2 Execute Gold Transformations
"""

GOLD_TRANSFORMS_CODE = r"""
# Initialize Gold transformer
gold_transformer = GoldTransformer(
    bucket_name=s3_bucket,
    aws_profile=aws_profile,
    use_local_cache=use_local_cache
)

print(f"Gold transformer: {type(gold_transformer).__name__}")
print(f"Target bucket: {s3_bucket}")

# Execute Gold transformations
# Uncomment to run (takes 1-2 minutes):
# gold_transformer.create_municipality_socioeconomic()
# gold_transformer.create_state_summary()
# gold_transformer.create_analysis_compliance()
# gold_transformer.create_consolidated_clustering()

print("\n[NOTE] Gold transformation code is commented out to prevent accidental re-runs.")
print("Uncomment the create_* calls above to execute.")
print("\nAlternatively, run the shell script:")
print("  ./scripts/03_gold_transformation.sh")
"""

GOLD_VERIFY_CODE = r"""
# Verify Gold data availability and preview
gold_dir = Path("../data/gold")
if gold_dir.exists():
    gold_datasets = [d for d in gold_dir.iterdir() if d.is_dir()]
    print(f"Gold directory: {gold_dir}")
    print(f"Found {len(gold_datasets)} datasets:\n")
    
    for dataset_dir in sorted(gold_datasets):
        parquet_file = dataset_dir / "data.parquet"
        if parquet_file.exists():
            try:
                df = pd.read_parquet(parquet_file)
                print(f"✓ {dataset_dir.name}")
                print(f"    Rows: {len(df):,} | Columns: {len(df.columns)}")
            except Exception as e:
                print(f"✗ {dataset_dir.name}: Error reading ({e})")
        else:
            print(f"~ {dataset_dir.name}: No data.parquet found")
else:
    print(f"WARNING: Gold directory not found: {gold_dir}")
    print("Run Gold transformation script first:")
    print("  ./scripts/03_gold_transformation.sh")
"""

VALIDATION_MD = r"""
---

## 5. ETL Validation & Quality Checks

### 5.1 Row Counts Across Layers
"""

VALIDATION_CODE = r"""
def count_layer_rows(layer_path: Path) -> dict:
    # Count rows in all Parquet files in a layer
    counts = {}
    if not layer_path.exists():
        return counts
    
    for parquet_file in layer_path.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            dataset = parquet_file.parent.name
            counts[dataset] = len(df)
        except Exception:
            pass
    return counts

bronze_counts = count_layer_rows(Path("../data/bronze"))
silver_counts = count_layer_rows(Path("../data/silver"))
gold_counts = count_layer_rows(Path("../data/gold"))

print("=" * 60)
print("ETL LAYER SUMMARY")
print("=" * 60)
print(f"\nBRONZE: {len(bronze_counts)} datasets, {sum(bronze_counts.values()):,} total rows")
print(f"SILVER: {len(silver_counts)} datasets, {sum(silver_counts.values()):,} total rows")
print(f"GOLD:   {len(gold_counts)} datasets, {sum(gold_counts.values()):,} total rows")

print("\n" + "=" * 60)
print("GOLD DATASETS (Downstream Analysis Ready)")
print("=" * 60)
for dataset, count in sorted(gold_counts.items()):
    print(f"  • {dataset}: {count:,} rows")
"""

NEXT_STEPS_MD = r"""
---

## 6. Next Steps

The ETL pipeline is now complete. Gold layer datasets are ready for analysis.

### Recommended Notebook Sequence:

1. **`00_etl_pipeline.ipynb`** ← You are here (ETL complete)
2. **`01_exploratory_data_analysis.ipynb`** — EDA, distributions, quality checks
3. **`02_statistical_analysis.ipynb`** — OLS regression, correlations
4. **`03_machine_learning.ipynb`** — Predictive models (ElasticNet, Random Forest)
5. **`04_clustering_analysis.ipynb`** — K-means segmentation with PCA
6. **`05_corruption_hdi_clusters.ipynb`** — Corruption vs HDI cluster-stratified analysis
7. **`06_complete_thesis_pipeline.ipynb`** — Master notebook (reruns ETL + all analyses)

### Or run the shell scripts directly:

```bash
# Complete pipeline from scratch (requires API keys)
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# Or all at once
./scripts/run_pipeline.sh
```

### Storage Locations:
- Local: `data/bronze/`, `data/silver/`, `data/gold/`
- S3: `s3://{s3_bucket}/bronze/`, `/silver/`, `/gold/`
"""

REFS_MD = r"""
---

## References

- **Armbrust, M. et al. (2020).** Lakehouse: A New Generation of Open Platforms. Databricks.
- **Databricks (2023).** Medallion Architecture: Best Practices.
- **Kimball, R.; Ross, M. (2013).** The Data Warehouse Toolkit. 3rd ed. Wiley.
- **Kleppmann, M. (2017).** Designing Data-Intensive Applications. O'Reilly.
- **Brasil (2011).** Lei nº 12.527/2011 — Lei de Acesso à Informação.
- **Brasil (2018).** Lei nº 13.709/2018 — LGPD.
"""

# -----------------------------------------------------------------------------
# Portuguese Content
# -----------------------------------------------------------------------------

TITLE_MD_PT = r"""
# 00 — Pipeline ETL Completo: Bronze → Silver → Gold

**Propósito**: Executar o pipeline completo de engenharia de dados para materializar as camadas Bronze, Silver e Gold a partir das fontes brutas.

**Pré-requisitos**:
- Credenciais AWS configuradas (`aws configure --profile mba-thesis`)
- Chaves de API no `.env` (para ingestão Bronze das APIs)
- Dependências Python instaladas (`pip install -r requirements.txt`)

**Saídas**:
- Camada Bronze: Respostas brutas de APIs e CSVs
- Camada Silver: Datasets normalizados, tipados e deduplicados
- Camada Gold: Agregações prontas para análise nos notebooks downstream (01-06)

---

## Visão Geral da Arquitetura

Este notebook implementa a **Arquitetura Medallion** (Databricks, 2023):

```
┌─────────────────────────────────────────────────────────────────┐
│  CAMADA BRONZE (Bruto)                                          │
│  • Respostas API IBGE (Censo 2010, 2022)                       │
│  • Portal da Transparência (transferências, sanções)           │
│  • Séries deflator IPCA/BCB                                    │
├─────────────────────────────────────────────────────────────────┤
│  CAMADA SILVER (Normalizada)                                    │
│  • Star schema com tabelas de dimensão + fato                  │
│  • Validação de tipos, deduplicação, deflação                │
│  • Código municipal IBGE padronizado (7 dígitos)              │
├─────────────────────────────────────────────────────────────────┤
│  CAMADA GOLD (Pronta para Análise)                             │
│  • Perfis socioeconômicos municipais consolidados              │
│  • Agregações estaduais                                         │
│  • Features para clusterização (normalizadas)                  │
│  • Datasets de análise para ML e estatística                    │
└─────────────────────────────────────────────────────────────────┘
```

**Notebooks Downstream**: Após executar este ETL, prossiga para:
- `01_exploratory_data_analysis.ipynb` — EDA nos datasets Gold
- `02_statistical_analysis.ipynb` — Regressão OLS
- `03_machine_learning.ipynb` — Modelos preditivos
- `04_clustering_analysis.ipynb` — Segmentação K-means
- `05_corruption_hdi_clusters_pt-BR.ipynb` — Análise Corrupção vs IDH
- `06_complete_thesis_pipeline.pt-BR.ipynb` — Notebook master da tese (reexecuta ETL + todas análises)
"""

SETUP_MD_PT = r"""
---

## 1. Configuração do Ambiente

### 1.1 Instalar Dependências (se necessário)
"""

IMPORTS_MD_PT = r"""
### 1.2 Imports e Configuração
"""

REPRODUCIBILITY_MD_PT = r"""
### 1.3 Configurações de Reprodutibilidade
"""

BRONZE_MD_PT = r"""
---

## 2. Camada Bronze — Ingestão de Dados Brutos

### 2.1 Metodologia

A camada Bronze preserva dados brutos exatamente como recebidos das fontes (Armbrust et al., 2020). Isso garante:
- **Auditabilidade**: Dados originais podem ser reexaminados
- **Reprodutibilidade**: Pipeline pode ser reexecutado do estado bruto
- **Compliance legal**: Requisitos LAI (2011) e LGPD (2018) para linhagem de dados

**Fontes de Dados**:
- API SIDRA IBGE: Dados municipais (Censo 2010, 2022)
- Portal da Transparência: Transferências federais e sanções
- BCB SGS: Série deflator IPCA

### 2.2 Carregar Configuração de Runtime
"""

BRONZE_INGEST_MD_PT = r"""
### 2.3 Executar Ingestão Bronze

**Nota**: Ingestão Bronze das APIs requer:
- IBGE: Sem chave de API (dados públicos)
- Portal da Transparência: `TRANSPARENCY_API_KEY` no `.env`

Para pular chamadas de API e usar dados locais existentes, configure:
```python
SKIP_BRONZE_INGESTION = True
```
"""

SILVER_MD_PT = r"""
---

## 3. Camada Silver — Normalização e Transformação

### 3.1 Metodologia

Camada Silver aplica:
- **Schema enforcement**: Validação de tipos via `config/silver_schemas.json`
- **Deflação**: Nominal → Real BRL usando IPCA (base 2022)
- **Deduplicação**: Remove duplicatas de cargas incrementais
- **Star schema**: Tabelas de dimensão + fato (Kimball & Ross, 2013)

**Pipeline de Transformação**:
1. Carregar dados brutos Bronze
2. Aplicar mapeamentos de tipo e validações
3. Deflacionar valores monetários (IPCA)
4. Padronizar códigos municipais (IBGE 7 dígitos)
5. Escrever arquivos Parquet Silver
"""

SILVER_TRANSFORMS_MD_PT = r"""
### 3.2 Executar Transformações Silver
"""

GOLD_MD_PT = r"""
---

## 4. Camada Gold — Agregações Prontas para Análise

### 4.1 Metodologia

Camada Gold cria datasets desnormalizados e otimizados para análise (Kleppmann, 2017):
- **Perfis municipais**: Indicadores socioeconômicos + deltas (2010→2022)
- **Resumos estaduais**: Agregados por UF (27 estados)
- **Features de clusterização**: Vetores normalizados para K-means
- **Datasets de ML**: Datasets etiquetados para aprendizado supervisionado

**Principais Datasets Produzidos**:
| Dataset | Registros | Propósito |
|---------|-----------|-----------|
| `agg_municipality_socioeconomic` | ~5.570 | Vetores de features municipais |
| `agg_state_summary` | 27 | Agregações estaduais |
| `analysis_compliance` | 27 | Dataset pronto para ML estadual |
| `analysis_compliance_municipality` | ~5.570 | Dataset pronto para ML municipal |
| `consolidated_clustering` | ~5.565 | Features normalizadas para clusterização |
"""

GOLD_TRANSFORMS_MD_PT = r"""
### 4.2 Executar Transformações Gold
"""

VALIDATION_MD_PT = r"""
---

## 5. Validação ETL e Quality Checks

### 5.1 Contagens de Registros nas Camadas
"""

NEXT_STEPS_MD_PT = r"""
---

## 6. Próximos Passos

O pipeline ETL está completo. Datasets da camada Gold estão prontos para análise.

### Sequência Recomendada de Notebooks:

1. **`00_etl_pipeline.ipynb`** ← Você está aqui (ETL completo)
2. **`01_exploratory_data_analysis.ipynb`** — EDA, distribuições, quality checks
3. **`02_statistical_analysis.ipynb`** — Regressão OLS, correlações
4. **`03_machine_learning.ipynb`** — Modelos preditivos (ElasticNet, Random Forest)
5. **`04_clustering_analysis.ipynb`** — Segmentação K-means com PCA
6. **`05_corruption_hdi_clusters_pt-BR.ipynb`** — Análise Corrupção vs IDH estratificada
7. **`06_complete_thesis_pipeline.pt-BR.ipynb`** — Notebook master (reexecuta ETL + todas análises)

### Ou execute os scripts shell diretamente:

```bash
# Pipeline completo do zero (requer chaves de API)
./scripts/01_bronze_ingestion.sh
./scripts/02_silver_transformation.sh
./scripts/03_gold_transformation.sh

# Ou tudo de uma vez
./scripts/run_pipeline.sh
```

### Locais de Armazenamento:
- Local: `data/bronze/`, `data/silver/`, `data/gold/`
- S3: `s3://{s3_bucket}/bronze/`, `/silver/`, `/gold/`
"""

REFS_MD_PT = r"""
---

## Referências

- **Armbrust, M. et al. (2020).** Lakehouse: A New Generation of Open Platforms. Databricks.
- **Databricks (2023).** Medallion Architecture: Best Practices.
- **Kimball, R.; Ross, M. (2013).** The Data Warehouse Toolkit. 3rd ed. Wiley.
- **Kleppmann, M. (2017).** Designing Data-Intensive Applications. O'Reilly.
- **Brasil (2011).** Lei nº 12.527/2011 — Lei de Acesso à Informação.
- **Brasil (2018).** Lei nº 13.709/2018 — LGPD.
"""


# -----------------------------------------------------------------------------
# Build Functions
# -----------------------------------------------------------------------------

def build_cells(lang: str = "en") -> list[dict]:
    """Build notebook cells for given language ('en' or 'pt')."""
    if lang == "pt":
        return [
            md(TITLE_MD_PT),
            md(SETUP_MD_PT),        code(SETUP_CODE),
            md(IMPORTS_MD_PT),      code(IMPORTS_CODE),
            md(REPRODUCIBILITY_MD_PT), code(REPRODUCIBILITY_CODE),
            md(BRONZE_MD_PT),       code(BRONZE_CONFIG_CODE),
            md(BRONZE_INGEST_MD_PT), code(BRONZE_INGEST_CODE),
                                    code(BRONZE_VERIFY_CODE),
            md(SILVER_MD_PT),       md(SILVER_TRANSFORMS_MD_PT),
                                    code(SILVER_TRANSFORMS_CODE),
                                    code(SILVER_VERIFY_CODE),
            md(GOLD_MD_PT),         md(GOLD_TRANSFORMS_MD_PT),
                                    code(GOLD_TRANSFORMS_CODE),
                                    code(GOLD_VERIFY_CODE),
            md(VALIDATION_MD_PT),   code(VALIDATION_CODE),
            md(NEXT_STEPS_MD_PT),
            md(REFS_MD_PT),
        ]
    else:
        return [
            md(TITLE_MD),
            md(SETUP_MD),           code(SETUP_CODE),
            md(IMPORTS_MD),         code(IMPORTS_CODE),
            md(REPRODUCIBILITY_MD), code(REPRODUCIBILITY_CODE),
            md(BRONZE_MD),          code(BRONZE_CONFIG_CODE),
            md(BRONZE_INGEST_MD),   code(BRONZE_INGEST_CODE),
                                    code(BRONZE_VERIFY_CODE),
            md(SILVER_MD),          md(SILVER_TRANSFORMS_MD),
                                    code(SILVER_TRANSFORMS_CODE),
                                    code(SILVER_VERIFY_CODE),
            md(GOLD_MD),            md(GOLD_TRANSFORMS_MD),
                                    code(GOLD_TRANSFORMS_CODE),
                                    code(GOLD_VERIFY_CODE),
            md(VALIDATION_MD),      code(VALIDATION_CODE),
            md(NEXT_STEPS_MD),
            md(REFS_MD),
        ]


def write_notebook(path: Path, lang: str) -> None:
    """Write a notebook for the specified language."""
    nb = {
        "cells": build_cells(lang),
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.10.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }
    path.write_text(json.dumps(nb, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] Wrote {path} ({path.stat().st_size:,} bytes, {len(nb['cells'])} cells)")


def main() -> None:
    write_notebook(OUT_EN, lang="en")
    write_notebook(OUT_PT, lang="pt")
    print("[OK] Both ETL notebooks generated successfully.")


if __name__ == "__main__":
    main()
