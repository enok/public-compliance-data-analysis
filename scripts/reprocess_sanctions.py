"""
Re-process sanctions data with state extraction from agency names.

This script:
1. Deletes existing Silver layer sanctions data
2. Re-runs the Transparency transformer with state extraction logic
3. Re-runs the Gold layer transformer to aggregate by state
"""

import os
import sys
import boto3
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing.transparency_transformer import TransparencyTransformer
from src.processing.gold_transformer import GoldTransformer

# AWS Configuration
BUCKET_NAME = "enok-mba-thesis-datalake"
AWS_PROFILE = "mba-thesis"
CONFIG_FILE = project_root / "config" / "silver_schemas.json"

print("=" * 80)
print("SANCTIONS DATA REPROCESSING WITH STATE EXTRACTION")
print("=" * 80)

# Set up AWS session with profile
os.environ['AWS_PROFILE'] = AWS_PROFILE
session = boto3.Session(profile_name=AWS_PROFILE)
s3 = session.client('s3')

print(f"\nUsing AWS profile: {AWS_PROFILE}")
print(f"Bucket: {BUCKET_NAME}")

# Step 1: Delete existing Silver sanctions data to force reprocessing
print("\n" + "=" * 80)
print("STEP 1: Clearing existing Silver sanctions data")
print("=" * 80)

silver_keys_to_delete = [
    'silver/fact_sanctions/data.parquet',
    'silver/fact_sanctions/_metadata.json'
]

for key in silver_keys_to_delete:
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"  Deleted: {key}")
    except Exception as e:
        print(f"  Skip (not found): {key}")

# Step 2: Re-run Silver layer transformation
print("\n" + "=" * 80)
print("STEP 2: Re-running Silver layer transformation (Transparency)")
print("=" * 80)

transformer = TransparencyTransformer(BUCKET_NAME, str(CONFIG_FILE))
success = transformer._transform_sanctions()

if not success:
    print("\nERROR: Silver layer transformation failed!")
    sys.exit(1)

print("\nSilver layer transformation completed successfully!")

# Step 3: Delete existing Gold layer data to force reprocessing
print("\n" + "=" * 80)
print("STEP 3: Clearing existing Gold layer data")
print("=" * 80)

gold_keys_to_delete = [
    'gold/agg_state_summary/data.parquet',
    'gold/agg_state_summary/data.json',
    'gold/agg_state_summary/_metadata.json',
    'gold/analysis_compliance/data.parquet',
    'gold/analysis_compliance/data.json',
    'gold/analysis_compliance/_metadata.json'
]

for key in gold_keys_to_delete:
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"  Deleted: {key}")
    except Exception as e:
        print(f"  Skip (not found): {key}")

# Step 4: Re-run Gold layer transformation
print("\n" + "=" * 80)
print("STEP 4: Re-running Gold layer transformation")
print("=" * 80)

gold_transformer = GoldTransformer(BUCKET_NAME, str(CONFIG_FILE))
success = gold_transformer.transform()

if not success:
    print("\nERROR: Gold layer transformation failed!")
    sys.exit(1)

print("\n" + "=" * 80)
print("SUCCESS: Sanctions data reprocessed with state extraction!")
print("=" * 80)
print("\nNext steps:")
print("1. Verify sanctions are distributed across states")
print("2. Re-run statistical analysis notebook")
print("3. Check correlation heatmap for non-NaN values")
