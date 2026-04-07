#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold Layer Audit Script
Validates gold layer outputs in S3 and performs data quality checks.
"""
import boto3
import pandas as pd
from pathlib import Path
import sys
from io import BytesIO

# Fix Windows console encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BUCKET_NAME = 'enok-mba-thesis-datalake'
GOLD_PREFIX = 'gold/'
AWS_PROFILE = 'mba-thesis'

def format_size(bytes_size):
    """Format bytes to human readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"

def audit_s3_structure():
    """Audit S3 gold layer structure."""
    print("=" * 80)
    print("GOLD LAYER S3 STRUCTURE AUDIT")
    print("=" * 80)
    print()
    
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client('s3')
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=GOLD_PREFIX)
        
        if 'Contents' not in response:
            print(f"❌ No objects found in s3://{BUCKET_NAME}/{GOLD_PREFIX}")
            return False
        
        objects = response['Contents']
        total_size = sum(obj['Size'] for obj in objects)
        
        print(f"📦 Bucket: s3://{BUCKET_NAME}/{GOLD_PREFIX}")
        print(f"📊 Total Objects: {len(objects)}")
        print(f"💾 Total Size: {format_size(total_size)}")
        print()
        
        # Group by dataset
        datasets = {}
        for obj in objects:
            key = obj['Key']
            parts = key.split('/')
            if len(parts) >= 2:
                dataset = parts[1]
                if dataset not in datasets:
                    datasets[dataset] = []
                datasets[dataset].append(obj)
        
        print("📁 Gold Layer Datasets:")
        print("-" * 80)
        for dataset, files in sorted(datasets.items()):
            dataset_size = sum(f['Size'] for f in files)
            print(f"\n  {dataset}/")
            print(f"    Files: {len(files)}")
            print(f"    Size: {format_size(dataset_size)}")
            print(f"    Last Modified: {max(f['LastModified'] for f in files)}")
            for f in files:
                filename = f['Key'].split('/')[-1]
                print(f"      - {filename} ({format_size(f['Size'])})")
        
        print()
        return True
        
    except Exception as e:
        print(f"❌ Error accessing S3: {e}")
        return False

def load_parquet_from_s3(s3_path):
    """Load parquet file from S3."""
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client('s3')
    parts = s3_path.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def audit_data_quality():
    """Perform data quality checks on gold layer datasets."""
    print("=" * 80)
    print("GOLD LAYER DATA QUALITY AUDIT")
    print("=" * 80)
    print()
    
    datasets = {
        'agg_municipality_socioeconomic': 'gold/agg_municipality_socioeconomic/data.parquet',
        'agg_state_summary': 'gold/agg_state_summary/data.parquet',
        'agg_sanctions_summary': 'gold/agg_sanctions_summary/data.parquet',
        'analysis_compliance': 'gold/analysis_compliance/data.parquet'
    }
    
    results = {}
    
    for name, path in datasets.items():
        print(f"🔍 Auditing: {name}")
        print("-" * 80)
        
        try:
            s3_path = f"s3://{BUCKET_NAME}/{path}"
            df = load_parquet_from_s3(s3_path)
            
            print(f"  ✓ Successfully loaded from {path}")
            print(f"  📊 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            print(f"  💾 Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            print()
            
            print(f"  📋 Columns ({len(df.columns)}):")
            for col in df.columns:
                dtype = df[col].dtype
                null_count = df[col].isnull().sum()
                null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0
                print(f"    - {col:40s} {str(dtype):15s} (nulls: {null_count:5d} / {null_pct:5.1f}%)")
            print()
            
            # Dataset-specific checks
            if name == 'agg_municipality_socioeconomic':
                print("  🎯 Municipality Socioeconomic Checks:")
                print(f"    - Unique municipalities: {df['municipality_code'].nunique():,}")
                print(f"    - States covered: {df['state_code'].nunique()}")
                
                # Check for key metrics
                key_metrics = ['population_2010', 'population_2022', 'population_change_pct', 
                              'literacy_rate_2010', 'literacy_rate_2022', 'avg_income_2010', 'avg_income_2022',
                              'avg_income_real_2010_2022_brl', 'avg_income_real_2022_2022_brl',
                              'income_change_real_pct']
                for metric in key_metrics:
                    if metric in df.columns:
                        valid = df[metric].notna().sum()
                        print(f"    - {metric}: {valid:,} valid values")
                
            elif name == 'agg_state_summary':
                print("  🎯 State Summary Checks:")
                print(f"    - States: {df['state_code'].nunique()}")
                print(f"    - Total population 2022: {df['total_population_2022'].sum():,.0f}")
                print(f"    - Total sanctions: {df['total_sanctions'].sum():,.0f}")
                if 'sanctions_per_100k' in df.columns:
                    valid_sanctions = df['sanctions_per_100k'].notna().sum()
                    if valid_sanctions > 0:
                        print(f"    - Avg sanctions per 100k: {df['sanctions_per_100k'].mean():.2f}")
                    else:
                        print(f"    - Sanctions per 100k: No valid data (all states have 0 sanctions)")
                
            elif name == 'agg_sanctions_summary':
                print("  🎯 Sanctions Summary Checks:")
                if 'registry_type' in df.columns:
                    print(f"    - Registry types: {df['registry_type'].tolist()}")
                if 'total_sanctions' in df.columns:
                    print(f"    - Total sanctions: {df['total_sanctions'].sum():,.0f}")
                
            elif name == 'analysis_compliance':
                print("  🎯 Analysis Compliance Checks:")
                print(f"    - Analysis units: {len(df)}")
                if 'state_code' in df.columns:
                    print(f"    - States: {df['state_code'].nunique()}")
                
                # Check for analysis-ready columns
                analysis_cols = ['n_sanctions', 'sanctions_per_100k', 
                               'avg_income', 'avg_literacy_rate']
                for col in analysis_cols:
                    if col in df.columns:
                        print(f"    - {col}: min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}")
            
            print()
            
            # Sample data
            print("  📄 Sample Data (first 3 rows):")
            print(df.head(3).to_string(index=False))
            print()
            print()
            
            results[name] = {
                'status': 'SUCCESS',
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            }
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            print()
            results[name] = {
                'status': 'FAILED',
                'error': str(e)
            }
    
    return results

def generate_summary(results):
    """Generate audit summary."""
    print("=" * 80)
    print("AUDIT SUMMARY")
    print("=" * 80)
    print()
    
    total_datasets = len(results)
    successful = sum(1 for r in results.values() if r['status'] == 'SUCCESS')
    failed = total_datasets - successful
    
    print(f"📊 Total Datasets: {total_datasets}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print()
    
    if successful > 0:
        total_rows = sum(r['rows'] for r in results.values() if r['status'] == 'SUCCESS')
        total_size = sum(r['size_mb'] for r in results.values() if r['status'] == 'SUCCESS')
        print(f"📈 Total Records: {total_rows:,}")
        print(f"💾 Total Size: {total_size:.2f} MB")
        print()
    
    print("Dataset Status:")
    print("-" * 80)
    for name, result in results.items():
        status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
        print(f"  {status_icon} {name:40s} {result['status']}")
        if result['status'] == 'SUCCESS':
            print(f"      Rows: {result['rows']:,} | Columns: {result['columns']} | Size: {result['size_mb']:.2f} MB")
    
    print()
    return failed == 0

def main():
    """Main audit function."""
    print()
    print("🔍 Starting Gold Layer Audit...")
    print()
    
    # Audit S3 structure
    s3_ok = audit_s3_structure()
    print()
    
    # Audit data quality
    results = audit_data_quality()
    
    # Generate summary
    all_ok = generate_summary(results)
    
    if s3_ok and all_ok:
        print("✅ Gold layer audit completed successfully!")
        return 0
    else:
        print("⚠️  Gold layer audit completed with issues.")
        return 1

if __name__ == '__main__':
    sys.exit(main())
