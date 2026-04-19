"""
Silver Layer Data Quality Audit Script

Validates Silver layer data consistency, completeness, and quality.
Generates a comprehensive audit report for thesis documentation.

Usage:
    python scripts/audit_silver.py
"""

import os
import sys
import json
import logging
import io
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BUCKET_NAME = "enok-mba-thesis-datalake"
OUTPUT_REPORT = Path(__file__).parent.parent / "docs" / "silver_coverage_report.md"


class SilverLayerAuditor:
    """Auditor for Silver layer data quality and consistency."""

    def __init__(self, bucket_name: str):
        aws_profile = os.getenv("AWS_PROFILE", "mba-thesis")
        session = boto3.Session(profile_name=aws_profile)
        self.s3 = session.client('s3')
        self.bucket = bucket_name
        self.results = {
            'audit_timestamp': datetime.now().isoformat(),
            'tables': {},
            'summary': {},
            'issues': []
        }

    def audit_all(self) -> Dict[str, Any]:
        """Run complete audit of Silver layer."""
        logger.info("🔍 Starting Silver layer audit...")

        tables = [
            ('dim_municipalities', 'silver/dim_municipalities/data.parquet'),
            ('fact_population', 'silver/fact_population/data.parquet'),
            ('fact_sanitation', 'silver/fact_sanitation/data.parquet'),
            ('fact_literacy', 'silver/fact_literacy/data.parquet'),
            ('fact_income', 'silver/fact_income/data.parquet'),
            ('fact_federal_transfers', 'silver/fact_federal_transfers/data.parquet'),
            ('fact_sanctions', 'silver/fact_sanctions/data.parquet')
        ]

        for table_name, s3_key in tables:
            logger.info(f"📊 Auditing {table_name}...")
            self._audit_table(table_name, s3_key)

        self._generate_summary()
        return self.results

    def _audit_table(self, table_name: str, s3_key: str):
        """Audit a single Silver table."""
        result = {
            'exists': False,
            'record_count': 0,
            'file_size_mb': 0,
            'columns': [],
            'metadata_exists': False,
            'quality_checks': {}
        }

        try:
            df = self._read_parquet(s3_key)
            if df is None:
                result['exists'] = False
                self.results['tables'][table_name] = result
                self.results['issues'].append(f"❌ {table_name}: File not found")
                return

            result['exists'] = True
            result['record_count'] = len(df)
            result['columns'] = list(df.columns)

            obj = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            result['file_size_mb'] = round(obj['ContentLength'] / (1024 * 1024), 2)

            metadata_key = s3_key.replace('data.parquet', '_metadata.json')
            result['metadata_exists'] = self._check_file_exists(metadata_key)

            if table_name == 'dim_municipalities':
                result['quality_checks'] = self._check_municipalities(df)
            elif table_name.startswith('fact_'):
                result['quality_checks'] = self._check_fact_table(df, table_name)

            self.results['tables'][table_name] = result

        except Exception as e:
            logger.error(f"❌ Error auditing {table_name}: {e}")
            result['error'] = str(e)
            self.results['tables'][table_name] = result
            self.results['issues'].append(f"❌ {table_name}: {str(e)}")

    def _read_parquet(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Read Parquet file from S3."""
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            body_bytes = obj['Body'].read()
            return pd.read_parquet(io.BytesIO(body_bytes))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise

    def _check_file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            return False

    def _check_municipalities(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Quality checks for municipalities dimension table."""
        checks = {}

        checks['total_municipalities'] = len(df)
        checks['unique_codes'] = df['municipality_code'].nunique()
        checks['has_duplicates'] = checks['total_municipalities'] != checks['unique_codes']

        checks['null_codes'] = int(df['municipality_code'].isna().sum())
        checks['null_names'] = int(df['municipality_name'].isna().sum())
        checks['null_states'] = int(df['state_code'].isna().sum())

        checks['unique_states'] = int(df['state_code'].nunique())
        checks['unique_regions'] = int(df['region_code'].nunique())

        checks['municipalities_per_state'] = df.groupby('state_code').size().to_dict()

        if checks['has_duplicates']:
            self.results['issues'].append(
                f"⚠️ dim_municipalities: {checks['total_municipalities'] - checks['unique_codes']} duplicate codes"
            )

        if checks['null_codes'] > 0:
            self.results['issues'].append(
                f"⚠️ dim_municipalities: {checks['null_codes']} null municipality codes"
            )

        return checks

    def _check_fact_table(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Quality checks for fact tables."""
        checks = {}

        checks['total_records'] = len(df)

        if 'municipality_code' in df.columns:
            checks['null_municipality_codes'] = int(df['municipality_code'].isna().sum())
            checks['unique_municipalities'] = int(df['municipality_code'].nunique())
            checks['municipality_code_coverage_pct'] = round(
                (1 - checks['null_municipality_codes'] / len(df)) * 100, 2
            ) if len(df) > 0 else 0

        if 'year' in df.columns:
            checks['years'] = sorted(df['year'].dropna().unique().tolist())
            checks['records_per_year'] = df['year'].value_counts().to_dict()

        if 'registry_type' in df.columns:
            checks['registry_types'] = df['registry_type'].value_counts().to_dict()

        if 'entity_type' in df.columns:
            checks['entity_types'] = df['entity_type'].value_counts().to_dict()

        null_counts = df.isna().sum()
        checks['columns_with_nulls'] = {
            col: int(count) for col, count in null_counts.items() if count > 0
        }

        duplicates = len(df) - len(df.drop_duplicates())
        if duplicates > 0:
            checks['duplicate_rows'] = duplicates
            self.results['issues'].append(
                f"⚠️ {table_name}: {duplicates} duplicate rows found"
            )

        return checks

    def _generate_summary(self):
        """Generate audit summary."""
        summary = {
            'total_tables': len(self.results['tables']),
            'tables_exist': sum(1 for t in self.results['tables'].values() if t['exists']),
            'tables_missing': sum(1 for t in self.results['tables'].values() if not t['exists']),
            'total_records': sum(t['record_count'] for t in self.results['tables'].values()),
            'total_size_mb': round(sum(t['file_size_mb'] for t in self.results['tables'].values()), 2),
            'tables_with_metadata': sum(1 for t in self.results['tables'].values() if t.get('metadata_exists', False)),
            'total_issues': len(self.results['issues'])
        }

        summary['coverage_pct'] = round(
            (summary['tables_exist'] / summary['total_tables']) * 100, 2
        ) if summary['total_tables'] > 0 else 0

        self.results['summary'] = summary

    def generate_report(self, output_path: Path):
        """Generate markdown audit report."""
        logger.info(f"📝 Generating audit report: {output_path}")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Silver Layer Data Quality Audit Report\n\n")
            f.write(f"**Generated:** {self.results['audit_timestamp']}\n\n")
            f.write("---\n\n")

            f.write("## Executive Summary\n\n")
            summary = self.results['summary']
            f.write(f"- **Total Tables:** {summary['total_tables']}\n")
            f.write(f"- **Tables Exist:** {summary['tables_exist']} / {summary['total_tables']}\n")
            f.write(f"- **Coverage:** {summary['coverage_pct']}%\n")
            f.write(f"- **Total Records:** {summary['total_records']:,}\n")
            f.write(f"- **Total Size:** {summary['total_size_mb']} MB\n")
            f.write(f"- **Tables with Metadata:** {summary['tables_with_metadata']}\n")
            f.write(f"- **Issues Found:** {summary['total_issues']}\n\n")

            if summary['tables_missing'] > 0:
                f.write(f"⚠️ **{summary['tables_missing']} table(s) missing**\n\n")

            f.write("---\n\n")

            f.write("## Table Details\n\n")
            for table_name, details in self.results['tables'].items():
                f.write(f"### {table_name}\n\n")

                if not details['exists']:
                    f.write("❌ **Status:** Not Found\n\n")
                    continue

                f.write(f"✅ **Status:** Exists\n\n")
                f.write(f"- **Records:** {details['record_count']:,}\n")
                f.write(f"- **Size:** {details['file_size_mb']} MB\n")
                f.write(f"- **Columns:** {len(details['columns'])}\n")
                f.write(f"- **Metadata:** {'✅ Yes' if details['metadata_exists'] else '❌ No'}\n\n")

                if details['columns']:
                    f.write("**Schema:**\n")
                    for col in details['columns']:
                        f.write(f"- `{col}`\n")
                    f.write("\n")

                if details['quality_checks']:
                    f.write("**Quality Checks:**\n\n")
                    checks = details['quality_checks']

                    if table_name == 'dim_municipalities':
                        f.write(f"- Total Municipalities: {checks.get('total_municipalities', 0):,}\n")
                        f.write(f"- Unique Codes: {checks.get('unique_codes', 0):,}\n")
                        f.write(f"- Duplicates: {'⚠️ Yes' if checks.get('has_duplicates') else '✅ No'}\n")
                        f.write(f"- Null Codes: {checks.get('null_codes', 0)}\n")
                        f.write(f"- Unique States: {checks.get('unique_states', 0)}\n")
                        f.write(f"- Unique Regions: {checks.get('unique_regions', 0)}\n\n")

                        if 'municipalities_per_state' in checks:
                            f.write("**Municipalities per State (Top 10):**\n\n")
                            state_counts = sorted(
                                checks['municipalities_per_state'].items(),
                                key=lambda x: x[1],
                                reverse=True
                            )[:10]
                            for state, count in state_counts:
                                f.write(f"- State {state}: {count:,}\n")
                            f.write("\n")

                    else:
                        f.write(f"- Total Records: {checks.get('total_records', 0):,}\n")

                        if 'years' in checks:
                            f.write(f"- Years: {', '.join(map(str, checks['years']))}\n")
                            if 'records_per_year' in checks:
                                f.write("- Records per Year:\n")
                                for year, count in sorted(checks['records_per_year'].items()):
                                    f.write(f"  - {year}: {count:,}\n")

                        if 'unique_municipalities' in checks:
                            f.write(f"- Unique Municipalities: {checks['unique_municipalities']:,}\n")
                            f.write(f"- Municipality Code Coverage: {checks.get('municipality_code_coverage_pct', 0)}%\n")

                        if 'registry_types' in checks:
                            f.write("- Registry Types:\n")
                            for reg_type, count in checks['registry_types'].items():
                                f.write(f"  - {reg_type}: {count:,}\n")

                        if 'entity_types' in checks:
                            f.write("- Entity Types:\n")
                            for ent_type, count in checks['entity_types'].items():
                                f.write(f"  - {ent_type}: {count:,}\n")

                        if 'columns_with_nulls' in checks and checks['columns_with_nulls']:
                            f.write("- Columns with Nulls:\n")
                            for col, count in list(checks['columns_with_nulls'].items())[:10]:
                                f.write(f"  - `{col}`: {count:,}\n")

                        f.write("\n")

            f.write("---\n\n")

            if self.results['issues']:
                f.write("## Issues & Warnings\n\n")
                for issue in self.results['issues']:
                    f.write(f"{issue}\n\n")
            else:
                f.write("## Issues & Warnings\n\n")
                f.write("✅ No issues found!\n\n")

            f.write("---\n\n")
            f.write("**End of Report**\n")

        logger.info(f"✅ Report generated: {output_path}")


def main():
    """Main execution."""
    try:
        auditor = SilverLayerAuditor(BUCKET_NAME)
        results = auditor.audit_all()

        auditor.generate_report(OUTPUT_REPORT)

        summary = results['summary']
        logger.info("=" * 60)
        logger.info("SILVER LAYER AUDIT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Tables Exist: {summary['tables_exist']} / {summary['total_tables']}")
        logger.info(f"Coverage: {summary['coverage_pct']}%")
        logger.info(f"Total Records: {summary['total_records']:,}")
        logger.info(f"Total Size: {summary['total_size_mb']} MB")
        logger.info(f"Issues: {summary['total_issues']}")
        logger.info("=" * 60)

        if summary['coverage_pct'] == 100 and summary['total_issues'] == 0:
            logger.info("✅ Silver layer is 100% complete with no issues!")
            return 0
        elif summary['coverage_pct'] == 100:
            logger.warning(f"⚠️ Silver layer is complete but has {summary['total_issues']} issue(s)")
            return 0
        else:
            logger.error(f"❌ Silver layer is incomplete: {summary['coverage_pct']}% coverage")
            return 1

    except Exception as e:
        logger.error(f"❌ Audit failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
