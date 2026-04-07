"""
Metadata Consistency Validation Script

Validates that all metadata configurations match the actual code implementation.
Checks for consistency across Bronze, Silver, and Gold layers.
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class MetadataValidator:
    """Validates metadata consistency across all layers."""

    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.issues = []
        self.warnings = []

    def validate_all(self) -> bool:
        """Run all validation checks."""
        print("Validating metadata consistency...\n")

        self.validate_ibge_metadata()
        self.validate_transparency_metadata()
        self.validate_silver_schemas()
        self.validate_metadata_key_consistency()

        return self.report_results()

    def validate_ibge_metadata(self):
        """Validate IBGE metadata configuration."""
        print("Validating IBGE metadata...")

        config_path = self.project_root / "config" / "ibge_metadata.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        expected_datasets = [
            'pop_2010', 'pop_2022',
            'sanitation_2010', 'sanitation_2022',
            'literacy_2010', 'literacy_2022',
            'income_2010', 'income_2022',
            'ipca_monthly',
        ]

        dataset_names = [d['name'] for d in config['datasets']]

        for expected in expected_datasets:
            if expected not in dataset_names:
                self.issues.append(f"IBGE: Missing dataset '{expected}' in metadata")

        for dataset in config['datasets']:
            if not dataset.get('filename'):
                self.issues.append(f"IBGE: Dataset '{dataset['name']}' missing filename")
            elif dataset['name'] != 'ipca_monthly' and not dataset['filename'].startswith('census_'):
                self.warnings.append(f"IBGE: Dataset '{dataset['name']}' filename doesn't follow 'census_' pattern")

        print(f"  OK - Found {len(config['datasets'])} IBGE datasets\n")

    def validate_transparency_metadata(self):
        """Validate Transparency Portal metadata configuration."""
        print("Validating Transparency metadata...")

        config_path = self.project_root / "config" / "transparency_metadata.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        federal_transfers = [d for d in config['datasets'] if d['name'].startswith('federal_transfers')]
        sanctions = [d for d in config['datasets'] if 'sanctions' in d['name']]

        print(f"  - Federal transfers: {len(federal_transfers)} datasets")
        print(f"  - Sanctions: {len(sanctions)} datasets")

        for dataset in federal_transfers:
            filename = dataset.get('filename', '')
            if not filename.endswith('.json'):
                self.issues.append(f"Transparency: Dataset '{dataset['name']}' filename missing .json extension")

            params = dataset.get('params', {})
            uses_monthly_expansion = (
                dataset.get('name') == 'federal_transfers'
                and params.get('mesAnoInicio')
                and params.get('mesAnoFim')
                and filename == 'federal_transfers.json'
            )
            if not uses_monthly_expansion and not re.match(r'federal_transfers_\d{4}_\d{2}\.json', filename):
                self.warnings.append(
                    f"Transparency: Dataset '{dataset['name']}' filename '{filename}' "
                    f"doesn't match pattern 'federal_transfers_YYYY_MM.json'"
                )

        expected_sanctions = ['ceis', 'cnep', 'cepim']
        sanction_types = [d['name'].split('_')[0] for d in sanctions]

        for expected in expected_sanctions:
            if expected not in sanction_types:
                self.issues.append(f"Transparency: Missing sanctions dataset for '{expected}'")

        print(f"  OK - Validated {len(config['datasets'])} Transparency datasets\n")

    def validate_silver_schemas(self):
        """Validate Silver schemas configuration."""
        print("Validating Silver schemas...")

        config_path = self.project_root / "config" / "silver_schemas.json"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        required_schemas = [
            'municipalities',
            'census_population',
            'census_sanitation',
            'census_literacy',
            'census_income',
            'inflation_index',
            'federal_transfers',
            'compliance_sanctions'
        ]

        schemas = config.get('schemas', {})

        for required in required_schemas:
            if required not in schemas:
                self.issues.append(f"Silver: Missing schema '{required}'")
            else:
                schema = schemas[required]
                if 'columns' not in schema:
                    self.issues.append(f"Silver: Schema '{required}' missing columns definition")
                if 'primary_key' not in schema and not required.startswith('gold_'):
                    self.warnings.append(f"Silver: Schema '{required}' missing primary_key definition")

        print(f"  OK - Validated {len(schemas)} Silver schemas\n")

    def validate_metadata_key_consistency(self):
        """Validate metadata key naming consistency across transformers."""
        print("Validating metadata key consistency...")

        transformer_files = [
            self.project_root / "src" / "processing" / "ibge_transformer.py",
            self.project_root / "src" / "processing" / "transparency_transformer.py",
            self.project_root / "src" / "processing" / "gold_transformer.py"
        ]

        dot_prefix_count = 0
        underscore_prefix_count = 0

        for file_path in transformer_files:
            if not file_path.exists():
                continue

            content = file_path.read_text(encoding='utf-8')

            dot_matches = re.findall(r"metadata_key\s*=\s*['\"].*?/\.metadata\.json['\"]", content)
            underscore_matches = re.findall(r"metadata_key\s*=\s*['\"].*?/_metadata\.json['\"]", content)

            dot_prefix_count += len(dot_matches)
            underscore_prefix_count += len(underscore_matches)

            if dot_matches:
                for match in dot_matches:
                    self.issues.append(
                        f"Inconsistent metadata key in {file_path.name}: "
                        f"Uses '.metadata.json' instead of '_metadata.json'"
                    )

        if dot_prefix_count > 0 and underscore_prefix_count > 0:
            self.issues.append(
                f"Mixed metadata key prefixes: {dot_prefix_count} use '.metadata.json', "
                f"{underscore_prefix_count} use '_metadata.json'"
            )
        elif underscore_prefix_count > 0:
            print(f"  OK - All {underscore_prefix_count} metadata keys use consistent '_metadata.json' pattern\n")
        else:
            print("  OK - No metadata keys found (or all use consistent pattern)\n")

    def report_results(self) -> bool:
        """Report validation results."""
        print("=" * 60)
        print("VALIDATION RESULTS")
        print("=" * 60)

        if not self.issues and not self.warnings:
            print("SUCCESS: All metadata configurations are consistent!")
            print("\nNo issues or warnings found.")
            return True

        if self.issues:
            print(f"\nISSUES FOUND: {len(self.issues)}")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")

        if self.warnings:
            print(f"\nWARNINGS: {len(self.warnings)}")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")

        print("\n" + "=" * 60)

        if self.issues:
            print("FAILED: Validation failed - please fix the issues above")
            return False
        else:
            print("SUCCESS: Validation passed with warnings")
            return True


def main():
    """Main execution."""
    validator = MetadataValidator()
    success = validator.validate_all()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
