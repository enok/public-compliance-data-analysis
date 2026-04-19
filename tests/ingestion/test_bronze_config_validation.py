import json
from pathlib import Path
import pytest


def test_transparency_config_matches_actual_data():
    """Verify federal transfers configuration is internally consistent.

    The project uses a date range (mesAnoInicio/mesAnoFim) which is expanded into
    monthly files by the ingestion logic.
    """

    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    federal_transfers = [d for d in config['datasets'] if d['name'].startswith('federal_transfers')]
    assert len(federal_transfers) > 0, "No federal_transfers datasets configured"

    # All configured entries must include mesAnoInicio/mesAnoFim and they must be valid mm/YYYY.
    for d in federal_transfers:
        params = d.get('params', {})
        assert 'mesAnoInicio' in params and 'mesAnoFim' in params, f"Missing date range params for {d.get('name')}"
        mm_start, yy_start = params['mesAnoInicio'].split('/')
        mm_end, yy_end = params['mesAnoFim'].split('/')
        assert 1 <= int(mm_start) <= 12
        assert 1 <= int(mm_end) <= 12
        assert len(yy_start) == 4 and len(yy_end) == 4


def test_no_orphaned_config_entries():
    """Verify config does not contain obviously invalid monthly ranges."""

    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    federal_transfers = [d for d in config['datasets'] if d['name'].startswith('federal_transfers')]
    for d in federal_transfers:
        params = d.get('params', {})
        mm_start, yy_start = params['mesAnoInicio'].split('/')
        mm_end, yy_end = params['mesAnoFim'].split('/')
        start_year, start_month = int(yy_start), int(mm_start)
        end_year, end_month = int(yy_end), int(mm_end)
        assert (start_year, start_month) <= (end_year, end_month), f"Invalid range for {d.get('name')}"


def test_federal_transfers_cover_full_intercensal_window():
    """Verify federal transfers cover the full 2010-2022 census comparison window."""

    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    federal_transfers = next(d for d in config['datasets'] if d['name'] == 'federal_transfers')
    params = federal_transfers['params']

    assert params['mesAnoInicio'] == '01/2010'
    assert params['mesAnoFim'] == '12/2022'


def test_config_coverage_summary():
    """Generate coverage summary for documentation."""

    config_path = Path(__file__).resolve().parents[2] / "config" / "transparency_metadata.json"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    federal_transfers = [d for d in config['datasets'] if d['name'].startswith('federal_transfers')]
    
    print(f"\n=== Bronze Config Coverage ===")
    print(f"Federal transfers datasets: {len(federal_transfers)}")
    
    for dataset in federal_transfers:
        params = dataset['params']
        print(f"  - {dataset['name']}: {params['mesAnoInicio']} to {params['mesAnoFim']}")
    
    assert len(federal_transfers) >= 1, "Expected at least one federal_transfers dataset entry"
