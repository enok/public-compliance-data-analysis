# Tests

Automated test suites for ingestion, processing, and analysis modules.

Portuguese version: [README.pt-BR.md](README.pt-BR.md)

---

## Layout

```text
tests/
├── ingestion/
│   ├── test_bronze_config_validation.py
│   ├── test_bronze_runner_imports.py
│   ├── test_ibge_client.py
│   ├── test_ibge_ingestion.py
│   ├── test_ibge_metadata.py
│   └── test_transparency_ingestion.py
├── processing/
│   ├── test_gold_transformer.py
│   ├── test_silver_config_validation.py
│   ├── test_silver_integration.py
│   ├── test_smart_caching.py
│   └── test_transformers.py
└── analysis/
    └── test_data_loader.py
```

---

## Run Tests

### Full suite

```bash
pytest tests/ -v
```

### By area

```bash
pytest tests/ingestion/ -v
pytest tests/processing/ -v
pytest tests/analysis/ -v
```

### Selected modules

```bash
pytest tests/processing/test_transformers.py -v
pytest tests/processing/test_smart_caching.py -v
pytest tests/processing/test_gold_transformer.py -v
```

### Collection count snapshot

```bash
pytest --collect-only -q tests
```

Current snapshot (2026-04-07): `117 tests collected`.

---

## Coverage

```bash
pytest tests/ --cov=src --cov-report=term-missing
pytest tests/ --cov=src --cov-report=html
```

---

## Notes

- some ingestion/integration tests require AWS credentials and S3 access
- test behavior may vary depending on local environment variables and profile setup
