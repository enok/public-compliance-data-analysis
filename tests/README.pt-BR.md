# Testes

Suítes automatizadas para módulos de ingestão, processamento e análise.

Versão em inglês: [README.md](README.md)

---

## Estrutura

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

## Execução dos Testes

### Suíte completa

```bash
pytest tests/ -v
```

### Por área

```bash
pytest tests/ingestion/ -v
pytest tests/processing/ -v
pytest tests/analysis/ -v
```

### Módulos específicos

```bash
pytest tests/processing/test_transformers.py -v
pytest tests/processing/test_smart_caching.py -v
pytest tests/processing/test_gold_transformer.py -v
```

### Snapshot de contagem

```bash
pytest --collect-only -q tests
```

Snapshot atual (2026-04-19): `140 tests collected`, `137 passed, 3 skipped`.

---

## Cobertura

```bash
pytest tests/ --cov=src --cov-report=term-missing
pytest tests/ --cov=src --cov-report=html
```

---

## Observações

- parte dos testes de ingestão/integração requer credenciais AWS e acesso ao S3
- o comportamento dos testes pode variar conforme variáveis de ambiente e perfil local
