# Project Overview

## Purpose

This repository supports an MBA thesis focused on public spending efficiency, compliance risk, and anomaly detection in Brazil.

The project combines:

- federal transfer data
- socioeconomic census indicators
- sanctions and compliance registries
- a medallion data pipeline on AWS
- bilingual analysis and documentation surfaces

## Technical Scope

- `config/`: dataset contracts and metadata definitions
- `src/ingestion/`: Bronze ingestion clients and helpers
- `src/processing/`: Silver and Gold transformations
- `src/analysis/`: notebook-facing data access helpers, including localized loaders
- `scripts/`: the implemented Bronze, Silver, and Gold orchestration entry points
- `dags/`: currently not the primary orchestration surface; do not assume MWAA DAG code exists here
- `infra/`: Terraform for the S3-backed data lake foundation and supporting AWS setup
- `notebooks/`: bilingual EDA, statistical analysis, machine learning, and clustering notebooks
- `tests/`: pytest suites for ingestion, processing, and analysis
- `docs/`: technical and research documentation

## Working Assumptions

- English is the default language for LLM configuration and technical guidance.
- Dataset metadata files in `config/` are part of the system contract and must be updated carefully.
- Bronze, Silver, and Gold boundaries are intentional and should not be blurred by convenience changes.
- This repository mixes engineering, analytics, and research documentation, so changes often require code and docs updates together.
- Repo code currently implements Python scripts plus S3-oriented data flows; references to MWAA or SageMaker in docs should be treated as adjacent context unless backed by committed code or infrastructure.

## Less Relevant Shared Rules

The shared toolkit includes rules for technologies that are not primary in this repository. Do not spend context on them unless the task truly needs them:

- frontend-specific guidance
- Java-specific guidance
- general JS/TS guidance outside shell tooling or documentation helpers
