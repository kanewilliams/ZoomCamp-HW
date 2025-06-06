# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Architecture

This is an MLOps learning repository structured around different modules from the MLOps Zoomcamp, with each module exploring different aspects of machine learning operations:

- **Module01**: Introduction with basic linear regression using marimo notebooks
- **Module02**: Experiment tracking with MLflow, featuring taxi trip duration prediction
- **Module03**: Orchestration with Prefect for horse racing data pipelines  
- **Module04**: Deployment (TODO - possibly Kinesis + Lambda)
- **Module05**: Monitoring/Observation (TODO)
- **django_practice**: Database query optimization experiments

### Key Technologies

- **uv**: Python dependency management (preferred over pip/conda)
- **marimo**: Interactive notebook alternative to Jupyter
- **MLflow**: Experiment tracking and model registry
- **Prefect**: Workflow orchestration
- **polars**: Modern pandas alternative for data processing
- **Django**: Web framework for database experimentation

## Common Development Commands

### Environment Setup
```bash
# Install dependencies using uv
uv sync

# Run any Python script with dependencies
uv run python script.py
```

### Module01 - Marimo Notebooks
```bash
cd Module01
uv run marimo edit MLOps_Homework01.py
```

### Module02 - MLflow Experiment Tracking
```bash
cd Module02

# Preprocess data
python preprocess_data.py --raw_data_path ./data/green/ --dest_path ./data_preprocessed/

# Start MLflow UI
mlflow ui

# Start MLflow server with artifacts
mlflow server \
    --backend-store-uri sqlite:///mlflow.db \
    --default-artifact-root ./artifacts \
    --host 0.0.0.0 \
    --port 5000

# Train models
python train.py --data_path ../data_preprocessed

# Hyperparameter optimization
python hpo.py

# Register models
python register_model.py
```

### Module03 - Prefect Orchestration
```bash
cd Module03

# Setup Prefect environment
python setup_prefect.py

# Run data scraping pipeline
python scrape.py

# Grab/process data
python grab_data.py
```

### Django Practice
```bash
cd django_practice

# Run Django server
python manage.py runserver

# Analyze database queries
python manage.py analyze_queries
```

## Data Pipeline Architecture

The repository follows a structured ML pipeline approach:

1. **Data Ingestion** (Module03): Web scraping with Prefect orchestration
2. **Data Preprocessing** (Module02): Feature engineering and data cleaning
3. **Experiment Tracking** (Module02): MLflow for model versioning and metrics
4. **Model Training** (Module02): Scikit-learn with hyperparameter optimization
5. **Model Registry** (Module02): MLflow model storage and versioning

### Data Flow
- Raw data stored in `data/` and `Module03/raw_data/`
- Preprocessed data in `data_preprocessed/` with pickle format
- MLflow artifacts in `Module02/artifacts/` and `Module02/mlruns/`

## Project Dependencies

Dependencies are managed via `pyproject.toml` with uv. Key packages:
- MLflow 2.22.0 (experiment tracking)
- Prefect 3.4.4+ (orchestration)
- scikit-learn 1.6.1+ (ML models)
- polars 1.30.0+ (data processing)
- marimo 0.13.10+ (notebooks)

## Development Notes

- Use `uv run` prefix for all Python commands to ensure proper dependency management
- MLflow tracking requires server setup before running experiments
- Prefect flows should be deployed after running setup script
- Django project includes custom management commands for query analysis