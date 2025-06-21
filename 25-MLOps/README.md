# MLOps Learning Journey

## Table of Contents
- [Module01 - Introduction](##module01)
- [Module02 - Experiment Tracking](##module02)
- [Module03 - Orchestration](##module03)
- [Module04 - Deployment](##module04)
- [Module05 - Monitoring](##module05)
- [Django Practice](##django-practice)

## Module01:

This module explores a basic linear regression. I expanded on it by exploring both:

- [uv](https://docs.astral.sh/uv/) - for python depedency management
- [marimo](https://marimo.io) - as a Jupyter Notebook alternative

Make sure you have uv installed (`pip install uv`), then:

```bash
cd Module01
uv run marimo edit MLOps_Homework01.py
```

### Alternate viewing options

It may just be easier to just view the [html file](/25-MLOps/Module01/__marimo__/MLOps_Homework01.html) :-) 

(Or even easier, [view the deployment here!!](https://static.marimo.app/static/mlops-homework01-kane-williams-cvfn) But who knows how long it will be available for..)


## Module02

Module02 uses:

- [MLflow](https://mlflow.org) - for its model registry + experiment tracker

[Link](https://github.com/DataTalksClub/mlops-zoomcamp/blob/main/cohorts/2024) to the homework questions.

### Notes

Q2 - To process the data, run:

```bash
cd Module02

python3 ./Module02/preprocess_data.py --raw_data_path ./data/green/ --dest_path ./data_preprocessed/
```

Q3 - Start with `mlflow ui` in the Module02 directory

Q4 - Run the following commands 

```bash
mkdir artifacts

mlflow server \
    --backend-store-uri sqlite:///mlflow.db \
    --default-artifact-root ./artifacts \
    --host 0.0.0.0 \
    --port 5000
```

## Module03

This module will use:

- [polars](https://pola.rs) - a modern pandas alternative

- [Prefect](https://www.prefect.io) - as the orchestrator

- [Claude CODE](https://docs.anthropic.com/en/docs/claude-code/overview) - as an experiment


I used **Mage** in the DE Zoomcamp, so out of the "main" alternatives (Airflow/Prefect/Dagster) went with **Prefect**.

Why? 

Recall that I want to eventually forecast horse race winners:

1) Airflow appeared too clunky+heavy duty for project, and
2) Dagster's asset-based approach is not as natural (says Claude) for horse-race forecasting compared with Prefect's task-based approach.

Let's try it and see!

---

#### Results:

- Claude CODE is cool but can easily go out of control.
- Did not get enough polars experience as I would like, but have a good understanding of tasks and flows in prefect now.

I would need to set everything up properly for the actual project.

## Module04

This module focuses on **batch inference deployment** using Docker containers.

[Full homework results and documentation](Module04/Module04.md)

### Key Learning Topics:
- Converting Jupyter notebooks to production scripts
- Docker containerization for ML deployment
- Batch processing vs. online inference
- Virtual environment management with pipenv
- Scikit-learn version compatibility issues

### Technologies Used:
- **Docker**: Containerization and deployment
- **pipenv**: Virtual environment with lock files
- **scikit-learn**: ML model (version compatibility critical)
- **NYC Taxi Dataset**: Real-world batch inference scenario

### Main Achievement:
Successfully deployed a batch inference pipeline that processes 3.4M taxi trip records and generates predictions in a containerized environment, demonstrating the importance of exact dependency matching between training and inference environments.

## Module05

This module implements production-grade ML monitoring using:

- **Evidently AI** - Statistical drift detection and data quality metrics
- **PostgreSQL** - Time-series storage for monitoring metrics  
- **Grafana** - Visualization dashboards and alerting

[Full homework results and documentation](Module05/homework_answers.md)

### Key Learning Topics:
- Data drift detection (ColumnDriftMetric, DatasetDriftMetric)
- Custom metric implementation (ColumnQuantileMetric)
- Time-series monitoring data storage
- Production monitoring dashboard design
- ML-specific statistical testing

## Django Practice

A bit unrelated to the above, nevertheless, an experimental Django project for exploring database query optimization and analysis.

Has:
- SQLite database integration
- Query performance analysis using EXPLAIN QUERY PLAN

SQLite uses `EXPLAIN QUERY PLAN` to analyze query execution (SQlite version of `EXPLAIN ANALYSE`):

Example usage:
```sql
EXPLAIN QUERY PLAN SELECT * FROM your_table WHERE condition;
```