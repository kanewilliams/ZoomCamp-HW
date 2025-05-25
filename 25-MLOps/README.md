# MLOps Learning Journey

## Table of Contents
- [Module01](##module01)
- [Module02](##module02)
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

**TODO**

Orchestration. Possibly Prefect?

## Module04

**TODO**

Deployment. Possibly kinesis + lambda?

## Module05

**TODO**

Observation.

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