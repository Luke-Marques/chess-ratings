# Chess.com and FIDE Chess Ratings Cloud ETL Pipeline

## Tech & Tools 🛠️

- Python 3
- Polars (Rust-based DataFrame library for data manipulation and cleaning)
- Prefect (Task Orchestration and scheduling)
- Docker (Containerization of compute jobs on GCP)
- Google Cloud Plaform
    - Google Cloud Storage (Datalake)
    - Google BigQuery (Data Warehouse)
    - Google Virtual Machines (Always-on compute for Prefect Agent)
    - Google Cloud Run Jobs (Serverless compute for Prefect flows)
    - Google Looker (BI tool for data visualization) **(Future)**
- Github Actions (CI/CD)
- Terraform (Infrastructure as Code for GCP resources)
- SQL & dbt (Data Modelling and Transformation)

## Graphical Project Overview

![pipeline_flow_chart](https://raw.githubusercontent.com/Luke-Marques/chess-ratings/dev/images/project_flow_chart.png)
