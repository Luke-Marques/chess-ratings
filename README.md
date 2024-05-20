# Chess.com and FIDE Chess Ratings Cloud ETL Pipeline

## Pipeline Description

In this project, which is the culmination of my learnings from the recent Data 
Engineering Zoomcamp, I have built an orchestrated, cloud-based, end-to-end ELT pipeline
for ingesting, transforming, and visualising data from two chess ratings datasets; 
Chess.com and the International Chess Federation (FIDE).

*My goals for this project are two-fold:*

1. To learn core data engineering concepts and technologies, such as data modelling,
   data transformation, and data orchestration.
2. To answer some interesting questions about the worlds of over-the-board (OTB) and
   online chess, such as:
   - How is the gender balance of OTB chess changing across time, if at all?
   - How does the rate of change in players ratings differ between OTB and online chess?
   - How volatile are rankings of the top 10 players in OTB vs. online chess?
   - Does playing a variety of time controls improve a player's average online rating?

It would be interesting to ask questions directly comparing FIDE players OTB ratings
to their Chess.com accounts, but this would require matching players by names across
the two datasets. Whilst this is trivial for the larger, more well-known players - who
tend to have verified Chess.com accounts - it is more difficult for the vast majority
of players who do not have verified accounts. This is a task for future work.

## Tech & Tools 🛠️

*The pipeline is built using the following technologies:*

- Python 3
- Poetry (Dependency management)
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

## Graphical Project Overview 📊

![pipeline_flow_chart](https://raw.githubusercontent.com/Luke-Marques/chess-ratings/dev/images/project_flow_chart.png)

## Prefect Flow Example

Below is an example of a completed flow which ingests data from the Chess.com API,
transforms the data into a format suitable for loading into BigQuery, loads the
data into BigQuery, and calls `dbt` to rerun all downstream models reliant on this data.
The flow is scheduled to run every 24 hours.

![prefect_flow_example](https://raw.githubusercontent.com/Luke-Marques/chess-ratings/dev/images/prefect_cdc_profiles_flow_screenshot.png)

## Github Actions CI/CD Example

Below is an example of a custom Github workflow which deploys required Prefect Cloud
blocks, and the Docker container `Dockerfile_Flows` to the Google Artifact Registry to
be used in Cloud Run Jobs. It then deploys all main flows to Prefect Cloud, enabling
them to be manually called or scheduled.

![github_actions_example](https://raw.githubusercontent.com/Luke-Marques/chess-ratings/dev/images/github_actions_screenshot.png)
