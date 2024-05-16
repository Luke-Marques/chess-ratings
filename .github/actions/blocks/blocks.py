"""
python blocks.py -b $GITHUB_REF_NAME -r "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
-n ${{ inputs.block_name }} -i ${{ inputs.image_uri }} --region ${{ inputs.region }}
"""

import argparse

from prefect.filesystems import GitHub
from prefect_gcp.cloud_run import CloudRunJob
from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import DbtCliProfile, BigQueryTargetConfigs

# parse command line arguments
REPO = "https://github.com/Luke-Marques/chess-ratings"
parser = argparse.ArgumentParser()
parser.add_argument("-b", "--branch", default="main")
parser.add_argument("-r", "--repo", default=REPO)
parser.add_argument("-n", "--block-name", default="default")
parser.add_argument("-g", "--gcp-creds-block-name", default="default")
parser.add_argument("-p", "--bucket-name", default="default")
parser.add_argument("-i", "--image")
parser.add_argument("--region", default="europe-west1")
args = parser.parse_args()

# load GCP credentials block
gcp_credentials = GcpCredentials.load(args.gcp_creds_block_name)

# create GitHub block and save to Prefect Cloud
github_block = GitHub(repository=args.repo, reference=args.branch)
github_block.save(args.block_name, overwrite=True)

# create GCP Cloud Run Job block and save to Prefect Cloud
cloud_run_job_block = CloudRunJob(
    image=args.image,
    region=args.region,
    credentials=gcp_credentials,
    cpu=8,
    memory=32,
    memory_unit="Gi",
    timeout=3600,
    max_retries=0,
)
cloud_run_job_block.save(args.block_name, overwrite=True)

# create BigQuery Target Configs block and save to Prefect Cloud
target_configs = BigQueryTargetConfigs(
    schema="chess_ratings",  # also known as dataset
    credentials=gcp_credentials,
)
target_configs.save(args.block_name, overwrite=True)

# create dbt CLI Profile block and save to Prefect Cloud
dbt_cli_profile = DbtCliProfile(
    name="dbt_chess_ratings_pipeline",
    target="dev",
    target_configs=target_configs,
)
dbt_cli_profile.save(args.block_name, overwrite=True)
