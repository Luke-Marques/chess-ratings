"""
python blocks.py -b $GITHUB_REF_NAME -r "$GITHUB_SERVER_URL/$GITHUB_REPOSITORY" \
-n ${{ inputs.block_name }} -i ${{ inputs.image_uri }} --region ${{ inputs.region }}
"""

import argparse

from prefect_gcp import GcsBucket
from prefect_gcp.cloud_run import CloudRunJob
from prefect_gcp.credentials import GcpCredentials

from prefect.filesystems import GitHub

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
print(gcp_credentials)

# create GitHub block and save to Prefect Cloud
github_block = GitHub(repository=args.repo, reference=args.branch)
github_block.save(args.block_name, overwrite=True)

# create GCP Cloud Run Job block and save to Prefect Cloud
cloud_run_job_block = CloudRunJob(
    image=args.image,
    region=args.region,
    credentials=gcp_credentials,
    cpu=4,
    memory=30,
    memory_unit="Gi",
)
cloud_run_job_block.save(args.block_name, overwrite=True)

# create GCP GCS Bucket block and save to Prefect Cloud
gcs_bucket_block = GcsBucket(bucket=args.bucket_name, credentials=gcp_credentials)
gcs_bucket_block.save(args.block_name, overwrite=True)
