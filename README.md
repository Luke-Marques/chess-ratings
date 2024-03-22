# Prefect and Google Cloud Platform - Cloud Run Deployment Template

This repository stores a default template for projects which use Prefect Cloud to  
orchestrate data tasks into *flows*, and deploy the flows to the Google Cloud Platform  
(GCP) using Cloud Run Jobs to avoid high usage costs.

## Requirements

This template assumes you have set up:

- A GCP account and project.
- A Prefect Cloud account (free forever for personal use).

## Template Usage Instructions

### 1. Fork this repository

The first step is to fork this repository and give your new repo a meaningful name for
your data project.

See [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo)
for instructions for forking GitHub repos.

### 2. Prefect Cloud Setup

If you haven't done so already, [sign up for Prefect Cloud](https://app.prefect.cloud/)
account.

Then:

1. Create a workspace and an API key.
2. Add both PREFECT_API_KEY and PREFECT_API_URL as GitHub Actions secrets.

### 3. Google Cloud Setup

Create a GCP project, if you don't have one already, and add service account. The 
following code creates a service account for Prefect. Run the code in the cloud shell
of your GCP project.

Remember to customise the names to your project!

```{bash}
# Create GCP account and project
# This will also set default project and region:
export CLOUDSDK_CORE_PROJECT="prefect-community"
export CLOUDSDK_COMPUTE_REGION=europe-north1  # change to a low-carbon region near you
export GCP_AR_REPO=prefect
export GCP_SA_NAME=prefect

# Enable required GCP services:
gcloud services enable iamcredentials.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable compute.googleapis.com

# Create service account named e.g. prefect:
gcloud iam service-accounts create $GCP_SA_NAME
export MEMBER=serviceAccount:"$GCP_SA_NAME"@"$CLOUDSDK_CORE_PROJECT".iam.gserviceaccount.com
gcloud projects add-iam-policy-binding $CLOUDSDK_CORE_PROJECT --member=$MEMBER --role="roles/run.admin"
gcloud projects add-iam-policy-binding $CLOUDSDK_CORE_PROJECT --member=$MEMBER --role="roles/compute.instanceAdmin.v1"
gcloud projects add-iam-policy-binding $CLOUDSDK_CORE_PROJECT --member=$MEMBER --role="roles/artifactregistry.admin"
gcloud projects add-iam-policy-binding $CLOUDSDK_CORE_PROJECT --member=$MEMBER --role="roles/iam.serviceAccountUser"

# Create JSON credentials file as follows, then copy-paste its content into your GHA Secret + Prefect GcpCredentials block:
gcloud iam service-accounts keys create prefect.json --iam-account="$GCP_SA_NAME"@"$CLOUDSDK_CORE_PROJECT".iam.gserviceaccount.com
```

GitHub Action secret named GCP_CREDENTIALS
Prefect GcpCredentials block named default- if you save this Prefect block with a different name, make sure to adjust it in your GitHub Action inputs, e.g. in .github/workflows/getting-started.yml:

The above code block will also generate a **GCP credentials JSON file**, called 
`prefect.json`. Copy the contents of this JSON file and add the contents to:

1. A GitHub Actions secret called `GCP_CREDENTIALS`.
2. A Prefect Cloud GCP credentials block - name this something appropriate, e.g.
`gcp-creds-<project-name>` - and make sure to change this in 
`.github/workflows/quick-start.yml`:

```{yaml}
 gcp_creds_block_name:
    description: 'Name of the GcpCredentials block'
    required: false
    default: "default"  # change this to your GCP credentials block name
    type: string
```

## What does the main `quick-start` action do?

1. It creates an Artifact Registry repository if one doesn't exist yet. That's why the 
permissions on your service account must be set to admin "roles/artifactregistry.admin" 
rather than just a writer: "roles/artifactregistry.writer" - if you prefer to manually 
create the repository and limit the service account permissions, you can do that.
2. It builds a Docker image and pushes it to that Artifact Registry repository, based on 
the Dockerfile.
3. It deploys a VM (if one such VM with the same name already exists, it gets deleted 
before a new VM gets created) and a Docker container running a Prefect worker process 
that deploys flow runs. By default, the flows are configured to be deployed as 
serverless containers using Google Cloud Run jobs. This makes it easy to scale your 
project as your needs grow - no need to monitor and maintain the underlying 
infrastructure - serverless containers gets spun up based on the provided Artifact 
Registry image and the resource allocation can be adjusted any time on the CloudRunJob 
block, even from the Prefect UI.
4. It automatically deploys your first Prefect blocks
5. It automatically deploys your first Prefect flows

## How does this automated GitHub Actions process deploy flows?

### The `deploy-flows` action

This action assumes that the name of your `flow_script.py` matches the name of the 
`flow`, e.g. a flow script parametrized.py has a function named parametrized() decorated 
with `@flow`. This means that if your script parametrized.py has multiple flows within, 
only the flow parametrized gets deployed (and potentially scheduled) as part of your 
Prefect Cloud deployment:

```{python}
from prefect import get_run_logger, flow
from typing import Any


@flow(log_prints=True)
def some_subflow():
    print("I'm a subflow")

    
@flow
def parametrized(
    user: str = "Marvin", question: str = "Ultimate", answer: Any = 42
) -> None:
    logger = get_run_logger()
    logger.info("Hello from Prefect, %s! 👋", user)
    logger.info("The answer to the %s question is %s! 🤖", question, answer)


if __name__ == "__main__":
    parametrized(user="World")
```

You can still create a deployment for the flow some_subflow if you want to, but the 
default GitHub Action here won't do it for you - this is not a Prefect limitation, it's 
only a choice made in this demo to make the example project here easier to follow (and 
more standardized to deploy in an automated CI/CD pipeline).