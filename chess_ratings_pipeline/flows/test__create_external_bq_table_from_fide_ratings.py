from prefect import flow
from chess_ratings_pipeline.core.integrations.google_bigquery import (
    create_external_bq_table_from_gcs_files,
)
from prefect_gcp import GcpCredentials, GcsBucket


@flow(log_prints=True)
def test__create_external_bq_table_from_fide_ratings():
    gcp_credentials_block = GcpCredentials.load("gcp-creds-chess-ratings")
    gcs_bucket_block = GcsBucket.load("chess-ratings-dev")
    cdc_stats_uri = f"gs://{gcs_bucket_block.bucket}/data/fide_ratings/*/*.parquet"
    create_external_bq_table_from_gcs_files(
        gcs_file_uris=cdc_stats_uri,
        dataset="chess_ratings",
        table="landing_fide__ratings_test",
        gcp_credentials=gcp_credentials_block,
        project="fide-chess-ratings",
    )
