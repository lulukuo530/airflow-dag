from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import secretmanager

GCP_CONN_ID = "composer_worker"


def get_credentials_from_secret_manager(secret_name):
    """
    Get the credentials from the secret manager of fazz-data (prod, d291209) project
    """
    hook = GoogleBaseHook(
        gcp_conn_id=GCP_CONN_ID,
    )
    credentials = hook.get_credentials()

    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    secret_id = f"projects/978528819584/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_id)
    return response.payload.data.decode("UTF-8")
