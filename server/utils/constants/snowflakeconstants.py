from decouple import config

serp_api_key = config("API_KEY")
gcloud_bucket_name = config("GCLOUD_BUCKET_NAME")
gcloud_project_name = config("GCP_PROJECT")
sf_account = config("SF_ACCOUNT")
sf_user = config("SF_USER")
sf_password = config("SF_PASSWORD")
sf_database = config("SF_DATABASE")
sf_schema = config("SF_SCHEMA")
sf_warehouse = config("SF_WAREHOUSE")
sf_role = config("SF_ROLE")
sf_region = config("SF_REGION")
sf_sql_limit = int(config("SF_SQL_LIMIT", default=15000))
