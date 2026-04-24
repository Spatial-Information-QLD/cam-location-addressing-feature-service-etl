import os

# Set mock environment variables for testing.
# This needs to happen before `address_etl.settings` is imported by any test.
os.environ.setdefault("SPARQL_ENDPOINT", "http://mock-sparql")
os.environ.setdefault("ESRI_USERNAME", "mock-user")
os.environ.setdefault("ESRI_PASSWORD", "mock-pass")
os.environ.setdefault("PLS_S3_BUCKET_NAME", "mock-bucket")
os.environ.setdefault("KAFKA_TOPIC", "mock-topic")
os.environ["DEBUG"] = "true"
os.environ.setdefault("AWS_ACCESS_KEY_ID", "mock-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "mock-secret")
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-southeast-2")
