# PLS ETL

This repository contains the PLS ETL.

The ETL pulls source data from QALI via SPARQL, imports geocodes and cached address PID mappings from ESRI, builds the PLS SQLite dataset, and uploads the resulting database snapshot to S3.

## Development

See [.env-template](.env-template) and [address_etl/settings.py](address_etl/settings.py) for the supported runtime configuration.

Run the ETL locally with the `dev` task in [Taskfile.yml](Taskfile.yml).

## Kafka Configuration

The ETL requires Kafka configuration because each successful snapshot upload publishes
the presigned SQLite URL to Kafka as a plain-text message body with ETL metadata in
the message headers.

Supported env vars:

- `KAFKA_TOPIC`
- `KAFKA_BOOTSTRAP_SERVER`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_MECHANISM`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`

For production, configure Kafka with SCRAM over SSL, for example:

```env
KAFKA_TOPIC=pls.artifact-url.v1
KAFKA_BOOTSTRAP_SERVER=b-1.example.kafka.amazonaws.com:9096
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=msk-user
KAFKA_SASL_PASSWORD=msk-password
```

For local development without SASL, `KAFKA_SECURITY_PROTOCOL=PLAINTEXT` can be used.

## Release

Releases are managed with GitHub Actions.

When a release is created, GitHub Actions builds the Docker image and pushes it to GitHub Container Registry.

The container image is then run as an ECS Fargate task and triggered by AWS EventBridge on a cron schedule.

Actual AWS resource deployment is managed in Terraform.
