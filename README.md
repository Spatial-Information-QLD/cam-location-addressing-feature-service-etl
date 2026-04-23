# PLS ETL

This repository contains the PLS ETL.

The ETL pulls source data from QALI via SPARQL, imports geocodes and cached address PID mappings from ESRI, builds the PLS SQLite dataset, and uploads the resulting database snapshot to S3.

## Development

See [.env-template](.env-template) and [address_etl/settings.py](address_etl/settings.py) for the supported runtime configuration.

Run the ETL locally with the `dev` task in [Taskfile.yml](Taskfile.yml).

## Release

Releases are managed with GitHub Actions.

When a release is created, GitHub Actions builds the Docker image and pushes it to GitHub Container Registry.

The container image is then run as an ECS Fargate task and triggered by AWS EventBridge on a cron schedule.

Actual AWS resource deployment is managed in Terraform.
