# Location Address Esri Feature Service ETL

This repository contains the code for the Location Address Esri Feature Service ETL.

It is designed to pull address data from QALI via a SPARQL endpoint, generate the location address flat table, find the difference between the previous ETL run and the current ETL run, and update the Esri feature service with the changes.

## Development

To run locally, see the [.env-template file](.env-template) and the [address_etl/settings.py file](address_etl/settings.py) to see what needs to be set.

See the `dev` command in the [Taskfile](Taskfile.yml) to run the ETL locally.

## Release

Releases are managed with GitHub Actions.

When a release is created, the GitHub Action will build the Docker image and push it to GitHub Container Registry.

This container image is pulled into ECS Fargate and run as a task. The ETL is triggered by AWS EventBridge on a cron schedule.

Actual AWS resource deployment is managed in Terraform.
