import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from address_etl.s3 import S3
from address_etl.settings import settings

MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2025-05-24T17-08-30Z"
MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"


@pytest.fixture(scope="function", autouse=True)
def minio_container():
    container = DockerContainer(MINIO_IMAGE)
    container.with_env("MINIO_ROOT_USER", MINIO_USER)
    container.with_env("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
    container.with_exposed_ports(9000, 9001)
    container.with_command('server /data --console-address ":9001"')
    container.start()

    wait_for_logs(container, "Docs: https://docs.min.io")

    settings.use_minio = True
    settings.minio_endpoint = f"http://localhost:{container.get_exposed_port(9000)}"

    yield container

    # Clean up
    container.stop()


@pytest.fixture(scope="function")
def s3():
    s3 = S3(settings)
    yield s3
