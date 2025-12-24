"""Podman container management for integration tests."""

from __future__ import annotations

import logging
import socket
import subprocess
import time

from minio import Minio

logger = logging.getLogger(__name__)

# PostgreSQL container configuration
POSTGRES_CONTAINER = "test-postgres"
POSTGRES_IMAGE = "docker.io/library/postgres:15"
POSTGRES_PORT = 15433  # Dev uses 15432, test uses 15433
POSTGRES_USER = "etl"
POSTGRES_PASSWORD = "etlpass"
POSTGRES_DB = "etl_test_db"

# MinIO container configuration
MINIO_CONTAINER = "test-minio"
MINIO_IMAGE = "docker.io/minio/minio:latest"
MINIO_PORT = 19010  # Dev uses 19000, test uses 19010
MINIO_CONSOLE_PORT = 19011  # Dev uses 19001, test uses 19011
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"


def run_podman(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    """Execute a podman command.

    Args:
        args: Command arguments to pass to podman.
        check: Whether to raise exception on non-zero exit code.

    Returns:
        CompletedProcess with command result.
    """
    cmd = ["podman", *args]
    logger.debug(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def is_container_running(name: str) -> bool:
    """Check if a container is running.

    Args:
        name: Container name.

    Returns:
        True if container is running, False otherwise.
    """
    result = run_podman(["ps", "-q", "-f", f"name=^{name}$"], check=False)
    return bool(result.stdout.strip())


def container_exists(name: str) -> bool:
    """Check if a container exists (running or stopped).

    Args:
        name: Container name.

    Returns:
        True if container exists, False otherwise.
    """
    result = run_podman(["ps", "-aq", "-f", f"name=^{name}$"], check=False)
    return bool(result.stdout.strip())


def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use.

    Args:
        port: Port number to check.

    Returns:
        True if port is in use, False otherwise.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def start_postgres_container() -> None:
    """Start PostgreSQL container.

    If container already exists, start it. Otherwise create a new one.
    """
    if is_container_running(POSTGRES_CONTAINER):
        logger.info(f"PostgreSQL container '{POSTGRES_CONTAINER}' is already running")
        return

    if container_exists(POSTGRES_CONTAINER):
        logger.info(f"Starting existing PostgreSQL container '{POSTGRES_CONTAINER}'")
        run_podman(["start", POSTGRES_CONTAINER])
        return

    logger.info(f"Creating and starting PostgreSQL container '{POSTGRES_CONTAINER}' on port {POSTGRES_PORT}")
    run_podman([
        "run", "-d",
        "--name", POSTGRES_CONTAINER,
        "-e", f"POSTGRES_USER={POSTGRES_USER}",
        "-e", f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
        "-e", f"POSTGRES_DB={POSTGRES_DB}",
        "-p", f"{POSTGRES_PORT}:5432",
        POSTGRES_IMAGE,
    ])


def start_minio_container() -> None:
    """Start MinIO container.

    If container already exists, start it. Otherwise create a new one.
    """
    if is_container_running(MINIO_CONTAINER):
        logger.info(f"MinIO container '{MINIO_CONTAINER}' is already running")
        return

    if container_exists(MINIO_CONTAINER):
        logger.info(f"Starting existing MinIO container '{MINIO_CONTAINER}'")
        run_podman(["start", MINIO_CONTAINER])
        return

    logger.info(f"Creating and starting MinIO container '{MINIO_CONTAINER}' on port {MINIO_PORT}")
    run_podman([
        "run", "-d",
        "--name", MINIO_CONTAINER,
        "-e", f"MINIO_ROOT_USER={MINIO_ACCESS_KEY}",
        "-e", f"MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}",
        "-p", f"{MINIO_PORT}:9000",
        "-p", f"{MINIO_CONSOLE_PORT}:9001",
        MINIO_IMAGE,
        "server", "/data", "--console-address", ":9001",
    ])


def stop_container(name: str) -> None:
    """Stop and remove a container.

    Args:
        name: Container name to stop and remove.
    """
    if not container_exists(name):
        logger.info(f"Container '{name}' does not exist, nothing to stop")
        return

    logger.info(f"Stopping container '{name}'")
    run_podman(["stop", name], check=False)
    run_podman(["rm", name], check=False)


def wait_for_port(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for a port to become available.

    Args:
        host: Host address.
        port: Port number.
        timeout: Maximum wait time in seconds.

    Returns:
        True if port is available, False if timeout.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(0.5)
    return False


def wait_for_postgres(timeout: int = 30) -> bool:
    """Wait for PostgreSQL to be ready to accept connections.

    Args:
        timeout: Maximum wait time in seconds.

    Returns:
        True if PostgreSQL is ready, False if timeout.
    """
    logger.info(f"Waiting for PostgreSQL on port {POSTGRES_PORT}...")

    if not wait_for_port("localhost", POSTGRES_PORT, timeout):
        logger.error("PostgreSQL port not available")
        return False

    # Use pg_isready to check if PostgreSQL is ready
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = run_podman([
            "exec", POSTGRES_CONTAINER,
            "pg_isready", "-U", POSTGRES_USER, "-d", POSTGRES_DB,
        ], check=False)
        if result.returncode == 0:
            logger.info("PostgreSQL is ready")
            return True
        time.sleep(0.5)

    logger.error("PostgreSQL not ready within timeout")
    return False


def wait_for_minio(timeout: int = 30) -> bool:
    """Wait for MinIO to be ready.

    Args:
        timeout: Maximum wait time in seconds.

    Returns:
        True if MinIO is ready, False if timeout.
    """
    logger.info(f"Waiting for MinIO on port {MINIO_PORT}...")

    if not wait_for_port("localhost", MINIO_PORT, timeout):
        logger.error("MinIO port not available")
        return False

    # Additional check: try to list buckets
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            client = Minio(
                f"localhost:{MINIO_PORT}",
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False,
            )
            client.list_buckets()
            logger.info("MinIO is ready")
            return True
        except Exception:
            time.sleep(0.5)

    logger.error("MinIO not ready within timeout")
    return False


def setup_minio_bucket(bucket_name: str) -> None:
    """Create a bucket in MinIO if it doesn't exist.

    Args:
        bucket_name: Name of the bucket to create.
    """
    client = Minio(
        f"localhost:{MINIO_PORT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(bucket_name):
        logger.info(f"Creating MinIO bucket '{bucket_name}'")
        client.make_bucket(bucket_name)
    else:
        logger.info(f"MinIO bucket '{bucket_name}' already exists")


# Dev MinIO configuration (for copying test data)
DEV_MINIO_PORT = 19000
DEV_MINIO_ACCESS_KEY = "minioadmin"
DEV_MINIO_SECRET_KEY = "minioadmin123"


def copy_test_data_from_dev(bucket_name: str) -> None:
    """Copy test data from dev MinIO to test MinIO.

    Args:
        bucket_name: Name of the bucket to copy data to.
    """
    from io import BytesIO

    dev_client = Minio(
        f"localhost:{DEV_MINIO_PORT}",
        access_key=DEV_MINIO_ACCESS_KEY,
        secret_key=DEV_MINIO_SECRET_KEY,
        secure=False,
    )

    test_client = Minio(
        f"localhost:{MINIO_PORT}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not dev_client.bucket_exists(bucket_name):
        logger.warning(f"Dev MinIO bucket '{bucket_name}' does not exist, skipping data copy")
        return

    # Copy all objects from dev to test
    objects = dev_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        logger.info(f"Copying {obj.object_name} from dev to test MinIO")
        response = dev_client.get_object(bucket_name, obj.object_name)
        data = response.read()
        response.close()
        response.release_conn()

        test_client.put_object(
            bucket_name,
            obj.object_name,
            BytesIO(data),
            length=len(data),
        )
