"""S3/MinIO connector for data extraction."""

import logging

from minio import Minio

logger = logging.getLogger(__name__)


class S3Connector:
    """S3/MinIO connector for downloading CSV files.

    Args:
        endpoint: S3/MinIO endpoint (e.g., 'localhost:19000').
        access_key: Access key for authentication.
        secret_key: Secret key for authentication.
        secure: Whether to use HTTPS (default: False for MinIO).
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False) -> None:
        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        logger.info(f"S3Connector initialized with endpoint: {endpoint}")

    def download_csv(self, bucket: str, object_name: str) -> bytes:
        """Download a CSV file from S3/MinIO.

        Args:
            bucket: Bucket name.
            object_name: Object path within the bucket.

        Returns:
            CSV file content as bytes.

        Raises:
            Exception: If download fails.
        """
        logger.info(f"Downloading {object_name} from bucket {bucket}")
        response = None
        try:
            response = self._client.get_object(bucket, object_name)
            data = response.read()
            logger.info(f"Downloaded {len(data)} bytes from {object_name}")
            return data
        finally:
            if response:
                response.close()
                response.release_conn()
