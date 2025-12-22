"""S3/MinIO connector for data extraction."""

import logging
from io import BytesIO

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

    def download_csv_to_stream(self, bucket: str, object_name: str) -> BytesIO:
        """Download a CSV file to a BytesIO stream.

        Args:
            bucket: Bucket name.
            object_name: Object path within the bucket.

        Returns:
            BytesIO stream containing CSV content.
        """
        data = self.download_csv(bucket, object_name)
        return BytesIO(data)

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """List objects in a bucket with optional prefix.

        Args:
            bucket: Bucket name.
            prefix: Object name prefix for filtering.

        Returns:
            List of object names.
        """
        objects = self._client.list_objects(bucket, prefix=prefix)
        return [obj.object_name for obj in objects if obj.object_name is not None]

    def bucket_exists(self, bucket: str) -> bool:
        """Check if a bucket exists.

        Args:
            bucket: Bucket name.

        Returns:
            True if bucket exists, False otherwise.
        """
        return self._client.bucket_exists(bucket)
