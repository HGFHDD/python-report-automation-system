"""
Cloud Storage - Upload files to cloud storage services
"""
from typing import Any, Dict, List, Optional
from pathlib import Path
from abc import ABC, abstractmethod
from src.core.logger import logger


class CloudStorage(ABC):
    """
    Abstract base class for cloud storage
    """

    def __init__(self):
        self.logger = logger

    @abstractmethod
    def upload(self, local_path: Path, remote_path: str) -> bool:
        """Upload file to cloud storage"""
        pass

    @abstractmethod
    def download(self, remote_path: str, local_path: Path) -> bool:
        """Download file from cloud storage"""
        pass

    @abstractmethod
    def list_files(self, prefix: str = '') -> List[str]:
        """List files in cloud storage"""
        pass

    @abstractmethod
    def delete(self, remote_path: str) -> bool:
        """Delete file from cloud storage"""
        pass


class AzureBlobStorage(CloudStorage):
    """
    Azure Blob Storage implementation
    """

    def __init__(self, connection_string: str = None, container_name: str = None):
        """
        Initialize Azure Blob Storage

        Args:
            connection_string: Azure storage connection string
            container_name: Container name
        """
        super().__init__()
        self.connection_string = connection_string
        self.container_name = container_name
        self._client = None

    def _get_client(self):
        """Get or create blob service client"""
        if self._client is None:
            try:
                from azure.storage.blob import BlobServiceClient
                self._client = BlobServiceClient.from_connection_string(self.connection_string)
            except ImportError:
                raise ImportError("azure-storage-blob package required. Install with: pip install azure-storage-blob")
        return self._client

    def upload(self, local_path: Path, remote_path: str = None) -> bool:
        """
        Upload file to Azure Blob Storage

        Args:
            local_path: Local file path
            remote_path: Blob name (optional, uses filename if not provided)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            blob_name = remote_path or local_path.name
            blob_client = client.get_blob_client(container=self.container_name, blob=blob_name)
            
            with open(local_path, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
            
            self.logger.info(f"Uploaded {local_path.name} to Azure Blob: {blob_name}")
            return True
        except Exception as e:
            self.logger.error(f"Azure upload failed: {str(e)}")
            return False

    def download(self, remote_path: str, local_path: Path) -> bool:
        """
        Download file from Azure Blob Storage

        Args:
            remote_path: Blob name
            local_path: Local destination path

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            blob_client = client.get_blob_client(container=self.container_name, blob=remote_path)
            
            with open(local_path, 'wb') as f:
                download_stream = blob_client.download_blob()
                f.write(download_stream.readall())
            
            self.logger.info(f"Downloaded {remote_path} from Azure Blob to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Azure download failed: {str(e)}")
            return False

    def list_files(self, prefix: str = '') -> List[str]:
        """
        List blobs in container

        Args:
            prefix: Blob name prefix filter

        Returns:
            List of blob names
        """
        try:
            client = self._get_client()
            container_client = client.get_container_client(self.container_name)
            blobs = container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            self.logger.error(f"Azure list failed: {str(e)}")
            return []

    def delete(self, remote_path: str) -> bool:
        """
        Delete blob from Azure

        Args:
            remote_path: Blob name

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            blob_client = client.get_blob_client(container=self.container_name, blob=remote_path)
            blob_client.delete_blob()
            self.logger.info(f"Deleted blob: {remote_path}")
            return True
        except Exception as e:
            self.logger.error(f"Azure delete failed: {str(e)}")
            return False


class S3Storage(CloudStorage):
    """
    AWS S3 Storage implementation
    """

    def __init__(self, bucket_name: str, aws_access_key: str = None,
                aws_secret_key: str = None, region: str = 'us-east-1'):
        """
        Initialize S3 Storage

        Args:
            bucket_name: S3 bucket name
            aws_access_key: AWS access key
            aws_secret_key: AWS secret key
            region: AWS region
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self._client = None

    def _get_client(self):
        """Get or create S3 client"""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.region
                )
            except ImportError:
                raise ImportError("boto3 package required. Install with: pip install boto3")
        return self._client

    def upload(self, local_path: Path, remote_path: str = None) -> bool:
        """
        Upload file to S3

        Args:
            local_path: Local file path
            remote_path: S3 key (optional)

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            key = remote_path or local_path.name
            client.upload_file(str(local_path), self.bucket_name, key)
            self.logger.info(f"Uploaded {local_path.name} to S3: {key}")
            return True
        except Exception as e:
            self.logger.error(f"S3 upload failed: {str(e)}")
            return False

    def download(self, remote_path: str, local_path: Path) -> bool:
        """
        Download file from S3

        Args:
            remote_path: S3 key
            local_path: Local destination path

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.download_file(self.bucket_name, remote_path, str(local_path))
            self.logger.info(f"Downloaded {remote_path} from S3 to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"S3 download failed: {str(e)}")
            return False

    def list_files(self, prefix: str = '') -> List[str]:
        """
        List objects in S3 bucket

        Args:
            prefix: Key prefix filter

        Returns:
            List of object keys
        """
        try:
            client = self._get_client()
            response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except Exception as e:
            self.logger.error(f"S3 list failed: {str(e)}")
            return []

    def delete(self, remote_path: str) -> bool:
        """
        Delete object from S3

        Args:
            remote_path: S3 key

        Returns:
            True if successful
        """
        try:
            client = self._get_client()
            client.delete_object(Bucket=self.bucket_name, Key=remote_path)
            self.logger.info(f"Deleted S3 object: {remote_path}")
            return True
        except Exception as e:
            self.logger.error(f"S3 delete failed: {str(e)}")
            return False
