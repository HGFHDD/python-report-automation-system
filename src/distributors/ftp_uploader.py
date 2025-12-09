"""
FTP Uploader - Upload files via FTP/SFTP
"""
from typing import Any, Dict, List, Optional
from pathlib import Path
import ftplib
from io import BytesIO
from src.core.logger import logger


class FTPUploader:
    """
    Class for uploading files via FTP
    """

    def __init__(self, host: str, username: str, password: str,
                port: int = 21, passive: bool = True):
        """
        Initialize FTP uploader

        Args:
            host: FTP server host
            username: FTP username
            password: FTP password
            port: FTP port
            passive: Use passive mode
        """
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.passive = passive
        self.logger = logger
        self._connection = None

    def connect(self) -> None:
        """Establish FTP connection"""
        try:
            self._connection = ftplib.FTP()
            self._connection.connect(self.host, self.port)
            self._connection.login(self.username, self.password)
            self._connection.set_pasv(self.passive)
            self.logger.info(f"Connected to FTP server: {self.host}")
        except Exception as e:
            self.logger.error(f"FTP connection failed: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close FTP connection"""
        if self._connection:
            try:
                self._connection.quit()
            except:
                self._connection.close()
            self._connection = None
            self.logger.info("FTP connection closed")

    def upload(self, local_path: Path, remote_path: str = None) -> bool:
        """
        Upload a file

        Args:
            local_path: Local file path
            remote_path: Remote file path (optional)

        Returns:
            True if successful
        """
        if not self._connection:
            raise ConnectionError("Not connected to FTP server")

        if not local_path.exists():
            raise FileNotFoundError(f"File not found: {local_path}")

        remote_filename = remote_path or local_path.name

        try:
            with open(local_path, 'rb') as f:
                self._connection.storbinary(f'STOR {remote_filename}', f)
            self.logger.info(f"Uploaded {local_path.name} to {remote_filename}")
            return True
        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            return False

    def upload_multiple(self, files: List[Path], remote_dir: str = None) -> Dict[str, bool]:
        """
        Upload multiple files

        Args:
            files: List of file paths
            remote_dir: Remote directory

        Returns:
            Dictionary of filename: success status
        """
        results = {}

        if remote_dir:
            try:
                self._connection.cwd(remote_dir)
            except:
                self._connection.mkd(remote_dir)
                self._connection.cwd(remote_dir)

        for file_path in files:
            results[file_path.name] = self.upload(file_path)

        return results

    def download(self, remote_path: str, local_path: Path) -> bool:
        """
        Download a file

        Args:
            remote_path: Remote file path
            local_path: Local destination path

        Returns:
            True if successful
        """
        if not self._connection:
            raise ConnectionError("Not connected to FTP server")

        try:
            with open(local_path, 'wb') as f:
                self._connection.retrbinary(f'RETR {remote_path}', f.write)
            self.logger.info(f"Downloaded {remote_path} to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Download failed: {str(e)}")
            return False

    def list_files(self, remote_dir: str = '.') -> List[str]:
        """
        List files in directory

        Args:
            remote_dir: Remote directory path

        Returns:
            List of filenames
        """
        if not self._connection:
            raise ConnectionError("Not connected to FTP server")

        try:
            return self._connection.nlst(remote_dir)
        except Exception as e:
            self.logger.error(f"Failed to list files: {str(e)}")
            return []

    def create_directory(self, remote_dir: str) -> bool:
        """
        Create remote directory

        Args:
            remote_dir: Directory path

        Returns:
            True if successful
        """
        if not self._connection:
            raise ConnectionError("Not connected to FTP server")

        try:
            self._connection.mkd(remote_dir)
            self.logger.info(f"Created directory: {remote_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create directory: {str(e)}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        """
        Delete a remote file

        Args:
            remote_path: File path to delete

        Returns:
            True if successful
        """
        if not self._connection:
            raise ConnectionError("Not connected to FTP server")

        try:
            self._connection.delete(remote_path)
            self.logger.info(f"Deleted file: {remote_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete file: {str(e)}")
            return False

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
