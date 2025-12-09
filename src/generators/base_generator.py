"""
Base Generator - Abstract class for all report generators
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from datetime import datetime
from src.core.config import settings
from src.core.logger import logger


class BaseGenerator(ABC):
    """
    Abstract base class for report generation
    All generators must inherit from this class
    """

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the generator

        Args:
            output_dir: Output directory for generated reports
        """
        self.output_dir = output_dir or settings.OUTPUT_DIR
        self.logger = logger
        self.generated_files = []

    @abstractmethod
    def generate(self, data: pd.DataFrame, filename: str, **kwargs) -> Path:
        """
        Generate a report from data

        Args:
            data: DataFrame with report data
            filename: Output filename
            **kwargs: Additional generation options

        Returns:
            Path to the generated file
        """
        pass

    def _prepare_filename(self, filename: str, extension: str) -> str:
        """
        Prepare filename with timestamp if needed

        Args:
            filename: Base filename
            extension: File extension

        Returns:
            Prepared filename
        """
        if not filename.endswith(extension):
            filename = f"{filename}{extension}"
        return filename

    def _get_output_path(self, filename: str, subdir: Optional[str] = None) -> Path:
        """
        Get full output path

        Args:
            filename: Filename
            subdir: Optional subdirectory

        Returns:
            Full path to output file
        """
        if subdir:
            output_path = self.output_dir / subdir / filename
        else:
            output_path = self.output_dir / filename

        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def _add_timestamp_to_filename(self, filename: str) -> str:
        """
        Add timestamp to filename

        Args:
            filename: Original filename

        Returns:
            Filename with timestamp
        """
        name_parts = filename.rsplit('.', 1)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if len(name_parts) == 2:
            return f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
        return f"{filename}_{timestamp}"

    def get_generated_files(self) -> List[Path]:
        """Get list of generated files"""
        return self.generated_files

    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate input data

        Args:
            data: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if data is None or data.empty:
            self.logger.warning("Empty or null data provided")
            return False
        return True
