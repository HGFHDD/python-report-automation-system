"""
Excel File Data Extractor
"""
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd
from src.extractors.base import BaseExtractor


class ExcelExtractor(BaseExtractor):
    """
    Extractor for Excel files (.xlsx, .xls)
    """

    def __init__(self, file_path: Optional[str] = None, **kwargs):
        """
        Initialize Excel extractor

        Args:
            file_path: Path to the Excel file
            **kwargs: Additional pandas read_excel parameters
        """
        super().__init__(kwargs)
        self.file_path = Path(file_path) if file_path else None
        self.read_options = kwargs

    def connect(self) -> None:
        """Validate file exists"""
        if self.file_path and not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
        self.logger.info(f"Excel file validated: {self.file_path}")

    def disconnect(self) -> None:
        """No connection to close for Excel files"""
        pass

    def extract(self, query: str = None, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Extract data from Excel file

        Args:
            query: File path (overrides constructor path) or sheet name
            params: Optional read parameters including 'sheet_name'

        Returns:
            DataFrame with Excel data
        """
        file_to_read = self.file_path
        sheet_name = 0  # Default to first sheet
        
        if query:
            # Check if query is a file path or sheet name
            if Path(query).suffix in ['.xlsx', '.xls']:
                file_to_read = Path(query)
            else:
                sheet_name = query
        
        if not file_to_read:
            raise ValueError("No file path provided")
        
        if not file_to_read.exists():
            raise FileNotFoundError(f"Excel file not found: {file_to_read}")

        read_params = {**self.read_options}
        if params:
            read_params.update(params)
        
        read_params['sheet_name'] = read_params.get('sheet_name', sheet_name)

        try:
            df = pd.read_excel(file_to_read, **read_params)
            self.logger.info(f"Successfully loaded Excel: {len(df)} rows from {file_to_read}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read Excel: {str(e)}")
            raise

    def extract_sheet(self, file_path: str, sheet_name: Union[str, int]) -> pd.DataFrame:
        """
        Extract data from a specific sheet

        Args:
            file_path: Path to Excel file
            sheet_name: Name or index of the sheet

        Returns:
            DataFrame with sheet data
        """
        return self.extract(file_path, params={'sheet_name': sheet_name})

    def extract_all_sheets(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        Extract data from all sheets

        Args:
            file_path: Path to Excel file

        Returns:
            Dictionary with sheet names as keys and DataFrames as values
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {path}")

        try:
            excel_file = pd.ExcelFile(path)
            sheets_data = {}
            
            for sheet_name in excel_file.sheet_names:
                sheets_data[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)
                self.logger.info(f"Loaded sheet '{sheet_name}': {len(sheets_data[sheet_name])} rows")
            
            return sheets_data
        except Exception as e:
            self.logger.error(f"Failed to read Excel sheets: {str(e)}")
            raise

    def get_sheet_names(self, file_path: str) -> List[str]:
        """
        Get list of sheet names in Excel file

        Args:
            file_path: Path to Excel file

        Returns:
            List of sheet names
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {path}")

        excel_file = pd.ExcelFile(path)
        return excel_file.sheet_names

    def extract_range(self, file_path: str, sheet_name: str = None,
                     start_row: int = 0, end_row: int = None,
                     start_col: int = 0, end_col: int = None) -> pd.DataFrame:
        """
        Extract a specific range from Excel file

        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name
            start_row: Starting row (0-indexed)
            end_row: Ending row
            start_col: Starting column (0-indexed)
            end_col: Ending column

        Returns:
            DataFrame with range data
        """
        params = {
            'skiprows': start_row,
            'nrows': end_row - start_row if end_row else None,
            'usecols': range(start_col, end_col) if end_col else None,
        }
        if sheet_name:
            params['sheet_name'] = sheet_name

        return self.extract(file_path, params=params)
