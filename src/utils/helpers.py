"""
Helpers - Utility helper functions
"""
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import json
import yaml
import zipfile
import os


def get_date_range(period: str, reference_date: datetime = None) -> tuple:
    """
    Get date range for a period

    Args:
        period: Period name ('today', 'yesterday', 'this_week', 'last_week', 
               'this_month', 'last_month', 'this_quarter', 'this_year')
        reference_date: Reference date (default: today)

    Returns:
        Tuple of (start_date, end_date)
    """
    ref = reference_date or datetime.now()
    
    if period == 'today':
        start = ref.replace(hour=0, minute=0, second=0, microsecond=0)
        end = ref.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period == 'yesterday':
        yesterday = ref - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period == 'this_week':
        start = ref - timedelta(days=ref.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    elif period == 'last_week':
        start = ref - timedelta(days=ref.weekday() + 7)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    elif period == 'this_month':
        start = ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Last day of month
        if ref.month == 12:
            end = ref.replace(year=ref.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = ref.replace(month=ref.month + 1, day=1) - timedelta(days=1)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period == 'last_month':
        first_of_this_month = ref.replace(day=1)
        end = first_of_this_month - timedelta(days=1)
        start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period == 'this_quarter':
        quarter = (ref.month - 1) // 3
        start = datetime(ref.year, quarter * 3 + 1, 1)
        if quarter == 3:
            end = datetime(ref.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = datetime(ref.year, (quarter + 1) * 3 + 1, 1) - timedelta(days=1)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period == 'this_year':
        start = datetime(ref.year, 1, 1)
        end = datetime(ref.year, 12, 31, 23, 59, 59, 999999)
    
    else:
        raise ValueError(f"Unknown period: {period}")
    
    return start, end


def format_filesize(size_bytes: int) -> str:
    """
    Format file size in human-readable format

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def generate_filename(base_name: str, extension: str, 
                     add_timestamp: bool = True,
                     date_format: str = '%Y%m%d_%H%M%S') -> str:
    """
    Generate filename with optional timestamp

    Args:
        base_name: Base filename
        extension: File extension
        add_timestamp: Whether to add timestamp
        date_format: Timestamp format

    Returns:
        Generated filename
    """
    if not extension.startswith('.'):
        extension = f'.{extension}'
    
    if add_timestamp:
        timestamp = datetime.now().strftime(date_format)
        return f"{base_name}_{timestamp}{extension}"
    
    return f"{base_name}{extension}"


def load_yaml_config(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load YAML configuration file

    Args:
        path: Path to YAML file

    Returns:
        Configuration dictionary
    """
    path = Path(path)
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def save_yaml_config(config: Dict[str, Any], path: Union[str, Path]) -> None:
    """
    Save configuration to YAML file

    Args:
        config: Configuration dictionary
        path: Output path
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)


def load_json_file(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load JSON file

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON data
    """
    path = Path(path)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json_file(data: Any, path: Union[str, Path], indent: int = 2) -> None:
    """
    Save data to JSON file

    Args:
        data: Data to save
        path: Output path
        indent: JSON indentation
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False, default=str)


def create_zip(files: List[Path], output_path: Path, 
               base_dir: Path = None) -> Path:
    """
    Create ZIP archive from files

    Args:
        files: List of file paths
        output_path: Output ZIP path
        base_dir: Base directory for archive paths

    Returns:
        Path to created ZIP
    """
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in files:
            if file_path.exists():
                if base_dir:
                    arcname = file_path.relative_to(base_dir)
                else:
                    arcname = file_path.name
                zf.write(file_path, arcname)
    
    return output_path


def extract_zip(zip_path: Path, extract_to: Path) -> List[Path]:
    """
    Extract ZIP archive

    Args:
        zip_path: Path to ZIP file
        extract_to: Extraction directory

    Returns:
        List of extracted file paths
    """
    extract_to.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_to)
        return [extract_to / name for name in zf.namelist()]


def calculate_file_hash(path: Path, algorithm: str = 'md5') -> str:
    """
    Calculate file hash

    Args:
        path: Path to file
        algorithm: Hash algorithm ('md5', 'sha256')

    Returns:
        Hash string
    """
    hash_func = hashlib.new(algorithm)
    
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure directory exists

    Args:
        path: Directory path

    Returns:
        Path object
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def clean_filename(filename: str, replacement: str = '_') -> str:
    """
    Clean filename by removing invalid characters

    Args:
        filename: Original filename
        replacement: Replacement character

    Returns:
        Cleaned filename
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, replacement)
    return filename.strip()


def merge_dicts(base: Dict, override: Dict) -> Dict:
    """
    Deep merge two dictionaries

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """
    Split list into chunks

    Args:
        lst: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
