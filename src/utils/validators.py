"""
Validators - Input validation utilities
"""
from typing import Any, Dict, List, Optional, Union
import re
from datetime import datetime
from pathlib import Path
import pandas as pd


class ValidationError(Exception):
    """Custom validation error"""
    pass


def validate_email(email: str) -> bool:
    """
    Validate email address format

    Args:
        email: Email address to validate

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        raise ValidationError(f"Invalid email format: {email}")
    return True


def validate_emails(emails: List[str]) -> bool:
    """
    Validate list of email addresses

    Args:
        emails: List of email addresses

    Returns:
        True if all valid
    """
    for email in emails:
        validate_email(email)
    return True


def validate_date(date_str: str, format: str = '%Y-%m-%d') -> datetime:
    """
    Validate and parse date string

    Args:
        date_str: Date string to validate
        format: Expected date format

    Returns:
        Parsed datetime object
    
    Raises:
        ValidationError if invalid
    """
    try:
        return datetime.strptime(date_str, format)
    except ValueError:
        raise ValidationError(f"Invalid date format: {date_str}. Expected: {format}")


def validate_date_range(start_date: str, end_date: str, 
                       format: str = '%Y-%m-%d') -> tuple:
    """
    Validate date range

    Args:
        start_date: Start date string
        end_date: End date string
        format: Date format

    Returns:
        Tuple of (start_datetime, end_datetime)
    
    Raises:
        ValidationError if invalid
    """
    start = validate_date(start_date, format)
    end = validate_date(end_date, format)
    
    if start > end:
        raise ValidationError("Start date must be before end date")
    
    return start, end


def validate_file_path(path: Union[str, Path], must_exist: bool = False,
                      allowed_extensions: List[str] = None) -> Path:
    """
    Validate file path

    Args:
        path: Path to validate
        must_exist: Whether file must exist
        allowed_extensions: List of allowed extensions

    Returns:
        Path object
    
    Raises:
        ValidationError if invalid
    """
    path = Path(path)
    
    if must_exist and not path.exists():
        raise ValidationError(f"File does not exist: {path}")
    
    if allowed_extensions:
        ext = path.suffix.lower()
        if ext not in [e.lower() if e.startswith('.') else f'.{e.lower()}' 
                       for e in allowed_extensions]:
            raise ValidationError(f"Invalid file extension: {ext}. Allowed: {allowed_extensions}")
    
    return path


def validate_dataframe(df: pd.DataFrame, required_columns: List[str] = None,
                      min_rows: int = 0, max_rows: int = None) -> bool:
    """
    Validate DataFrame

    Args:
        df: DataFrame to validate
        required_columns: Required column names
        min_rows: Minimum number of rows
        max_rows: Maximum number of rows

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    if df is None:
        raise ValidationError("DataFrame is None")
    
    if len(df) < min_rows:
        raise ValidationError(f"DataFrame has {len(df)} rows, minimum required: {min_rows}")
    
    if max_rows and len(df) > max_rows:
        raise ValidationError(f"DataFrame has {len(df)} rows, maximum allowed: {max_rows}")
    
    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValidationError(f"Missing required columns: {missing}")
    
    return True


def validate_config(config: Dict[str, Any], required_keys: List[str] = None,
                   schema: Dict[str, type] = None) -> bool:
    """
    Validate configuration dictionary

    Args:
        config: Configuration to validate
        required_keys: Required keys
        schema: Expected types for keys

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    if not isinstance(config, dict):
        raise ValidationError("Config must be a dictionary")
    
    if required_keys:
        missing = set(required_keys) - set(config.keys())
        if missing:
            raise ValidationError(f"Missing required config keys: {missing}")
    
    if schema:
        for key, expected_type in schema.items():
            if key in config and not isinstance(config[key], expected_type):
                raise ValidationError(
                    f"Config key '{key}' must be {expected_type.__name__}, "
                    f"got {type(config[key]).__name__}"
                )
    
    return True


def validate_cron_expression(expression: str) -> bool:
    """
    Validate cron expression (basic validation)

    Args:
        expression: Cron expression

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    parts = expression.split()
    
    if len(parts) not in [5, 6]:
        raise ValidationError(
            f"Invalid cron expression: {expression}. "
            "Expected 5 or 6 fields (minute hour day month weekday [year])"
        )
    
    return True


def validate_numeric_range(value: Union[int, float], min_val: float = None,
                          max_val: float = None, name: str = "Value") -> bool:
    """
    Validate numeric value is within range

    Args:
        value: Value to validate
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        name: Name for error messages

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    if min_val is not None and value < min_val:
        raise ValidationError(f"{name} must be >= {min_val}, got {value}")
    
    if max_val is not None and value > max_val:
        raise ValidationError(f"{name} must be <= {max_val}, got {value}")
    
    return True


def validate_string_length(value: str, min_len: int = 0, max_len: int = None,
                          name: str = "String") -> bool:
    """
    Validate string length

    Args:
        value: String to validate
        min_len: Minimum length
        max_len: Maximum length
        name: Name for error messages

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    if len(value) < min_len:
        raise ValidationError(f"{name} must be at least {min_len} characters")
    
    if max_len and len(value) > max_len:
        raise ValidationError(f"{name} must be at most {max_len} characters")
    
    return True


def validate_choice(value: Any, choices: List[Any], name: str = "Value") -> bool:
    """
    Validate value is in allowed choices

    Args:
        value: Value to validate
        choices: Allowed values
        name: Name for error messages

    Returns:
        True if valid
    
    Raises:
        ValidationError if invalid
    """
    if value not in choices:
        raise ValidationError(f"{name} must be one of {choices}, got {value}")
    
    return True
