"""
Decorators - Utility decorators for report automation
"""
import functools
import time
from typing import Any, Callable, Optional
from src.core.logger import logger


def retry(max_attempts: int = 3, delay: float = 1.0, 
         exceptions: tuple = (Exception,), backoff: float = 2.0):
    """
    Retry decorator with exponential backoff

    Args:
        max_attempts: Maximum retry attempts
        delay: Initial delay between retries
        exceptions: Exceptions to catch
        backoff: Backoff multiplier

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            
            raise last_exception
        
        return wrapper
    return decorator


def timer(func: Callable) -> Callable:
    """
    Timer decorator to log execution time

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"{func.__name__} completed in {elapsed:.2f}s")
        return result
    
    return wrapper


def log_call(func: Callable) -> Callable:
    """
    Log function calls

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        logger.debug(f"Calling {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"{func.__name__} completed successfully")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} failed: {str(e)}")
            raise
    
    return wrapper


def catch_exceptions(default_value: Any = None, exceptions: tuple = (Exception,),
                    log_error: bool = True):
    """
    Catch exceptions and return default value

    Args:
        default_value: Value to return on exception
        exceptions: Exceptions to catch
        log_error: Whether to log the error

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                if log_error:
                    logger.error(f"{func.__name__} failed: {str(e)}")
                return default_value
        
        return wrapper
    return decorator


def cache(ttl_seconds: int = 300):
    """
    Simple cache decorator with TTL

    Args:
        ttl_seconds: Time to live in seconds

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        cache_data = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Create cache key from arguments
            key = str(args) + str(sorted(kwargs.items()))
            
            # Check cache
            if key in cache_data:
                value, timestamp = cache_data[key]
                if time.time() - timestamp < ttl_seconds:
                    logger.debug(f"Cache hit for {func.__name__}")
                    return value
            
            # Call function and cache result
            result = func(*args, **kwargs)
            cache_data[key] = (result, time.time())
            
            return result
        
        # Add method to clear cache
        def clear_cache():
            cache_data.clear()
        
        wrapper.clear_cache = clear_cache
        return wrapper
    
    return decorator


def validate_args(**validators):
    """
    Validate function arguments

    Args:
        **validators: Argument name to validator function mapping

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get argument names
            import inspect
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            
            # Validate arguments
            for arg_name, validator in validators.items():
                if arg_name in bound.arguments:
                    value = bound.arguments[arg_name]
                    if not validator(value):
                        raise ValueError(f"Invalid value for {arg_name}: {value}")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def deprecated(message: str = None):
    """
    Mark function as deprecated

    Args:
        message: Deprecation message

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            msg = message or f"{func.__name__} is deprecated"
            logger.warning(f"DEPRECATED: {msg}")
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def singleton(cls):
    """
    Singleton decorator for classes

    Args:
        cls: Class to make singleton

    Returns:
        Singleton class
    """
    instances = {}
    
    @functools.wraps(cls)
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return get_instance


def run_async(func: Callable) -> Callable:
    """
    Run function in a thread

    Args:
        func: Function to run async

    Returns:
        Decorated function
    """
    import threading
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    
    return wrapper


def throttle(calls_per_second: float):
    """
    Throttle function calls

    Args:
        calls_per_second: Maximum calls per second

    Returns:
        Decorated function
    """
    min_interval = 1.0 / calls_per_second
    
    def decorator(func: Callable) -> Callable:
        last_call = [0.0]
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            elapsed = time.time() - last_call[0]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            
            result = func(*args, **kwargs)
            last_call[0] = time.time()
            
            return result
        
        return wrapper
    return decorator
