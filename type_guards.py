"""Type guards and runtime validation for safer code."""

from typing import Any, TypeGuard, Optional, Dict, List, Union
from pathlib import Path
from urllib.parse import urlparse
import re


def is_valid_url(value: Any) -> TypeGuard[str]:
    """Check if value is a valid URL."""
    if not isinstance(value, str):
        return False
    
    try:
        result = urlparse(value)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_valid_http_url(value: Any) -> TypeGuard[str]:
    """Check if value is a valid HTTP/HTTPS URL."""
    if not is_valid_url(value):
        return False
    
    parsed = urlparse(value)
    return parsed.scheme in ('http', 'https')


def is_safe_path(value: Any, base_dir: Optional[Path] = None) -> TypeGuard[Path]:
    """Check if value is a safe file path (no traversal)."""
    if not isinstance(value, (str, Path)):
        return False
    
    try:
        path = Path(value)
        
        # Check for path traversal attempts
        if '..' in str(path):
            return False
        
        # If base_dir provided, ensure path is within it
        if base_dir:
            try:
                path.resolve().relative_to(base_dir.resolve())
            except ValueError:
                return False
        
        return True
    except Exception:
        return False


def is_valid_domain(value: Any) -> TypeGuard[str]:
    """Check if value is a valid domain name."""
    if not isinstance(value, str):
        return False
    
    # Basic domain validation regex
    domain_regex = re.compile(
        r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?$'
    )
    
    return bool(domain_regex.match(value))


def is_valid_file_extension(value: Any, allowed_extensions: Optional[List[str]] = None) -> TypeGuard[str]:
    """Check if value is a valid file extension."""
    if not isinstance(value, str):
        return False
    
    if not value.startswith('.'):
        return False
    
    # Must be alphanumeric after the dot
    if not re.match(r'^\.[a-zA-Z0-9]+$', value):
        return False
    
    if allowed_extensions:
        return value.lower() in [ext.lower() for ext in allowed_extensions]
    
    return True


def is_valid_css_content(value: Any) -> TypeGuard[str]:
    """Check if value appears to be valid CSS content."""
    if not isinstance(value, str):
        return False
    
    # Basic CSS validation - contains CSS-like patterns
    css_patterns = [
        r'\{[^}]*\}',  # CSS rules
        r'[a-zA-Z-]+\s*:\s*[^;]+;',  # CSS properties
        r'@import\s+',  # Import statements
        r'@media\s+',  # Media queries
    ]
    
    return any(re.search(pattern, value) for pattern in css_patterns)


def is_valid_html_content(value: Any) -> TypeGuard[str]:
    """Check if value appears to be valid HTML content."""
    if not isinstance(value, str):
        return False
    
    # Basic HTML validation - contains HTML-like patterns
    html_patterns = [
        r'<html[^>]*>',
        r'<body[^>]*>',
        r'<head[^>]*>',
        r'<[a-zA-Z]+[^>]*>.*</[a-zA-Z]+>',  # Any HTML tag
    ]
    
    return any(re.search(pattern, value, re.IGNORECASE) for pattern in html_patterns)


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class RuntimeValidator:
    """Runtime validation helper."""
    
    @staticmethod
    def validate_url(url: Any, allow_relative: bool = False) -> str:
        """Validate and return a URL."""
        if not isinstance(url, str):
            raise ValidationError(f"URL must be a string, got {type(url).__name__}")
        
        if not url:
            raise ValidationError("URL cannot be empty")
        
        if allow_relative:
            # Just check it's a non-empty string
            return url
        
        if not is_valid_http_url(url):
            raise ValidationError(f"Invalid HTTP/HTTPS URL: {url}")
        
        return url
    
    @staticmethod
    def validate_path(path: Any, must_exist: bool = False, 
                     base_dir: Optional[Path] = None) -> Path:
        """Validate and return a Path."""
        if isinstance(path, str):
            path = Path(path)
        elif not isinstance(path, Path):
            raise ValidationError(f"Path must be string or Path, got {type(path).__name__}")
        
        if not is_safe_path(path, base_dir):
            raise ValidationError(f"Unsafe path: {path}")
        
        if must_exist and not path.exists():
            raise ValidationError(f"Path does not exist: {path}")
        
        return path
    
    @staticmethod
    def validate_domain(domain: Any) -> str:
        """Validate and return a domain."""
        if not is_valid_domain(domain):
            raise ValidationError(f"Invalid domain: {domain}")
        
        return domain.lower()
    
    @staticmethod
    def validate_file_size(size: Any, max_size: Optional[int] = None) -> int:
        """Validate file size."""
        if not isinstance(size, (int, float)):
            raise ValidationError(f"Size must be numeric, got {type(size).__name__}")
        
        size = int(size)
        
        if size < 0:
            raise ValidationError(f"Size cannot be negative: {size}")
        
        if max_size and size > max_size:
            raise ValidationError(f"Size {size} exceeds maximum {max_size}")
        
        return size
    
    @staticmethod
    def validate_headers(headers: Any) -> Dict[str, str]:
        """Validate HTTP headers."""
        if headers is None:
            return {}
        
        if not isinstance(headers, dict):
            raise ValidationError(f"Headers must be a dict, got {type(headers).__name__}")
        
        validated = {}
        for key, value in headers.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValidationError(f"Header key and value must be strings: {key}={value}")
            
            # Basic header name validation
            if not re.match(r'^[a-zA-Z0-9\-_]+$', key):
                raise ValidationError(f"Invalid header name: {key}")
            
            validated[key] = value
        
        return validated
    
    @staticmethod
    def validate_config(config: Any, schema: Dict[str, type]) -> Dict[str, Any]:
        """Validate configuration against a schema."""
        if not isinstance(config, dict):
            raise ValidationError(f"Config must be a dict, got {type(config).__name__}")
        
        validated = {}
        
        for key, expected_type in schema.items():
            if key not in config:
                continue
            
            value = config[key]
            
            # Handle Union types
            if hasattr(expected_type, '__origin__') and expected_type.__origin__ is Union:
                # Get the non-None type from Optional
                types = [t for t in expected_type.__args__ if t != type(None)]
                if types and not isinstance(value, tuple(types)):
                    raise ValidationError(
                        f"Config '{key}' must be {types}, got {type(value).__name__}"
                    )
            elif not isinstance(value, expected_type):
                raise ValidationError(
                    f"Config '{key}' must be {expected_type.__name__}, got {type(value).__name__}"
                )
            
            validated[key] = value
        
        return validated


def safe_url_join(base: str, url: str) -> Optional[str]:
    """Safely join URLs with validation."""
    try:
        base = RuntimeValidator.validate_url(base)
        # url can be relative
        url = RuntimeValidator.validate_url(url, allow_relative=True)
        
        from urllib.parse import urljoin
        result = urljoin(base, url)
        
        # Validate result is still a valid URL
        return RuntimeValidator.validate_url(result)
    except ValidationError:
        return None


def safe_file_write(file_path: Union[str, Path], content: Union[str, bytes],
                   base_dir: Optional[Path] = None) -> bool:
    """Safely write to a file with validation."""
    try:
        path = RuntimeValidator.validate_path(file_path, base_dir=base_dir)
        
        # Create parent directory if needed
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if isinstance(content, str):
            path.write_text(content, encoding='utf-8')
        else:
            path.write_bytes(content)
        
        return True
    except (ValidationError, OSError):
        return False