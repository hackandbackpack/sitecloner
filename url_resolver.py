"""URL resolution and normalization utilities."""

import re
import hashlib
import unicodedata
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode, unquote
from typing import Optional, Set, Tuple
from pathlib import Path, PurePath


class URLResolver:
    """Handles URL resolution, normalization, and validation."""
    
    def __init__(self, base_url: str):
        """Initialize with a base URL for resolving relative URLs."""
        self.base_url = base_url
        self.base_parsed = urlparse(base_url)
        self.base_domain = self.base_parsed.netloc.lower()
        
        # Common file extensions that shouldn't be processed for assets
        self.binary_extensions = {
            '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2',
            '.exe', '.msi', '.dmg', '.pkg', '.deb', '.rpm',
            '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'
        }
    
    def resolve_url(self, url: str, current_page_url: Optional[str] = None) -> Optional[str]:
        """
        Resolve a URL against the base URL or current page URL.
        Returns None if the URL is invalid or should be skipped.
        """
        if not url or not url.strip():
            return None
        
        url = url.strip()
        
        # Skip data URLs, javascript URLs, mailto URLs, etc.
        if self._should_skip_url(url):
            return None
        
        # Handle protocol-relative URLs (//example.com/path)
        if url.startswith('//'):
            # Use the same scheme as the current page or base URL
            base_for_resolution = current_page_url or self.base_url
            base_scheme = urlparse(base_for_resolution).scheme
            url = f"{base_scheme}:{url}"
        
        # Validate URL format before processing
        if not self._is_valid_url_format(url):
            return None
        
        # Use current page URL as base if provided, otherwise use the main base URL
        base_for_resolution = current_page_url or self.base_url
        
        try:
            # Resolve the URL
            resolved = urljoin(base_for_resolution, url)
            
            # Additional validation after resolution
            if not self._is_safe_resolved_url(resolved):
                return None
            
            # Parse and normalize
            parsed = urlparse(resolved)
            
            # Remove fragment (anchor)
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
            
            return self._normalize_url(normalized)
            
        except Exception:
            return None
    
    def _should_skip_url(self, url: str) -> bool:
        """Check if URL should be skipped entirely."""
        url_lower = url.lower()
        
        # Skip various protocol schemes
        skip_schemes = ['data:', 'javascript:', 'mailto:', 'tel:', 'ftp:', 'file:', 'blob:']
        if any(url_lower.startswith(scheme) for scheme in skip_schemes):
            return True
        
        # Skip anchor-only links
        if url.startswith('#'):
            return True
        
        # Skip empty or whitespace-only URLs
        if not url.strip():
            return True
        
        # Skip potentially malicious URLs
        if self._contains_suspicious_patterns(url):
            return True
        
        return False
    
    def _is_valid_url_format(self, url: str) -> bool:
        """Validate URL format for basic safety."""
        try:
            parsed = urlparse(url)
            
            # Must have a scheme (http/https)
            if not parsed.scheme or parsed.scheme.lower() not in ['http', 'https']:
                return False
            
            # Must have a netloc (domain)
            if not parsed.netloc:
                return False
            
            # Check for excessively long URLs
            if len(url) > 2048:
                return False
            
            return True
        except Exception:
            return False
    
    def _is_safe_resolved_url(self, url: str) -> bool:
        """Additional safety checks after URL resolution."""
        try:
            parsed = urlparse(url)
            
            # Check for localhost or private IP ranges
            netloc_lower = parsed.netloc.lower()
            if any(netloc_lower.startswith(prefix) for prefix in [
                'localhost', '127.', '10.', '192.168.', '172.'
            ]):
                return False
            
            # Check for file:// protocol
            if parsed.scheme.lower() == 'file':
                return False
            
            return True
        except Exception:
            return False
    
    def _contains_suspicious_patterns(self, url: str) -> bool:
        """Check for suspicious patterns in URLs."""
        suspicious_patterns = [
            '../', '..\\', '%2e%2e', '%2f%2e%2e',
            'file://', 'ftp://', 'ldap://', 'gopher://'
        ]
        
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in suspicious_patterns)
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL by removing redundant elements."""
        parsed = urlparse(url)
        
        # Normalize path
        path_parts = [part for part in parsed.path.split('/') if part and part != '.']
        normalized_path_parts = []
        
        for part in path_parts:
            if part == '..':
                if normalized_path_parts:
                    normalized_path_parts.pop()
            else:
                normalized_path_parts.append(part)
        
        normalized_path = '/' + '/'.join(normalized_path_parts)
        if parsed.path.endswith('/') and not normalized_path.endswith('/'):
            normalized_path += '/'
        
        # Normalize query parameters
        if parsed.query:
            query_params = parse_qs(parsed.query)
            # Sort parameters for consistency
            sorted_params = sorted(query_params.items())
            normalized_query = urlencode(sorted_params, doseq=True)
        else:
            normalized_query = ''
        
        return urlunparse((
            parsed.scheme,
            parsed.netloc.lower(),
            normalized_path,
            parsed.params,
            normalized_query,
            ''
        ))
    
    def is_same_domain(self, url: str) -> bool:
        """Check if URL is from the same domain as the base URL."""
        parsed = urlparse(url)
        return parsed.netloc.lower() == self.base_domain
    
    def get_domain(self, url: str) -> str:
        """Get the domain from a URL."""
        return urlparse(url).netloc.lower()
    
    def url_to_local_path(self, url: str, output_dir: Path) -> Path:
        """
        Convert a URL to a local file path with comprehensive security validation.
        Prevents path traversal and ensures all paths stay within output directory.
        """
        parsed = urlparse(url)
        
        # Special handling for the main page (root URL)
        if url == self.base_url:
            return output_dir / 'index.html'
        
        # Start with the domain (sanitized)
        domain = self._secure_sanitize_filename(parsed.netloc)
        if not domain:
            domain = 'unknown_domain'
        
        path_parts = [domain]
        
        # Process path components with security validation
        if parsed.path and parsed.path != '/':
            # URL decode the path first
            decoded_path = unquote(parsed.path)
            
            # Split and sanitize each component
            path_components = [comp for comp in decoded_path.split('/') if comp]
            
            for comp in path_components:
                sanitized_comp = self._secure_sanitize_filename(comp)
                if sanitized_comp and sanitized_comp not in ['.', '..']:
                    path_parts.append(sanitized_comp)
        
        # Handle query parameters securely
        if parsed.query:
            if path_parts:
                # Create a secure hash of query parameters
                query_hash = hashlib.md5(parsed.query.encode('utf-8')).hexdigest()[:8]
                last_part = path_parts[-1]
                if '.' in last_part:
                    name, ext = last_part.rsplit('.', 1)
                    # Ensure extension is safe
                    ext = self._sanitize_extension(ext)
                    path_parts[-1] = f"{name}_q{query_hash}.{ext}"
                else:
                    path_parts[-1] = f"{last_part}_q{query_hash}.html"
        
        # Ensure we have proper file extension
        if not path_parts or not path_parts[-1] or '.' not in path_parts[-1]:
            path_parts.append('index.html')
        elif not self._has_valid_extension(path_parts[-1]):
            # Add .html extension for files without clear extensions
            last_part = path_parts[-1]
            if not self._is_binary_extension(last_part):
                path_parts[-1] = f"{last_part}.html"
        
        # Construct the path and validate it's safe
        try:
            local_path = output_dir / Path(*path_parts)
            
            # Critical security check: ensure path is within output directory
            resolved_output = output_dir.resolve()
            resolved_local = local_path.resolve()
            
            # Check if the resolved path starts with the output directory
            try:
                resolved_local.relative_to(resolved_output)
            except ValueError:
                # Path traversal attempt detected - use safe fallback
                safe_filename = self._create_safe_fallback_name(url)
                return output_dir / safe_filename
            
            return local_path
            
        except (OSError, ValueError) as e:
            # If path construction fails, create a safe fallback
            safe_filename = self._create_safe_fallback_name(url)
            return output_dir / safe_filename
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem compatibility (legacy method)."""
        return self._secure_sanitize_filename(filename)
    
    def _secure_sanitize_filename(self, filename: str) -> str:
        """Securely sanitize filename with comprehensive validation."""
        if not filename:
            return 'unnamed'
        
        # Normalize Unicode characters to prevent homograph attacks
        filename = unicodedata.normalize('NFKC', filename)
        
        # Remove null bytes and other dangerous characters
        filename = filename.replace('\x00', '').replace('\x0a', '').replace('\x0d', '')
        
        # Remove/replace problematic characters with strict whitelist approach
        # Allow only alphanumeric, dots, hyphens, underscores, and spaces
        sanitized = re.sub(r'[^\w\s.-]', '_', filename)
        
        # Remove control characters
        sanitized = re.sub(r'[\x00-\x1f\x7f]', '', sanitized)
        
        # Remove path traversal attempts
        sanitized = sanitized.replace('..', '_').replace('/', '_').replace('\\', '_')
        
        # Remove leading/trailing dots and spaces
        sanitized = sanitized.strip('. ')
        
        # Prevent Windows reserved names
        windows_reserved = {
            'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5',
            'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3', 'LPT4',
            'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }
        
        if sanitized.upper() in windows_reserved:
            sanitized = f"_{sanitized}"
        
        # Limit length
        if len(sanitized) > 100:
            # Keep extension if present
            if '.' in sanitized:
                name, ext = sanitized.rsplit('.', 1)
                max_name_length = 100 - len(ext) - 1
                sanitized = f"{name[:max_name_length]}.{ext}"
            else:
                sanitized = sanitized[:100]
        
        # Ensure it's not empty
        if not sanitized or sanitized == '.':
            sanitized = 'unnamed'
        
        return sanitized
    
    def _sanitize_extension(self, extension: str) -> str:
        """Sanitize file extension."""
        if not extension:
            return 'html'
        
        # Remove dangerous characters from extension
        ext = re.sub(r'[^a-zA-Z0-9]', '', extension.lower())
        
        # Limit extension length
        if len(ext) > 10:
            ext = ext[:10]
        
        # Default to html if empty
        if not ext:
            ext = 'html'
        
        return ext
    
    def _has_valid_extension(self, filename: str) -> bool:
        """Check if filename has a valid extension."""
        if '.' not in filename:
            return False
        
        ext = filename.split('.')[-1].lower()
        valid_extensions = {
            'html', 'htm', 'css', 'js', 'json', 'xml', 'txt', 'svg',
            'jpg', 'jpeg', 'png', 'gif', 'webp', 'ico', 'bmp',
            'woff', 'woff2', 'ttf', 'otf', 'eot',
            'mp4', 'webm', 'mp3', 'wav', 'pdf', 'zip'
        }
        
        return ext in valid_extensions
    
    def _is_binary_extension(self, filename: str) -> bool:
        """Check if filename has a binary extension."""
        if '.' not in filename:
            return False
        
        ext = filename.split('.')[-1].lower()
        binary_extensions = {
            'jpg', 'jpeg', 'png', 'gif', 'webp', 'ico', 'bmp', 'tiff',
            'woff', 'woff2', 'ttf', 'otf', 'eot',
            'mp4', 'webm', 'mp3', 'wav', 'pdf', 'zip', 'exe', 'dll'
        }
        
        return ext in binary_extensions
    
    def _create_safe_fallback_name(self, url: str) -> str:
        """Create a safe fallback filename when path construction fails."""
        # Create a hash of the URL for uniqueness
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
        return f"safe_file_{url_hash}.html"
    
    def should_process_for_assets(self, url: str) -> bool:
        """
        Check if a file should be processed to extract additional assets.
        Returns True for HTML, CSS, and similar text files.
        """
        parsed = urlparse(url)
        path = Path(parsed.path)
        ext = path.suffix.lower()
        
        # Process these file types for embedded assets
        processable_extensions = {
            '.html', '.htm', '.xhtml',
            '.css', '.scss', '.sass',
            '.js', '.json',
            '.xml', '.svg'
        }
        
        return ext in processable_extensions
    
    def extract_urls_from_css_content(self, css_content: str, base_url: str) -> Set[str]:
        """Extract URLs from CSS content using regex patterns."""
        urls = set()
        
        # Pattern for url() functions in CSS
        url_pattern = r'url\s*\(\s*["\']?([^"\')]+)["\']?\s*\)'
        
        matches = re.finditer(url_pattern, css_content, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            resolved = self.resolve_url(url, base_url)
            if resolved:
                urls.add(resolved)
        
        # Pattern for @import statements
        import_pattern = r'@import\s+["\']([^"\']+)["\']'
        
        matches = re.finditer(import_pattern, css_content, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            resolved = self.resolve_url(url, base_url)
            if resolved:
                urls.add(resolved)
        
        return urls
    
    def create_url_mapping(self, original_url: str, local_path: Path, output_dir: Path) -> str:
        """Create a relative URL mapping from original URL to local path."""
        try:
            relative_path = local_path.relative_to(output_dir)
            # Convert to URL-style path with forward slashes
            return str(relative_path).replace('\\', '/')
        except ValueError:
            # If relative_to fails, return the filename
            return local_path.name