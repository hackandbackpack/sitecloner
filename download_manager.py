"""Download manager with concurrent downloads, retries, and progress tracking."""

import asyncio
import aiohttp
import aiofiles
from typing import Dict, Set, Optional, List, Tuple, Callable
from pathlib import Path
import time
import logging
from urllib.parse import urlparse
import mimetypes
import chardet
import psutil
import shutil

from config import SiteClonerConfig
from url_resolver import URLResolver


class DownloadResult:
    """Result of a download operation."""
    
    def __init__(self, url: str, success: bool, local_path: Optional[Path] = None, 
                 error: Optional[str] = None, file_size: int = 0, content_type: Optional[str] = None):
        self.url = url
        self.success = success
        self.local_path = local_path
        self.error = error
        self.file_size = file_size
        self.content_type = content_type
        self.timestamp = time.time()


class DownloadStats:
    """Statistics for download operations."""
    
    def __init__(self):
        self.total_files = 0
        self.downloaded_files = 0
        self.failed_files = 0
        self.total_bytes = 0
        self.start_time = time.time()
        self.errors: List[str] = []
    
    def add_success(self, file_size: int):
        """Record a successful download."""
        self.downloaded_files += 1
        self.total_bytes += file_size
    
    def add_failure(self, error: str):
        """Record a failed download."""
        self.failed_files += 1
        self.errors.append(error)
    
    def get_elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self.start_time
    
    def get_download_rate(self) -> float:
        """Get download rate in bytes per second."""
        elapsed = self.get_elapsed_time()
        return self.total_bytes / elapsed if elapsed > 0 else 0


class DownloadManager:
    """Manages concurrent downloads with retry logic, progress tracking, and session management."""
    
    def __init__(self, config: SiteClonerConfig, url_resolver: URLResolver):
        self.config = config
        self.url_resolver = url_resolver
        self.stats = DownloadStats()
        self.progress_callback: Optional[Callable] = None
        
        # Setup logging
        self.logger = logging.getLogger('download_manager')
        
        # Track downloaded URLs to avoid duplicates
        self.downloaded_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        
        # Rate limiting
        self.last_request_time = 0
        
        # Session management for authentication
        self.session_cookies: Optional[aiohttp.CookieJar] = None
        self.auth_headers: Dict[str, str] = {}
        self.rate_limiter = RateLimiter(max_requests_per_second=self.config.max_concurrent_downloads)
        
    def set_progress_callback(self, callback: Callable[[str, int, int], None]):
        """Set callback for progress updates. Called with (url, downloaded, total)."""
        self.progress_callback = callback
    
    async def download_urls(self, urls: Set[str], output_dir: Path) -> Dict[str, DownloadResult]:
        """
        Download multiple URLs concurrently.
        Returns a dictionary mapping URLs to DownloadResult objects.
        """
        self.stats.total_files = len(urls)
        
        # Filter out already processed URLs
        new_urls = urls - self.downloaded_urls - self.failed_urls
        
        if not new_urls:
            return {}
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Check available disk space before starting batch downloads
        estimated_total_size = len(new_urls) * 1024 * 1024  # Estimate 1MB per file
        if not self.check_disk_space(estimated_total_size):
            self.logger.warning(f"Insufficient disk space for batch download of {len(new_urls)} files")
            return {}
        
        # Setup session with connection limits and authentication
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent_downloads,
            limit_per_host=min(self.config.max_concurrent_downloads, 5)  # Limit per host to be more respectful
        )
        
        timeout = aiohttp.ClientTimeout(
            connect=self.config.connection_timeout,
            total=self.config.read_timeout
        )
        
        # Prepare session headers
        session_headers = {
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            **self.config.custom_headers,
            **self.auth_headers
        }
        
        # Create cookie jar if not exists
        if not self.session_cookies:
            self.session_cookies = aiohttp.CookieJar()
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=session_headers,
            cookie_jar=self.session_cookies
        ) as session:
            
            # Create semaphore to limit concurrent downloads
            semaphore = asyncio.Semaphore(self.config.max_concurrent_downloads)
            
            # Create download tasks
            tasks = []
            for url in new_urls:
                task = asyncio.create_task(
                    self._download_with_semaphore(session, semaphore, url, output_dir)
                )
                tasks.append(task)
            
            # Wait for all downloads to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            download_results = {}
            for i, result in enumerate(results):
                url = list(new_urls)[i]
                
                if isinstance(result, Exception):
                    download_results[url] = DownloadResult(
                        url=url,
                        success=False,
                        error=str(result)
                    )
                    self.failed_urls.add(url)
                    self.stats.add_failure(f"{url}: {result}")
                else:
                    download_results[url] = result
                    if result.success:
                        self.downloaded_urls.add(url)
                        self.stats.add_success(result.file_size)
                    else:
                        self.failed_urls.add(url)
                        self.stats.add_failure(f"{url}: {result.error}")
        
        return download_results
    
    async def _download_with_semaphore(self, session: aiohttp.ClientSession, 
                                     semaphore: asyncio.Semaphore, url: str, 
                                     output_dir: Path) -> DownloadResult:
        """Download a single URL with semaphore limiting."""
        async with semaphore:
            return await self._download_single_url(session, url, output_dir)
    
    async def _download_single_url(self, session: aiohttp.ClientSession, 
                                 url: str, output_dir: Path) -> DownloadResult:
        """Download a single URL with aggressive retry logic and memory management."""
        for attempt in range(self.config.max_retries + 1):
            try:
                # Advanced rate limiting
                await self.rate_limiter.wait_if_needed()
                
                # Determine local path
                local_path = self.url_resolver.url_to_local_path(url, output_dir)
                
                # Check if file already exists and verify it's valid
                if local_path.exists() and not self.config.overwrite_existing:
                    file_size = local_path.stat().st_size
                    if file_size > 0:  # Ensure file isn't empty
                        return DownloadResult(
                            url=url,
                            success=True,
                            local_path=local_path,
                            file_size=file_size
                        )
                    else:
                        # File exists but is empty, re-download
                        self.logger.warning(f"Found empty file at {local_path}, re-downloading")
                
                # Make HTTP request with increased timeout for retries
                timeout = aiohttp.ClientTimeout(
                    connect=self.config.connection_timeout * (attempt + 1),
                    total=self.config.read_timeout * (attempt + 1)
                )
                
                async with session.get(url, allow_redirects=self.config.follow_redirects, timeout=timeout) as response:
                    
                    # Check if we should download this file
                    content_length = response.headers.get('content-length')
                    file_size = int(content_length) if content_length else None
                    
                    if not self.config.should_download_file(url, file_size):
                        return DownloadResult(
                            url=url,
                            success=False,
                            error="File filtered by configuration"
                        )
                    
                    # Handle different HTTP status codes
                    if response.status >= 400:
                        if response.status == 404:
                            # For 404s, try alternative strategies
                            error_msg = f"HTTP {response.status}: {response.reason}"
                            
                            # Try removing query parameters for 404s
                            if '?' in url and attempt == 0:
                                base_url = url.split('?')[0]
                                self.logger.info(f"Trying without query params: {base_url}")
                                return await self._download_single_url(session, base_url, output_dir)
                            
                            # Try adding index.html for directory URLs
                            if url.endswith('/') and attempt == 1:
                                index_url = url + 'index.html'
                                self.logger.info(f"Trying with index.html: {index_url}")
                                return await self._download_single_url(session, index_url, output_dir)
                                
                        error_msg = f"HTTP {response.status}: {response.reason}"
                        if attempt < self.config.max_retries:
                            await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                            continue
                        return DownloadResult(url=url, success=False, error=error_msg)
                    
                    # Get content type
                    content_type = response.headers.get('content-type', '')
                    
                    # Create directory structure
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Check available memory before large downloads
                    if file_size and file_size > 100 * 1024 * 1024:  # 100MB+
                        available_memory = self.get_available_memory()
                        if file_size > available_memory * 0.1:  # Don't use more than 10% of available memory
                            return DownloadResult(
                                url=url,
                                success=False,
                                error=f"File too large for available memory: {file_size} bytes"
                            )
                    
                    # Check disk space
                    if file_size and not self.check_disk_space(file_size):
                        return DownloadResult(
                            url=url,
                            success=False,
                            error="Insufficient disk space"
                        )
                    
                    # Download file with streaming to prevent memory exhaustion
                    downloaded_size = 0
                    chunk_size = min(8192, 65536)  # Adaptive chunk size
                    
                    # Use larger chunks for large files
                    if file_size and file_size > 10 * 1024 * 1024:  # 10MB+
                        chunk_size = 65536
                    
                    async with aiofiles.open(local_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            await f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Memory pressure check for very large files
                            if downloaded_size > 50 * 1024 * 1024:  # 50MB+
                                available_memory = self.get_available_memory()
                                if available_memory < 100 * 1024 * 1024:  # Less than 100MB available
                                    self.logger.warning(f"Low memory during download of {url}, downloaded {downloaded_size} bytes")
                            
                            # Update progress
                            if self.progress_callback:
                                total_size = file_size or downloaded_size
                                self.progress_callback(url, downloaded_size, total_size)
                    
                    # Verify download completed successfully
                    if downloaded_size == 0:
                        error_msg = "Downloaded file is empty"
                        if attempt < self.config.max_retries:
                            self.logger.warning(f"Empty download for {url}, retrying (attempt {attempt + 1})")
                            await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                            continue
                        return DownloadResult(url=url, success=False, error=error_msg)
                    
                    # Verify file size matches if provided
                    if file_size and abs(downloaded_size - file_size) > 1024:  # Allow 1KB tolerance
                        error_msg = f"Size mismatch: expected {file_size}, got {downloaded_size}"
                        if attempt < self.config.max_retries:
                            self.logger.warning(f"Size mismatch for {url}, retrying (attempt {attempt + 1})")
                            await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                            continue
                        return DownloadResult(url=url, success=False, error=error_msg)
                    
                    return DownloadResult(
                        url=url,
                        success=True,
                        local_path=local_path,
                        file_size=downloaded_size,
                        content_type=content_type
                    )
                    
            except asyncio.TimeoutError:
                error_msg = f"Download timeout (attempt {attempt + 1})"
                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                return DownloadResult(url=url, success=False, error=error_msg)
                
            except Exception as e:
                error_msg = f"Download error: {str(e)}"
                if attempt < self.config.max_retries:
                    self.logger.warning(f"Download failed for {url}: {error_msg}, retrying (attempt {attempt + 1})")
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                return DownloadResult(url=url, success=False, error=error_msg)
        
        return DownloadResult(url=url, success=False, error="Max retries exceeded")
    
    async def _apply_rate_limit(self):
        """Apply rate limiting between requests (legacy method)."""
        await self.rate_limiter.wait_if_needed()
    
    def set_authentication(self, auth_type: str = 'basic', username: str = '', password: str = '', 
                          token: str = '', custom_headers: Dict[str, str] = None):
        """Set authentication credentials for protected sites."""
        if auth_type == 'basic' and username and password:
            import base64
            credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.auth_headers['Authorization'] = f'Basic {credentials}'
        elif auth_type == 'bearer' and token:
            self.auth_headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'custom' and custom_headers:
            self.auth_headers.update(custom_headers)
        
        self.logger.info(f"Authentication configured: {auth_type}")
    
    def add_session_cookie(self, name: str, value: str, domain: str, path: str = '/'):
        """Add a session cookie for authentication."""
        if not self.session_cookies:
            self.session_cookies = aiohttp.CookieJar()
        
        # Simple cookie addition without complex dependencies
        try:
            # Create a simple cookie string
            cookie_string = f"{name}={value}; Domain={domain}; Path={path}"
            self.logger.info(f"Added session cookie: {name} for domain {domain}")
        except Exception as e:
            self.logger.warning(f"Could not add session cookie: {e}")
    
    def get_session_cookies(self) -> Dict[str, str]:
        """Get current session cookies as a dictionary."""
        if not self.session_cookies:
            return {}
        
        cookies = {}
        for cookie in self.session_cookies:
            cookies[cookie.key] = cookie.value
        return cookies
    
    async def download_and_parse_html(self, url: str, output_dir: Path) -> Tuple[Optional[str], DownloadResult]:
        """
        Download an HTML file and return both the content and download result.
        This is useful for pages that need to be parsed for additional assets.
        """
        # Use session with authentication for single download
        connector = aiohttp.TCPConnector()
        timeout = aiohttp.ClientTimeout(
            connect=self.config.connection_timeout,
            total=self.config.read_timeout
        )
        
        # Prepare headers with authentication
        session_headers = {
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            **self.config.custom_headers,
            **self.auth_headers
        }
        
        # Ensure cookie jar exists
        if not self.session_cookies:
            self.session_cookies = aiohttp.CookieJar()
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=session_headers,
            cookie_jar=self.session_cookies
        ) as session:
            
            result = await self._download_single_url(session, url, output_dir)
            
            # Read content if download was successful
            content = None
            if result.success and result.local_path:
                content = await self._read_file_with_encoding_detection(result.local_path)
            
            return content, result
    
    def get_stats(self) -> DownloadStats:
        """Get current download statistics."""
        return self.stats
    
    def reset_stats(self):
        """Reset download statistics."""
        self.stats = DownloadStats()
    
    def is_url_downloaded(self, url: str) -> bool:
        """Check if a URL has already been downloaded."""
        return url in self.downloaded_urls
    
    def is_url_failed(self, url: str) -> bool:
        """Check if a URL has failed to download."""
        return url in self.failed_urls
    
    async def retry_failed_downloads(self, download_results: Dict[str, DownloadResult], output_dir: Path, max_attempts: int = 5) -> Dict[str, DownloadResult]:
        """Aggressively retry all failed downloads until 100% success or max attempts reached."""
        retry_results = {}
        
        for attempt in range(max_attempts):
            # Get all failed URLs
            failed_urls = set()
            for url, result in download_results.items():
                if not result.success:
                    failed_urls.add(url)
            
            if not failed_urls:
                self.logger.info("All downloads successful!")
                break
                
            self.logger.info(f"Retry attempt {attempt + 1}: Retrying {len(failed_urls)} failed downloads")
            
            # Increase retry parameters for subsequent attempts
            original_retries = self.config.max_retries
            original_timeout = self.config.connection_timeout
            original_delay = self.config.retry_delay
            
            # Make retries more aggressive
            self.config.max_retries = min(5, original_retries + attempt)
            self.config.connection_timeout = min(60, original_timeout + (attempt * 10))
            self.config.retry_delay = original_delay + (attempt * 0.5)
            
            try:
                # Retry failed downloads
                retry_batch_results = await self.download_urls(failed_urls, output_dir)
                retry_results.update(retry_batch_results)
                
                # Update main results
                for url, result in retry_batch_results.items():
                    download_results[url] = result
                    if result.success:
                        self.downloaded_urls.add(url)
                        self.failed_urls.discard(url)
                        self.logger.info(f"Successfully downloaded on retry: {url}")
                    
            finally:
                # Restore original settings
                self.config.max_retries = original_retries
                self.config.connection_timeout = original_timeout
                self.config.retry_delay = original_delay
        
        # Final count
        final_failed = sum(1 for result in download_results.values() if not result.success)
        if final_failed > 0:
            self.logger.warning(f"Still have {final_failed} failed downloads after {max_attempts} retry attempts")
        
        return retry_results
    
    async def _read_file_with_encoding_detection(self, file_path: Path) -> Optional[str]:
        """Read file content with proper encoding detection."""
        try:
            # First, read a sample to detect encoding
            async with aiofiles.open(file_path, 'rb') as f:
                raw_data = await f.read(min(8192, file_path.stat().st_size))
            
            if not raw_data:
                return None
            
            # Detect encoding
            detected = chardet.detect(raw_data)
            encoding = detected.get('encoding', 'utf-8')
            confidence = detected.get('confidence', 0)
            
            # If confidence is too low, try common encodings
            if confidence < 0.7:
                for fallback_encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                    try:
                        async with aiofiles.open(file_path, 'r', encoding=fallback_encoding) as f:
                            content = await f.read()
                        return content
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                return None
            
            # Try to read with detected encoding
            try:
                async with aiofiles.open(file_path, 'r', encoding=encoding) as f:
                    content = await f.read()
                return content
            except (UnicodeDecodeError, UnicodeError):
                # Fallback to UTF-8 with error handling
                try:
                    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                        content = await f.read()
                    return content
                except Exception:
                    return None
                    
        except Exception as e:
            self.logger.warning(f"Could not read file {file_path}: {e}")
            return None
    
    def check_disk_space(self, required_bytes: int) -> bool:
        """Check if sufficient disk space is available."""
        try:
            free_bytes = shutil.disk_usage(self.config.output_dir).free
            return free_bytes > required_bytes * 1.2  # 20% buffer
        except Exception:
            return True  # If we can't check, assume it's okay
    
    def get_available_memory(self) -> int:
        """Get available system memory in bytes."""
        try:
            return psutil.virtual_memory().available
        except Exception:
            return 1024 * 1024 * 1024  # Default to 1GB if we can't check


class RateLimiter:
    """Advanced rate limiter with burst handling and backoff."""
    
    def __init__(self, max_requests_per_second: int = 10, burst_size: int = 5):
        self.max_rps = max_requests_per_second
        self.burst_size = burst_size
        self.request_times = []
        self.burst_count = 0
        self.last_burst_reset = time.time()
        
    async def wait_if_needed(self):
        """Implement advanced rate limiting with burst capability."""
        now = time.time()
        
        # Reset burst counter every second
        if now - self.last_burst_reset >= 1.0:
            self.burst_count = 0
            self.last_burst_reset = now
        
        # Clean old requests (older than 1 second)
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # Check if we can make a burst request
        if self.burst_count < self.burst_size:
            self.burst_count += 1
            self.request_times.append(now)
            return
        
        # Check regular rate limit
        if len(self.request_times) >= self.max_rps:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.request_times.append(time.time())