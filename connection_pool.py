"""Connection pool with circuit breaker pattern for resilient HTTP connections."""

import asyncio
import aiohttp
from typing import Dict, Optional, Any, Callable
from datetime import datetime, timedelta
from enum import Enum
import logging
from contextlib import asynccontextmanager


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker for a specific domain."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60,
                 success_threshold: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout  # seconds
        self.success_threshold = success_threshold
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_state_change = datetime.now()
        
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger('circuit_breaker')
    
    async def call(self, func: Callable, *args, **kwargs):
        """Execute a function through the circuit breaker."""
        async with self._lock:
            if not await self._can_proceed():
                raise Exception(f"Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise
    
    async def _can_proceed(self) -> bool:
        """Check if request can proceed based on circuit state."""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout:
                    self.logger.info("Circuit breaker attempting recovery (HALF_OPEN)")
                    self.state = CircuitState.HALF_OPEN
                    self.last_state_change = datetime.now()
                    return True
            return False
        
        # HALF_OPEN state
        return True
    
    async def _on_success(self):
        """Handle successful request."""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.logger.info("Circuit breaker recovered (CLOSED)")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.last_state_change = datetime.now()
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0
    
    async def _on_failure(self):
        """Handle failed request."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self.logger.warning(f"Circuit breaker tripped (OPEN) after {self.failure_count} failures")
                    self.state = CircuitState.OPEN
                    self.last_state_change = datetime.now()
            elif self.state == CircuitState.HALF_OPEN:
                self.logger.warning("Circuit breaker failed during recovery (OPEN)")
                self.state = CircuitState.OPEN
                self.success_count = 0
                self.last_state_change = datetime.now()
    
    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status."""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'last_state_change': self.last_state_change.isoformat()
        }


class ConnectionPool:
    """Advanced connection pool with circuit breakers per domain."""
    
    def __init__(self, 
                 max_connectors: int = 10,
                 max_per_host: int = 5,
                 ttl: int = 600,  # connector TTL in seconds
                 circuit_breaker_config: Optional[Dict[str, Any]] = None):
        self.max_connectors = max_connectors
        self.max_per_host = max_per_host
        self.ttl = ttl
        self.circuit_breaker_config = circuit_breaker_config or {}
        
        self.connectors: Dict[str, aiohttp.TCPConnector] = {}
        self.connector_creation_times: Dict[str, datetime] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger('connection_pool')
        
        # Cleanup task
        self._cleanup_task = None
    
    async def start(self):
        """Start the connection pool and cleanup tasks."""
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
    
    async def stop(self):
        """Stop the connection pool and close all connectors."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close all connectors
        async with self._lock:
            for connector in self.connectors.values():
                await connector.close()
            self.connectors.clear()
            self.connector_creation_times.clear()
    
    @asynccontextmanager
    async def get_session(self, base_url: str, headers: Optional[Dict[str, str]] = None,
                         timeout: Optional[aiohttp.ClientTimeout] = None):
        """Get an aiohttp session with connection pooling and circuit breaker."""
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        domain = parsed.netloc
        
        # Get or create connector
        connector = await self._get_connector(domain)
        
        # Get or create circuit breaker
        circuit_breaker = await self._get_circuit_breaker(domain)
        
        # Create session
        session = aiohttp.ClientSession(
            connector=connector,
            headers=headers,
            timeout=timeout or aiohttp.ClientTimeout(total=60)
        )
        
        try:
            # Wrap session methods with circuit breaker
            original_get = session.get
            original_post = session.post
            original_put = session.put
            original_delete = session.delete
            
            async def wrapped_get(*args, **kwargs):
                return await circuit_breaker.call(original_get, *args, **kwargs)
            
            async def wrapped_post(*args, **kwargs):
                return await circuit_breaker.call(original_post, *args, **kwargs)
            
            async def wrapped_put(*args, **kwargs):
                return await circuit_breaker.call(original_put, *args, **kwargs)
            
            async def wrapped_delete(*args, **kwargs):
                return await circuit_breaker.call(original_delete, *args, **kwargs)
            
            session.get = wrapped_get
            session.post = wrapped_post
            session.put = wrapped_put
            session.delete = wrapped_delete
            
            yield session
        finally:
            await session.close()
    
    async def _get_connector(self, domain: str) -> aiohttp.TCPConnector:
        """Get or create a connector for a domain."""
        async with self._lock:
            # Check if connector exists and is not expired
            if domain in self.connectors:
                creation_time = self.connector_creation_times.get(domain)
                if creation_time:
                    age = (datetime.now() - creation_time).total_seconds()
                    if age < self.ttl:
                        return self.connectors[domain]
                    else:
                        # Connector expired, close it
                        await self.connectors[domain].close()
                        del self.connectors[domain]
                        del self.connector_creation_times[domain]
            
            # Create new connector
            connector = aiohttp.TCPConnector(
                limit=self.max_connectors,
                limit_per_host=self.max_per_host,
                ttl_dns_cache=300,  # 5 minutes DNS cache
                enable_cleanup_closed=True
            )
            
            self.connectors[domain] = connector
            self.connector_creation_times[domain] = datetime.now()
            
            self.logger.debug(f"Created new connector for {domain}")
            return connector
    
    async def _get_circuit_breaker(self, domain: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a domain."""
        async with self._lock:
            if domain not in self.circuit_breakers:
                self.circuit_breakers[domain] = CircuitBreaker(**self.circuit_breaker_config)
            return self.circuit_breakers[domain]
    
    async def _periodic_cleanup(self):
        """Periodically clean up expired connectors."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                async with self._lock:
                    expired_domains = []
                    
                    for domain, creation_time in self.connector_creation_times.items():
                        age = (datetime.now() - creation_time).total_seconds()
                        if age >= self.ttl:
                            expired_domains.append(domain)
                    
                    for domain in expired_domains:
                        if domain in self.connectors:
                            await self.connectors[domain].close()
                            del self.connectors[domain]
                            del self.connector_creation_times[domain]
                            self.logger.debug(f"Cleaned up expired connector for {domain}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        stats = {
            'active_connectors': len(self.connectors),
            'circuit_breakers': {}
        }
        
        for domain, breaker in self.circuit_breakers.items():
            stats['circuit_breakers'][domain] = breaker.get_status()
        
        return stats


class ResilientDownloader:
    """Wrapper for resilient downloading with connection pool and circuit breaker."""
    
    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self.logger = logging.getLogger('resilient_downloader')
    
    async def download(self, url: str, headers: Optional[Dict[str, str]] = None,
                      max_retries: int = 3) -> bytes:
        """Download a URL with automatic retries and circuit breaker protection."""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                async with self.connection_pool.get_session(url, headers) as session:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        return await response.read()
            
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    self.logger.warning(f"Retry {attempt + 1} for {url}: {e}")
        
        raise last_error or Exception("Download failed")