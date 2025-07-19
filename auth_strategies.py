"""Authentication strategies using the Strategy pattern."""

from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import base64
import aiohttp
from urllib.parse import urlparse


class AuthStrategy(ABC):
    """Abstract base class for authentication strategies."""
    
    @abstractmethod
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply authentication to the session and return headers."""
        pass
    
    @abstractmethod
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Get cookies for this authentication method."""
        pass


class NoAuthStrategy(AuthStrategy):
    """No authentication required."""
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """No authentication to apply."""
        return {}
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """No cookies needed."""
        return None


class BasicAuthStrategy(AuthStrategy):
    """HTTP Basic Authentication."""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self._auth_header = None
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply basic authentication headers."""
        if not self._auth_header:
            credentials = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode()
            self._auth_header = f"Basic {credentials}"
        
        return {"Authorization": self._auth_header}
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Basic auth doesn't use cookies."""
        return None


class BearerTokenStrategy(AuthStrategy):
    """Bearer token authentication (OAuth 2.0, JWT, etc.)."""
    
    def __init__(self, token: str):
        self.token = token
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply bearer token authentication."""
        return {"Authorization": f"Bearer {self.token}"}
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Bearer auth doesn't use cookies."""
        return None


class CookieAuthStrategy(AuthStrategy):
    """Cookie-based authentication."""
    
    def __init__(self):
        self.cookie_jar = aiohttp.CookieJar()
        self.cookies: Dict[str, Dict[str, str]] = {}
    
    def add_cookie(self, name: str, value: str, domain: str, 
                   path: str = "/", secure: bool = True, httponly: bool = True):
        """Add a cookie for authentication."""
        if domain not in self.cookies:
            self.cookies[domain] = {}
        
        self.cookies[domain][name] = {
            "value": value,
            "path": path,
            "secure": secure,
            "httponly": httponly
        }
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply cookies to the session."""
        # Cookies are handled by the CookieJar, no headers needed
        return {}
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Get the cookie jar."""
        return self.cookie_jar


class CustomHeadersStrategy(AuthStrategy):
    """Custom headers authentication (API keys, etc.)."""
    
    def __init__(self, headers: Dict[str, str]):
        self.headers = headers
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply custom headers."""
        return self.headers.copy()
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Custom headers don't use cookies."""
        return None


class CompositeAuthStrategy(AuthStrategy):
    """Combine multiple authentication strategies."""
    
    def __init__(self, strategies: list[AuthStrategy]):
        self.strategies = strategies
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply all authentication strategies and merge headers."""
        all_headers = {}
        for strategy in self.strategies:
            headers = await strategy.apply(session, url)
            all_headers.update(headers)
        return all_headers
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Get cookies from the first strategy that has them."""
        for strategy in self.strategies:
            cookies = strategy.get_cookies()
            if cookies:
                return cookies
        return None


class DomainBasedAuthStrategy(AuthStrategy):
    """Apply different authentication strategies based on domain."""
    
    def __init__(self, default_strategy: AuthStrategy = None):
        self.domain_strategies: Dict[str, AuthStrategy] = {}
        self.default_strategy = default_strategy or NoAuthStrategy()
    
    def add_domain_auth(self, domain: str, strategy: AuthStrategy):
        """Add authentication strategy for a specific domain."""
        self.domain_strategies[domain.lower()] = strategy
    
    async def apply(self, session: aiohttp.ClientSession, url: str) -> Dict[str, str]:
        """Apply authentication based on the URL domain."""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Check for exact domain match
        if domain in self.domain_strategies:
            return await self.domain_strategies[domain].apply(session, url)
        
        # Check for subdomain matches
        for registered_domain, strategy in self.domain_strategies.items():
            if domain.endswith(f".{registered_domain}"):
                return await strategy.apply(session, url)
        
        # Use default strategy
        return await self.default_strategy.apply(session, url)
    
    def get_cookies(self) -> Optional[aiohttp.CookieJar]:
        """Get cookies from all strategies that have them."""
        # Create a composite cookie jar
        composite_jar = aiohttp.CookieJar()
        
        # Add cookies from default strategy
        default_cookies = self.default_strategy.get_cookies()
        if default_cookies:
            # Merge cookies (simplified for now)
            return default_cookies
        
        # In a real implementation, we'd merge all cookie jars
        for strategy in self.domain_strategies.values():
            cookies = strategy.get_cookies()
            if cookies:
                return cookies
        
        return None


class AuthStrategyFactory:
    """Factory for creating authentication strategies."""
    
    @staticmethod
    def create_from_config(config: Dict[str, Any]) -> AuthStrategy:
        """Create an authentication strategy from configuration."""
        auth_type = config.get("type", "none").lower()
        
        if auth_type == "none":
            return NoAuthStrategy()
        
        elif auth_type == "basic":
            username = config.get("username", "")
            password = config.get("password", "")
            return BasicAuthStrategy(username, password)
        
        elif auth_type == "bearer":
            token = config.get("token", "")
            return BearerTokenStrategy(token)
        
        elif auth_type == "cookie":
            strategy = CookieAuthStrategy()
            cookies = config.get("cookies", [])
            for cookie in cookies:
                strategy.add_cookie(
                    name=cookie.get("name"),
                    value=cookie.get("value"),
                    domain=cookie.get("domain"),
                    path=cookie.get("path", "/"),
                    secure=cookie.get("secure", True),
                    httponly=cookie.get("httponly", True)
                )
            return strategy
        
        elif auth_type == "custom":
            headers = config.get("headers", {})
            return CustomHeadersStrategy(headers)
        
        elif auth_type == "domain_based":
            default_config = config.get("default", {"type": "none"})
            default_strategy = AuthStrategyFactory.create_from_config(default_config)
            
            domain_strategy = DomainBasedAuthStrategy(default_strategy)
            
            domains = config.get("domains", {})
            for domain, domain_config in domains.items():
                domain_auth = AuthStrategyFactory.create_from_config(domain_config)
                domain_strategy.add_domain_auth(domain, domain_auth)
            
            return domain_strategy
        
        else:
            raise ValueError(f"Unknown authentication type: {auth_type}")