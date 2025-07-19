"""Configuration management for SiteCloner."""

import dataclasses
from pathlib import Path
from typing import Set, Optional
import json


@dataclasses.dataclass
class SiteClonerConfig:
    """Configuration for site cloning operations."""
    
    # Target settings
    target_url: str = ""
    output_dir: Path = Path("cloned_site")
    
    # Scope control
    max_depth: int = 3
    same_domain_only: bool = True
    follow_external_assets: bool = True
    allowed_domains: Set[str] = dataclasses.field(default_factory=set)
    blocked_domains: Set[str] = dataclasses.field(default_factory=set)
    
    # File type filters
    allowed_extensions: Set[str] = dataclasses.field(default_factory=lambda: {
        '.html', '.htm', '.css', '.js', '.json',
        '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.ico',
        '.woff', '.woff2', '.ttf', '.otf', '.eot',
        '.mp4', '.webm', '.mp3', '.wav', '.pdf',
        '.xml', '.txt', '.manifest'
    })
    blocked_extensions: Set[str] = dataclasses.field(default_factory=set)
    
    # Size limits (in bytes)
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    max_total_size: int = 1024 * 1024 * 1024  # 1GB
    
    # Performance settings
    max_concurrent_downloads: int = 10
    connection_timeout: int = 30
    read_timeout: int = 60
    max_retries: int = 5  # Increased for better reliability
    retry_delay: float = 1.0
    rate_limit_delay: float = 0.1
    
    # Behavior settings
    overwrite_existing: bool = False
    follow_redirects: bool = True
    respect_robots_txt: bool = True
    user_agent: str = "SiteCloner/1.0 (Website Archiving Tool)"
    custom_headers: dict = dataclasses.field(default_factory=dict)
    
    # Output settings
    preserve_directory_structure: bool = True
    create_index_file: bool = True
    save_metadata: bool = True
    verbose_logging: bool = False
    
    # Advanced settings
    extract_css_assets: bool = True
    extract_js_assets: bool = True
    handle_spa_content: bool = False  # Future feature
    enable_javascript_execution: bool = False  # Future feature
    
    @classmethod
    def from_file(cls, config_path: Path) -> 'SiteClonerConfig':
        """Load configuration from JSON file."""
        if not config_path.exists():
            return cls()
        
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert sets and paths
        if 'output_dir' in data:
            data['output_dir'] = Path(data['output_dir'])
        if 'allowed_domains' in data:
            data['allowed_domains'] = set(data['allowed_domains'])
        if 'blocked_domains' in data:
            data['blocked_domains'] = set(data['blocked_domains'])
        if 'allowed_extensions' in data:
            data['allowed_extensions'] = set(data['allowed_extensions'])
        if 'blocked_extensions' in data:
            data['blocked_extensions'] = set(data['blocked_extensions'])
        if 'custom_headers' in data and data['custom_headers'] is None:
            data['custom_headers'] = {}
            
        return cls(**data)
    
    def to_file(self, config_path: Path) -> None:
        """Save configuration to JSON file."""
        data = dataclasses.asdict(self)
        
        # Convert non-serializable types
        data['output_dir'] = str(data['output_dir'])
        data['allowed_domains'] = list(data['allowed_domains'])
        data['blocked_domains'] = list(data['blocked_domains'])
        data['allowed_extensions'] = list(data['allowed_extensions'])
        data['blocked_extensions'] = list(data['blocked_extensions'])
        
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    
    def should_download_file(self, url: str, file_size: Optional[int] = None) -> bool:
        """Check if a file should be downloaded based on configuration."""
        from urllib.parse import urlparse
        
        parsed = urlparse(url)
        
        # Check file extension
        path = Path(parsed.path)
        ext = path.suffix.lower()
        
        # Allow files without extensions (like root pages)
        if not ext and (not parsed.path or parsed.path == '/' or parsed.path.endswith('/')):
            ext = '.html'  # Treat as HTML
        
        if self.allowed_extensions and ext and ext not in self.allowed_extensions:
            return False
        
        if ext in self.blocked_extensions:
            return False
        
        # Check file size
        if file_size and file_size > self.max_file_size:
            return False
        
        # Check domain
        domain = parsed.netloc.lower()
        
        if self.blocked_domains and domain in self.blocked_domains:
            return False
        
        if self.allowed_domains and domain not in self.allowed_domains:
            return False
        
        return True