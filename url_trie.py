"""Trie-based URL storage for efficient lookups and pattern matching."""

from typing import Dict, Optional, List, Set, Tuple
from urllib.parse import urlparse
from pathlib import Path
import re


class URLTrieNode:
    """Node in the URL Trie structure."""
    
    def __init__(self):
        self.children: Dict[str, 'URLTrieNode'] = {}
        self.is_url_end = False
        self.local_path: Optional[Path] = None
        self.full_url: Optional[str] = None
        self.metadata: Dict[str, any] = {}


class URLTrie:
    """Trie data structure optimized for URL storage and lookups."""
    
    def __init__(self):
        self.root = URLTrieNode()
        self.size = 0
        
    def _tokenize_url(self, url: str) -> List[str]:
        """Tokenize URL into components for Trie insertion."""
        parsed = urlparse(url)
        
        # Start with scheme and domain
        tokens = []
        if parsed.scheme:
            tokens.append(parsed.scheme)
        
        if parsed.netloc:
            # Split domain by dots (reversed for better prefix sharing)
            domain_parts = parsed.netloc.lower().split('.')
            tokens.extend(reversed(domain_parts))
        
        # Add path components
        if parsed.path and parsed.path != '/':
            path_parts = [p for p in parsed.path.split('/') if p]
            tokens.extend(path_parts)
        
        # Add query as a single token if present
        if parsed.query:
            tokens.append(f"?{parsed.query}")
            
        return tokens
    
    def insert(self, url: str, local_path: Optional[Path] = None, metadata: Optional[Dict] = None):
        """Insert a URL into the Trie with optional local path mapping."""
        tokens = self._tokenize_url(url)
        node = self.root
        
        for token in tokens:
            if token not in node.children:
                node.children[token] = URLTrieNode()
            node = node.children[token]
        
        node.is_url_end = True
        node.full_url = url
        node.local_path = local_path
        if metadata:
            node.metadata.update(metadata)
        self.size += 1
    
    def search(self, url: str) -> Optional[Tuple[str, Optional[Path], Dict]]:
        """Search for an exact URL match."""
        tokens = self._tokenize_url(url)
        node = self.root
        
        for token in tokens:
            if token not in node.children:
                return None
            node = node.children[token]
        
        if node.is_url_end:
            return (node.full_url, node.local_path, node.metadata)
        return None
    
    def starts_with(self, prefix: str) -> List[Tuple[str, Optional[Path], Dict]]:
        """Find all URLs that start with the given prefix."""
        tokens = self._tokenize_url(prefix)
        node = self.root
        
        # Navigate to the prefix node
        for token in tokens:
            if token not in node.children:
                return []
            node = node.children[token]
        
        # Collect all URLs under this prefix
        results = []
        self._collect_urls(node, results)
        return results
    
    def _collect_urls(self, node: URLTrieNode, results: List[Tuple[str, Optional[Path], Dict]]):
        """Recursively collect all URLs from a node."""
        if node.is_url_end:
            results.append((node.full_url, node.local_path, node.metadata))
        
        for child in node.children.values():
            self._collect_urls(child, results)
    
    def find_by_domain(self, domain: str) -> List[Tuple[str, Optional[Path], Dict]]:
        """Find all URLs for a specific domain."""
        results = []
        self._find_by_domain_helper(self.root, domain.lower(), [], results)
        return results
    
    def _find_by_domain_helper(self, node: URLTrieNode, target_domain: str, 
                               current_tokens: List[str], results: List[Tuple[str, Optional[Path], Dict]]):
        """Helper to find URLs by domain using DFS."""
        if node.is_url_end and node.full_url:
            parsed = urlparse(node.full_url)
            if parsed.netloc.lower() == target_domain:
                results.append((node.full_url, node.local_path, node.metadata))
        
        for token, child in node.children.items():
            self._find_by_domain_helper(child, target_domain, current_tokens + [token], results)
    
    def pattern_match(self, pattern: str) -> List[Tuple[str, Optional[Path], Dict]]:
        """Find URLs matching a glob-like pattern."""
        regex_pattern = self._glob_to_regex(pattern)
        results = []
        self._pattern_match_helper(self.root, regex_pattern, results)
        return results
    
    def _glob_to_regex(self, pattern: str) -> re.Pattern:
        """Convert glob pattern to regex."""
        # Escape special regex characters except * and ?
        escaped = re.escape(pattern)
        # Convert glob wildcards to regex
        escaped = escaped.replace(r'\*', '.*')
        escaped = escaped.replace(r'\?', '.')
        return re.compile(f'^{escaped}$', re.IGNORECASE)
    
    def _pattern_match_helper(self, node: URLTrieNode, pattern: re.Pattern, 
                             results: List[Tuple[str, Optional[Path], Dict]]):
        """Helper for pattern matching using DFS."""
        if node.is_url_end and node.full_url:
            if pattern.match(node.full_url):
                results.append((node.full_url, node.local_path, node.metadata))
        
        for child in node.children.values():
            self._pattern_match_helper(child, pattern, results)
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the Trie."""
        stats = {
            'total_urls': self.size,
            'total_nodes': 0,
            'max_depth': 0,
            'domains': set()
        }
        
        self._calculate_stats(self.root, 0, stats)
        stats['unique_domains'] = len(stats['domains'])
        del stats['domains']  # Remove the set from final stats
        
        return stats
    
    def _calculate_stats(self, node: URLTrieNode, depth: int, stats: Dict):
        """Calculate Trie statistics recursively."""
        stats['total_nodes'] += 1
        stats['max_depth'] = max(stats['max_depth'], depth)
        
        if node.is_url_end and node.full_url:
            parsed = urlparse(node.full_url)
            if parsed.netloc:
                stats['domains'].add(parsed.netloc.lower())
        
        for child in node.children.values():
            self._calculate_stats(child, depth + 1, stats)
    
    def bulk_insert(self, url_mappings: Dict[str, Path], metadata: Optional[Dict] = None):
        """Efficiently insert multiple URLs at once."""
        for url, path in url_mappings.items():
            self.insert(url, path, metadata)
    
    def clear(self):
        """Clear all URLs from the Trie."""
        self.root = URLTrieNode()
        self.size = 0