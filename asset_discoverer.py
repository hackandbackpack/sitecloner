"""Asset discovery from HTML, CSS, and other web files."""

import re
from typing import Set, Dict, List, Optional
from urllib.parse import urlparse
from bs4 import BeautifulSoup, Tag
import tinycss2
from pathlib import Path

from url_resolver import URLResolver


class AssetDiscoverer:
    """Discovers all assets referenced in web files."""
    
    def __init__(self, url_resolver: URLResolver):
        self.url_resolver = url_resolver
        
        # HTML attributes that contain URLs
        self.url_attributes = {
            'src': ['img', 'script', 'iframe', 'embed', 'source', 'track', 'audio', 'video'],
            'href': ['link', 'a', 'area'],
            'data-src': ['img'],  # Lazy loading
            'data-background': ['div', 'section'],  # Background images
            'poster': ['video'],
            'content': ['meta'],  # For meta refresh
            'action': ['form'],
            'cite': ['blockquote', 'q'],
            'data': ['object'],
            'formaction': ['button', 'input'],
            'manifest': ['html'],
            'ping': ['a', 'area'],
            'srcset': ['img', 'source'],
        }
        
        # CSS properties that can contain URLs
        self.css_url_properties = {
            'background', 'background-image', 'border-image', 'border-image-source',
            'content', 'cursor', 'filter', 'list-style-image', 'mask', 'mask-image',
            'shape-outside', 'src'  # For @font-face
        }
    
    def discover_assets_in_html(self, html_content: str, page_url: str) -> Dict[str, Set[str]]:
        """
        Discover all assets referenced in HTML content.
        Returns a dictionary with asset types as keys and sets of URLs as values.
        """
        assets = {
            'stylesheets': set(),
            'scripts': set(),
            'images': set(),
            'fonts': set(),
            'media': set(),
            'documents': set(),
            'manifests': set(),
            'service_workers': set(),
            'preload': set(),
            'other': set()
        }
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
        except Exception:
            # Fallback to html.parser if lxml fails
            soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find assets by HTML attributes
        self._discover_from_attributes(soup, assets, page_url)
        
        # Find assets in inline styles
        self._discover_from_inline_styles(soup, assets, page_url)
        
        # Find assets in style tags
        self._discover_from_style_tags(soup, assets, page_url)
        
        # Find assets in script tags (limited static analysis)
        self._discover_from_scripts(soup, assets, page_url)
        
        # Find meta-declared assets
        self._discover_from_meta_tags(soup, assets, page_url)
        
        # Find web manifest files
        self._discover_web_manifests(soup, assets, page_url)
        
        # Find service worker registrations
        self._discover_service_workers(soup, assets, page_url)
        
        # Find preload/prefetch resources
        self._discover_preload_resources(soup, assets, page_url)
        
        # Find CSS custom properties and modern features
        self._discover_modern_css_features(soup, assets, page_url)
        
        return assets
    
    def _discover_web_manifests(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover web manifest files."""
        # Look for manifest link tags
        manifest_links = soup.find_all('link', {'rel': 'manifest'})
        for link in manifest_links:
            href = link.get('href')
            if href:
                resolved_url = self.url_resolver.resolve_url(href, page_url)
                if resolved_url:
                    assets['manifests'].add(resolved_url)
    
    def _discover_service_workers(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover service worker registrations in JavaScript."""
        scripts = soup.find_all('script')
        
        for script in scripts:
            script_content = script.string
            if script_content and 'serviceWorker' in script_content:
                # Look for navigator.serviceWorker.register() calls
                import re
                
                # Pattern to match service worker registration
                sw_patterns = [
                    r'serviceWorker\.register\s*\(\s*["\']([^"\'\']+)["\']',
                    r'registerServiceWorker\s*\(\s*["\']([^"\'\']+)["\']',
                    r'register\s*\(\s*["\']([^"\'\']+)["\'].*serviceWorker'
                ]
                
                for pattern in sw_patterns:
                    matches = re.finditer(pattern, script_content, re.IGNORECASE)
                    for match in matches:
                        sw_url = match.group(1)
                        resolved_url = self.url_resolver.resolve_url(sw_url, page_url)
                        if resolved_url:
                            assets['service_workers'].add(resolved_url)
    
    def _discover_preload_resources(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover preload and prefetch resources."""
        # Look for link tags with rel="preload", "prefetch", "preconnect", etc.
        preload_rels = ['preload', 'prefetch', 'preconnect', 'dns-prefetch', 'modulepreload']
        
        for rel in preload_rels:
            links = soup.find_all('link', {'rel': rel})
            for link in links:
                href = link.get('href')
                if href:
                    resolved_url = self.url_resolver.resolve_url(href, page_url)
                    if resolved_url:
                        assets['preload'].add(resolved_url)
                        
                        # Also categorize by type if specified
                        as_type = link.get('as', '').lower()
                        if as_type == 'style':
                            assets['stylesheets'].add(resolved_url)
                        elif as_type == 'script':
                            assets['scripts'].add(resolved_url)
                        elif as_type == 'image':
                            assets['images'].add(resolved_url)
                        elif as_type == 'font':
                            assets['fonts'].add(resolved_url)
    
    def _discover_modern_css_features(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets referenced in modern CSS features."""
        # Process all style tags for modern CSS
        style_tags = soup.find_all('style')
        
        for style_tag in style_tags:
            if style_tag.string:
                self._extract_modern_css_assets(style_tag.string, assets, page_url)
        
        # Process inline styles for modern CSS
        elements_with_style = soup.find_all(attrs={'style': True})
        
        for element in elements_with_style:
            style_content = element.get('style', '')
            if style_content:
                self._extract_modern_css_assets(style_content, assets, page_url)
    
    def _extract_modern_css_assets(self, css_content: str, assets: Dict[str, Set[str]], page_url: str):
        """Extract assets from modern CSS features like custom properties, calc(), etc."""
        import re
        
        # CSS custom properties (CSS variables) with URL values
        # Example: --bg-image: url('image.jpg');
        var_pattern = r'--[\w-]+\s*:\s*url\s*\(\s*["\']?([^"\')]+)["\']?\s*\)'
        
        matches = re.finditer(var_pattern, css_content, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            resolved_url = self.url_resolver.resolve_url(url, page_url)
            if resolved_url:
                self._categorize_asset_by_extension(resolved_url, assets)
        
        # CSS calc() functions with URL values (less common but possible)
        # CSS supports() queries with URLs
        # CSS image-set() function
        image_set_pattern = r'image-set\s*\(\s*url\s*\(\s*["\']?([^"\')]+)["\']?\s*\)'
        
        matches = re.finditer(image_set_pattern, css_content, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            resolved_url = self.url_resolver.resolve_url(url, page_url)
            if resolved_url:
                assets['images'].add(resolved_url)
        
        # CSS element() function (Firefox)
        element_pattern = r'element\s*\(\s*#([^)]+)\s*\)'
        # This doesn't create URLs but we track for completeness
        
        # Modern CSS features like backdrop-filter
        # These can reference SVG filters which are URLs
        filter_pattern = r'(?:filter|backdrop-filter)\s*:\s*url\s*\(\s*["\']?([^"\')]+)["\']?\s*\)'
        
        matches = re.finditer(filter_pattern, css_content, re.IGNORECASE)
        for match in matches:
            url = match.group(1).strip()
            if url.startswith('#'):
                # Internal fragment reference, skip
                continue
            resolved_url = self.url_resolver.resolve_url(url, page_url)
            if resolved_url:
                assets['other'].add(resolved_url)
    
    def _discover_from_attributes(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets from HTML element attributes."""
        for attr, tags in self.url_attributes.items():
            elements = soup.find_all(tags)
            
            for element in elements:
                if not isinstance(element, Tag):
                    continue
                    
                urls = self._extract_urls_from_attribute(element, attr)
                
                for url in urls:
                    resolved_url = self.url_resolver.resolve_url(url, page_url)
                    if resolved_url:
                        asset_type = self._categorize_asset(resolved_url, element.name, attr)
                        assets[asset_type].add(resolved_url)
    
    def _extract_urls_from_attribute(self, element: Tag, attr: str) -> List[str]:
        """Extract URLs from an HTML attribute, handling special cases."""
        attr_value = element.get(attr)
        if not attr_value:
            return []
        
        urls = []
        
        if attr == 'srcset':
            # Handle srcset attribute with multiple URLs and descriptors
            srcset_parts = attr_value.split(',')
            for part in srcset_parts:
                url = part.strip().split()[0]  # Take only the URL part
                if url:
                    urls.append(url)
        elif attr == 'content' and element.name == 'meta':
            # Handle meta refresh
            if element.get('http-equiv', '').lower() == 'refresh':
                match = re.search(r'url=([^;]+)', attr_value, re.IGNORECASE)
                if match:
                    urls.append(match.group(1).strip())
        else:
            urls.append(attr_value)
        
        return urls
    
    def _discover_from_inline_styles(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets from inline style attributes."""
        elements_with_style = soup.find_all(attrs={'style': True})
        
        for element in elements_with_style:
            style_content = element.get('style', '')
            urls = self.url_resolver.extract_urls_from_css_content(style_content, page_url)
            
            for url in urls:
                asset_type = self._categorize_asset(url)
                assets[asset_type].add(url)
    
    def _discover_from_style_tags(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets from <style> tags."""
        style_tags = soup.find_all('style')
        
        for style_tag in style_tags:
            if style_tag.string:
                urls = self.url_resolver.extract_urls_from_css_content(style_tag.string, page_url)
                
                for url in urls:
                    asset_type = self._categorize_asset(url)
                    assets[asset_type].add(url)
    
    def _discover_from_scripts(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets from script tags (limited static analysis)."""
        script_tags = soup.find_all('script')
        
        for script_tag in script_tags:
            if script_tag.string:
                # Look for common patterns in JavaScript that might reference assets
                # This is limited static analysis - can't execute JavaScript
                script_content = script_tag.string
                
                # Pattern for common asset loading patterns
                patterns = [
                    r'["\']([^"\']+\.(?:js|css|png|jpg|jpeg|gif|svg|webp|woff|woff2|ttf|otf))["\']',
                    r'src\s*:\s*["\']([^"\']+)["\']',
                    r'url\s*:\s*["\']([^"\']+)["\']',
                    r'href\s*:\s*["\']([^"\']+)["\']',
                ]
                
                for pattern in patterns:
                    matches = re.finditer(pattern, script_content, re.IGNORECASE)
                    for match in matches:
                        url = match.group(1)
                        resolved_url = self.url_resolver.resolve_url(url, page_url)
                        if resolved_url:
                            asset_type = self._categorize_asset(resolved_url)
                            assets[asset_type].add(resolved_url)
    
    def _discover_from_meta_tags(self, soup: BeautifulSoup, assets: Dict[str, Set[str]], page_url: str):
        """Discover assets from meta tags."""
        meta_tags = soup.find_all('meta')
        
        for meta_tag in meta_tags:
            # Handle various meta tag types
            name = meta_tag.get('name', '').lower()
            property_attr = meta_tag.get('property', '').lower()
            content = meta_tag.get('content', '')
            
            if not content:
                continue
            
            # Open Graph and Twitter Card images
            if name in ['twitter:image', 'twitter:image:src'] or property_attr in ['og:image', 'og:video', 'og:audio']:
                resolved_url = self.url_resolver.resolve_url(content, page_url)
                if resolved_url:
                    asset_type = self._categorize_asset(resolved_url)
                    assets[asset_type].add(resolved_url)
            
            # Apple touch icons and other meta-declared resources
            elif name in ['msapplication-tileimage', 'msapplication-config']:
                resolved_url = self.url_resolver.resolve_url(content, page_url)
                if resolved_url:
                    assets['other'].add(resolved_url)
    
    def discover_assets_in_css(self, css_content: str, css_url: str) -> Dict[str, Set[str]]:
        """
        Discover all assets referenced in CSS content.
        """
        assets = {
            'stylesheets': set(),
            'fonts': set(),
            'images': set(),
            'other': set()
        }
        
        try:
            # Use tinycss2 for proper CSS parsing
            rules = tinycss2.parse_stylesheet(css_content)
            self._process_css_rules(rules, assets, css_url)
        except Exception:
            # Fallback to regex-based parsing if tinycss2 fails
            urls = self.url_resolver.extract_urls_from_css_content(css_content, css_url)
            for url in urls:
                asset_type = self._categorize_asset(url)
                assets[asset_type].add(url)
        
        return assets
    
    def _process_css_rules(self, rules: List, assets: Dict[str, Set[str]], css_url: str):
        """Process CSS rules recursively to find asset URLs."""
        for rule in rules:
            if hasattr(rule, 'content'):
                # Process nested rules (media queries, etc.)
                if hasattr(rule.content, '__iter__'):
                    self._process_css_rules(rule.content, assets, css_url)
            
            if hasattr(rule, 'prelude') and hasattr(rule, 'content'):
                # @import rules
                if rule.type == 'at-rule' and rule.lower_at_keyword == 'import':
                    url_token = None
                    for token in rule.prelude:
                        if token.type in ['url', 'string']:
                            url_token = token
                            break
                    
                    if url_token:
                        url = url_token.value
                        resolved_url = self.url_resolver.resolve_url(url, css_url)
                        if resolved_url:
                            assets['stylesheets'].add(resolved_url)
                
                # @font-face rules
                elif rule.type == 'at-rule' and rule.lower_at_keyword == 'font-face':
                    self._extract_urls_from_declarations(rule.content, assets, css_url)
                
                # Regular style rules
                elif rule.type == 'qualified-rule':
                    self._extract_urls_from_declarations(rule.content, assets, css_url)
    
    def _extract_urls_from_declarations(self, declarations, assets: Dict[str, Set[str]], css_url: str):
        """Extract URLs from CSS declarations."""
        if not hasattr(declarations, '__iter__'):
            return
        
        for declaration in declarations:
            if hasattr(declaration, 'name') and hasattr(declaration, 'value'):
                prop_name = declaration.name.lower()
                
                # Look for URL tokens in the value
                for token in declaration.value:
                    if token.type == 'url':
                        url = token.value
                        resolved_url = self.url_resolver.resolve_url(url, css_url)
                        if resolved_url:
                            asset_type = self._categorize_css_asset(resolved_url, prop_name)
                            assets[asset_type].add(resolved_url)
    
    def _categorize_asset(self, url: str, element_name: Optional[str] = None, attr_name: Optional[str] = None) -> str:
        """Categorize an asset URL into a type."""
        parsed = urlparse(url)
        path = Path(parsed.path)
        ext = path.suffix.lower()
        
        # Image extensions
        image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico', '.tiff'}
        if ext in image_exts:
            return 'images'
        
        # Font extensions
        font_exts = {'.woff', '.woff2', '.ttf', '.otf', '.eot'}
        if ext in font_exts:
            return 'fonts'
        
        # Stylesheet extensions
        if ext in {'.css', '.scss', '.sass'}:
            return 'stylesheets'
        
        # Script extensions
        if ext in {'.js', '.mjs', '.ts'}:
            return 'scripts'
        
        # Media extensions
        media_exts = {'.mp4', '.webm', '.ogg', '.mp3', '.wav', '.m4a', '.aac'}
        if ext in media_exts:
            return 'media'
        
        # Document extensions
        doc_exts = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'}
        if ext in doc_exts:
            return 'documents'
        
        # Categorize by element context if no clear extension
        if element_name:
            if element_name in ['img', 'picture']:
                return 'images'
            elif element_name in ['script']:
                return 'scripts'
            elif element_name in ['link'] and attr_name == 'href':
                return 'stylesheets'
            elif element_name in ['audio', 'video', 'source']:
                return 'media'
        
        return 'other'
    
    def _categorize_css_asset(self, url: str, css_property: str) -> str:
        """Categorize an asset from CSS based on the property it's used in."""
        parsed = urlparse(url)
        path = Path(parsed.path)
        ext = path.suffix.lower()
        
        # Font extensions are always fonts
        if ext in {'.woff', '.woff2', '.ttf', '.otf', '.eot'}:
            return 'fonts'
        
        # CSS imports are stylesheets
        if ext in {'.css', '.scss', '.sass'}:
            return 'stylesheets'
        
        # Check CSS property context
        if css_property in ['src'] and 'font-face' in str(css_property):  # @font-face src
            return 'fonts'
        elif css_property in ['background', 'background-image', 'border-image', 'mask', 'mask-image']:
            return 'images'
        elif css_property == 'cursor':
            return 'images'
        
        # Default categorization by extension
        return self._categorize_asset(url)