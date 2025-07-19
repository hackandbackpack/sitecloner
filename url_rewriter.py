"""URL rewriting to convert external URLs to local paths in downloaded files."""

import re
import asyncio
import aiofiles
from typing import Dict, Set, Optional
from pathlib import Path
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup, NavigableString
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from url_resolver import URLResolver


class URLRewriter:
    """Rewrites URLs in downloaded files to point to local versions."""
    
    def __init__(self, url_resolver: URLResolver):
        self.url_resolver = url_resolver
        self.logger = logging.getLogger('url_rewriter')
        
        # URL mapping: original_url -> local_path_relative
        self.url_mapping: Dict[str, str] = {}
        
        # HTML attributes that contain URLs and should be rewritten
        self.rewrite_attributes = {
            'src': ['img', 'script', 'iframe', 'embed', 'source', 'track', 'audio', 'video'],
            'href': ['link', 'a', 'area'],
            'data-src': ['img'],  # Lazy loading
            'poster': ['video'],
            'action': ['form'],
            'cite': ['blockquote', 'q'],
            'data': ['object'],
            'formaction': ['button', 'input'],
            'srcset': ['img', 'source'],
        }
    
    def add_url_mapping(self, original_url: str, local_path: Path, output_dir: Path):
        """Add a mapping from original URL to local relative path."""
        try:
            relative_path = local_path.relative_to(output_dir)
            # Convert to URL-style path with forward slashes
            local_url = str(relative_path).replace('\\', '/')
            self.url_mapping[original_url] = local_url
        except ValueError:
            # If relative_to fails, just use the filename
            self.url_mapping[original_url] = local_path.name
    
    def add_url_mappings_from_dict(self, mappings: Dict[str, Path], output_dir: Path):
        """Add multiple URL mappings from a dictionary."""
        for original_url, local_path in mappings.items():
            self.add_url_mapping(original_url, local_path, output_dir)
    
    def rewrite_html_file(self, html_path: Path, output_dir: Path) -> bool:
        """
        Rewrite URLs in an HTML file to point to local versions.
        Returns True if any changes were made.
        """
        try:
            # Read the HTML file
            with open(html_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                with open(html_path, 'r', encoding='latin1') as f:
                    content = f.read()
            except Exception as e:
                self.logger.error(f"Could not read HTML file {html_path}: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Could not read HTML file {html_path}: {e}")
            return False
        
        # Parse HTML
        try:
            soup = BeautifulSoup(content, 'lxml')
        except Exception:
            soup = BeautifulSoup(content, 'html.parser')
        
        changes_made = False
        
        # Get the current page URL for relative path calculation
        current_page_relative = html_path.relative_to(output_dir)
        current_page_dir = current_page_relative.parent
        
        # Rewrite URLs in attributes
        changes_made |= self._rewrite_html_attributes(soup, current_page_dir)
        
        # Rewrite URLs in style attributes
        changes_made |= self._rewrite_inline_styles(soup, current_page_dir)
        
        # Rewrite URLs in <style> tags
        changes_made |= self._rewrite_style_tags(soup, current_page_dir)
        
        # Write back if changes were made
        if changes_made:
            try:
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(str(soup))
                self.logger.info(f"Rewrote URLs in {html_path}")
            except Exception as e:
                self.logger.error(f"Could not write HTML file {html_path}: {e}")
                return False
        
        return changes_made
    
    def _rewrite_html_attributes(self, soup: BeautifulSoup, current_page_dir: Path) -> bool:
        """Rewrite URLs in HTML element attributes."""
        changes_made = False
        
        for attr, tags in self.rewrite_attributes.items():
            elements = soup.find_all(tags)
            
            for element in elements:
                if element.has_attr(attr):
                    original_value = element[attr]
                    
                    if attr == 'srcset':
                        # Handle srcset with multiple URLs
                        new_value = self._rewrite_srcset(original_value, current_page_dir)
                    else:
                        # Handle single URL
                        new_value = self._rewrite_single_url(original_value, current_page_dir)
                    
                    if new_value != original_value:
                        element[attr] = new_value
                        changes_made = True
        
        return changes_made
    
    def _rewrite_srcset(self, srcset_value: str, current_page_dir: Path) -> str:
        """Rewrite URLs in a srcset attribute."""
        if not srcset_value:
            return srcset_value
        
        # Split by commas and process each entry
        entries = []
        for entry in srcset_value.split(','):
            entry = entry.strip()
            if not entry:
                continue
            
            # Split by whitespace - first part is URL, rest are descriptors
            parts = entry.split()
            if not parts:
                continue
            
            url = parts[0]
            descriptors = parts[1:] if len(parts) > 1 else []
            
            # Rewrite the URL
            new_url = self._rewrite_single_url(url, current_page_dir)
            
            # Reconstruct the entry
            new_entry = new_url
            if descriptors:
                new_entry += ' ' + ' '.join(descriptors)
            
            entries.append(new_entry)
        
        return ', '.join(entries)
    
    def _rewrite_single_url(self, url: str, current_page_dir: Path) -> str:
        """Rewrite a single URL to local version if available."""
        if not url or url.startswith('#') or url.startswith('data:') or url.startswith('javascript:'):
            return url
        
        # Try to find this URL in our mapping
        # First, try to resolve the URL to get the canonical form
        resolved_url = self.url_resolver.resolve_url(url)
        
        if resolved_url and resolved_url in self.url_mapping:
            local_path = self.url_mapping[resolved_url]
            
            # Calculate relative path from current page to the asset
            local_path_obj = Path(local_path)
            
            try:
                if current_page_dir == Path('.'):
                    # Current page is in root, use local path as-is
                    relative_path = local_path
                else:
                    # Calculate relative path
                    relative_path = self._calculate_relative_path(current_page_dir, local_path_obj)
                
                return relative_path
            except Exception:
                # Fallback to absolute local path
                return local_path
        
        # If no mapping found, return original URL
        return url
    
    def _calculate_relative_path(self, from_dir: Path, to_path: Path) -> str:
        """Calculate relative path from one directory to a file."""
        # Count directory levels to go up
        levels_up = len(from_dir.parts)
        
        # Create relative path
        if levels_up == 0:
            return str(to_path).replace('\\', '/')
        
        prefix = '../' * levels_up
        return prefix + str(to_path).replace('\\', '/')
    
    def _rewrite_inline_styles(self, soup: BeautifulSoup, current_page_dir: Path) -> bool:
        """Rewrite URLs in inline style attributes."""
        changes_made = False
        
        elements_with_style = soup.find_all(attrs={'style': True})
        
        for element in elements_with_style:
            original_style = element['style']
            new_style = self._rewrite_css_content(original_style, current_page_dir)
            
            if new_style != original_style:
                element['style'] = new_style
                changes_made = True
        
        return changes_made
    
    def _rewrite_style_tags(self, soup: BeautifulSoup, current_page_dir: Path) -> bool:
        """Rewrite URLs in <style> tags."""
        changes_made = False
        
        style_tags = soup.find_all('style')
        
        for style_tag in style_tags:
            if style_tag.string:
                original_css = style_tag.string
                new_css = self._rewrite_css_content(original_css, current_page_dir)
                
                if new_css != original_css:
                    style_tag.string.replace_with(new_css)
                    changes_made = True
        
        return changes_made
    
    def _rewrite_css_content(self, css_content: str, current_page_dir: Path) -> str:
        """Rewrite URLs in CSS content."""
        def replace_url(match):
            original_url = match.group(1).strip('\'"')
            new_url = self._rewrite_single_url(original_url, current_page_dir)
            
            # Preserve the quote style from the original
            if match.group(0).startswith('url("'):
                return f'url("{new_url}")'
            elif match.group(0).startswith("url('"):
                return f"url('{new_url}')"
            else:
                return f'url({new_url})'
        
        # Pattern for url() functions in CSS
        url_pattern = r'url\s*\(\s*["\']?([^"\')]+)["\']?\s*\)'
        
        return re.sub(url_pattern, replace_url, css_content, flags=re.IGNORECASE)
    
    def rewrite_css_file(self, css_path: Path, output_dir: Path) -> bool:
        """
        Rewrite URLs in a CSS file to point to local versions.
        Returns True if any changes were made.
        """
        try:
            # Read the CSS file
            with open(css_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            try:
                with open(css_path, 'r', encoding='latin1') as f:
                    content = f.read()
            except Exception as e:
                self.logger.error(f"Could not read CSS file {css_path}: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Could not read CSS file {css_path}: {e}")
            return False
        
        # Get the current CSS file's directory for relative path calculation
        current_css_relative = css_path.relative_to(output_dir)
        current_css_dir = current_css_relative.parent
        
        # Rewrite URLs in CSS content
        new_content = self._rewrite_css_content(content, current_css_dir)
        
        # Write back if changes were made
        if new_content != content:
            try:
                with open(css_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                self.logger.info(f"Rewrote URLs in {css_path}")
                return True
            except Exception as e:
                self.logger.error(f"Could not write CSS file {css_path}: {e}")
                return False
        
        return False
    
    def rewrite_all_files(self, output_dir: Path, max_workers: int = 4):
        """Rewrite URLs in all HTML and CSS files using parallel processing."""
        html_files = list(output_dir.glob('**/*.html')) + list(output_dir.glob('**/*.htm'))
        css_files = list(output_dir.glob('**/*.css'))
        
        total_files = len(html_files) + len(css_files)
        self.logger.info(f"Rewriting URLs in {len(html_files)} HTML files and {len(css_files)} CSS files using {max_workers} workers")
        
        if total_files == 0:
            self.logger.info("No files to rewrite")
            return
        
        # Process files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit HTML file tasks
            html_futures = {
                executor.submit(self._rewrite_html_file_safe, html_file, output_dir): html_file 
                for html_file in html_files
            }
            
            # Submit CSS file tasks
            css_futures = {
                executor.submit(self._rewrite_css_file_safe, css_file, output_dir): css_file 
                for css_file in css_files
            }
            
            # Combine all futures
            all_futures = {**html_futures, **css_futures}
            
            # Process completed tasks
            completed_count = 0
            failed_count = 0
            
            for future in as_completed(all_futures):
                file_path = all_futures[future]
                try:
                    success = future.result()
                    if success:
                        completed_count += 1
                    else:
                        failed_count += 1
                        self.logger.warning(f"Failed to rewrite URLs in {file_path}")
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"Error processing {file_path}: {e}")
                
                # Log progress
                if (completed_count + failed_count) % 10 == 0 or (completed_count + failed_count) == total_files:
                    self.logger.info(f"Progress: {completed_count + failed_count}/{total_files} files processed")
        
        self.logger.info(f"URL rewriting completed: {completed_count} successful, {failed_count} failed")
    
    def _rewrite_html_file_safe(self, html_path: Path, output_dir: Path) -> bool:
        """Thread-safe wrapper for HTML file rewriting."""
        try:
            return self.rewrite_html_file(html_path, output_dir)
        except Exception as e:
            self.logger.error(f"Error rewriting HTML file {html_path}: {e}")
            return False
    
    def _rewrite_css_file_safe(self, css_path: Path, output_dir: Path) -> bool:
        """Thread-safe wrapper for CSS file rewriting."""
        try:
            return self.rewrite_css_file(css_path, output_dir)
        except Exception as e:
            self.logger.error(f"Error rewriting CSS file {css_path}: {e}")
            return False
    
    def save_url_mapping(self, output_dir: Path):
        """Save URL mapping to a JSON file for reference."""
        import json
        
        mapping_file = output_dir / 'metadata' / 'url-mapping.json'
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(self.url_mapping, f, indent=2)
            self.logger.info(f"Saved URL mapping to {mapping_file}")
        except Exception as e:
            self.logger.error(f"Could not save URL mapping: {e}")
    
    def get_rewrite_statistics(self) -> Dict[str, int]:
        """Get statistics about URL rewriting operations."""
        return {
            'total_mappings': len(self.url_mapping),
            'unique_domains': len(set(urlparse(url).netloc for url in self.url_mapping.keys())),
            'local_files': len(set(self.url_mapping.values()))
        }