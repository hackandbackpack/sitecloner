"""Main SiteCloner class that orchestrates the website cloning process."""

import asyncio
import logging
import json
import time
import signal
import sys
from pathlib import Path
from typing import Set, Dict, Optional, List
from urllib.parse import urlparse, urljoin
from tqdm import tqdm
import colorama
from colorama import Fore, Style

from config import SiteClonerConfig
from url_resolver import URLResolver
from asset_discoverer import AssetDiscoverer
from download_manager import DownloadManager, DownloadResult
from url_rewriter import URLRewriter


class SiteCloner:
    """Main class for cloning websites."""
    
    def __init__(self, config: SiteClonerConfig):
        self.config = config
        self.url_resolver = URLResolver(config.target_url)
        self.asset_discoverer = AssetDiscoverer(self.url_resolver)
        self.download_manager = DownloadManager(config, self.url_resolver)
        self.url_rewriter = URLRewriter(self.url_resolver)
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger('sitecloner')
        
        # Initialize colorama for colored output
        colorama.init()
        
        # Tracking
        self.discovered_urls: Set[str] = set()
        self.processed_urls: Set[str] = set()
        self.download_results: Dict[str, DownloadResult] = {}
        
        # Progress tracking
        self.progress_bar: Optional[tqdm] = None
        
        # Error recovery and state management
        self.state_file = self.config.output_dir / 'sitecloner_state.json'
        self.recovery_checkpoint_interval = 50  # Save state every 50 downloads
        self.max_recovery_attempts = 3
        self.shutdown_requested = False
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
    def _setup_logging(self):
        """Setup logging configuration."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        log_level = logging.DEBUG if self.config.verbose_logging else logging.INFO
        
        # Create output directory for log file
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.config.output_dir / 'sitecloner.log')
            ]
        )
    
    async def clone_site(self) -> bool:
        """
        Main method to clone a website.
        Returns True if the cloning was successful.
        """
        self.logger.info(f"Starting to clone {self.config.target_url}")
        self.logger.info(f"Output directory: {self.config.output_dir}")
        
        try:
            # Create output directory
            self.config.output_dir.mkdir(parents=True, exist_ok=True)
            
            # Phase 1: Download main page and discover initial assets
            self.logger.info("Phase 1: Downloading main page...")
            main_page_content, main_result = await self.download_manager.download_and_parse_html(
                self.config.target_url, self.config.output_dir
            )
            
            if not main_result.success:
                self.logger.error(f"Failed to download main page: {main_result.error}")
                return False
            
            # Add main page to URL mapping
            if main_result.local_path:
                self.url_rewriter.add_url_mapping(
                    self.config.target_url, 
                    main_result.local_path, 
                    self.config.output_dir
                )
            
            # Discover assets in main page
            if main_page_content:
                main_assets = self.asset_discoverer.discover_assets_in_html(
                    main_page_content, self.config.target_url
                )
                
                # Collect all asset URLs
                all_assets = set()
                for asset_type, urls in main_assets.items():
                    all_assets.update(urls)
                    self.logger.info(f"Found {len(urls)} {asset_type}")
                
                self.discovered_urls.update(all_assets)
            
            # Phase 2: Download all discovered assets
            if self.discovered_urls:
                self.logger.info(f"Phase 2: Downloading {len(self.discovered_urls)} assets...")
                
                # Setup progress bar
                if not self.config.verbose_logging:
                    self.progress_bar = tqdm(
                        total=len(self.discovered_urls),
                        desc="Downloading assets",
                        unit="files"
                    )
                    self.download_manager.set_progress_callback(self._update_progress)
                
                # Download assets in batches to manage memory
                batch_size = min(self.config.max_concurrent_downloads * 2, 50)
                asset_urls = list(self.discovered_urls)
                
                for i in range(0, len(asset_urls), batch_size):
                    batch = set(asset_urls[i:i + batch_size])
                    batch_results = await self.download_manager.download_urls(batch, self.config.output_dir)
                    
                    # Add successful downloads to URL mapping
                    for url, result in batch_results.items():
                        if result.success and result.local_path:
                            self.url_rewriter.add_url_mapping(url, result.local_path, self.config.output_dir)
                        self.download_results[url] = result
                    
                    # Process downloaded CSS files for additional assets
                    await self._process_css_files(batch_results)
                    
                    # Download any newly discovered CSS assets immediately
                    new_assets_to_download = self.discovered_urls - set(self.download_results.keys())
                    if new_assets_to_download:
                        self.logger.info(f"Found {len(new_assets_to_download)} new assets from CSS processing")
                        new_results = await self.download_manager.download_urls(new_assets_to_download, self.config.output_dir)
                        for url, result in new_results.items():
                            if result.success and result.local_path:
                                self.url_rewriter.add_url_mapping(url, result.local_path, self.config.output_dir)
                            self.download_results[url] = result
                
                if self.progress_bar:
                    self.progress_bar.close()
            
            # Phase 3: Process additional assets found in CSS files (iteratively)
            await self._process_css_files_recursively()
            
            # Phase 3.5: Aggressively retry all failed downloads
            self.logger.info("Phase 3.5: Retrying failed downloads for 100% success...")
            retry_results = await self.download_manager.retry_failed_downloads(self.download_results, self.config.output_dir)
            
            # Update URL mapping for any successful retries
            for url, result in retry_results.items():
                if result.success and result.local_path:
                    self.url_rewriter.add_url_mapping(url, result.local_path, self.config.output_dir)
            
            # Phase 4: Rewrite URLs to point to local files
            self.logger.info("Phase 4: Rewriting URLs to local paths...")
            self.url_rewriter.rewrite_all_files(self.config.output_dir)
            
            # Phase 5: Save metadata
            if self.config.save_metadata:
                self._save_metadata()
            
            # Phase 6: Create index file if needed
            if self.config.create_index_file:
                self._create_index_file()
            
            # Print summary
            self._print_summary()
            
            self.logger.info("Site cloning completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during site cloning: {e}", exc_info=True)
            self.save_state()  # Save state on error for recovery
            return False
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True
            if self.progress_bar:
                self.progress_bar.close()
        
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
        except Exception as e:
            self.logger.warning(f"Could not setup signal handlers: {e}")
    
    def save_state(self):
        """Save current cloning state for recovery."""
        try:
            state = {
                'target_url': self.config.target_url,
                'discovered_urls': list(self.discovered_urls),
                'processed_urls': list(self.processed_urls),
                'download_results': {
                    url: {
                        'success': result.success,
                        'local_path': str(result.local_path) if result.local_path else None,
                        'error': result.error,
                        'file_size': result.file_size
                    } for url, result in self.download_results.items()
                },
                'url_mapping': dict(self.url_rewriter.url_mapping),
                'timestamp': time.time()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            
            self.logger.debug(f"State saved to {self.state_file}")
        except Exception as e:
            self.logger.warning(f"Could not save state: {e}")
    
    def load_state(self) -> bool:
        """Load previous cloning state for recovery."""
        try:
            if not self.state_file.exists():
                return False
            
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # Verify this state matches current target
            if state.get('target_url') != self.config.target_url:
                self.logger.warning("State file is for different target URL, ignoring")
                return False
            
            # Restore state
            self.discovered_urls = set(state.get('discovered_urls', []))
            self.processed_urls = set(state.get('processed_urls', []))
            
            # Restore download results
            for url, result_data in state.get('download_results', {}).items():
                local_path = Path(result_data['local_path']) if result_data['local_path'] else None
                self.download_results[url] = DownloadResult(
                    url=url,
                    success=result_data['success'],
                    local_path=local_path,
                    error=result_data.get('error'),
                    file_size=result_data.get('file_size', 0)
                )
            
            # Restore URL mapping
            url_mapping = state.get('url_mapping', {})
            for original_url, local_path in url_mapping.items():
                self.url_rewriter.url_mapping[original_url] = local_path
            
            # Update download manager state
            for url, result in self.download_results.items():
                if result.success:
                    self.download_manager.downloaded_urls.add(url)
                else:
                    self.download_manager.failed_urls.add(url)
            
            self.logger.info(f"Restored state: {len(self.discovered_urls)} discovered URLs, {len(self.download_results)} download results")
            return True
            
        except Exception as e:
            self.logger.warning(f"Could not load state: {e}")
            return False
    
    def cleanup_state(self):
        """Clean up state file after successful completion."""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
                self.logger.debug("State file cleaned up")
        except Exception as e:
            self.logger.warning(f"Could not cleanup state file: {e}")
    
    async def clone_site_with_recovery(self) -> bool:
        """Main method to clone a website with comprehensive error recovery."""
        recovery_attempt = 0
        
        while recovery_attempt < self.max_recovery_attempts:
            try:
                # Try to load previous state
                if recovery_attempt > 0:
                    self.logger.info(f"Recovery attempt {recovery_attempt}/{self.max_recovery_attempts}")
                    self.load_state()
                
                # Attempt cloning
                success = await self.clone_site()
                
                if success:
                    self.cleanup_state()
                    return True
                else:
                    recovery_attempt += 1
                    if recovery_attempt < self.max_recovery_attempts:
                        self.logger.warning(f"Cloning failed, will retry (attempt {recovery_attempt + 1})")
                        await asyncio.sleep(5)  # Brief pause before retry
                    
            except KeyboardInterrupt:
                self.logger.info("Cloning interrupted by user")
                self.save_state()
                return False
            except Exception as e:
                self.logger.error(f"Unexpected error during cloning: {e}", exc_info=True)
                recovery_attempt += 1
                if recovery_attempt < self.max_recovery_attempts:
                    self.logger.warning(f"Will retry after error (attempt {recovery_attempt + 1})")
                    self.save_state()
                    await asyncio.sleep(10)  # Longer pause after error
        
        self.logger.error(f"Failed after {self.max_recovery_attempts} attempts")
        return False
    
    async def _process_css_files(self, download_results: Dict[str, DownloadResult]):
        """Process downloaded CSS files to find additional assets."""
        css_files = []
        
        for url, result in download_results.items():
            if (result.success and result.local_path and 
                self.url_resolver.should_process_for_assets(url) and
                (url.endswith('.css') or '.css' in url)):
                css_files.append((url, result.local_path))
        
        # Process CSS files to find additional assets
        new_assets = set()
        for css_url, css_path in css_files:
            try:
                with open(css_path, 'r', encoding='utf-8') as f:
                    css_content = f.read()
                
                css_assets = self.asset_discoverer.discover_assets_in_css(css_content, css_url)
                
                for asset_type, urls in css_assets.items():
                    new_assets.update(urls)
                    self.logger.debug(f"Found {len(urls)} {asset_type} in {css_url}")
                    
            except Exception as e:
                self.logger.warning(f"Could not process CSS file {css_path}: {e}")
        
        # Filter out already discovered assets
        truly_new_assets = new_assets - self.discovered_urls - set(self.download_results.keys())
        
        if truly_new_assets:
            self.logger.info(f"Found {len(truly_new_assets)} additional assets in CSS files")
            for url in list(truly_new_assets)[:10]:  # Log first 10 for debugging
                self.logger.debug(f"New CSS asset: {url}")
            self.discovered_urls.update(truly_new_assets)
    
    async def _process_css_files_recursively(self):
        """Recursively process CSS files with comprehensive safety measures."""
        max_iterations = 10  # Increased but still bounded
        max_total_css_files = 500  # Prevent processing too many CSS files
        max_css_depth = 8  # Track import depth to prevent deep recursion
        iteration = 0
        
        # Track CSS import chains to detect cycles
        css_import_graph = {}  # url -> set of imported urls
        processed_css_files = set()  # Track which CSS files we've already processed
        css_depth_tracker = {}  # url -> depth level
        
        while iteration < max_iterations:
            iteration += 1
            self.logger.info(f"CSS processing iteration {iteration}")
            
            # Find new CSS files to process
            css_files_to_process = {}
            for url, result in self.download_results.items():
                if (result.success and result.local_path and 
                    self.url_resolver.should_process_for_assets(url) and
                    (url.endswith('.css') or '.css' in url) and
                    url not in processed_css_files):
                    
                    # Check if we've reached the CSS file limit
                    if len(processed_css_files) >= max_total_css_files:
                        self.logger.warning(f"Reached maximum CSS file limit ({max_total_css_files})")
                        break
                    
                    css_files_to_process[url] = result.local_path
            
            if not css_files_to_process:
                self.logger.info(f"No new CSS files to process in iteration {iteration}")
                break
            
            # Process CSS files for additional assets with safety checks
            new_assets = set()
            for css_url, css_path in css_files_to_process.items():
                try:
                    # Mark as processed to prevent reprocessing
                    processed_css_files.add(css_url)
                    
                    # Check file size before processing
                    if css_path.stat().st_size > 10 * 1024 * 1024:  # 10MB limit
                        self.logger.warning(f"Skipping large CSS file {css_path} ({css_path.stat().st_size} bytes)")
                        continue
                    
                    # Use proper encoding detection for CSS files
                    css_content = await self.download_manager._read_file_with_encoding_detection(css_path)
                    if not css_content:
                        self.logger.warning(f"Could not read CSS file {css_path}")
                        continue
                    
                    # Discover assets in this CSS file
                    css_assets = self.asset_discoverer.discover_assets_in_css(css_content, css_url)
                    
                    # Track imports for cycle detection
                    imported_css_urls = set()
                    if 'css' in css_assets:
                        imported_css_urls = css_assets['css']
                        css_import_graph[css_url] = imported_css_urls
                        
                        # Check for import cycles
                        if self._has_css_import_cycle(css_url, css_import_graph):
                            self.logger.warning(f"Detected CSS import cycle involving {css_url}, breaking chain")
                            continue
                        
                        # Set depth for imported CSS files
                        current_depth = css_depth_tracker.get(css_url, 0)
                        for imported_url in imported_css_urls:
                            new_depth = current_depth + 1
                            if new_depth > max_css_depth:
                                self.logger.warning(f"CSS import depth limit reached for {imported_url}")
                                continue
                            css_depth_tracker[imported_url] = new_depth
                    
                    # Add all discovered assets
                    for asset_type, urls in css_assets.items():
                        new_assets.update(urls)
                        
                except Exception as e:
                    self.logger.warning(f"Could not process CSS file {css_path}: {e}")
                    # Mark as processed even if failed to prevent retry loops
                    processed_css_files.add(css_url)
            
            # Filter out already discovered assets and depth-limited ones
            already_discovered = set()
            depth_limited = set()
            
            for url in new_assets:
                if url in self.download_results and self.download_results[url].success:
                    already_discovered.add(url)
                elif url.endswith('.css') and css_depth_tracker.get(url, 0) > max_css_depth:
                    depth_limited.add(url)
            
            truly_new_assets = new_assets - already_discovered - depth_limited
            
            if not truly_new_assets:
                self.logger.info(f"No new CSS assets found in iteration {iteration}")
                break
                
            self.logger.info(f"Found {len(truly_new_assets)} new CSS assets in iteration {iteration}")
            if depth_limited:
                self.logger.info(f"Skipped {len(depth_limited)} assets due to depth limits")
            
            for url in list(truly_new_assets)[:5]:  # Log first 5 for debugging
                self.logger.debug(f"New CSS asset: {url}")
            
            # Add to discovered assets
            self.discovered_urls.update(truly_new_assets)
            
            # Download the new assets
            additional_results = await self.download_manager.download_urls(
                truly_new_assets, self.config.output_dir
            )
            
            # Add to URL mapping and results
            for url, result in additional_results.items():
                if result.success and result.local_path:
                    self.url_rewriter.add_url_mapping(url, result.local_path, self.config.output_dir)
                self.download_results[url] = result
        
        if iteration >= max_iterations:
            self.logger.warning(f"Reached maximum CSS processing iterations ({max_iterations})")
        
        self.logger.info(f"CSS processing completed: {len(processed_css_files)} files processed, max depth: {max(css_depth_tracker.values()) if css_depth_tracker else 0}")
    
    def _has_css_import_cycle(self, start_url: str, import_graph: dict, visited: set = None, path: set = None) -> bool:
        """Detect cycles in CSS import chains using DFS."""
        if visited is None:
            visited = set()
        if path is None:
            path = set()
        
        if start_url in path:
            return True  # Cycle detected
        
        if start_url in visited:
            return False  # Already checked this branch
        
        visited.add(start_url)
        path.add(start_url)
        
        # Check all imports from this CSS file
        for imported_url in import_graph.get(start_url, set()):
            if self._has_css_import_cycle(imported_url, import_graph, visited, path):
                return True
        
        path.remove(start_url)
        return False
    
    async def _download_additional_css_assets(self):
        """Download any additional assets found in CSS files."""
        # Get assets that haven't been downloaded yet
        remaining_assets = self.discovered_urls - set(self.download_results.keys())
        
        if remaining_assets:
            self.logger.info(f"Downloading {len(remaining_assets)} additional assets...")
            
            additional_results = await self.download_manager.download_urls(
                remaining_assets, self.config.output_dir
            )
            
            # Add to URL mapping and results
            for url, result in additional_results.items():
                if result.success and result.local_path:
                    self.url_rewriter.add_url_mapping(url, result.local_path, self.config.output_dir)
                self.download_results[url] = result
    
    def _update_progress(self, url: str, downloaded: int, total: int):
        """Update progress bar."""
        if self.progress_bar:
            # We don't have per-file progress, so just update the total files completed
            pass
    
    def _save_metadata(self):
        """Save metadata about the cloning process."""
        metadata_dir = self.config.output_dir / 'metadata'
        metadata_dir.mkdir(exist_ok=True)
        
        # Save URL mapping
        self.url_rewriter.save_url_mapping(self.config.output_dir)
        
        # Save download statistics
        stats = self.download_manager.get_stats()
        stats_data = {
            'total_files': stats.total_files,
            'downloaded_files': stats.downloaded_files,
            'failed_files': stats.failed_files,
            'total_bytes': stats.total_bytes,
            'elapsed_time': stats.get_elapsed_time(),
            'download_rate': stats.get_download_rate(),
            'errors': stats.errors[:100]  # Limit error list size
        }
        
        with open(metadata_dir / 'download-stats.json', 'w') as f:
            json.dump(stats_data, f, indent=2)
        
        # Save clone information
        clone_info = {
            'target_url': self.config.target_url,
            'clone_timestamp': time.time(),
            'clone_time_iso': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
            'total_urls_discovered': len(self.discovered_urls),
            'successful_downloads': len([r for r in self.download_results.values() if r.success]),
            'failed_downloads': len([r for r in self.download_results.values() if not r.success]),
            'output_directory': str(self.config.output_dir),
            'config': {
                'max_depth': self.config.max_depth,
                'same_domain_only': self.config.same_domain_only,
                'follow_external_assets': self.config.follow_external_assets,
                'max_file_size': self.config.max_file_size,
                'max_concurrent_downloads': self.config.max_concurrent_downloads
            }
        }
        
        with open(metadata_dir / 'clone-info.json', 'w') as f:
            json.dump(clone_info, f, indent=2)
        
        self.logger.info(f"Saved metadata to {metadata_dir}")
    
    def _create_index_file(self):
        """Ensure there's a proper index.html file for easy hosting."""
        index_path = self.config.output_dir / 'index.html'
        
        # The main page should already be saved as index.html by the URL resolver
        # But let's check if it exists and create one if needed
        if not index_path.exists():
            # Find the main page file
            main_page_path = None
            for url, result in self.download_results.items():
                if url == self.config.target_url and result.success and result.local_path:
                    main_page_path = result.local_path
                    break
            
            if main_page_path:
                try:
                    if main_page_path != index_path:
                        # Copy the main page content to index.html
                        with open(main_page_path, 'r', encoding='utf-8') as src:
                            content = src.read()
                        with open(index_path, 'w', encoding='utf-8') as dst:
                            dst.write(content)
                        self.logger.info(f"Created index.html from {main_page_path}")
                    else:
                        self.logger.info("Main page already saved as index.html")
                        
                except Exception as e:
                    self.logger.warning(f"Could not create index.html: {e}")
            else:
                self.logger.warning("No main page found to create index.html")
    
    def _is_essential_for_rendering(self, url: str, error: str = "") -> bool:
        """Check if a failed URL is essential for proper site rendering."""
        url_lower = url.lower()
        
        # Non-essential external domains
        non_essential_domains = [
            'twitter.com', 'facebook.com', 'linkedin.com', 'instagram.com',
            'youtube.com', 'google-analytics.com', 'googletagmanager.com',
            'doubleclick.net', 'googlesyndication.com', 'hs-scripts.com',
            'hubspot.com', 'mixpanel.com', 'segment.com'
        ]
        
        # Non-essential file patterns
        non_essential_patterns = [
            'xmlrpc.php', 'wp-json', 'wp-admin', 'wp-login',
            'analytics', 'tracking', 'gtag', 'facebook.com',
            'twitter.com', 'linkedin.com'
        ]
        
        # Non-essential errors
        non_essential_errors = [
            'File filtered by configuration',
            'HTTP 403', 'HTTP 401'  # Permission denied
        ]
        
        # Check domains
        for domain in non_essential_domains:
            if domain in url_lower:
                return False
        
        # Check patterns
        for pattern in non_essential_patterns:
            if pattern in url_lower:
                return False
        
        # Check error types
        for error_pattern in non_essential_errors:
            if error_pattern in error:
                return False
        
        # Everything else is considered essential
        return True
    
    def _verify_downloads(self):
        """Verify that all discovered URLs were actually downloaded and files exist."""
        # Check for URLs that were never attempted
        missing_urls = self.discovered_urls - set(self.download_results.keys())
        
        # Check for failed downloads - separate essential vs non-essential
        essential_failed = set()
        non_essential_failed = set()
        
        for url, result in self.download_results.items():
            if not result.success:
                if self._is_essential_for_rendering(url, result.error or ""):
                    essential_failed.add(url)
                else:
                    non_essential_failed.add(url)
        
        # Check for missing files (successful downloads but file doesn't exist)
        missing_files = set()
        for url, result in self.download_results.items():
            if result.success and result.local_path:
                if not result.local_path.exists():
                    missing_files.add(url)
                elif result.local_path.stat().st_size == 0:
                    missing_files.add(url)
        
        # Only essential issues count as real problems
        essential_issues = missing_urls | essential_failed | missing_files
        
        if missing_urls:
            self.logger.warning(f"Found {len(missing_urls)} discovered URLs that were never downloaded:")
            for url in list(missing_urls)[:5]:
                self.logger.warning(f"  Never attempted: {url}")
        
        if essential_failed:
            self.logger.warning(f"Found {len(essential_failed)} ESSENTIAL failed downloads:")
            for url in list(essential_failed)[:5]:
                result = self.download_results[url]
                self.logger.warning(f"  ESSENTIAL Failed: {url} - {result.error}")
        
        if non_essential_failed:
            self.logger.info(f"Found {len(non_essential_failed)} non-essential failed downloads (analytics, social media, etc.):")
            for url in list(non_essential_failed)[:3]:
                result = self.download_results[url]
                self.logger.info(f"  Non-essential: {url} - {result.error}")
        
        if missing_files:
            self.logger.warning(f"Found {len(missing_files)} successful downloads with missing files:")
            for url in list(missing_files)[:5]:
                result = self.download_results[url]
                self.logger.warning(f"  Missing file: {url} -> {result.local_path}")
        
        if not essential_issues:
            self.logger.info("All ESSENTIAL URLs were successfully downloaded and verified")
            return True
        else:
            self.logger.warning(f"ESSENTIAL issues found: {len(essential_issues)}")
            return False
    
    def _print_summary(self):
        """Print a summary of the cloning process."""
        stats = self.download_manager.get_stats()
        
        # Verify downloads
        all_downloaded = self._verify_downloads()
        
        print(f"\n{Fore.GREEN}=== Site Cloning Summary ==={Style.RESET_ALL}")
        print(f"Target URL: {self.config.target_url}")
        print(f"Output Directory: {self.config.output_dir}")
        print(f"Total URLs discovered: {len(self.discovered_urls)}")
        # Calculate essential vs non-essential failures
        essential_failures = 0
        non_essential_failures = 0
        
        for url, result in self.download_results.items():
            if not result.success:
                if self._is_essential_for_rendering(url, result.error or ""):
                    essential_failures += 1
                else:
                    non_essential_failures += 1
        
        print(f"Successfully downloaded: {Fore.GREEN}{stats.downloaded_files}{Style.RESET_ALL}")
        
        if essential_failures > 0:
            print(f"ESSENTIAL failures: {Fore.RED}{essential_failures}{Style.RESET_ALL}")
        else:
            print(f"ESSENTIAL failures: {Fore.GREEN}0{Style.RESET_ALL}")
            
        if non_essential_failures > 0:
            print(f"Non-essential failures: {Fore.YELLOW}{non_essential_failures}{Style.RESET_ALL} (analytics, social media)")
        
        missing_count = len(self.discovered_urls - set(self.download_results.keys()))
        if missing_count > 0:
            print(f"Missing downloads: {Fore.YELLOW}{missing_count}{Style.RESET_ALL}")
        
        print(f"Total data downloaded: {self._format_bytes(stats.total_bytes)}")
        print(f"Time elapsed: {self._format_time(stats.get_elapsed_time())}")
        print(f"Average speed: {self._format_bytes(stats.get_download_rate())}/s")
        
        if essential_failures > 0:
            print(f"\n{Fore.RED}ESSENTIAL failed downloads:{Style.RESET_ALL}")
            for url, result in self.download_results.items():
                if not result.success and self._is_essential_for_rendering(url, result.error or ""):
                    print(f"  {url}: {result.error}")
        
        if non_essential_failures > 0 and essential_failures == 0:
            print(f"\n{Fore.YELLOW}Non-essential failed downloads (site will still work):{Style.RESET_ALL}")
            count = 0
            for url, result in self.download_results.items():
                if not result.success and not self._is_essential_for_rendering(url, result.error or ""):
                    print(f"  {url}: {result.error}")
                    count += 1
                    if count >= 3:  # Limit output
                        break
        
        if essential_failures == 0:
            print(f"\n{Fore.GREEN}SUCCESS: All essential files downloaded - site should render properly!{Style.RESET_ALL}")
        elif not all_downloaded:
            print(f"\n{Fore.RED}WARNING: Essential files missing - site may not render correctly. Check logs for details.{Style.RESET_ALL}")
        
        print(f"\n{Fore.CYAN}Files saved to: {self.config.output_dir}{Style.RESET_ALL}")
        
        # Check for index file
        index_file = self.config.output_dir / 'index.html'
        if index_file.exists():
            print(f"Open {Fore.CYAN}{index_file}{Style.RESET_ALL} in your browser to view the cloned site.")
    
    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_count < 1024:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024
        return f"{bytes_count:.1f} TB"
    
    def _format_time(self, seconds: float) -> str:
        """Format time in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"


# CLI Interface
async def main():
    """Main CLI interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clone a website for local hosting')
    parser.add_argument('url', help='URL of the website to clone')
    parser.add_argument('-o', '--output', type=Path, default=Path('cloned_site'),
                       help='Output directory (default: cloned_site)')
    parser.add_argument('-d', '--max-depth', type=int, default=3,
                       help='Maximum link depth to follow (default: 3)')
    parser.add_argument('-c', '--concurrent', type=int, default=10,
                       help='Maximum concurrent downloads (default: 10)')
    parser.add_argument('--same-domain-only', action='store_true',
                       help='Only download assets from the same domain')
    parser.add_argument('--no-external-assets', action='store_true',
                       help='Do not download external assets (CSS, images, etc.)')
    parser.add_argument('--max-file-size', type=int, default=100*1024*1024,
                       help='Maximum file size in bytes (default: 100MB)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--config', type=Path,
                       help='Load configuration from JSON file')
    parser.add_argument('--save-config', type=Path,
                       help='Save current configuration to JSON file')
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config and args.config.exists():
        config = SiteClonerConfig.from_file(args.config)
    else:
        config = SiteClonerConfig()
    
    # Override with command line arguments
    config.target_url = args.url
    config.output_dir = args.output
    config.max_depth = args.max_depth
    config.max_concurrent_downloads = args.concurrent
    config.same_domain_only = args.same_domain_only
    config.follow_external_assets = not args.no_external_assets
    config.max_file_size = args.max_file_size
    config.verbose_logging = args.verbose
    
    # Save configuration if requested
    if args.save_config:
        config.to_file(args.save_config)
        print(f"Configuration saved to {args.save_config}")
        return
    
    # Create and run cloner
    cloner = SiteCloner(config)
    success = await cloner.clone_site()
    
    return 0 if success else 1


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    exit(exit_code)