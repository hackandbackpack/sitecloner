# SiteCloner

A comprehensive Python tool for cloning websites with all their assets for local hosting and modification. SiteCloner downloads HTML pages along with all referenced assets including images, CSS files, JavaScript, fonts, and media files, then rewrites URLs to create a fully functional local copy.

## Features

### Core Functionality
- **Complete Asset Discovery**: Finds and downloads all referenced assets (images, CSS, JS, fonts, media)
- **Smart URL Resolution**: Handles relative, absolute, and protocol-relative URLs correctly
- **CSS Deep Parsing**: Extracts assets from CSS files including @import statements and url() functions
- **Concurrent Downloads**: High-performance parallel downloading with configurable limits
- **URL Rewriting**: Automatically converts external URLs to local paths for offline functionality
- **Progress Tracking**: Real-time progress bars and detailed logging

### Advanced Features
- **Multiple File Format Support**: HTML, CSS, JavaScript, images, fonts, videos, PDFs, and more
- **Robust Error Handling**: Retry logic, timeout handling, and graceful failure recovery
- **Flexible Configuration**: Extensive options for customizing behavior via JSON config files
- **Domain Filtering**: Control which domains to include/exclude
- **File Size Limits**: Prevent downloading oversized files
- **Rate Limiting**: Respectful crawling with configurable delays
- **Metadata Preservation**: Saves detailed information about the cloning process

### Output Features
- **Editable Structure**: Organized file layout for easy modification
- **Index Generation**: Automatic creation of index.html for easy access
- **URL Mapping**: Complete mapping of original URLs to local paths
- **Download Statistics**: Detailed reports on success/failure rates and transfer speeds

## Installation

1. Clone this repository:
```bash
git clone https://github.com/hackandbackpack/sitecloner.git
cd sitecloner
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

Clone a website with default settings:
```bash
python sitecloner.py https://example.com
```

Clone with custom output directory:
```bash
python sitecloner.py https://example.com -o my_cloned_site
```

Clone with specific configuration:
```bash
python sitecloner.py https://example.com --config my-config.json
```

## Command Line Options

```
usage: sitecloner.py [-h] [-o OUTPUT] [-d MAX_DEPTH] [-c CONCURRENT]
                     [--same-domain-only] [--no-external-assets]
                     [--max-file-size MAX_FILE_SIZE] [-v] [--config CONFIG]
                     [--save-config SAVE_CONFIG]
                     url

positional arguments:
  url                   URL of the website to clone

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output directory (default: cloned_site)
  -d MAX_DEPTH, --max-depth MAX_DEPTH
                        Maximum link depth to follow (default: 3)
  -c CONCURRENT, --concurrent CONCURRENT
                        Maximum concurrent downloads (default: 10)
  --same-domain-only    Only download assets from the same domain
  --no-external-assets  Do not download external assets (CSS, images, etc.)
  --max-file-size MAX_FILE_SIZE
                        Maximum file size in bytes (default: 100MB)
  -v, --verbose         Enable verbose logging
  --config CONFIG       Load configuration from JSON file
  --save-config SAVE_CONFIG
                        Save current configuration to JSON file
```

## Configuration

SiteCloner supports extensive configuration through JSON files. See `example-config.json` for a complete example.

### Key Configuration Options

#### Scope Control
- `max_depth`: How deep to follow links (default: 3)
- `same_domain_only`: Only download from the original domain
- `follow_external_assets`: Download external CSS, images, etc.
- `allowed_domains`/`blocked_domains`: Domain allowlists/blocklists

#### File Filtering
- `allowed_extensions`/`blocked_extensions`: File type filters
- `max_file_size`: Maximum individual file size (100MB default)
- `max_total_size`: Maximum total download size (1GB default)

#### Performance
- `max_concurrent_downloads`: Parallel download limit (10 default)
- `connection_timeout`/`read_timeout`: Network timeout settings
- `max_retries`: Retry attempts for failed downloads
- `rate_limit_delay`: Delay between requests (0.1s default)

#### Behavior
- `overwrite_existing`: Replace existing files
- `follow_redirects`: Handle HTTP redirects
- `user_agent`: Custom user agent string
- `custom_headers`: Additional HTTP headers

## Examples

### Basic Website Cloning
```bash
# Clone a simple website
python sitecloner.py https://example.com

# Clone with custom settings
python sitecloner.py https://example.com -o sites/example -c 5 --verbose
```

### Advanced Configuration
```bash
# Create a custom config file
python sitecloner.py https://example.com --save-config my-config.json

# Edit the config file, then use it
python sitecloner.py https://example.com --config my-config.json
```

### Domain-Specific Cloning
```bash
# Only download from the same domain
python sitecloner.py https://example.com --same-domain-only

# Don't download external assets (faster, smaller)
python sitecloner.py https://example.com --no-external-assets
```

## Output Structure

SiteCloner creates an organized directory structure:

```
cloned_site/
├── index.html              # Main page or redirect
├── example.com/             # Domain-based organization
│   ├── index.html
│   ├── page1.html
│   └── subdirectory/
├── assets/                  # (if preserve_directory_structure is false)
│   ├── css/
│   ├── js/
│   ├── images/
│   └── fonts/
├── metadata/                # Clone information
│   ├── url-mapping.json     # URL to local path mapping
│   ├── download-stats.json  # Download statistics
│   └── clone-info.json      # General information
└── sitecloner.log          # Detailed log file
```

## Supported File Types

SiteCloner automatically handles:

- **Web Pages**: HTML, XHTML
- **Stylesheets**: CSS, SCSS, SASS
- **Scripts**: JavaScript, JSON
- **Images**: JPG, PNG, GIF, SVG, WebP, ICO, BMP
- **Fonts**: WOFF, WOFF2, TTF, OTF, EOT
- **Media**: MP4, WebM, MP3, WAV, OGG
- **Documents**: PDF, and more

## Limitations

- **Dynamic Content**: Cannot execute JavaScript to discover dynamically loaded content
- **Authentication**: Does not handle login-protected content
- **Single Page Applications**: Limited support for SPAs that rely heavily on JavaScript
- **Real-time Content**: Cannot capture live/streaming content
- **Server-side Rendering**: Cannot replicate server-side functionality

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure write permissions to output directory
2. **Network Timeouts**: Increase timeout values in configuration
3. **Memory Issues**: Reduce concurrent downloads for large sites
4. **Encoding Problems**: Some files may have encoding issues; check logs

### Verbose Logging
Use the `-v` flag for detailed logging:
```bash
python sitecloner.py https://example.com -v
```

### Failed Downloads
Check the metadata directory for failed download information and retry manually if needed.

## Legal and Ethical Considerations

- **Respect robots.txt**: Enabled by default
- **Rate Limiting**: Built-in delays to avoid overloading servers  
- **Terms of Service**: Always check and comply with website terms
- **Copyright**: Respect intellectual property rights
- **Personal Use**: Intended for archival and educational purposes

## Development

### Architecture

SiteCloner uses a modular architecture:

- `sitecloner.py`: Main orchestrator
- `config.py`: Configuration management
- `url_resolver.py`: URL resolution and normalization
- `asset_discoverer.py`: Asset discovery in HTML/CSS
- `download_manager.py`: Concurrent download handling
- `url_rewriter.py`: URL rewriting for local paths

### Testing

Test with various website types:
```bash
# Simple static site
python sitecloner.py https://example.com

# Site with external assets
python sitecloner.py https://getbootstrap.com

# Content-heavy site (use size limits)
python sitecloner.py https://wikipedia.org --max-file-size 10485760
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.