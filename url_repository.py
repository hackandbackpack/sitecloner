"""Repository pattern for URL storage and retrieval."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
import json
import sqlite3
import asyncio
from datetime import datetime

from url_trie import URLTrie


class URLRecord:
    """Record for a URL and its metadata."""
    
    def __init__(self, url: str, local_path: Optional[Path] = None, 
                 status: str = 'pending', metadata: Optional[Dict] = None):
        self.url = url
        self.local_path = local_path
        self.status = status  # pending, downloading, completed, failed
        self.metadata = metadata or {}
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.attempts = 0
        self.last_error = None


class URLRepository(ABC):
    """Abstract base class for URL repositories."""
    
    @abstractmethod
    async def add(self, url: str, local_path: Optional[Path] = None, 
                  metadata: Optional[Dict] = None) -> URLRecord:
        """Add a URL to the repository."""
        pass
    
    @abstractmethod
    async def get(self, url: str) -> Optional[URLRecord]:
        """Get a URL record by URL."""
        pass
    
    @abstractmethod
    async def update(self, url: str, **kwargs) -> bool:
        """Update a URL record."""
        pass
    
    @abstractmethod
    async def delete(self, url: str) -> bool:
        """Delete a URL from the repository."""
        pass
    
    @abstractmethod
    async def find_by_status(self, status: str) -> List[URLRecord]:
        """Find URLs by status."""
        pass
    
    @abstractmethod
    async def find_by_domain(self, domain: str) -> List[URLRecord]:
        """Find URLs by domain."""
        pass
    
    @abstractmethod
    async def count(self) -> int:
        """Get total count of URLs."""
        pass
    
    @abstractmethod
    async def clear(self):
        """Clear all URLs from the repository."""
        pass


class InMemoryURLRepository(URLRepository):
    """In-memory URL repository using URLTrie for efficient lookups."""
    
    def __init__(self):
        self.trie = URLTrie()
        self.records: Dict[str, URLRecord] = {}
        self._lock = asyncio.Lock()
    
    async def add(self, url: str, local_path: Optional[Path] = None, 
                  metadata: Optional[Dict] = None) -> URLRecord:
        """Add a URL to the repository."""
        async with self._lock:
            if url in self.records:
                return self.records[url]
            
            record = URLRecord(url, local_path, metadata=metadata)
            self.records[url] = record
            self.trie.insert(url, local_path, metadata or {})
            return record
    
    async def get(self, url: str) -> Optional[URLRecord]:
        """Get a URL record by URL."""
        async with self._lock:
            return self.records.get(url)
    
    async def update(self, url: str, **kwargs) -> bool:
        """Update a URL record."""
        async with self._lock:
            if url not in self.records:
                return False
            
            record = self.records[url]
            for key, value in kwargs.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            
            record.updated_at = datetime.now()
            
            # Update trie if local_path changed
            if 'local_path' in kwargs:
                self.trie.insert(url, kwargs['local_path'], record.metadata)
            
            return True
    
    async def delete(self, url: str) -> bool:
        """Delete a URL from the repository."""
        async with self._lock:
            if url in self.records:
                del self.records[url]
                # Note: URLTrie doesn't support deletion, would need to rebuild
                return True
            return False
    
    async def find_by_status(self, status: str) -> List[URLRecord]:
        """Find URLs by status."""
        async with self._lock:
            return [r for r in self.records.values() if r.status == status]
    
    async def find_by_domain(self, domain: str) -> List[URLRecord]:
        """Find URLs by domain."""
        async with self._lock:
            trie_results = self.trie.find_by_domain(domain)
            urls = [result[0] for result in trie_results]
            return [self.records[url] for url in urls if url in self.records]
    
    async def count(self) -> int:
        """Get total count of URLs."""
        async with self._lock:
            return len(self.records)
    
    async def clear(self):
        """Clear all URLs from the repository."""
        async with self._lock:
            self.records.clear()
            self.trie.clear()
    
    async def get_statistics(self) -> Dict[str, int]:
        """Get repository statistics."""
        async with self._lock:
            stats = {
                'total': len(self.records),
                'pending': 0,
                'downloading': 0,
                'completed': 0,
                'failed': 0
            }
            
            for record in self.records.values():
                if record.status in stats:
                    stats[record.status] += 1
            
            return stats


class SQLiteURLRepository(URLRepository):
    """SQLite-based URL repository for persistence."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize the database schema."""
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                local_path TEXT,
                status TEXT,
                metadata TEXT,
                created_at TEXT,
                updated_at TEXT,
                attempts INTEGER,
                last_error TEXT
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON urls(status)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_domain ON urls(url)')
        conn.commit()
        conn.close()
    
    async def add(self, url: str, local_path: Optional[Path] = None, 
                  metadata: Optional[Dict] = None) -> URLRecord:
        """Add a URL to the repository."""
        record = URLRecord(url, local_path, metadata=metadata)
        
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute('''
                INSERT OR IGNORE INTO urls 
                (url, local_path, status, metadata, created_at, updated_at, attempts, last_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                url,
                str(local_path) if local_path else None,
                record.status,
                json.dumps(record.metadata),
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                record.attempts,
                record.last_error
            ))
            conn.commit()
        finally:
            conn.close()
        
        return record
    
    async def get(self, url: str) -> Optional[URLRecord]:
        """Get a URL record by URL."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            cursor = conn.execute('SELECT * FROM urls WHERE url = ?', (url,))
            row = cursor.fetchone()
            
            if row:
                record = URLRecord(row['url'])
                record.local_path = Path(row['local_path']) if row['local_path'] else None
                record.status = row['status']
                record.metadata = json.loads(row['metadata']) if row['metadata'] else {}
                record.created_at = datetime.fromisoformat(row['created_at'])
                record.updated_at = datetime.fromisoformat(row['updated_at'])
                record.attempts = row['attempts']
                record.last_error = row['last_error']
                return record
            
            return None
        finally:
            conn.close()
    
    async def update(self, url: str, **kwargs) -> bool:
        """Update a URL record."""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Build update query dynamically
            update_fields = []
            values = []
            
            for key, value in kwargs.items():
                if key in ['local_path', 'status', 'metadata', 'attempts', 'last_error']:
                    update_fields.append(f'{key} = ?')
                    
                    if key == 'local_path':
                        values.append(str(value) if value else None)
                    elif key == 'metadata':
                        values.append(json.dumps(value))
                    else:
                        values.append(value)
            
            if update_fields:
                update_fields.append('updated_at = ?')
                values.append(datetime.now().isoformat())
                values.append(url)
                
                query = f"UPDATE urls SET {', '.join(update_fields)} WHERE url = ?"
                cursor = conn.execute(query, values)
                conn.commit()
                
                return cursor.rowcount > 0
            
            return False
        finally:
            conn.close()
    
    async def delete(self, url: str) -> bool:
        """Delete a URL from the repository."""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.execute('DELETE FROM urls WHERE url = ?', (url,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    async def find_by_status(self, status: str) -> List[URLRecord]:
        """Find URLs by status."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            cursor = conn.execute('SELECT * FROM urls WHERE status = ?', (status,))
            records = []
            
            for row in cursor:
                record = URLRecord(row['url'])
                record.local_path = Path(row['local_path']) if row['local_path'] else None
                record.status = row['status']
                record.metadata = json.loads(row['metadata']) if row['metadata'] else {}
                record.created_at = datetime.fromisoformat(row['created_at'])
                record.updated_at = datetime.fromisoformat(row['updated_at'])
                record.attempts = row['attempts']
                record.last_error = row['last_error']
                records.append(record)
            
            return records
        finally:
            conn.close()
    
    async def find_by_domain(self, domain: str) -> List[URLRecord]:
        """Find URLs by domain."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            # Simple LIKE query for domain matching
            cursor = conn.execute(
                'SELECT * FROM urls WHERE url LIKE ?', 
                (f'%://{domain}/%',)
            )
            records = []
            
            for row in cursor:
                record = URLRecord(row['url'])
                record.local_path = Path(row['local_path']) if row['local_path'] else None
                record.status = row['status']
                record.metadata = json.loads(row['metadata']) if row['metadata'] else {}
                record.created_at = datetime.fromisoformat(row['created_at'])
                record.updated_at = datetime.fromisoformat(row['updated_at'])
                record.attempts = row['attempts']
                record.last_error = row['last_error']
                records.append(record)
            
            return records
        finally:
            conn.close()
    
    async def count(self) -> int:
        """Get total count of URLs."""
        conn = sqlite3.connect(self.db_path)
        
        try:
            cursor = conn.execute('SELECT COUNT(*) FROM urls')
            return cursor.fetchone()[0]
        finally:
            conn.close()
    
    async def clear(self):
        """Clear all URLs from the repository."""
        conn = sqlite3.connect(self.db_path)
        
        try:
            conn.execute('DELETE FROM urls')
            conn.commit()
        finally:
            conn.close()