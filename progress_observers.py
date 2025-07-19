"""Progress tracking using the Observer pattern."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import asyncio
from enum import Enum


class EventType(Enum):
    """Types of progress events."""
    DOWNLOAD_STARTED = "download_started"
    DOWNLOAD_PROGRESS = "download_progress"
    DOWNLOAD_COMPLETED = "download_completed"
    DOWNLOAD_FAILED = "download_failed"
    PHASE_STARTED = "phase_started"
    PHASE_COMPLETED = "phase_completed"
    DISCOVERY_STARTED = "discovery_started"
    DISCOVERY_COMPLETED = "discovery_completed"
    REWRITE_STARTED = "rewrite_started"
    REWRITE_COMPLETED = "rewrite_completed"
    ERROR_OCCURRED = "error_occurred"
    WARNING_RAISED = "warning_raised"


@dataclass
class ProgressEvent:
    """Event data for progress notifications."""
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any]
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ProgressObserver(ABC):
    """Abstract base class for progress observers."""
    
    @abstractmethod
    async def update(self, event: ProgressEvent):
        """Receive and process a progress event."""
        pass


class ConsoleProgressObserver(ProgressObserver):
    """Observer that logs progress to console."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.download_count = 0
        self.total_downloads = 0
    
    async def update(self, event: ProgressEvent):
        """Log progress to console."""
        if event.event_type == EventType.DOWNLOAD_STARTED:
            if self.verbose:
                print(f"⬇️  Downloading: {event.data.get('url', 'Unknown')}")
        
        elif event.event_type == EventType.DOWNLOAD_COMPLETED:
            self.download_count += 1
            if self.verbose:
                size = event.data.get('size', 0)
                print(f"✅ Downloaded: {event.data.get('url', 'Unknown')} ({size:,} bytes)")
            elif self.download_count % 10 == 0:
                print(f"Progress: {self.download_count} files downloaded")
        
        elif event.event_type == EventType.DOWNLOAD_FAILED:
            print(f"❌ Failed: {event.data.get('url', 'Unknown')} - {event.data.get('error', 'Unknown error')}")
        
        elif event.event_type == EventType.PHASE_STARTED:
            print(f"\n🚀 Starting {event.data.get('phase', 'Unknown phase')}...")
        
        elif event.event_type == EventType.PHASE_COMPLETED:
            print(f"✅ Completed {event.data.get('phase', 'Unknown phase')}")
        
        elif event.event_type == EventType.ERROR_OCCURRED:
            print(f"❌ ERROR: {event.data.get('message', 'Unknown error')}")
        
        elif event.event_type == EventType.WARNING_RAISED:
            print(f"⚠️  WARNING: {event.data.get('message', 'Unknown warning')}")


class FileProgressObserver(ProgressObserver):
    """Observer that logs progress to a file."""
    
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.file_lock = asyncio.Lock()
    
    async def update(self, event: ProgressEvent):
        """Log progress to file."""
        async with self.file_lock:
            try:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{event.timestamp.isoformat()} - {event.event_type.value} - {event.data}\n")
            except Exception as e:
                print(f"Failed to write to progress log: {e}")


class StatisticsObserver(ProgressObserver):
    """Observer that collects statistics."""
    
    def __init__(self):
        self.stats = {
            'downloads_started': 0,
            'downloads_completed': 0,
            'downloads_failed': 0,
            'total_bytes': 0,
            'errors': 0,
            'warnings': 0,
            'phases_completed': [],
            'start_time': datetime.now(),
            'failures': []
        }
    
    async def update(self, event: ProgressEvent):
        """Update statistics based on events."""
        if event.event_type == EventType.DOWNLOAD_STARTED:
            self.stats['downloads_started'] += 1
        
        elif event.event_type == EventType.DOWNLOAD_COMPLETED:
            self.stats['downloads_completed'] += 1
            self.stats['total_bytes'] += event.data.get('size', 0)
        
        elif event.event_type == EventType.DOWNLOAD_FAILED:
            self.stats['downloads_failed'] += 1
            self.stats['failures'].append({
                'url': event.data.get('url'),
                'error': event.data.get('error'),
                'timestamp': event.timestamp
            })
        
        elif event.event_type == EventType.PHASE_COMPLETED:
            self.stats['phases_completed'].append(event.data.get('phase'))
        
        elif event.event_type == EventType.ERROR_OCCURRED:
            self.stats['errors'] += 1
        
        elif event.event_type == EventType.WARNING_RAISED:
            self.stats['warnings'] += 1
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        return {
            'duration_seconds': elapsed,
            'downloads_started': self.stats['downloads_started'],
            'downloads_completed': self.stats['downloads_completed'],
            'downloads_failed': self.stats['downloads_failed'],
            'success_rate': self.stats['downloads_completed'] / max(self.stats['downloads_started'], 1),
            'total_bytes': self.stats['total_bytes'],
            'errors': self.stats['errors'],
            'warnings': self.stats['warnings'],
            'phases_completed': self.stats['phases_completed'],
            'failures': self.stats['failures'][:10]  # Top 10 failures
        }


class ProgressBarObserver(ProgressObserver):
    """Observer that updates a progress bar (tqdm compatible)."""
    
    def __init__(self, progress_bar=None):
        self.progress_bar = progress_bar
        self.total_expected = 0
    
    async def update(self, event: ProgressEvent):
        """Update progress bar."""
        if not self.progress_bar:
            return
        
        if event.event_type == EventType.DISCOVERY_COMPLETED:
            self.total_expected = event.data.get('total_urls', 0)
            if hasattr(self.progress_bar, 'total'):
                self.progress_bar.total = self.total_expected
        
        elif event.event_type == EventType.DOWNLOAD_COMPLETED:
            self.progress_bar.update(1)
            if hasattr(self.progress_bar, 'set_postfix'):
                self.progress_bar.set_postfix({
                    'file': event.data.get('url', '')[-30:],  # Last 30 chars
                    'size': f"{event.data.get('size', 0) / 1024:.1f}KB"
                })
        
        elif event.event_type == EventType.DOWNLOAD_FAILED:
            self.progress_bar.update(1)


class CompositeProgressObserver(ProgressObserver):
    """Observer that forwards events to multiple observers."""
    
    def __init__(self, observers: List[ProgressObserver] = None):
        self.observers = observers or []
    
    def add_observer(self, observer: ProgressObserver):
        """Add an observer to the composite."""
        self.observers.append(observer)
    
    def remove_observer(self, observer: ProgressObserver):
        """Remove an observer from the composite."""
        self.observers.remove(observer)
    
    async def update(self, event: ProgressEvent):
        """Forward event to all observers."""
        # Run all observer updates concurrently
        tasks = [observer.update(event) for observer in self.observers]
        await asyncio.gather(*tasks, return_exceptions=True)


class ProgressSubject:
    """Subject that notifies observers about progress events."""
    
    def __init__(self):
        self.observers: List[ProgressObserver] = []
        self._lock = asyncio.Lock()
    
    async def attach(self, observer: ProgressObserver):
        """Attach an observer."""
        async with self._lock:
            if observer not in self.observers:
                self.observers.append(observer)
    
    async def detach(self, observer: ProgressObserver):
        """Detach an observer."""
        async with self._lock:
            if observer in self.observers:
                self.observers.remove(observer)
    
    async def notify(self, event: ProgressEvent):
        """Notify all observers of an event."""
        async with self._lock:
            observers = self.observers.copy()
        
        # Notify all observers concurrently
        tasks = [observer.update(event) for observer in observers]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # Convenience methods for common events
    async def notify_download_started(self, url: str, **kwargs):
        """Notify that a download has started."""
        event = ProgressEvent(
            event_type=EventType.DOWNLOAD_STARTED,
            timestamp=datetime.now(),
            data={'url': url, **kwargs}
        )
        await self.notify(event)
    
    async def notify_download_completed(self, url: str, size: int, **kwargs):
        """Notify that a download has completed."""
        event = ProgressEvent(
            event_type=EventType.DOWNLOAD_COMPLETED,
            timestamp=datetime.now(),
            data={'url': url, 'size': size, **kwargs}
        )
        await self.notify(event)
    
    async def notify_download_failed(self, url: str, error: str, **kwargs):
        """Notify that a download has failed."""
        event = ProgressEvent(
            event_type=EventType.DOWNLOAD_FAILED,
            timestamp=datetime.now(),
            data={'url': url, 'error': error, **kwargs}
        )
        await self.notify(event)
    
    async def notify_phase_started(self, phase: str, **kwargs):
        """Notify that a phase has started."""
        event = ProgressEvent(
            event_type=EventType.PHASE_STARTED,
            timestamp=datetime.now(),
            data={'phase': phase, **kwargs}
        )
        await self.notify(event)
    
    async def notify_phase_completed(self, phase: str, **kwargs):
        """Notify that a phase has completed."""
        event = ProgressEvent(
            event_type=EventType.PHASE_COMPLETED,
            timestamp=datetime.now(),
            data={'phase': phase, **kwargs}
        )
        await self.notify(event)