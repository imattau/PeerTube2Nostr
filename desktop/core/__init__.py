import sys
import os

_desktop_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_webapp_backend = os.path.join(_desktop_dir, '..', 'webapp', 'backend')

if _webapp_backend not in sys.path:
    sys.path.insert(0, os.path.normpath(_webapp_backend))

from core.database import Store
from core.peertube import PeerTubeClient, IngestPipeline
from core.nostr import NostrPublisher
from core.runner import Runner, RateLimiter
from core.utils import UrlNormaliser
from core.models import DashboardMetrics

__all__ = [
    'Store', 'PeerTubeClient', 'IngestPipeline',
    'NostrPublisher', 'Runner', 'RateLimiter',
    'UrlNormaliser', 'DashboardMetrics',
]
