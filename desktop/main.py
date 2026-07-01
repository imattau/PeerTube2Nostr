#!/usr/bin/env python3
"""
PeerTube2Nostr — GNOME 3 / GTK 3 Desktop UI

Launches the native desktop application. Coexists with the existing
web dashboard by sharing the same SQLite database and core business
logic classes (imported from webapp/backend/core/).
"""

import sys
import os

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_backend_dir = os.path.join(_project_root, 'webapp', 'backend')
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from desktop.app import main

if __name__ == '__main__':
    main()
