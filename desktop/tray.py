import os

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Gio

try:
    gi.require_version('AppIndicator3', '0.1')
    from gi.repository import AppIndicator3
    _HAS_APPINDICATOR = True
except (ValueError, ImportError):
    _HAS_APPINDICATOR = False

_ICON_DIR = os.path.join(os.path.dirname(__file__), 'styles', 'icons')


class TrayIcon:
    def __init__(self, on_show=None, on_quit=None):
        self._indicator = None
        self._on_show = on_show
        self._on_quit = on_quit

        if not _HAS_APPINDICATOR:
            return

        icon_path = os.path.join(_ICON_DIR, 'app_icon_32.png')
        if not os.path.exists(icon_path):
            icon_path = 'application-x-executable'

        self._indicator = AppIndicator3.Indicator.new(
            'peertube2nostr-tray',
            icon_path,
            AppIndicator3.IndicatorCategory.APPLICATION_STATUS,
        )
        self._indicator.set_status(AppIndicator3.IndicatorStatus.ACTIVE)
        self._indicator.set_icon_full(icon_path, 'PeerTube2Nostr')

        menu = Gtk.Menu()

        show_item = Gtk.MenuItem(label='Show PeerTube2Nostr')
        show_item.connect('activate', self._on_show_clicked)
        menu.append(show_item)

        menu.append(Gtk.SeparatorMenuItem())

        quit_item = Gtk.MenuItem(label='Quit')
        quit_item.connect('activate', self._on_quit_clicked)
        menu.append(quit_item)

        menu.show_all()
        self._indicator.set_menu(menu)

    @property
    def is_available(self) -> bool:
        return self._indicator is not None

    def set_tooltip(self, text: str):
        if self._indicator:
            self._indicator.set_title(text)

    def _on_show_clicked(self, _item):
        if self._on_show:
            self._on_show()

    def _on_quit_clicked(self, _item):
        if self._on_quit:
            self._on_quit()
