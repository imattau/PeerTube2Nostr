import os

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Gdk, Gio

from desktop.screens.overview import OverviewScreen
from desktop.screens.queue import QueueScreen
from desktop.screens.sources import SourcesScreen
from desktop.screens.relays import RelaysScreen
from desktop.screens.activity import ActivityScreen
from desktop.screens.diagnostics import DiagnosticsScreen
from desktop.screens.preferences import PreferencesScreen
from desktop.widgets.empty_state import EmptyState


class MainWindow(Gtk.ApplicationWindow):
    NAV_ITEMS = [
        ('Overview',    '\U0001F302',  'overview',     False),
        ('Queue',       '\u2261',     'queue',        True),
        ('Sources',     '\u25C9',     'sources',      False),
        ('Relays',      '\u2301',     'relays',       False),
        ('Activity',    '\u25A4',     'activity',     False),
        ('Diagnostics', '\u271A',     'diagnostics',  False),
        ('Preferences', '\u2699',     'preferences',  False),
    ]

    def __init__(self, store=None, **kwargs):
        super().__init__(**kwargs)
        self.store = store
        self.set_title('PeerTube2Nostr')
        self.set_default_size(1280, 820)
        self.set_resizable(False)
        self.set_position(Gtk.WindowPosition.CENTER)

        icon_dir = os.path.join(
            os.path.dirname(__file__), 'styles', 'icons'
        )
        icon_path = os.path.join(icon_dir, 'app_icon_48.png')
        if os.path.exists(icon_path):
            self.set_icon_from_file(icon_path)

        self._nav_buttons: list[Gtk.EventBox] = []
        self._nav_badges: dict[str, Gtk.Label] = {}
        self._current_view = 'overview'

        self._build_ui()

    def _build_ui(self):
        self._build_header_bar()
        body = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        body.pack_start(self._build_sidebar(), False, False, 0)
        body.pack_start(self._build_content(), True, True, 0)
        self.add(body)

    def _build_header_bar(self):
        header = Gtk.HeaderBar()
        header.set_show_close_button(True)
        header.props.title = 'PeerTube2Nostr'
        self.set_titlebar(header)

        status_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        self._dot = Gtk.Label(label='\u25CF')
        self._dot.set_name('status-dot')
        self._dot.get_style_context().add_class('status-running')
        self._status_label = Gtk.Label(label='Running')
        self._status_label.set_name('status-label')
        status_box.pack_start(self._dot, False, False, 0)
        status_box.pack_start(self._status_label, False, False, 0)

        refresh_btn = Gtk.Button(label='\u21BB')
        refresh_btn.set_name('header-refresh-btn')
        refresh_btn.get_style_context().add_class('button-icon')
        refresh_btn.connect('clicked', lambda _: self._on_refresh())

        menu_btn = Gtk.MenuButton()
        menu_btn.set_label('\u22EE')
        menu_btn.set_name('header-menu-btn')
        menu_btn.get_style_context().add_class('button-icon')
        menu_model = Gio.Menu()
        menu_model.append('About', 'app.about')
        menu_model.append('Quit', 'app.quit')
        menu_btn.set_menu_model(menu_model)

        header.pack_end(menu_btn)
        header.pack_end(refresh_btn)
        header.pack_end(status_box)

    def _build_sidebar(self):
        sidebar = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        sidebar.set_size_request(240, -1)
        sidebar.get_style_context().add_class('sidebar')

        brand = Gtk.Label(label='PeerTube2Nostr')
        brand.set_name('sidebar-brand')
        brand.get_style_context().add_class('sidebar-header')
        brand.set_halign(Gtk.Align.START)
        brand.set_margin_start(16)
        brand.set_margin_top(20)
        brand.set_margin_bottom(12)
        sidebar.pack_start(brand, False, False, 0)

        for label, icon, name, has_badge in self.NAV_ITEMS:
            row = Gtk.EventBox()
            row.get_style_context().add_class('nav-item')
            row.set_name(f'nav-{name}')
            row.set_margin_start(16)
            row.set_margin_end(16)
            row.set_margin_top(4)
            row.set_margin_bottom(4)

            box = Gtk.Box(
                orientation=Gtk.Orientation.HORIZONTAL, spacing=8
            )
            box.set_margin_start(12)
            box.set_margin_end(12)
            box.set_margin_top(9)
            box.set_margin_bottom(9)

            icon_w = Gtk.Label(label=icon)
            icon_w.get_style_context().add_class('nav-icon')
            box.pack_start(icon_w, False, False, 0)

            text_w = Gtk.Label(label=label)
            text_w.get_style_context().add_class('nav-label')
            box.pack_start(text_w, False, False, 0)

            if has_badge:
                badge = Gtk.Label(label='0')
                badge.get_style_context().add_class('badge')
                badge.get_style_context().add_class('badge-accent')
                box.pack_end(badge, False, False, 0)
                self._nav_badges[name] = badge

            row.add(box)
            row.connect('button-press-event', self._on_nav_click, name)
            row.connect('enter-notify-event', lambda _, e, r=row:
                r.get_style_context().add_class('nav-item-hover'))
            row.connect('leave-notify-event', lambda _, e, r=row:
                r.get_style_context().remove_class('nav-item-hover'))

            sidebar.pack_start(row, False, False, 0)
            self._nav_buttons.append((row, name))

        sidebar.pack_end(Gtk.Box(), True, True, 0)
        return sidebar

    def _build_content(self):
        self._stack = Gtk.Stack()
        self._stack.set_transition_type(Gtk.StackTransitionType.CROSSFADE)
        self._stack.set_transition_duration(200)

        self._screen_overview = OverviewScreen(self)
        self._stack.add_titled(self._screen_overview, 'overview', 'Overview')

        self._screen_queue = QueueScreen(self)
        self._stack.add_titled(self._screen_queue, 'queue', 'Queue')

        self._screen_sources = SourcesScreen(self)
        self._stack.add_titled(self._screen_sources, 'sources', 'Sources')

        self._screen_relays = RelaysScreen(self)
        self._stack.add_titled(self._screen_relays, 'relays', 'Relays')

        self._screen_activity = ActivityScreen(self)
        self._stack.add_titled(self._screen_activity, 'activity', 'Activity')

        self._screen_diagnostics = DiagnosticsScreen(self)
        self._stack.add_titled(
            self._screen_diagnostics, 'diagnostics', 'Diagnostics'
        )

        self._screen_preferences = PreferencesScreen(self)
        self._stack.add_titled(
            self._screen_preferences, 'preferences', 'Preferences'
        )

        self._error_backend = EmptyState(
            icon='\u21AF',
            title='Cannot connect to the backend',
            body='PeerTube2Nostr may not be running, or the API address may be incorrect.',
            button_label='Try again',
        )
        self._stack.add_named(self._error_backend, 'error-backend')

        self._error_empty = EmptyState(
            icon='\u25C9',
            title='No sources configured',
            body='Add a PeerTube channel or RSS feed to begin discovering videos.',
            button_label='Add source',
        )
        self._stack.add_named(self._error_empty, 'error-empty')

        self._error_db = EmptyState(
            icon='!',
            title='Database is temporarily unavailable',
            body='Another process is using the database. The application will retry automatically.',
            button_label='Retry now',
        )
        self._stack.add_named(self._error_db, 'error-database')

        self._stack.set_visible_child_name('overview')
        self._update_nav_active('overview')

        content = Gtk.Box(orientation=Gtk.Orientation.VERTICAL)
        content.get_style_context().add_class('content-area')
        content.pack_start(self._stack, True, True, 0)
        return content

    def _on_nav_click(self, _widget, _event, view_name: str):
        self._stack.set_visible_child_name(view_name)
        self._update_nav_active(view_name)
        self._current_view = view_name
        screen = getattr(self, f'_screen_{view_name}', None)
        if screen and hasattr(screen, 'refresh'):
            screen.refresh()

    def _update_nav_active(self, active_name: str):
        for row, name in self._nav_buttons:
            ctx = row.get_style_context()
            if name == active_name:
                ctx.add_class('nav-item-active')
            else:
                ctx.remove_class('nav-item-active')

    def _on_refresh(self):
        if hasattr(self, f'_screen_{self._current_view}'):
            screen = getattr(self, f'_screen_{self._current_view}')
            if hasattr(screen, 'refresh'):
                screen.refresh()

    def set_status(self, running: bool, label: str = ''):
        ctx = self._dot.get_style_context()
        if running:
            ctx.remove_class('status-stopped')
            ctx.add_class('status-running')
            self._status_label.set_text(label or 'Running')
        else:
            ctx.remove_class('status-running')
            ctx.add_class('status-stopped')
            self._status_label.set_text(label or 'Stopped')

    def show_error(self, error_type: str):
        self._stack.set_visible_child_name(f'error-{error_type}')

    def hide_error(self):
        self._stack.set_visible_child_name(self._current_view)

    def switch_to(self, view_name: str):
        self._on_nav_click(None, None, view_name)

    def update_badge(self, name: str, count: int):
        if name in self._nav_badges:
            self._nav_badges[name].set_text(str(count))
