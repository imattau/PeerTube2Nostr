import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.search_input import SearchInput
from desktop.widgets.log_viewer import LogViewer


class ActivityScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Activity')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Live application and publishing events'
        )
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header.pack_start(title_box, True, True, 0)

        pause_btn = Gtk.Button(label='Pause')
        pause_btn.get_style_context().add_class('button-default')
        header.pack_end(pause_btn, False, False, 0)
        self.pack_start(header, False, False, 0)

        toolbar = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        toolbar.set_margin_top(24)
        toolbar.set_margin_bottom(16)

        self._search = SearchInput(placeholder='Search logs')
        toolbar.pack_start(self._search, False, False, 0)

        self._level_combo = Gtk.ComboBoxText()
        self._level_combo.append_text('All levels')
        self._level_combo.append_text('INFO')
        self._level_combo.append_text('WARN')
        self._level_combo.append_text('ERROR')
        self._level_combo.set_active(0)
        toolbar.pack_start(self._level_combo, False, False, 0)

        auto_scroll_lbl = Gtk.Label(label='Auto-scroll')
        auto_scroll_lbl.get_style_context().add_class('toolbar-label')
        toolbar.pack_start(auto_scroll_lbl, False, False, 0)
        self._auto_scroll = Gtk.CheckButton()
        self._auto_scroll.set_active(True)
        toolbar.pack_start(self._auto_scroll, False, False, 0)

        export_btn = Gtk.Button(label='Export')
        export_btn.get_style_context().add_class('button-default')
        toolbar.pack_end(export_btn, False, False, 0)

        self.pack_start(toolbar, False, False, 0)

        self._log_viewer = LogViewer()
        self.pack_start(self._log_viewer, True, True, 0)

        self._log_viewer.append_log('14:32:20', 'INFO',
            'Published "GNOME 48 Release Highlights"')
        self._log_viewer.append_log('14:32:16', 'ERROR',
            'relay.example timed out')
        self._log_viewer.append_log('14:32:11', 'WARN',
            'nos.lol responded in 420 ms')
        self._log_viewer.append_log('14:32:09', 'INFO',
            'Found 2 new videos')
        self._log_viewer.append_log('14:32:08', 'INFO',
            'Polling GNOME Foundation')

    def refresh(self):
        pass
