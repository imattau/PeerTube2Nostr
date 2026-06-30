import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class DiagnosticsScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('content-area')
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        title = Gtk.Label(label='Diagnostics')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        self.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Application health and system status'
        )
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        self.pack_start(subtitle, False, False, 0)

    def refresh(self):
        pass
