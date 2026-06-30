import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class StatusCard(Gtk.Box):
    RUNNING = 'running'
    STOPPED = 'stopped'

    def __init__(self):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('card')
        self.set_size_request(920, 104)
        self.set_margin_bottom(16)

        inner = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=10)
        inner.get_style_context().add_class('status-card-inner')
        inner.set_margin_start(20)
        inner.set_margin_end(20)
        inner.set_margin_top(25)
        inner.set_margin_bottom(25)

        self._dot = Gtk.Label(label='\u25CF')
        self._dot.set_name('status-dot')
        inner.pack_start(self._dot, False, False, 0)

        text_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
        text_box.get_style_context().add_class('status-card-text')
        self._title = Gtk.Label(label='Publishing is running')
        self._title.get_style_context().add_class('heading-3')
        self._title.set_halign(Gtk.Align.START)
        text_box.pack_start(self._title, False, False, 0)

        self._subtitle = Gtk.Label(
            label='Next source check in 3 minutes'
        )
        self._subtitle.get_style_context().add_class('body')
        self._subtitle.set_halign(Gtk.Align.START)
        text_box.pack_start(self._subtitle, False, False, 0)

        inner.pack_start(text_box, True, True, 0)
        self.pack_start(inner, False, False, 0)

    def set_status(self, status: str, subtitle: str = ''):
        ctx = self._dot.get_style_context()
        if status == self.RUNNING:
            ctx.remove_class('status-stopped')
            ctx.add_class('status-running')
            self._title.set_text('Publishing is running')
        else:
            ctx.remove_class('status-running')
            ctx.add_class('status-stopped')
            self._title.set_text('Publishing is stopped')
        if subtitle:
            self._subtitle.set_text(subtitle)
