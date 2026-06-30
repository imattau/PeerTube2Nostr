import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class MetricCard(Gtk.EventBox):
    def __init__(self, title: str, value: str = '0', subtitle: str = ''):
        super().__init__()
        self.get_style_context().add_class('metric-card')
        self.set_size_request(220, 104)

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        box.set_margin_start(16)
        box.set_margin_end(16)
        box.set_margin_top(14)
        box.set_margin_bottom(14)

        self._title_label = Gtk.Label(label=title.upper())
        self._title_label.get_style_context().add_class('metric-label')
        self._title_label.set_halign(Gtk.Align.START)
        box.pack_start(self._title_label, False, False, 0)

        self._value_label = Gtk.Label(label=value)
        self._value_label.get_style_context().add_class('metric-value')
        self._value_label.set_halign(Gtk.Align.START)
        box.pack_start(self._value_label, False, False, 0)

        if subtitle:
            self._subtitle_label = Gtk.Label(label=subtitle)
            self._subtitle_label.get_style_context().add_class('body-small')
            self._subtitle_label.set_halign(Gtk.Align.START)
            box.pack_start(self._subtitle_label, False, False, 0)

        self.add(box)

    def set_value(self, value: str):
        self._value_label.set_text(str(value))
