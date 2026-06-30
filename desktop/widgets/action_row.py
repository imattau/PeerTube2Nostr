import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class ActionRow(Gtk.ListBoxRow):
    def __init__(
        self,
        title: str,
        subtitle: str = '',
        icon_text: str = '\u25C9',
        icon_color: str = '#2EC27E',
    ):
        super().__init__()
        self.set_size_request(680, 68)

        box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        box.set_margin_start(16)
        box.set_margin_end(16)
        box.set_margin_top(13)
        box.set_margin_bottom(13)

        self._icon = Gtk.Label(label=icon_text)
        self._icon.set_name('action-row-icon')
        self._icon.set_markup(
            f'<span foreground="{icon_color}" size="large">{icon_text}</span>'
        )
        box.pack_start(self._icon, False, False, 0)

        text_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=2)
        self._title = Gtk.Label(label=title)
        self._title.get_style_context().add_class('heading-4')
        self._title.set_halign(Gtk.Align.START)
        text_box.pack_start(self._title, False, False, 0)

        self._subtitle = Gtk.Label(label=subtitle)
        self._subtitle.get_style_context().add_class('body')
        self._subtitle.set_halign(Gtk.Align.START)
        text_box.pack_start(self._subtitle, False, False, 0)

        box.pack_start(text_box, True, True, 0)

        self._right_box = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        box.pack_end(self._right_box, False, False, 0)

        self.add(box)

    def set_icon_color(self, color: str, icon: str = '\u25C9'):
        self._icon.set_markup(
            f'<span foreground="{color}" size="large">{icon}</span>'
        )

    def set_right_widget(self, widget: Gtk.Widget):
        for child in self._right_box.get_children():
            self._right_box.remove(child)
        self._right_box.pack_start(widget, False, False, 0)
