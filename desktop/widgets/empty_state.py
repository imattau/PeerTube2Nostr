import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class EmptyState(Gtk.Box):
    def __init__(
        self,
        icon: str = '\u25C9',
        title: str = '',
        body: str = '',
        button_label: str = '',
    ):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=12)
        self.get_style_context().add_class('card')
        self.set_valign(Gtk.Align.CENTER)
        self.set_halign(Gtk.Align.CENTER)
        self.set_size_request(640, 360)

        icon_lbl = Gtk.Label(label=icon)
        icon_lbl.set_name('empty-state-icon')
        self.pack_start(icon_lbl, False, False, 0)

        if title:
            title_lbl = Gtk.Label(label=title)
            title_lbl.get_style_context().add_class('heading-2')
            self.pack_start(title_lbl, False, False, 0)

        if body:
            body_lbl = Gtk.Label(label=body)
            body_lbl.get_style_context().add_class('body')
            body_lbl.set_max_width_chars(50)
            body_lbl.set_line_wrap(True)
            self.pack_start(body_lbl, False, False, 0)

        if button_label:
            self._action_btn = Gtk.Button(label=button_label)
            self._action_btn.get_style_context().add_class('button-default')
            self.pack_start(self._action_btn, False, False, 0)

    def set_action_callback(self, callback):
        if hasattr(self, '_action_btn'):
            self._action_btn.connect('clicked', lambda _: callback())
