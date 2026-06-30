import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class Banner(Gtk.Revealer):
    WARNING = 'warning'
    ERROR = 'error'
    INFO = 'info'

    def __init__(self, title: str, body: str = '', variant: str = WARNING):
        super().__init__()
        self.set_transition_type(Gtk.RevealerTransitionType.SLIDE_DOWN)
        self.set_transition_duration(200)
        self.set_valign(Gtk.Align.START)

        box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=10)
        box.set_size_request(920, 76)

        style_map = {
            self.WARNING: 'banner-warning',
            self.ERROR: 'banner-error',
            self.INFO: 'banner-info',
        }
        box.get_style_context().add_class(style_map.get(variant, 'banner-warning'))
        box.set_margin_start(16)
        box.set_margin_end(16)
        box.set_margin_top(16)
        box.set_margin_bottom(16)

        dot_colors = {
            self.WARNING: '#E5A50A',
            self.ERROR: '#E01B24',
            self.INFO: '#3584E4',
        }
        dot = Gtk.Label(label='\u25CF')
        dot.set_markup(
            f'<span foreground="{dot_colors.get(variant, "#E5A50A")}">\u25CF</span>'
        )
        box.pack_start(dot, False, False, 0)

        text_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
        text_box.get_style_context().add_class('banner-text')
        title_lbl = Gtk.Label(label=title)
        title_lbl.get_style_context().add_class('heading-4')
        title_lbl.set_halign(Gtk.Align.START)
        text_box.pack_start(title_lbl, False, False, 0)

        if body:
            body_lbl = Gtk.Label(label=body)
            body_lbl.get_style_context().add_class('body')
            body_lbl.set_halign(Gtk.Align.START)
            text_box.pack_start(body_lbl, False, False, 0)

        box.pack_start(text_box, True, True, 0)

        self._action_box = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        self._action_box.get_style_context().add_class('banner-action')
        box.pack_end(self._action_box, False, False, 0)

        self.add(box)

    def set_action_widget(self, widget: Gtk.Widget):
        self._action_box.pack_start(widget, False, False, 0)

    def show(self):
        self.set_reveal_child(True)

    def hide(self):
        self.set_reveal_child(False)
