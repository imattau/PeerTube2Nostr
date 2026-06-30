import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class Badge(Gtk.Label):
    SUCCESS = 'badge-success'
    ACCENT = 'badge-accent'
    WARNING = 'badge-warning'
    ERROR = 'badge-error'

    def __init__(self, text: str = '', variant: str = SUCCESS):
        super().__init__(label=text)
        self._variant = variant
        self.get_style_context().add_class('badge')
        self.get_style_context().add_class(variant)
        self.set_valign(Gtk.Align.CENTER)

    def set_variant(self, variant: str):
        self.get_style_context().remove_class(self._variant)
        self._variant = variant
        self.get_style_context().add_class(variant)

    def set_text(self, text: str):
        super().set_text(text)
