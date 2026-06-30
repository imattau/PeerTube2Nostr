import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class SearchInput(Gtk.SearchEntry):
    def __init__(self, placeholder: str = 'Search...'):
        super().__init__()
        self.set_placeholder_text(placeholder)
        self.set_size_request(540, 40)
        self.get_style_context().add_class('search-entry')
