import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class SourcePage(Gtk.Box):
    def __init__(self, wizard=None):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=16)
        self.get_style_context().add_class('content-area')
        self._wizard = wizard
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        title = Gtk.Label(label='Add your first source')
        title.get_style_context().add_class('heading-3')
        title.set_halign(Gtk.Align.START)
        self.pack_start(title, False, False, 0)

        body = Gtk.Label(
            label='Enter a PeerTube channel URL or an RSS feed URL to '
                  'start discovering videos.'
        )
        body.get_style_context().add_class('body')
        body.set_halign(Gtk.Align.START)
        body.set_max_width_chars(60)
        body.set_line_wrap(True)
        self.pack_start(body, False, False, 0)

        url_label = Gtk.Label(label='PeerTube channel or RSS URL')
        url_label.get_style_context().add_class('heading-4')
        url_label.set_halign(Gtk.Align.START)
        url_label.set_margin_top(16)
        self.pack_start(url_label, False, False, 0)

        self._entry = Gtk.Entry()
        self._entry.set_placeholder_text(
            'https://peertube.example.com/c/channel-name'
        )
        self._entry.set_size_request(596, 46)
        self._entry.connect('changed', self._on_changed)
        self.pack_start(self._entry, False, False, 0)

        skip_label = Gtk.Label(
            label='You can skip this and add sources later.'
        )
        skip_label.get_style_context().add_class('body-small')
        skip_label.set_halign(Gtk.Align.START)
        skip_label.set_margin_top(8)
        self.pack_start(skip_label, False, False, 0)

    def _on_changed(self, entry):
        self._update_complete(bool(entry.get_text().strip()))

    def _update_complete(self, complete: bool):
        if self._wizard:
            self._wizard.set_page_complete(self, complete)

    def get_url(self) -> str:
        return self._entry.get_text().strip()
