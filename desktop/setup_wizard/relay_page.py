import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class RelayPage(Gtk.Box):
    def __init__(self):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=16)
        self.get_style_context().add_class('content-area')
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        title = Gtk.Label(label='Add a relay')
        title.get_style_context().add_class('heading-3')
        title.set_halign(Gtk.Align.START)
        self.pack_start(title, False, False, 0)

        body = Gtk.Label(
            label='Nostr relays distribute your notes across the network. '
                  'You can add more later.'
        )
        body.get_style_context().add_class('body')
        body.set_halign(Gtk.Align.START)
        body.set_max_width_chars(60)
        body.set_line_wrap(True)
        self.pack_start(body, False, False, 0)

        self._entry = Gtk.Entry()
        self._entry.set_placeholder_text('wss://relay.damus.io')
        self._entry.set_size_request(596, 46)
        self._entry.set_margin_top(8)
        self.pack_start(self._entry, False, False, 0)

        skip_label = Gtk.Label(
            label='You can skip this and add relays later from Preferences.'
        )
        skip_label.get_style_context().add_class('body-small')
        skip_label.set_halign(Gtk.Align.START)
        skip_label.set_margin_top(8)
        self.pack_start(skip_label, False, False, 0)

        self._import_check = Gtk.CheckButton(
            label='Import relays from my Nostr profile (NIP-65)'
        )
        self._import_check.set_margin_top(20)
        self.pack_start(self._import_check, False, False, 0)

    def get_url(self) -> str:
        return self._entry.get_text().strip()

    def get_import_nip65(self) -> bool:
        return self._import_check.get_active()
