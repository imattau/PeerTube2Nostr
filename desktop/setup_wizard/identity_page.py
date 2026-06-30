import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.badge import Badge


class IdentityPage(Gtk.Box):
    def __init__(self, wizard=None):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=16)
        self._wizard = wizard
        self._method = None
        self._nsec = ''
        self._bunker_url = ''

        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        title = Gtk.Label(label='Choose a signing method')
        title.get_style_context().add_class('heading-3')
        title.set_halign(Gtk.Align.START)
        self.pack_start(title, False, False, 0)

        body = Gtk.Label(
            label='Use a local NSEC or connect through a NIP-46 bunker'
        )
        body.get_style_context().add_class('body')
        body.set_halign(Gtk.Align.START)
        self.pack_start(body, False, False, 0)

        self._nsec_box = Gtk.EventBox()
        self._nsec_box.get_style_context().add_class('card')
        self._nsec_box.set_margin_top(16)
        self._nsec_box.connect('button-press-event', self._on_select_nsec)
        nsec_inner = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        nsec_inner.set_margin_start(16)
        nsec_inner.set_margin_end(16)
        nsec_inner.set_margin_top(16)
        nsec_inner.set_margin_bottom(16)

        nsec_label = Gtk.Box(
            orientation=Gtk.Orientation.VERTICAL, spacing=4
        )
        nsec_title = Gtk.Label(label='Local NSEC')
        nsec_title.get_style_context().add_class('heading-4')
        nsec_title.set_halign(Gtk.Align.START)
        nsec_label.pack_start(nsec_title, False, False, 0)
        nsec_sub = Gtk.Label(label='Store your key securely on this device')
        nsec_sub.get_style_context().add_class('body')
        nsec_sub.set_halign(Gtk.Align.START)
        nsec_label.pack_start(nsec_sub, False, False, 0)
        nsec_inner.pack_start(nsec_label, True, True, 0)

        badge = Badge(text='Recommended', variant=Badge.ACCENT)
        nsec_inner.pack_end(badge, False, False, 0)

        self._nsec_box.add(nsec_inner)
        self.pack_start(self._nsec_box, False, False, 0)

        self._bunker_box = Gtk.EventBox()
        self._bunker_box.get_style_context().add_class('card')
        self._bunker_box.connect('button-press-event', self._on_select_bunker)
        bunker_inner = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        bunker_inner.set_margin_start(16)
        bunker_inner.set_margin_end(16)
        bunker_inner.set_margin_top(16)
        bunker_inner.set_margin_bottom(16)

        bunker_label = Gtk.Box(
            orientation=Gtk.Orientation.VERTICAL, spacing=4
        )
        bunker_title = Gtk.Label(label='NIP-46 bunker')
        bunker_title.get_style_context().add_class('heading-4')
        bunker_title.set_halign(Gtk.Align.START)
        bunker_label.pack_start(bunker_title, False, False, 0)
        bunker_sub = Gtk.Label(
            label='Connect through a remote signing service'
        )
        bunker_sub.get_style_context().add_class('body')
        bunker_sub.set_halign(Gtk.Align.START)
        bunker_label.pack_start(bunker_sub, False, False, 0)
        bunker_inner.pack_start(bunker_label, True, True, 0)

        self._bunker_box.add(bunker_inner)
        self.pack_start(self._bunker_box, False, False, 0)

        self._entry_box = Gtk.Box(
            orientation=Gtk.Orientation.VERTICAL, spacing=8
        )
        self._entry_box.set_margin_top(16)
        self._nsec_entry = Gtk.Entry()
        self._nsec_entry.set_placeholder_text('nsec1...')
        self._nsec_entry.set_visibility(False)
        self._nsec_entry.set_size_request(596, 46)
        self._nsec_entry.connect('changed', self._on_entry_changed)

        self._bunker_entry = Gtk.Entry()
        self._bunker_entry.set_placeholder_text('bunker://...')
        self._bunker_entry.set_size_request(596, 46)
        self._bunker_entry.connect('changed', self._on_entry_changed)

        self.pack_start(self._entry_box, False, False, 0)

    def _on_select_nsec(self, *_args):
        self._method = 'nsec'
        self._nsec_box.get_style_context().add_class('card-selected')
        self._bunker_box.get_style_context().remove_class('card-selected')
        for c in self._entry_box.get_children():
            self._entry_box.remove(c)
        self._entry_box.pack_start(self._nsec_entry, False, False, 0)
        self._nsec_entry.show()
        self._on_entry_changed(self._nsec_entry)

    def _on_select_bunker(self, *_args):
        self._method = 'bunker'
        self._bunker_box.get_style_context().add_class('card-selected')
        self._nsec_box.get_style_context().remove_class('card-selected')
        for c in self._entry_box.get_children():
            self._entry_box.remove(c)
        self._entry_box.pack_start(self._bunker_entry, False, False, 0)
        self._bunker_entry.show()
        self._on_entry_changed(self._bunker_entry)

    def _on_entry_changed(self, entry):
        text = entry.get_text().strip()
        if entry is self._nsec_entry:
            self._nsec = text
        else:
            self._bunker_url = text
        self._update_complete(bool(text))

    def _update_complete(self, complete: bool):
        if self._wizard:
            self._wizard.set_page_complete(self, complete)

    def get_data(self) -> dict:
        return {
            'method': self._method,
            'nsec': self._nsec,
            'bunker_url': self._bunker_url,
        }
