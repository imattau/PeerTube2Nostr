import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class AddSourceDialog(Gtk.Dialog):
    def __init__(self, parent):
        super().__init__(
            title='Add source',
            transient_for=parent,
            modal=True,
            destroy_with_parent=True,
        )
        self.set_default_size(500, 200)

        content = self.get_content_area()
        content.get_style_context().add_class('content-area')
        content.set_margin_start(24)
        content.set_margin_end(24)
        content.set_margin_top(24)
        content.set_margin_bottom(16)

        title_lbl = Gtk.Label(label='Add a PeerTube channel or RSS feed')
        title_lbl.get_style_context().add_class('dialog-title')
        title_lbl.set_halign(Gtk.Align.START)
        content.pack_start(title_lbl, False, False, 0)

        self._entry = Gtk.Entry()
        self._entry.set_placeholder_text(
            'PeerTube channel or RSS URL'
        )
        self._entry.set_size_request(596, 46)
        self._entry.set_margin_top(16)
        content.pack_start(self._entry, False, False, 0)

        btn_box = self.get_action_area()
        btn_box.set_margin_top(16)

        cancel_btn = Gtk.Button(label='Cancel')
        cancel_btn.get_style_context().add_class('button-default')
        cancel_btn.connect('clicked', lambda _: self.response(Gtk.ResponseType.CANCEL))
        btn_box.pack_start(cancel_btn, False, False, 0)

        add_btn = Gtk.Button(label='Add')
        add_btn.get_style_context().add_class('button-primary')
        add_btn.connect('clicked', lambda _: self.response(Gtk.ResponseType.OK))
        btn_box.pack_end(add_btn, False, False, 0)

        self.show_all()

    def get_url(self) -> str:
        return self._entry.get_text().strip()
