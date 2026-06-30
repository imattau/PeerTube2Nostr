import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class ConfirmDialog(Gtk.Dialog):
    PRIMARY = 'primary'
    DESTRUCTIVE = 'destructive'

    def __init__(self, parent, title: str, body: str,
                 confirm_label: str = 'Confirm',
                 confirm_style: str = 'primary',
                 cancel_label: str = 'Cancel'):
        super().__init__(
            title=title,
            transient_for=parent,
            modal=True,
            destroy_with_parent=True,
        )
        self.set_default_size(500, 240)
        self.get_content_area().set_margin_start(24)
        self.get_content_area().set_margin_end(24)
        self.get_content_area().set_margin_top(24)
        self.get_content_area().set_margin_bottom(16)

        title_lbl = Gtk.Label(label=title)
        title_lbl.get_style_context().add_class('heading-3')
        title_lbl.set_halign(Gtk.Align.START)
        self.get_content_area().pack_start(title_lbl, False, False, 0)

        body_lbl = Gtk.Label(label=body)
        body_lbl.get_style_context().add_class('body')
        body_lbl.set_halign(Gtk.Align.START)
        body_lbl.set_margin_top(16)
        self.get_content_area().pack_start(body_lbl, False, False, 0)

        btn_box = self.get_action_area()
        btn_box.set_margin_top(16)

        cancel_btn = Gtk.Button(label=cancel_label)
        cancel_btn.get_style_context().add_class('button-default')
        cancel_btn.connect('clicked', lambda _: self.response(Gtk.ResponseType.CANCEL))
        btn_box.pack_start(cancel_btn, False, False, 0)

        confirm_btn = Gtk.Button(label=confirm_label)
        if confirm_style == self.DESTRUCTIVE:
            confirm_btn.get_style_context().add_class('button-destructive')
        else:
            confirm_btn.get_style_context().add_class('button-primary')
        confirm_btn.connect('clicked', lambda _: self.response(Gtk.ResponseType.OK))
        btn_box.pack_end(confirm_btn, False, False, 0)

        self.show_all()
