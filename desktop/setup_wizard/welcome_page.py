import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk


class WelcomePage(Gtk.Box):
    def __init__(self):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=16)
        self.set_valign(Gtk.Align.CENTER)
        self.set_halign(Gtk.Align.CENTER)
        self.set_margin_start(60)
        self.set_margin_end(60)

        icon_card = Gtk.EventBox()
        icon_card.get_style_context().add_class('card')
        icon_card.set_margin_bottom(8)

        icon_box = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=8
        )
        icon_box.set_halign(Gtk.Align.CENTER)
        icon_box.set_margin_start(24)
        icon_box.set_margin_end(24)
        icon_box.set_margin_top(20)
        icon_box.set_margin_bottom(20)

        play = Gtk.Label(label='\u25B6')
        play.set_markup('<span size="xx-large" foreground="#2E3436">\u25B6</span>')
        icon_box.pack_start(play, False, False, 0)

        arrow = Gtk.Label(label='\u2192')
        arrow.set_markup(
            '<span size="xx-large" foreground="#3584E4">\u2192</span>'
        )
        icon_box.pack_start(arrow, False, False, 0)

        bolt = Gtk.Label(label='\u26A1')
        bolt.set_markup(
            '<span size="xx-large" foreground="#E5A50A">\u26A1</span>'
        )
        icon_box.pack_start(bolt, False, False, 0)

        icon_card.add(icon_box)
        self.pack_start(icon_card, False, False, 0)

        title = Gtk.Label(label='Welcome')
        title.get_style_context().add_class('heading-2')
        title.set_margin_top(16)
        self.pack_start(title, False, False, 0)

        body = Gtk.Label(
            label='Connect your PeerTube channels to the Nostr ecosystem. '
                  'Videos you publish on PeerTube will be automatically '
                  'announced on Nostr as verifiable notes.'
        )
        body.get_style_context().add_class('body')
        body.set_max_width_chars(50)
        body.set_line_wrap(True)
        body.set_justify(Gtk.Justification.CENTER)
        self.pack_start(body, False, False, 0)
