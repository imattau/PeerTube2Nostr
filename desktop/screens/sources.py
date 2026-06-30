import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.action_row import ActionRow


class SourcesScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Sources')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(label='PeerTube channels and RSS feeds')
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header.pack_start(title_box, True, True, 0)

        add_btn = Gtk.Button(label='+ Add source')
        add_btn.get_style_context().add_class('button-primary')
        header.pack_end(add_btn, False, False, 0)
        self.pack_start(header, False, False, 0)

        self.pack_start(self._make_section(
            'PeerTube channels',
            [
                ('GNOME Foundation', 'gnome.org/c/gnome', '#2EC27E'),
                ('Open Media', 'video.example/c/open', '#2EC27E'),
                ('Protocol Lab', 'instance unavailable', '#E01B24'),
            ],
        ), False, False, 0)

        self.pack_start(self._make_section(
            'RSS-only sources',
            [
                ('Weekly PeerTube Picks', 'feeds.example/weekly', '#2EC27E'),
            ],
        ), False, False, 0)

    def _make_section(self, section_title: str, items: list):
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        box.set_margin_top(24)

        label = Gtk.Label(label=section_title)
        label.get_style_context().add_class('heading-4')
        label.set_halign(Gtk.Align.START)
        label.set_margin_bottom(8)
        box.pack_start(label, False, False, 0)

        list_box = Gtk.ListBox()
        list_box.set_selection_mode(Gtk.SelectionMode.NONE)

        for title, subtitle, dot_color in items:
            row = ActionRow(
                title=title,
                subtitle=subtitle,
                icon_text='\u25C9',
                icon_color=dot_color,
            )
            switch = Gtk.Switch()
            switch.set_active(True)
            row.set_right_widget(switch)
            list_box.add(row)

        box.pack_start(list_box, False, False, 0)
        return box

    def refresh(self):
        pass
