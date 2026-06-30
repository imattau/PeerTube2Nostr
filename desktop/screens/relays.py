import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.action_row import ActionRow
from desktop.widgets.badge import Badge


class RelaysScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Relays')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Nostr relay connectivity and publishing health'
        )
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header.pack_start(title_box, True, True, 0)

        add_btn = Gtk.Button(label='+ Add relay')
        add_btn.get_style_context().add_class('button-primary')
        header.pack_end(add_btn, False, False, 0)
        self.pack_start(header, False, False, 0)

        self.pack_start(self._make_section(
            'Configured relays',
            [
                ('relay.damus.io', 'Connected  ·  84 ms',
                 '#2EC27E', Badge.SUCCESS, 'Healthy'),
                ('nos.lol', 'Connected  ·  420 ms',
                 '#E5A50A', Badge.WARNING, 'High latency'),
                ('relay.example', 'Unavailable  ·  4 failed checks',
                 '#E01B24', Badge.ERROR, 'Offline'),
            ],
        ), False, False, 0)

        disabled_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        disabled_box.set_margin_top(24)
        label = Gtk.Label(label='Disabled')
        label.get_style_context().add_class('heading-4')
        label.set_halign(Gtk.Align.START)
        label.set_margin_bottom(8)
        disabled_box.pack_start(label, False, False, 0)

        disabled_list = Gtk.ListBox()
        disabled_list.set_selection_mode(Gtk.SelectionMode.NONE)
        archive_row = ActionRow(
            title='archive.relay',
            subtitle='Disabled',
            icon_text='\u25C9',
            icon_color='#C7C5C2',
        )
        switch = Gtk.Switch()
        switch.set_active(False)
        archive_row.set_right_widget(switch)
        disabled_list.add(archive_row)
        disabled_box.pack_start(disabled_list, False, False, 0)
        self.pack_start(disabled_box, False, False, 0)

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

        for title, subtitle, dot_color, badge_var, badge_text in items:
            row = ActionRow(
                title=title,
                subtitle=subtitle,
                icon_text='\u25CF',
                icon_color=dot_color,
            )
            badge = Badge(text=badge_text, variant=badge_var)
            row.set_right_widget(badge)
            list_box.add(row)

        box.pack_start(list_box, False, False, 0)
        return box

    def refresh(self):
        pass
