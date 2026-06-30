import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.action_row import ActionRow
from desktop.widgets.badge import Badge


class PreferencesScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Preferences')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Publishing, identity, security and maintenance'
        )
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        self.pack_start(title_box, False, False, 0)

        self.pack_start(self._make_setting_group(
            'Publishing',
            [
                ('Minimum interval', 'Minimum time between posts',
                 '20 min', None),
                ('Maximum posts per hour', 'Hourly publishing cap',
                 '3', None),
                ('Daily source limit', 'Max posts per source per day',
                 '1', None),
            ],
        ), False, False, 0)

        self.pack_start(self._make_setting_group(
            'Nostr identity',
            [
                ('Signing method', 'Local NSEC stored securely',
                 None, Badge.SUCCESS, None),
                ('Synchronise profile',
                 'Fetch metadata and NIP-65 relay list',
                 None, None, 'Sync'),
            ],
        ), False, False, 0)

        self.pack_start(self._make_setting_group(
            'Maintenance',
            [
                ('Repair database',
                 'Normalise and repair stored records',
                 None, None, 'Repair'),
            ],
        ), False, False, 0)

    def _make_setting_group(self, section_title: str, items: list):
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        box.set_margin_top(24)

        label = Gtk.Label(label=section_title)
        label.get_style_context().add_class('heading-4')
        label.set_halign(Gtk.Align.START)
        label.set_margin_bottom(8)
        box.pack_start(label, False, False, 0)

        list_box = Gtk.ListBox()
        list_box.set_selection_mode(Gtk.SelectionMode.NONE)

        for item in items:
            if len(item) == 5:
                title, subtitle, value_text, badge_var, button_label = item
            else:
                title, subtitle, value_text, badge_var = item
                button_label = None
            row = ActionRow(
                title=title,
                subtitle=subtitle,
            )
            if badge_var:
                badge = Badge(text='Configured', variant=badge_var)
                row.set_right_widget(badge)
            elif value_text:
                val_lbl = Gtk.Label(label=value_text)
                val_lbl.get_style_context().add_class('heading-4')
                row.set_right_widget(val_lbl)
            elif button_label:
                act_btn = Gtk.Button(label=button_label)
                act_btn.get_style_context().add_class('button-default')
                row.set_right_widget(act_btn)

            list_box.add(row)

        box.pack_start(list_box, False, False, 0)
        return box

    def refresh(self):
        pass
