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

        subtitle = Gtk.Label(label='Publishing, identity, security and maintenance')
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        self.pack_start(title_box, False, False, 0)

        self._publishing_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._publishing_box.set_margin_top(24)
        self.pack_start(self._publishing_box, False, False, 0)

        self._identity_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._identity_box.set_margin_top(24)
        self.pack_start(self._identity_box, False, False, 0)

        self._maintenance_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._maintenance_box.set_margin_top(24)
        self.pack_start(self._maintenance_box, False, False, 0)

    def refresh(self):
        for box in [self._publishing_box, self._identity_box, self._maintenance_box]:
            for child in box.get_children():
                box.remove(child)

        store = getattr(self._window, 'store', None)

        min_int = 20
        hourly = 3
        daily = 1
        has_nsec = False
        if store:
            raw = store.get_setting('min_publish_interval_seconds')
            if raw:
                min_int = int(raw) // 60
            raw = store.get_setting('max_posts_per_hour')
            if raw:
                hourly = int(raw)
            raw = store.get_setting('max_posts_per_day_per_source')
            if raw:
                daily = int(raw)
            from core.database import get_stored_nsec
            has_nsec = bool(get_stored_nsec(store.db_path))

        pub_label = Gtk.Label(label='Publishing')
        pub_label.get_style_context().add_class('heading-4')
        pub_label.set_margin_bottom(8)
        pub_label.set_halign(Gtk.Align.START)
        self._publishing_box.pack_start(pub_label, False, False, 0)

        pub_list = Gtk.ListBox()
        pub_list.set_selection_mode(Gtk.SelectionMode.NONE)
        for title, sub, val in [
            ('Minimum interval', 'Minimum time between posts', f'{min_int} min'),
            ('Maximum posts per hour', 'Hourly publishing cap', str(hourly)),
            ('Daily source limit', 'Max posts per source per day', str(daily)),
        ]:
            row = ActionRow(title=title, subtitle=sub)
            lbl = Gtk.Label(label=val)
            lbl.get_style_context().add_class('heading-4')
            row.set_right_widget(lbl)
            pub_list.add(row)
        self._publishing_box.pack_start(pub_list, False, False, 0)

        id_label = Gtk.Label(label='Nostr identity')
        id_label.get_style_context().add_class('heading-4')
        id_label.set_margin_bottom(8)
        id_label.set_halign(Gtk.Align.START)
        self._identity_box.pack_start(id_label, False, False, 0)

        id_list = Gtk.ListBox()
        id_list.set_selection_mode(Gtk.SelectionMode.NONE)
        sign_row = ActionRow(
            title='Signing method',
            subtitle='Local NSEC stored securely',
        )
        if has_nsec:
            badge = Badge(text='Configured', variant=Badge.SUCCESS)
            sign_row.set_right_widget(badge)
        else:
            btn = Gtk.Button(label='Configure')
            btn.get_style_context().add_class('button-default')
            sign_row.set_right_widget(btn)
        id_list.add(sign_row)

        sync_row = ActionRow(
            title='Synchronise profile',
            subtitle='Fetch metadata and NIP-65 relay list',
        )
        sync_btn = Gtk.Button(label='Sync')
        sync_btn.get_style_context().add_class('button-default')
        sync_row.set_right_widget(sync_btn)
        id_list.add(sync_row)
        self._identity_box.pack_start(id_list, False, False, 0)

        maint_label = Gtk.Label(label='Maintenance')
        maint_label.get_style_context().add_class('heading-4')
        maint_label.set_margin_bottom(8)
        maint_label.set_halign(Gtk.Align.START)
        self._maintenance_box.pack_start(maint_label, False, False, 0)

        maint_list = Gtk.ListBox()
        maint_list.set_selection_mode(Gtk.SelectionMode.NONE)
        repair_row = ActionRow(
            title='Repair database',
            subtitle='Normalise and repair stored records',
        )
        repair_btn = Gtk.Button(label='Repair')
        repair_btn.get_style_context().add_class('button-default')
        repair_row.set_right_widget(repair_btn)
        maint_list.add(repair_row)
        self._maintenance_box.pack_start(maint_list, False, False, 0)

        self.show_all()
