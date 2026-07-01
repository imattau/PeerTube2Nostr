import os
import threading

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GLib

from desktop.widgets.action_row import ActionRow
from desktop.widgets.badge import Badge
from desktop.dialogs.set_nsec import SetNsecDialog
from core.database import get_stored_nsec, set_stored_nsec, clear_stored_nsec
from core.sync import import_nip65_relays


class PreferencesScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('content-area')
        self.get_style_context().add_class('preferences-screen')
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
            has_nsec = bool(get_stored_nsec(store.db_path))

        pub_label = Gtk.Label(label='Publishing')
        pub_label.get_style_context().add_class('heading-4')
        pub_label.set_margin_bottom(8)
        pub_label.set_halign(Gtk.Align.START)
        self._publishing_box.pack_start(pub_label, False, False, 0)

        pub_list = Gtk.ListBox()
        pub_list.set_selection_mode(Gtk.SelectionMode.NONE)

        interval_row = ActionRow(
            title='Minimum interval', subtitle='Minimum time between posts (minutes)'
        )
        interval_spin = Gtk.SpinButton.new_with_range(1, 1440, 1)
        interval_spin.set_value(min_int)
        interval_spin.get_style_context().add_class('heading-4')
        interval_spin.connect('value-changed', self._on_interval_changed)
        interval_spin.set_width_chars(5)
        interval_row.set_right_widget(interval_spin)
        pub_list.add(interval_row)

        hourly_row = ActionRow(
            title='Maximum posts per hour', subtitle='Hourly publishing cap'
        )
        hourly_spin = Gtk.SpinButton.new_with_range(0, 100, 1)
        hourly_spin.set_value(hourly)
        hourly_spin.get_style_context().add_class('heading-4')
        hourly_spin.connect('value-changed', self._on_hourly_changed)
        hourly_spin.set_width_chars(5)
        hourly_row.set_right_widget(hourly_spin)
        pub_list.add(hourly_row)

        daily_row = ActionRow(
            title='Daily source limit', subtitle='Max posts per source per day'
        )
        daily_spin = Gtk.SpinButton.new_with_range(0, 100, 1)
        daily_spin.set_value(daily)
        daily_spin.get_style_context().add_class('heading-4')
        daily_spin.connect('value-changed', self._on_daily_changed)
        daily_spin.set_width_chars(5)
        daily_row.set_right_widget(daily_spin)
        pub_list.add(daily_row)

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
        right_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        if has_nsec:
            badge = Badge(text='Configured', variant=Badge.SUCCESS)
            right_box.pack_start(badge, False, False, 0)
            change_btn = Gtk.Button(label='Change')
            change_btn.get_style_context().add_class('button-default')
            change_btn.connect('clicked', lambda _: self._on_configure_nsec())
            right_box.pack_start(change_btn, False, False, 0)
            remove_btn = Gtk.Button(label='Remove')
            remove_btn.get_style_context().add_class('button-default')
            remove_btn.connect('clicked', lambda _: self._on_remove_nsec())
            right_box.pack_start(remove_btn, False, False, 0)
        else:
            configure_btn = Gtk.Button(label='Configure')
            configure_btn.get_style_context().add_class('button-default')
            configure_btn.connect('clicked', lambda _: self._on_configure_nsec())
            right_box.pack_start(configure_btn, False, False, 0)
        sign_row.set_right_widget(right_box)
        id_list.add(sign_row)

        self._sync_btn = Gtk.Button(label='Sync')
        self._sync_btn.get_style_context().add_class('button-default')
        self._sync_btn.connect('clicked', lambda _: self._on_sync_profile())
        sync_row = ActionRow(
            title='Synchronise profile',
            subtitle='Fetch metadata and NIP-65 relay list from relays',
        )
        sync_row.set_right_widget(self._sync_btn)
        id_list.add(sync_row)
        self._identity_box.pack_start(id_list, False, False, 0)

        maint_label = Gtk.Label(label='Maintenance')
        maint_label.get_style_context().add_class('heading-4')
        maint_label.set_margin_bottom(8)
        maint_label.set_halign(Gtk.Align.START)
        self._maintenance_box.pack_start(maint_label, False, False, 0)

        maint_list = Gtk.ListBox()
        maint_list.set_selection_mode(Gtk.SelectionMode.NONE)

        self._repair_btn = Gtk.Button(label='Repair')
        self._repair_btn.get_style_context().add_class('button-default')
        self._repair_btn.connect('clicked', lambda _: self._on_repair_db())
        repair_row = ActionRow(
            title='Repair database',
            subtitle='Normalise and repair stored records',
        )
        repair_row.set_right_widget(self._repair_btn)
        maint_list.add(repair_row)
        self._maintenance_box.pack_start(maint_list, False, False, 0)

        self.show_all()

    def _on_interval_changed(self, widget):
        store = getattr(self._window, 'store', None)
        if store:
            minutes = int(widget.get_value())
            store.set_setting('min_publish_interval_seconds', str(minutes * 60))

    def _on_hourly_changed(self, widget):
        store = getattr(self._window, 'store', None)
        if store:
            store.set_setting('max_posts_per_hour', str(int(widget.get_value())))

    def _on_daily_changed(self, widget):
        store = getattr(self._window, 'store', None)
        if store:
            store.set_setting('max_posts_per_day_per_source', str(int(widget.get_value())))

    def _on_configure_nsec(self):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        dialog = SetNsecDialog(self._window)
        response = dialog.run()
        if response == Gtk.ResponseType.OK:
            nsec = dialog.get_nsec()
            if nsec:
                set_stored_nsec(store.db_path, nsec)
        dialog.destroy()
        self.refresh()

    def _on_remove_nsec(self):
        store = getattr(self._window, 'store', None)
        if not store:
            return

        dialog = Gtk.MessageDialog(
            transient_for=self._window,
            modal=True,
            message_type=Gtk.MessageType.QUESTION,
            buttons=Gtk.ButtonsType.YES_NO,
            text='Remove stored NSEC?',
        )
        response = dialog.run()
        dialog.destroy()
        if response != Gtk.ResponseType.YES:
            return

        clear_stored_nsec(store.db_path)
        nsec_file = os.path.abspath(store.db_path) + '.nsec'
        if os.path.exists(nsec_file):
            os.remove(nsec_file)

        self.refresh()

    def _on_sync_profile(self):
        store = getattr(self._window, 'store', None)
        if not store:
            return

        nsec = get_stored_nsec(store.db_path)
        if not nsec:
            self._show_info(
                'No NSEC configured',
                'Configure your Nostr signing key first.',
            )
            return

        bootstrap_relays = store.get_enabled_relays()
        if not bootstrap_relays:
            self._show_info(
                'No relays configured',
                'Add at least one relay to bootstrap the NIP-65 profile lookup.',
            )
            return

        self._sync_btn.set_sensitive(False)
        self._sync_btn.set_label('Syncing...')

        def _do_sync():
            try:
                imported = import_nip65_relays(
                    nsec=nsec,
                    store=store,
                    n=store.n,
                    bootstrap_relays=bootstrap_relays,
                    log_fn=lambda msg: None,
                )
                GLib.idle_add(self._on_sync_result, imported, None)
            except Exception as e:
                GLib.idle_add(self._on_sync_result, 0, str(e))

        t = threading.Thread(target=_do_sync, daemon=True)
        t.start()

    def _on_sync_result(self, imported, error):
        self._sync_btn.set_sensitive(True)
        self._sync_btn.set_label('Sync')

        if error:
            self._show_info('Sync failed', str(error))
            return

        if imported > 0:
            self._show_info(
                'Profile synced',
                f'Imported {imported} relay(s) from Nostr profile.',
            )
            if hasattr(self._window, 'switch_to'):
                self._window.switch_to('relays')
        else:
            self._show_info(
                'Profile synced',
                'No new relays found in NIP-65 relay list.',
            )

    def _on_repair_db(self):
        store = getattr(self._window, 'store', None)
        if not store or not hasattr(store, 'n'):
            return

        self._repair_btn.set_sensitive(False)
        self._repair_btn.set_label('Repairing...')

        def _do_repair():
            try:
                n = store.n
                counts = {'relays': 0, 'sources': 0, 'videos': 0, 'published_ts': 0}

                rows = store.conn.execute(
                    'SELECT id, relay_url FROM relays'
                ).fetchall()
                for rid, relay_url in rows:
                    try:
                        norm = n.normalise_relay_url(relay_url)
                        store.conn.execute(
                            'UPDATE relays SET relay_url_norm=? WHERE id=?',
                            (norm, rid),
                        )
                        counts['relays'] += 1
                    except Exception:
                        pass

                rows = store.conn.execute(
                    'SELECT id, api_base, api_channel_url, rss_url FROM sources'
                ).fetchall()
                for sid, api_base, api_channel_url, rss_url in rows:
                    if api_base:
                        try:
                            base_norm = n.normalise_http_url(api_base)
                            store.conn.execute(
                                'UPDATE sources SET api_base_norm=? WHERE id=?',
                                (base_norm, sid),
                            )
                        except Exception:
                            pass
                    if api_channel_url:
                        try:
                            chan_norm = n.normalise_http_url(api_channel_url)
                            store.conn.execute(
                                'UPDATE sources SET api_channel_url_norm=? WHERE id=?',
                                (chan_norm, sid),
                            )
                        except Exception:
                            pass
                    if rss_url:
                        try:
                            rss_norm = n.normalise_feed_url(rss_url)
                            store.conn.execute(
                                'UPDATE sources SET rss_url_norm=? WHERE id=?',
                                (rss_norm, sid),
                            )
                        except Exception:
                            pass
                    counts['sources'] += 1

                rows = store.conn.execute(
                    'SELECT id, watch_url, published_ts, first_seen_ts FROM videos'
                ).fetchall()
                for vid, watch_url, published_ts, first_seen_ts in rows:
                    try:
                        norm = n.normalise_watch_url(watch_url)
                        store.conn.execute(
                            'UPDATE videos SET watch_url_norm=? WHERE id=?',
                            (norm, vid),
                        )
                        counts['videos'] += 1
                    except Exception:
                        pass
                    if published_ts is None and first_seen_ts is not None:
                        store.conn.execute(
                            'UPDATE videos SET published_ts=? WHERE id=?',
                            (int(first_seen_ts), vid),
                        )
                        counts['published_ts'] += 1

                store.conn.commit()
                GLib.idle_add(self._on_repair_result, counts, None)
            except Exception as e:
                GLib.idle_add(self._on_repair_result, None, str(e))

        t = threading.Thread(target=_do_repair, daemon=True)
        t.start()

    def _on_repair_result(self, counts, error):
        self._repair_btn.set_sensitive(True)
        self._repair_btn.set_label('Repair')

        if error:
            self._show_info('Repair failed', str(error))
        else:
            msg = (
                f'Relays: {counts["relays"]}\n'
                f'Sources: {counts["sources"]}\n'
                f'Videos: {counts["videos"]}\n'
                f'Published timestamps filled: {counts["published_ts"]}'
            )
            self._show_info('Repair complete', msg)

        self.refresh()

    def _show_info(self, title, message):
        dialog = Gtk.MessageDialog(
            transient_for=self._window,
            modal=True,
            message_type=Gtk.MessageType.INFO,
            buttons=Gtk.ButtonsType.OK,
            text=title,
        )
        dialog.format_secondary_text(message)
        dialog.run()
        dialog.destroy()
