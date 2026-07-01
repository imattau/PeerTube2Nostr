import threading

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GLib

from desktop.widgets.action_row import ActionRow
from desktop.widgets.badge import Badge
from desktop.dialogs.add_relay import AddRelayDialog
from desktop.dialogs.confirm import ConfirmDialog


class RelaysScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('content-area')
        self.get_style_context().add_class('relays-screen')
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
        add_btn.connect('clicked', self._on_add)
        header.pack_end(add_btn, False, False, 0)

        self._import_btn = Gtk.Button(label='Import from NIP-65')
        self._import_btn.get_style_context().add_class('button-default')
        self._import_btn.set_margin_end(8)
        self._import_btn.connect('clicked', self._on_import_nip65)
        header.pack_end(self._import_btn, False, False, 0)

        self.pack_start(header, False, False, 0)

        self._relay_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._relay_box.set_margin_top(24)
        self.pack_start(self._relay_box, False, False, 0)

        self.refresh()

    def _on_add(self, _btn):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        dialog = AddRelayDialog(self._window)
        response = dialog.run()
        url = dialog.get_url()
        dialog.destroy()
        if response == Gtk.ResponseType.OK and url:
            try:
                store.add_relay(url)
            except Exception as e:
                err_dialog = Gtk.MessageDialog(
                    transient_for=self._window,
                    modal=True,
                    message_type=Gtk.MessageType.ERROR,
                    buttons=Gtk.ButtonsType.OK,
                    text='Failed to add relay',
                    secondary_text=str(e),
                )
                err_dialog.run()
                err_dialog.destroy()
            self.refresh()

    def _on_import_nip65(self, _btn):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        from core.database import get_stored_nsec
        from core.sync import import_nip65_relays
        from core.utils import UrlNormaliser
        nsec = get_stored_nsec(store.db_path)
        if not nsec:
            dialog = Gtk.MessageDialog(
                transient_for=self._window,
                modal=True,
                message_type=Gtk.MessageType.WARNING,
                buttons=Gtk.ButtonsType.OK,
                text='No nsec configured',
                secondary_text='Set a Nostr private key in Preferences first.',
            )
            dialog.run()
            dialog.destroy()
            return
        relays = store.get_enabled_relays()
        if not relays:
            dialog = Gtk.MessageDialog(
                transient_for=self._window,
                modal=True,
                message_type=Gtk.MessageType.WARNING,
                buttons=Gtk.ButtonsType.OK,
                text='No relays configured',
                secondary_text='Add at least one relay to bootstrap the NIP-65 profile lookup, then try again.',
            )
            dialog.run()
            dialog.destroy()
            return
        self._import_btn.set_sensitive(False)
        self._import_btn.set_label('Importing...')

        def _do_import():
            try:
                count = import_nip65_relays(
                    nsec, store, UrlNormaliser(), relays, log_fn=print,
                )
                GLib.idle_add(self._on_import_nip65_result, count, None)
            except Exception as e:
                GLib.idle_add(self._on_import_nip65_result, 0, str(e))

        t = threading.Thread(target=_do_import, daemon=True)
        t.start()

    def _on_import_nip65_result(self, count, error):
        self._import_btn.set_sensitive(True)
        self._import_btn.set_label('Import from NIP-65')

        if error:
            dialog = Gtk.MessageDialog(
                transient_for=self._window,
                modal=True,
                message_type=Gtk.MessageType.ERROR,
                buttons=Gtk.ButtonsType.OK,
                text='Import failed',
                secondary_text=str(error),
            )
            dialog.run()
            dialog.destroy()
            return

        dialog = Gtk.MessageDialog(
            transient_for=self._window,
            modal=True,
            message_type=Gtk.MessageType.INFO,
            buttons=Gtk.ButtonsType.OK,
            text=f'Imported {count} relay(s)',
            secondary_text='New relays have been added from your Nostr profile.',
        )
        dialog.run()
        dialog.destroy()
        self.refresh()

    def _on_toggle(self, relay_id: int, switch: Gtk.Switch):
        store = getattr(self._window, 'store', None)
        if store:
            store.set_relay_enabled(relay_id, switch.get_active())

    def _on_remove(self, relay_id: int, url: str):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        dialog = ConfirmDialog(
            self._window,
            title='Remove relay?',
            body=f'Remove {url} from your configured relays.',
            confirm_label='Remove',
            confirm_style=ConfirmDialog.DESTRUCTIVE,
        )
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.OK:
            store.remove_relay(relay_id)
            self.refresh()

    def refresh(self):
        for child in self._relay_box.get_children():
            self._relay_box.remove(child)

        store = getattr(self._window, 'store', None)
        if not store:
            return

        relays = store.list_relays()
        enabled_relays = [r for r in relays if r.get('enabled', 1)]
        disabled_relays = [r for r in relays if not r.get('enabled', 1)]

        if enabled_relays:
            label = Gtk.Label(label='Configured relays')
            label.get_style_context().add_class('heading-4')
            label.set_halign(Gtk.Align.START)
            label.set_margin_bottom(8)
            self._relay_box.pack_start(label, False, False, 0)

            list_box = Gtk.ListBox()
            list_box.set_selection_mode(Gtk.SelectionMode.NONE)
            for r in enabled_relays:
                row = self._build_relay_row(r)
                list_box.add(row)
            self._relay_box.pack_start(list_box, False, False, 0)

        if disabled_relays:
            label = Gtk.Label(label='Disabled')
            label.get_style_context().add_class('heading-4')
            label.set_halign(Gtk.Align.START)
            label.set_margin_bottom(8)
            self._relay_box.pack_start(label, False, False, 0)

            list_box = Gtk.ListBox()
            list_box.set_selection_mode(Gtk.SelectionMode.NONE)
            for r in disabled_relays:
                row = self._build_relay_row(r)
                list_box.add(row)
            self._relay_box.pack_start(list_box, False, False, 0)

        self.show_all()

    def _build_relay_row(self, relay: dict) -> ActionRow:
        rid = relay['id']
        url = relay.get('relay_url', '')
        enabled = bool(relay.get('enabled', 1))
        latency = relay.get('latency_ms')

        if not enabled:
            dot_color = '#C7C5C2'
            subtitle = 'Disabled'
            badge_variant = None
            badge_text = None
        elif latency is None:
            dot_color = '#E01B24'
            subtitle = 'Unavailable'
            badge_variant = Badge.ERROR
            badge_text = 'Offline'
        elif latency < 200:
            dot_color = '#2EC27E'
            subtitle = f'Connected  ·  {latency} ms'
            badge_variant = Badge.SUCCESS
            badge_text = 'Healthy'
        elif latency < 1000:
            dot_color = '#E5A50A'
            subtitle = f'Connected  ·  {latency} ms'
            badge_variant = Badge.WARNING
            badge_text = 'High latency'
        else:
            dot_color = '#E01B24'
            subtitle = f'Connected  ·  {latency} ms'
            badge_variant = Badge.ERROR
            badge_text = 'High latency'

        row = ActionRow(
            title=url,
            subtitle=subtitle,
            icon_text='\u25CF',
            icon_color=dot_color,
        )

        action_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=4)

        if enabled and badge_variant:
            badge = Badge(text=badge_text, variant=badge_variant)
            action_box.pack_end(badge, False, False, 0)

        menu_btn = Gtk.Button(label='\u22EE')
        menu_btn.get_style_context().add_class('button-icon')
        menu_btn.connect('clicked', lambda _, i=rid, u=url: self._on_remove(i, u))
        action_box.pack_end(menu_btn, False, False, 0)

        switch = Gtk.Switch()
        switch.set_active(enabled)
        switch.connect('notify::active', lambda s, _, i=rid: self._on_toggle(i, s))
        action_box.pack_end(switch, False, False, 0)

        row.set_right_widget(action_box)
        return row
