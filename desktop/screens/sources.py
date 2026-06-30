import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.action_row import ActionRow
from desktop.dialogs.add_source import AddSourceDialog
from desktop.dialogs.confirm import ConfirmDialog


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
        add_btn.connect('clicked', self._on_add)
        header.pack_end(add_btn, False, False, 0)
        self.pack_start(header, False, False, 0)

        self._channel_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._channel_box.set_margin_top(24)
        self.pack_start(self._channel_box, False, False, 0)

        self._rss_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._rss_box.set_margin_top(24)
        self.pack_start(self._rss_box, False, False, 0)

    def _on_add(self, _btn):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        dialog = AddSourceDialog(self._window)
        response = dialog.run()
        url = dialog.get_url()
        dialog.destroy()
        if response == Gtk.ResponseType.OK and url:
            try:
                store.add_channel_source(url)
            except Exception:
                try:
                    store.add_rss_source(url)
                except Exception:
                    pass
            self.refresh()

    def _on_toggle(self, source_id: int, switch: Gtk.Switch):
        store = getattr(self._window, 'store', None)
        if store:
            store.set_source_enabled(source_id, switch.get_active())

    def _on_remove(self, source_id: int, name: str):
        store = getattr(self._window, 'store', None)
        if not store:
            return
        dialog = ConfirmDialog(
            self._window,
            title='Remove source?',
            body=f'Videos already published from "{name}" will remain on Nostr.',
            confirm_label='Remove',
            confirm_style=ConfirmDialog.DESTRUCTIVE,
        )
        response = dialog.run()
        dialog.destroy()
        if response == Gtk.ResponseType.OK:
            store.remove_source(source_id)
            self.refresh()

    def refresh(self):
        for child in self._channel_box.get_children():
            self._channel_box.remove(child)
        for child in self._rss_box.get_children():
            self._rss_box.remove(child)

        store = getattr(self._window, 'store', None)
        if not store:
            return

        sources = store.list_sources()
        channels = [s for s in sources if s.get('api_base')]
        rss = [s for s in sources if not s.get('api_base') and s.get('rss_url')]

        if channels:
            label = Gtk.Label(label='PeerTube channels')
            label.get_style_context().add_class('heading-4')
            label.set_halign(Gtk.Align.START)
            label.set_margin_bottom(8)
            self._channel_box.pack_start(label, False, False, 0)

            list_box = Gtk.ListBox()
            list_box.set_selection_mode(Gtk.SelectionMode.NONE)
            for src in channels:
                row = self._build_source_row(src)
                list_box.add(row)
            self._channel_box.pack_start(list_box, False, False, 0)

        if rss:
            label = Gtk.Label(label='RSS-only sources')
            label.get_style_context().add_class('heading-4')
            label.set_halign(Gtk.Align.START)
            label.set_margin_bottom(8)
            self._rss_box.pack_start(label, False, False, 0)

            list_box = Gtk.ListBox()
            list_box.set_selection_mode(Gtk.SelectionMode.NONE)
            for src in rss:
                row = self._build_source_row(src)
                list_box.add(row)
            self._rss_box.pack_start(list_box, False, False, 0)

        self.show_all()

    def _build_source_row(self, src: dict) -> ActionRow:
        sid = src['id']
        enabled = bool(src.get('enabled', 1))
        title = (src.get('api_channel') or '').strip() or (src.get('rss_url') or '').strip()
        subtitle = src.get('api_channel_url') or src.get('rss_url') or ''
        last_error = src.get('last_error') or ''
        dot_color = '#2EC27E'
        icon = '\u25C9'
        if not enabled:
            dot_color = '#C7C5C2'
        elif last_error:
            dot_color = '#E01B24'
            icon = '\u25CF'

        row = ActionRow(
            title=title,
            subtitle=last_error if last_error else subtitle,
            icon_text=icon,
            icon_color=dot_color,
        )

        switch = Gtk.Switch()
        switch.set_active(enabled)
        switch.connect('notify::active', lambda s, _, i=sid: self._on_toggle(i, s))
        row.set_right_widget(switch)

        menu_btn = Gtk.Button(label='\u22EE')
        menu_btn.get_style_context().add_class('button-icon')
        menu_btn.connect('clicked', lambda _, i=sid, n=title: self._on_remove(i, n))
        row.set_right_widget(switch)

        action_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=4)
        action_box.pack_end(menu_btn, False, False, 0)
        action_box.pack_end(switch, False, False, 0)
        row.set_right_widget(action_box)

        return row
