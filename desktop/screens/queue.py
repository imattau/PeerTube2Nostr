from datetime import datetime

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.search_input import SearchInput
from desktop.widgets.queue_row import QueueRow


class QueueScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self._window = window
        self._filter = 'pending'
        self._all_items: list[dict] = []

        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Queue')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(label='Videos waiting to be published')
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header.pack_start(title_box, True, True, 0)

        add_btn = Gtk.Button(label='+ Add source')
        add_btn.get_style_context().add_class('button-primary')
        header.pack_end(add_btn, False, False, 0)
        self.pack_start(header, False, False, 0)

        toolbar = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        toolbar.set_margin_top(24)
        toolbar.set_margin_bottom(16)

        self._search = SearchInput(placeholder='Search queued videos')
        self._search.connect('search-changed', self._on_search)
        toolbar.pack_start(self._search, False, False, 0)

        self._filter_pending = Gtk.Button(label='Pending')
        self._filter_pending.get_style_context().add_class('button-primary')
        self._filter_pending.connect('clicked', lambda _: self._set_filter('pending'))
        toolbar.pack_start(self._filter_pending, False, False, 0)

        self._filter_failed = Gtk.Button(label='Failed')
        self._filter_failed.get_style_context().add_class('button-default')
        self._filter_failed.connect('clicked', lambda _: self._set_filter('failed'))
        toolbar.pack_start(self._filter_failed, False, False, 0)

        self._filter_published = Gtk.Button(label='Published')
        self._filter_published.get_style_context().add_class('button-default')
        self._filter_published.connect('clicked', lambda _: self._set_filter('published'))
        toolbar.pack_start(self._filter_published, False, False, 0)

        self.pack_start(toolbar, False, False, 0)

        scroll = Gtk.ScrolledWindow()
        scroll.set_size_request(920, 340)

        self._list = Gtk.ListBox()
        self._list.set_selection_mode(Gtk.SelectionMode.NONE)
        scroll.add(self._list)
        self.pack_start(scroll, True, True, 0)

    def refresh(self):
        store = getattr(self._window, 'store', None)
        if not store:
            return

        pending = store.list_videos(status='pending', limit=200)
        failed = store.list_videos(status='failed', limit=200)
        published = store.list_videos(status='posted', limit=200)

        self._all_items = pending + failed + published
        self._filter_pending.set_label(f'Pending {len(pending)}')
        self._filter_failed.set_label(f'Failed {len(failed)}')

        self._rebuild_list()

    def _rebuild_list(self, search_text: str = ''):
        for child in self._list.get_children():
            self._list.remove(child)

        items_by_status = {'pending': [], 'failed': [], 'posted': []}
        for item in self._all_items:
            s = item.get('status', 'pending')
            items_by_status.get(s, []).append(item)

        items = items_by_status.get(self._filter, [])

        for v in items:
            title = v.get('title') or ''
            if search_text and search_text.lower() not in title.lower():
                continue
            chan = v.get('channel_name') or ''
            ts = v.get('first_seen_ts') or 0
            label = f'discovered {_relative_time(ts)}' if ts else ''
            status = v.get('status', 'pending')
            row = QueueRow(
                title=title,
                channel=chan,
                timestamp=label,
                status='posted' if status == 'posted' else status,
            )
            self._list.add(row)

        self._list.show_all()

    def _on_search(self, entry):
        self._rebuild_list(search_text=entry.get_text().strip())

    def _set_filter(self, status: str):
        self._filter = status
        for btn in [self._filter_pending, self._filter_failed, self._filter_published]:
            ctx = btn.get_style_context()
            ctx.remove_class('button-primary')
            ctx.add_class('button-default')
        active = {
            'pending': self._filter_pending,
            'failed': self._filter_failed,
            'published': self._filter_published,
        }[status]
        ctx = active.get_style_context()
        ctx.remove_class('button-default')
        ctx.add_class('button-primary')
        self._rebuild_list(search_text=self._search.get_text().strip())


def _relative_time(ts: int) -> str:
    diff = int(datetime.now().timestamp()) - ts
    if diff < 60:
        return 'just now'
    if diff < 3600:
        return f'{diff // 60} min ago'
    if diff < 86400:
        return f'{diff // 3600} hours ago'
    return f'{diff // 86400} days ago'
