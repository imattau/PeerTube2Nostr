from datetime import datetime

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.metric_card import MetricCard
from desktop.widgets.status_card import StatusCard
from desktop.widgets.banner import Banner
from desktop.widgets.queue_row import QueueRow


class OverviewScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('content-area')
        self.get_style_context().add_class('overview-screen')
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Overview')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(label='Monitor publishing health and upcoming posts')
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header_box.pack_start(title_box, True, True, 0)

        self._stop_btn = Gtk.Button(label='Stop')
        self._stop_btn.get_style_context().add_class('button-default')
        header_box.pack_end(self._stop_btn, False, False, 0)
        self.pack_start(header_box, False, False, 0)

        self._status_card = StatusCard()
        self.pack_start(self._status_card, False, False, 0)

        metrics_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=16)
        metrics_box.set_margin_bottom(16)
        self._metric_queued = MetricCard('Queued', '0')
        metrics_box.pack_start(self._metric_queued, False, False, 0)
        self._metric_published = MetricCard('Published today', '0')
        metrics_box.pack_start(self._metric_published, False, False, 0)
        self._metric_failed = MetricCard('Failed', '0')
        metrics_box.pack_start(self._metric_failed, False, False, 0)
        self._metric_sources = MetricCard('Active sources', '0')
        metrics_box.pack_start(self._metric_sources, False, False, 0)
        self.pack_start(metrics_box, False, False, 0)

        self._banner = Banner(
            title='Needs attention',
            body='',
            variant=Banner.WARNING,
        )
        self._banner.set_margin_bottom(16)
        review_btn = Gtk.Button(label='Review')
        review_btn.get_style_context().add_class('button-default')
        self._banner.set_action_widget(review_btn)
        self.pack_start(self._banner, False, False, 0)

        self._section_header = Gtk.Label(label='Next to publish')
        self._section_header.get_style_context().add_class('heading-3')
        self._section_header.set_halign(Gtk.Align.START)
        self._section_header.set_margin_bottom(12)
        self.pack_start(self._section_header, False, False, 0)

        self._next_row_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.pack_start(self._next_row_box, False, False, 0)

    def refresh(self):
        store = getattr(self._window, 'store', None)
        if not store:
            return

        pending = store.count_pending()
        today_ts = int(datetime.now().timestamp()) - 86400
        posted_today = store.count_posted_since(today_ts)
        failed = store.count_failed()
        sources = len(store.list_sources())

        self._metric_queued.set_value(str(pending))
        self._metric_published.set_value(str(posted_today))
        self._metric_failed.set_value(str(failed))
        self._metric_sources.set_value(str(sources))

        has_issues = failed > 0
        if has_issues:
            self._banner.show()
        else:
            self._banner.hide()

        for child in self._next_row_box.get_children():
            self._next_row_box.remove(child)

        if pending > 0:
            next_video = store.list_videos(status='pending', limit=1)
            if next_video:
                v = next_video[0]
                chan = v.get('channel_name') or ''
                ts = v.get('first_seen_ts') or 0
                label = f'discovered {_relative_time(ts)}' if ts else ''
                row = QueueRow(
                    title=v.get('title') or '',
                    channel=chan,
                    timestamp=label,
                    status='pending',
                )
                self._next_row_box.pack_start(row, False, False, 0)
            self._section_header.show()
        else:
            self._section_header.hide()

        self.show_all()


def _relative_time(ts: int) -> str:
    diff = int(datetime.now().timestamp()) - ts
    if diff < 60:
        return 'just now'
    if diff < 3600:
        return f'{diff // 60} min ago'
    if diff < 86400:
        return f'{diff // 3600} hours ago'
    return f'{diff // 86400} days ago'
