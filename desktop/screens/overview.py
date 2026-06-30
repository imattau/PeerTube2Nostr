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
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header_box = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=0
        )
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Overview')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Monitor publishing health and upcoming posts'
        )
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

        metrics_box = Gtk.Box(
            orientation=Gtk.Orientation.HORIZONTAL, spacing=16
        )
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
            body='One failed publication and two unavailable relays.',
            variant=Banner.WARNING,
        )
        self._banner.set_margin_bottom(16)

        review_btn = Gtk.Button(label='Review')
        review_btn.get_style_context().add_class('button-default')
        self._banner.set_action_widget(review_btn)
        self.pack_start(self._banner, False, False, 0)

        section_header = Gtk.Label(label='Next to publish')
        section_header.get_style_context().add_class('heading-3')
        section_header.set_halign(Gtk.Align.START)
        section_header.set_margin_bottom(12)
        self.pack_start(section_header, False, False, 0)

        self._queue_row = QueueRow(
            title='GNOME 48 Release Highlights',
            channel='GNOME Foundation',
            timestamp='discovered 12 min ago',
            status='pending',
        )
        self.pack_start(self._queue_row, False, False, 0)

    def set_metrics(self, metrics: dict):
        self._metric_queued.set_value(str(metrics.get('pending', 0)))
        self._metric_published.set_value(str(metrics.get('posted_today', 0)))
        self._metric_failed.set_value(str(metrics.get('failed', 0)))
        self._metric_sources.set_value(str(metrics.get('active_sources', 0)))

        if metrics.get('failed', 0) > 0 or metrics.get('unhealthy_relays', 0) > 0:
            self._banner.show()
        else:
            self._banner.hide()

    def set_runner_status(self, running: bool, next_check: str = ''):
        if running:
            self._status_card.set_status(
                StatusCard.RUNNING,
                f'Next source check in {next_check}' if next_check else '',
            )
            self._stop_btn.set_label('Stop')
        else:
            self._status_card.set_status(StatusCard.STOPPED)
            self._stop_btn.set_label('Start')

    def refresh(self):
        pass
