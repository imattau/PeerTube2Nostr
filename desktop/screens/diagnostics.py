import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.metric_card import MetricCard
from desktop.widgets.status_card import StatusCard


class DiagnosticsScreen(Gtk.Box):
    def __init__(self, window):
        super().__init__(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        self.get_style_context().add_class('content-area')
        self.get_style_context().add_class('diagnostics-screen')
        self._window = window
        self.set_margin_start(40)
        self.set_margin_end(40)
        self.set_margin_top(32)

        header = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=0)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        title = Gtk.Label(label='Diagnostics')
        title.get_style_context().add_class('heading-1')
        title.set_halign(Gtk.Align.START)
        title_box.pack_start(title, False, False, 0)

        subtitle = Gtk.Label(
            label='Application health and system status'
        )
        subtitle.get_style_context().add_class('body')
        subtitle.set_halign(Gtk.Align.START)
        subtitle.set_margin_top(8)
        title_box.pack_start(subtitle, False, False, 0)
        header.pack_start(title_box, True, True, 0)
        self.pack_start(header, False, False, 0)

        scroll = Gtk.ScrolledWindow()
        scroll.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        scroll.set_size_request(920, 600)

        content = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        content.set_margin_top(24)

        self._status_card = StatusCard()
        content.pack_start(self._status_card, False, False, 0)

        metrics_box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=16)
        metrics_box.set_margin_bottom(16)
        self._metric_pending = MetricCard('Pending', '0')
        metrics_box.pack_start(self._metric_pending, False, False, 0)
        self._metric_published = MetricCard('Published', '0')
        metrics_box.pack_start(self._metric_published, False, False, 0)
        self._metric_failed = MetricCard('Failed', '0')
        metrics_box.pack_start(self._metric_failed, False, False, 0)
        self._metric_sources = MetricCard('Sources', '0')
        metrics_box.pack_start(self._metric_sources, False, False, 0)
        content.pack_start(metrics_box, False, False, 0)

        details_card = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        details_card.get_style_context().add_class('card')
        details_card.set_size_request(920, -1)
        details_card.set_margin_bottom(16)

        inner = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=0)
        inner.set_margin_start(20)
        inner.set_margin_end(20)
        inner.set_margin_top(20)
        inner.set_margin_bottom(20)

        sys_heading = Gtk.Label(label='System')
        sys_heading.get_style_context().add_class('heading-3')
        sys_heading.set_halign(Gtk.Align.START)
        inner.pack_start(sys_heading, False, False, 0)

        self._detail_db_path = self._make_detail_row('Database path')
        inner.pack_start(self._detail_db_path, False, False, 0)
        self._detail_app_status = self._make_detail_row('Application')
        inner.pack_start(self._detail_app_status, False, False, 0)
        self._detail_background = self._make_detail_row('Background runner')
        inner.pack_start(self._detail_background, False, False, 0)

        inner.pack_start(Gtk.Separator(orientation=Gtk.Orientation.HORIZONTAL), False, False, 0)
        inner.pack_start(Gtk.Label(), False, False, 0)

        relay_heading = Gtk.Label(label='Relays')
        relay_heading.get_style_context().add_class('heading-3')
        relay_heading.set_halign(Gtk.Align.START)
        inner.pack_start(relay_heading, False, False, 0)

        self._detail_relay_total = self._make_detail_row('Total configured')
        inner.pack_start(self._detail_relay_total, False, False, 0)
        self._detail_relay_healthy = self._make_detail_row('Healthy')
        inner.pack_start(self._detail_relay_healthy, False, False, 0)
        self._detail_relay_slow = self._make_detail_row('High latency')
        inner.pack_start(self._detail_relay_slow, False, False, 0)
        self._detail_relay_offline = self._make_detail_row('Offline')
        inner.pack_start(self._detail_relay_offline, False, False, 0)

        inner.pack_start(Gtk.Separator(orientation=Gtk.Orientation.HORIZONTAL), False, False, 0)
        inner.pack_start(Gtk.Label(), False, False, 0)

        config_heading = Gtk.Label(label='Configuration')
        config_heading.get_style_context().add_class('heading-3')
        config_heading.set_halign(Gtk.Align.START)
        inner.pack_start(config_heading, False, False, 0)

        self._detail_pub_limits = self._make_detail_row('Publish limits')
        inner.pack_start(self._detail_pub_limits, False, False, 0)
        self._detail_daily_source = self._make_detail_row('Daily source limit')
        inner.pack_start(self._detail_daily_source, False, False, 0)
        self._detail_setup = self._make_detail_row('Setup complete')
        inner.pack_start(self._detail_setup, False, False, 0)

        details_card.pack_start(inner, False, False, 0)
        content.pack_start(details_card, False, False, 0)

        errors_heading = Gtk.Label(label='Recent errors')
        errors_heading.get_style_context().add_class('heading-3')
        errors_heading.set_halign(Gtk.Align.START)
        errors_heading.set_margin_bottom(8)
        content.pack_start(errors_heading, False, False, 0)

        self._error_list = Gtk.ListBox()
        self._error_list.set_selection_mode(Gtk.SelectionMode.NONE)
        content.pack_start(self._error_list, False, False, 0)

        scroll.add(content)
        self.pack_start(scroll, True, True, 0)

    def _make_detail_row(self, label: str) -> Gtk.Box:
        row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        row.set_margin_top(4)
        row.set_margin_bottom(4)

        lbl = Gtk.Label(label=label)
        lbl.get_style_context().add_class('body')
        lbl.set_halign(Gtk.Align.START)
        row.pack_start(lbl, False, False, 0)

        expander = Gtk.Box()
        row.pack_start(expander, True, True, 0)

        val = Gtk.Label(label='')
        val.get_style_context().add_class('body')
        val.set_halign(Gtk.Align.END)
        val.set_xalign(1.0)
        row.pack_end(val, False, False, 0)
        row._value_label = val
        return row

    def _get_manager(self):
        manager = getattr(self._window, 'manager', None)
        if manager:
            return manager
        app = self._window.get_application()
        if app:
            return getattr(app, 'manager', None)
        return None

    def refresh(self):
        store = getattr(self._window, 'store', None)
        manager = self._get_manager()

        if not store:
            self._status_card.set_status(StatusCard.STOPPED, 'Database unavailable')
            for metric in [self._metric_pending, self._metric_published,
                           self._metric_failed, self._metric_sources]:
                metric.set_value('--')
            self._set_detail(self._detail_db_path, 'Not connected')
            self._set_detail(self._detail_app_status, 'Unavailable')
            self._set_detail(self._detail_background, 'N/A')
            self._set_detail(self._detail_relay_total, 'N/A')
            self._set_detail(self._detail_relay_healthy, 'N/A')
            self._set_detail(self._detail_relay_slow, 'N/A')
            self._set_detail(self._detail_relay_offline, 'N/A')
            self._set_detail(self._detail_pub_limits, 'N/A')
            self._set_detail(self._detail_daily_source, 'N/A')
            self._set_detail(self._detail_setup, 'N/A')
            self.show_all()
            return

        pending = store.count_pending()
        published = store.count_posted()
        failed = store.count_failed()
        source_count = store.count_sources()
        relay_count = store.count_relays()

        self._metric_pending.set_value(str(pending))
        self._metric_published.set_value(str(published))
        self._metric_failed.set_value(str(failed))
        self._metric_sources.set_value(str(source_count))

        runner_active = False
        runner_error = None
        if manager:
            runner_active = manager.is_running
            runner_error = getattr(manager, 'error_state', None)
            subtitle = runner_error if runner_error else (
                'Background runner active' if runner_active else 'Background runner inactive'
            )
            self._status_card.set_status(
                StatusCard.RUNNING if runner_active else StatusCard.STOPPED,
                subtitle
            )
        else:
            self._status_card.set_status(
                StatusCard.STOPPED if store else StatusCard.RUNNING,
                'Manager not available'
            )

        self._set_detail(self._detail_db_path, store.db_path)
        self._set_detail(self._detail_app_status,
                         'Running' if runner_active else 'Stopped')
        if runner_error:
            self._set_detail(self._detail_background, f'Error: {runner_error}')
        elif manager:
            self._set_detail(self._detail_background, 'Active')
        else:
            self._set_detail(self._detail_background, 'Not available')

        self._set_detail(self._detail_relay_total, str(relay_count))
        if relay_count > 0:
            relays = store.list_relays()
            healthy = sum(1 for r in relays if r.get('enabled') and r.get('latency_ms') is not None and r['latency_ms'] < 200)
            slow = sum(1 for r in relays if r.get('enabled') and r.get('latency_ms') is not None and r['latency_ms'] >= 200)
            offline = sum(1 for r in relays if r.get('enabled') and r.get('latency_ms') is None)
            self._set_detail(self._detail_relay_healthy, str(healthy))
            self._set_detail(self._detail_relay_slow, str(slow))
            self._set_detail(self._detail_relay_offline, str(offline))
        else:
            self._set_detail(self._detail_relay_healthy, '0')
            self._set_detail(self._detail_relay_slow, '0')
            self._set_detail(self._detail_relay_offline, '0')

        try:
            per_day, per_hour = store.get_publish_limits()
            self._set_detail(self._detail_pub_limits,
                             f'{per_day}/day, {per_hour}/hour')
        except Exception:
            self._set_detail(self._detail_pub_limits, 'Default')

        try:
            daily_limit = store.get_daily_source_limit()
            self._set_detail(self._detail_daily_source, str(daily_limit))
        except Exception:
            self._set_detail(self._detail_daily_source, 'N/A')

        if store.get_setting('setup_complete') in ('1', 'true', True):
            self._set_detail(self._detail_setup, 'Yes')
        else:
            self._set_detail(self._detail_setup, 'No')

        for child in self._error_list.get_children():
            self._error_list.remove(child)

        if manager:
            logs = manager.get_logs(max_lines=200)
            error_lines = [l for l in logs if 'ERROR' in l.upper()][-20:]
            for line in error_lines:
                label = Gtk.Label(label=line)
                label.get_style_context().add_class('body')
                label.set_halign(Gtk.Align.START)
                label.set_xalign(0.0)
                label.set_margin_start(12)
                label.set_margin_end(12)
                label.set_margin_top(6)
                label.set_margin_bottom(6)
                label.set_ellipsize(True)
                row = Gtk.ListBoxRow()
                row.add(label)
                self._error_list.add(row)

        if not self._error_list.get_children():
            empty_lbl = Gtk.Label(label='No recent errors')
            empty_lbl.get_style_context().add_class('body')
            empty_lbl.set_halign(Gtk.Align.START)
            empty_lbl.set_margin_start(12)
            empty_lbl.set_margin_end(12)
            empty_lbl.set_margin_top(6)
            empty_lbl.set_margin_bottom(6)
            row = Gtk.ListBoxRow()
            row.add(empty_lbl)
            self._error_list.add(row)

        self.show_all()

    def _set_detail(self, row: Gtk.Box, value: str):
        row._value_label.set_text(str(value))
