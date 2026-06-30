import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

from desktop.widgets.badge import Badge


class QueueRow(Gtk.ListBoxRow):
    def __init__(
        self,
        title: str,
        channel: str,
        timestamp: str = '',
        status: str = 'pending',
    ):
        super().__init__()
        self.set_size_request(920, 78)

        box = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=12)
        box.set_margin_start(12)
        box.set_margin_end(12)
        box.set_margin_top(12)
        box.set_margin_bottom(12)

        thumbnail = Gtk.Box(
            orientation=Gtk.Orientation.VERTICAL, spacing=0
        )
        thumbnail.set_size_request(96, 54)
        thumbnail.get_style_context().add_class('queue-thumbnail')
        play_label = Gtk.Label(label='\u25B6')
        play_label.set_name('queue-play-icon')
        thumbnail.pack_start(play_label, True, True, 0)
        box.pack_start(thumbnail, False, False, 0)

        text_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
        self._title = Gtk.Label(label=title)
        self._title.get_style_context().add_class('heading-4')
        self._title.set_halign(Gtk.Align.START)
        text_box.pack_start(self._title, False, False, 0)

        subtitle = f'{channel}  \u2022  {timestamp}' if timestamp else channel
        self._meta = Gtk.Label(label=subtitle)
        self._meta.get_style_context().add_class('body')
        self._meta.set_halign(Gtk.Align.START)
        text_box.pack_start(self._meta, False, False, 0)

        box.pack_start(text_box, True, True, 0)

        badge_map = {
            'pending': (Badge.ACCENT, 'Pending'),
            'failed': (Badge.ERROR, 'Failed'),
            'published': (Badge.SUCCESS, 'Published'),
        }
        variant, label = badge_map.get(status, (Badge.ACCENT, 'Pending'))
        self._badge = Badge(text=label, variant=variant)
        box.pack_end(self._badge, False, False, 0)

        self._menu_btn = Gtk.Button(label='\u22EE')
        self._menu_btn.get_style_context().add_class('button-icon')
        box.pack_end(self._menu_btn, False, False, 0)

        self.add(box)
