import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, Pango


class LogViewer(Gtk.ScrolledWindow):
    def __init__(self):
        super().__init__()
        self.set_size_request(920, 500)
        self.get_style_context().add_class('log-viewer')

        self._buffer = Gtk.TextBuffer()
        self._text_view = Gtk.TextView(buffer=self._buffer)
        self._text_view.set_editable(False)
        self._text_view.set_cursor_visible(False)
        self._text_view.set_wrap_mode(Gtk.WrapMode.NONE)
        self._text_view.modify_font(
            Pango.FontDescription('monospace 11')
        )

        self._tag_info = self._buffer.create_tag(
            'info', foreground='#8A8A8A'
        )
        self._tag_warn = self._buffer.create_tag(
            'warn', foreground='#E5A50A'
        )
        self._tag_error = self._buffer.create_tag(
            'error', foreground='#E01B24'
        )
        self._tag_timestamp = self._buffer.create_tag(
            'timestamp', foreground='#555555'
        )

        self.add(self._text_view)

    def append_log(self, timestamp: str, level: str, message: str):
        end_iter = self._buffer.get_end_iter()
        self._buffer.insert_with_tags(
            end_iter, f'{timestamp}  ', self._tag_timestamp
        )
        tag = {
            'INFO': self._tag_info,
            'WARN': self._tag_warn,
            'ERROR': self._tag_error,
        }.get(level.upper(), self._tag_info)

        end_iter = self._buffer.get_end_iter()
        self._buffer.insert_with_tags(end_iter, f'{level:8}', tag)
        end_iter = self._buffer.get_end_iter()
        self._buffer.insert(end_iter, f'  {message}\n')

        textview = self.get_child()
        if textview:
            adj = self.get_vadjustment()
            if adj:
                adj.set_value(adj.get_upper() - adj.get_page_size())

    def clear(self):
        self._buffer.set_text('')
