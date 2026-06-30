import os
import sys

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('Gdk', '3.0')
from gi.repository import Gtk, Gio, Gdk

from desktop.core import Store, UrlNormaliser


def _default_db_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'peertube2nostr.db',
    )


class PeerTube2NostrApp(Gtk.Application):
    def __init__(self):
        super().__init__(
            application_id='com.peertube2nostr.desktop',
            flags=Gio.ApplicationFlags.FLAGS_NONE,
        )
        self.store: Store | None = None
        self.window: Gtk.ApplicationWindow | None = None

    def do_startup(self):
        Gtk.Application.do_startup(self)

        css_path = os.path.join(
            os.path.dirname(__file__), 'styles', 'adwaita.css'
        )
        if os.path.exists(css_path):
            css_provider = Gtk.CssProvider()
            css_provider.load_from_path(css_path)
            screen = Gdk.Screen.get_default()
            if screen:
                Gtk.StyleContext.add_provider_for_screen(
                    screen,
                    css_provider,
                    Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION,
                )

        quit_action = Gio.SimpleAction.new('quit', None)
        quit_action.connect('activate', lambda *_: self.quit())
        self.add_action(quit_action)
        self.set_accels_for_action('app.quit', ['<Primary>q'])

    def do_activate(self):
        if self.window is not None:
            self.window.present()
            return

        db_path = os.environ.get('PEERTUBE2NOSTR_DB_PATH') or _default_db_path()
        normaliser = UrlNormaliser()
        try:
            self.store = Store(db_path, normaliser)
            self.store.init_schema()
        except Exception as e:
            dialog = Gtk.MessageDialog(
                transient_for=None,
                modal=True,
                message_type=Gtk.MessageType.ERROR,
                buttons=Gtk.ButtonsType.CLOSE,
                text=f'Failed to open database:\n{e}',
            )
            dialog.run()
            dialog.destroy()
            self.quit()
            return

        is_complete = self.store.get_setting('setup_complete') == '1'

        if is_complete:
            self._show_main_window()
        else:
            from desktop.setup_wizard.wizard import SetupWizard
            wizard = SetupWizard(self.store)
            self.add_window(wizard)
            wizard.set_modal(True)
            wizard.connect('apply', self._on_wizard_finished)
            wizard.show()

    def _on_wizard_finished(self, wizard):
        self._show_main_window()
        wizard.destroy()

    def _show_main_window(self):
        from desktop.window import MainWindow
        self.window = MainWindow(application=self, store=self.store)
        self.add_window(self.window)
        self.window.show_all()


def main():
    app = PeerTube2NostrApp()
    app.run()


if __name__ == '__main__':
    main()
