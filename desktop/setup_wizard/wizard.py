import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GLib

from desktop.core import Store
from core.database import set_stored_nsec
from desktop.setup_wizard.welcome_page import WelcomePage
from desktop.setup_wizard.identity_page import IdentityPage
from desktop.setup_wizard.relay_page import RelayPage
from desktop.setup_wizard.source_page import SourcePage


class SetupWizard(Gtk.Assistant):
    def __init__(self, store: Store):
        super().__init__()
        self._store = store
        self.set_title('PeerTube2Nostr Setup')
        self.set_default_size(680, 560)
        self.set_resizable(False)
        self.set_position(Gtk.WindowPosition.CENTER)
        self.set_modal(True)

        self._pages = {}

        self._pages['welcome'] = WelcomePage()
        self.append_page(self._pages['welcome'])
        self.set_page_title(self._pages['welcome'], '')
        self.set_page_type(self._pages['welcome'], Gtk.AssistantPageType.INTRO)
        self.set_page_complete(self._pages['welcome'], True)

        self._pages['identity'] = IdentityPage(wizard=self)
        self.append_page(self._pages['identity'])
        self.set_page_title(self._pages['identity'], '')
        self.set_page_type(self._pages['identity'], Gtk.AssistantPageType.CONTENT)
        self.set_page_complete(self._pages['identity'], False)

        self._pages['relay'] = RelayPage()
        self.append_page(self._pages['relay'])
        self.set_page_title(self._pages['relay'], '')
        self.set_page_type(self._pages['relay'], Gtk.AssistantPageType.CONTENT)
        self.set_page_complete(self._pages['relay'], True)

        self._pages['source'] = SourcePage(wizard=self)
        self.append_page(self._pages['source'])
        self.set_page_title(self._pages['source'], '')
        self.set_page_type(self._pages['source'], Gtk.AssistantPageType.CONTENT)
        self.set_page_complete(self._pages['source'], False)

        self._pages['ready'] = Gtk.Box(
            orientation=Gtk.Orientation.VERTICAL, spacing=16
        )
        self._pages['ready'].set_valign(Gtk.Align.CENTER)
        self._pages['ready'].set_halign(Gtk.Align.CENTER)
        self._pages['ready'].set_margin_start(40)
        self._pages['ready'].set_margin_end(40)

        check_icon = Gtk.Label(label='\u2714')
        check_icon.set_name('wizard-ready-icon')
        self._pages['ready'].pack_start(check_icon, False, False, 0)

        ready_title = Gtk.Label(label='Ready to publish')
        ready_title.get_style_context().add_class('heading-2')
        self._pages['ready'].pack_start(ready_title, False, False, 0)

        self._summary = Gtk.Label(label='')
        self._summary.get_style_context().add_class('body')
        self._summary.set_margin_top(8)
        self._pages['ready'].pack_start(self._summary, False, False, 0)

        self.append_page(self._pages['ready'])
        self.set_page_title(self._pages['ready'], '')
        self.set_page_type(self._pages['ready'], Gtk.AssistantPageType.CONFIRM)
        self.set_page_complete(self._pages['ready'], True)

        self.connect('apply', self._on_apply)
        self.connect('cancel', lambda _: self.destroy())

        self.show_all()

    def _on_apply(self, _assistant):
        identity_data = self._pages['identity'].get_data()
        relay_url = self._pages['relay'].get_url()
        source_url = self._pages['source'].get_url()

        if identity_data.get('method') == 'nsec' and identity_data.get('nsec'):
            self._store.set_setting('signing_method', 'nsec')
            set_stored_nsec(self._store.db_path, identity_data['nsec'])
        elif identity_data.get('method') == 'bunker' and identity_data.get('bunker_url'):
            self._store.set_setting('signing_method', 'bunker')
            self._store.set_setting('bunker_url', identity_data['bunker_url'])

        if relay_url:
            try:
                self._store.add_relay(relay_url)
            except Exception:
                pass

        if source_url:
            try:
                self._store.add_channel_source(source_url)
            except Exception:
                try:
                    self._store.add_rss_source(source_url)
                except Exception:
                    pass

        self._store.set_setting('setup_complete', 'true')

    def update_summary(self):
        source_count = len(self._store.list_sources()) if hasattr(self._store, 'list_sources') else 1
        relay_count = len(self._store.list_relays()) if hasattr(self._store, 'list_relays') else 1
        self._summary.set_text(
            f'{source_count} source \u00B7 {relay_count} relays \u00B7 identity configured'
        )
