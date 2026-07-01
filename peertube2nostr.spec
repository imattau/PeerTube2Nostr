Name:           peertube2nostr
Version:        0.0.2
Release:        1%{?dist}
Summary:        PeerTube to Nostr publisher - GNOME desktop application

License:        LGPL-2.1+
URL:            https://github.com/mattthomson/PeerTube2Nostr
Source0:        %{url}/archive/v%{version}/%{name}-%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
BuildRequires:  python3-pip
BuildRequires:  python3-wheel
BuildRequires:  python3-virtualenv
BuildRequires:  desktop-file-utils

Requires:       python3 >= 3.10
Requires:       gtk3
Requires:       python3-gobject
Requires:       python3-cairo
Requires:       python3-keyring
Requires:       python3-requests
Requires:       python3-feedparser
Recommends:     adwaita-icon-theme
Recommends:     libappindicator-gtk3

%description
PeerTube2Nostr publishes PeerTube channel videos to Nostr relays with
proper attribution. This package provides the native GNOME 3 desktop
interface built with GTK 3.

Features:
- Channel video ingestion via PeerTube API with RSS fallback
- Automatic Nostr note publishing with embedded MP4/HLS links
- SQLite-backed deduplication and relay management
- Secure nsec storage via OS keyring
- Native GNOME 3 UI with Adwaita theming

%prep
%autosetup -n %{name}-%{version}

%build
%py3_build_wheel

%install
%py3_install_wheel %{name}-%{version}-py3-none-any.whl

mkdir -p %{buildroot}%{_datadir}/applications
mkdir -p %{buildroot}%{_datadir}/icons/hicolor/32x32/apps
mkdir -p %{buildroot}%{_datadir}/icons/hicolor/48x48/apps
mkdir -p %{buildroot}%{_datadir}/icons/hicolor/64x64/apps
mkdir -p %{buildroot}%{_datadir}/icons/hicolor/96x96/apps
mkdir -p %{buildroot}%{_datadir}/icons/hicolor/128x128/apps

install -m 0644 %{_sourcedir}/%{name}-%{version}/debian/peertube2nostr.desktop %{buildroot}%{_datadir}/applications/
install -m 0644 %{_sourcedir}/%{name}-%{version}/desktop/styles/icons/app_icon_32.png %{buildroot}%{_datadir}/icons/hicolor/32x32/apps/peertube2nostr.png
install -m 0644 %{_sourcedir}/%{name}-%{version}/desktop/styles/icons/app_icon_48.png %{buildroot}%{_datadir}/icons/hicolor/48x48/apps/peertube2nostr.png
install -m 0644 %{_sourcedir}/%{name}-%{version}/desktop/styles/icons/app_icon_64.png %{buildroot}%{_datadir}/icons/hicolor/64x64/apps/peertube2nostr.png
install -m 0644 %{_sourcedir}/%{name}-%{version}/desktop/styles/icons/app_icon_96.png %{buildroot}%{_datadir}/icons/hicolor/96x96/apps/peertube2nostr.png
install -m 0644 %{_sourcedir}/%{name}-%{version}/desktop/styles/icons/app_icon_128.png %{buildroot}%{_datadir}/icons/hicolor/128x128/apps/peertube2nostr.png

desktop-file-validate %{buildroot}%{_datadir}/applications/peertube2nostr.desktop

%files
%license LICENSE
%doc README.md
%{python3_sitelib}/core/
%{python3_sitelib}/%{name}/
%{python3_sitelib}/%{name}-%{version}.dist-info/
%{_datadir}/applications/peertube2nostr.desktop
%{_datadir}/icons/hicolor/*/apps/peertube2nostr.png

%changelog
* Wed Jul 01 2026 Matt Thomson <peertube2nostr@users.noreply.github.com> - 0.0.2-1
- Initial RPM release
