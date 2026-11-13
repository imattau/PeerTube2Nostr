mod commands;
mod db;
mod models;
mod nsec;

use db::Database;
use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(|app| {
            let db = Database::new().expect("Failed to initialise database");
            app.manage(db);

            if let (Some(window), Some(icon)) = (
                app.get_webview_window("main"),
                app.default_window_icon(),
            ) {
                let _ = window.set_icon(icon.clone());
            }

            #[cfg(feature = "tray-icon")]
            {
                use tauri::menu::{Menu, MenuItem};
                use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};

                #[cfg(target_os = "linux")]
                let tray_supported = {
                    // Tauri's Linux tray uses libappindicator via dlopen, which
                    // panics when the library is missing (e.g. inside Flatpak,
                    // where the GNOME runtime does not ship it).
                    ["libayatana-appindicator3.so.1", "libappindicator3.so.1"]
                        .iter()
                        .any(|name| unsafe { libloading::Library::new(name).is_ok() })
                };
                #[cfg(not(target_os = "linux"))]
                let tray_supported = true;

                if tray_supported {
                    let show = MenuItem::with_id(app, "show", "Show Window", true, None::<&str>)?;
                    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
                    let menu = Menu::with_items(app, &[&show, &quit])?;

                    let mut tray_builder = TrayIconBuilder::new()
                        .menu(&menu)
                        .on_menu_event(|app, event| match event.id.as_ref() {
                            "show" => {
                                if let Some(window) = app.get_webview_window("main") {
                                    let _ = window.show();
                                    let _ = window.set_focus();
                                }
                            }
                            "quit" => app.exit(0),
                            _ => {}
                        })
                        .on_tray_icon_event(|tray, event| {
                            if let TrayIconEvent::Click {
                                button: MouseButton::Left,
                                button_state: MouseButtonState::Up,
                                ..
                            } = event
                            {
                                let app = tray.app_handle();
                                if let Some(window) = app.get_webview_window("main") {
                                    let _ = window.show();
                                    let _ = window.set_focus();
                                }
                            }
                        });

                    if let Some(icon) = app.default_window_icon() {
                        tray_builder = tray_builder.icon(icon.clone());
                    }

                    let _tray = tray_builder.build(app)?;
                } else {
                    eprintln!("System tray support is unavailable; continuing without a tray icon");
                }
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::sources::list_sources,
            commands::sources::add_source,
            commands::sources::remove_source,
            commands::sources::enable_source,
            commands::sources::disable_source,
            commands::relays::list_relays,
            commands::relays::add_relay,
            commands::relays::remove_relay,
            commands::relays::enable_relay,
            commands::relays::disable_relay,
            commands::queue::list_videos,
            commands::queue::retry_failed,
            commands::queue::get_queue_counts,
            commands::metrics::get_metrics,
            commands::settings::get_settings,
            commands::settings::update_settings,
            commands::nsec::get_nsec_status,
            commands::nsec::set_nsec,
            commands::nsec::delete_nsec,
            commands::import_nip65::import_nip65_relays,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
