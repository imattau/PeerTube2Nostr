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

            #[cfg(feature = "tray-icon")]
            {
                use tauri::menu::{Menu, MenuItem};
                use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};

                let show = MenuItem::with_id(app, "show", "Show Window", true, None::<&str>)?;
                let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
                let menu = Menu::with_items(app, &[&show, &quit])?;

                let _tray = TrayIconBuilder::new()
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
                    })
                    .build(app)?;
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
            commands::metrics::get_metrics,
            commands::settings::get_settings,
            commands::settings::update_settings,
            commands::nsec::get_nsec_status,
            commands::nsec::set_nsec,
            commands::nsec::delete_nsec,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
