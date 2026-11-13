#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("NO_AT_BRIDGE").is_none() {
            std::env::set_var("NO_AT_BRIDGE", "1");
        }
        if std::env::var_os("WEBKIT_A11Y_BUS_ADDRESS").is_none() {
            std::env::set_var("WEBKIT_A11Y_BUS_ADDRESS", "");
        }
    }
    peertube2nostr_lib::run()
}
