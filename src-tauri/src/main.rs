#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    peertube2nostr_lib::run()
}
