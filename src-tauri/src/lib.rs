mod storage;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(storage::activity::ActivityDebugState::default())
        .manage(storage::SyncState::default())
        .invoke_handler(tauri::generate_handler![
            storage::activity::get_activity_debug_log_state,
            storage::activity::open_activity_debug_log_folder,
            storage::commands::validate_storage_connection,
            storage::commands::list_provider_capabilities_command,
            storage::commands::list_credentials_command,
            storage::commands::create_credential_command,
            storage::commands::test_credential_command,
            storage::commands::delete_credential_command,
            storage::commands::load_profile,
            storage::commands::save_profile,
            storage::commands::save_profile_settings,
            storage::commands::get_sync_status,
            storage::commands::start_sync,
            storage::commands::pause_sync,
            storage::commands::list_sync_locations,
            storage::location_service::add_sync_location,
            storage::location_service::update_sync_location,
            storage::location_service::set_sync_location_versioning,
            storage::location_service::remove_sync_location,
            storage::commands::list_file_entries,
            storage::commands::list_bin_entries,
            storage::commands::list_file_versions,
            storage::commands::list_version_counts,
            storage::commands::restore_file_version,
            storage::commands::prepare_version_comparison,
            storage::commands::delete_file_version,
            storage::commands::restore_bin_entry,
            storage::commands::restore_bin_entries,
            storage::commands::purge_bin_entries,
            storage::commands::reveal_tree_entry,
            storage::commands::prepare_conflict_comparison,
            storage::commands::open_path,
            storage::commands::resolve_conflict,
            storage::commands::toggle_local_copy,
            storage::commands::delete_file,
            storage::commands::delete_folder,
            storage::commands::change_storage_class,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
