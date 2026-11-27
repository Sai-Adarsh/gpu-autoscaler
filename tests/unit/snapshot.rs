use super::{File, Store};

pub(crate) fn replace_directory_sync_handle(store: &mut Store, replacement: File) {
    store.dir = replacement;
}
