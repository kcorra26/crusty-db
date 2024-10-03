use crate::heap_page::HeapPage;
use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::{fs, num};

pub const STORAGE_DIR: &str = "heapstore";

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_dir: PathBuf,
    is_temp: bool,
    container_to_hf: Arc<RwLock<HashMap<ContainerId, PathBuf>>>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let container_to_hf = self.container_to_hf.write().unwrap();
        if container_to_hf.contains_key(&container_id) {
            let hf_name = container_to_hf.get(&container_id).unwrap();
            let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id);
            match heapfile {
                Ok(hf) => {
                    if hf.num_pages() > page_id {
                        let result = hf.read_page_from_file(page_id);
                        match result {
                            Ok(pg) => {
                                return Some(pg);
                            }
                            Err(e) => {
                                return None;
                            }
                        }
                    }
                }
                Err(e) => {
                    return None;
                }
            }
        }
        None
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: &Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        // pull the heapfile associated with the containerID
        let container_to_hf = self.container_to_hf.write().unwrap();
        if container_to_hf.contains_key(&container_id) {
            let hf_name = container_to_hf.get(&container_id).unwrap();
            let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id)?;
            // write the page to the heapfile
            match heapfile.write_page_to_file(page) {
                Ok(()) => Ok(()),
                Err(e) => Err(CrustyError::CrustyError(String::from(
                    "Could not write page to file",
                ))),
            }
        } else {
            Err(CrustyError::CrustyError(String::from(
                "Container id does not exist",
            )))
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();
        heapfile.num_pages()
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();
        (
            heapfile.read_count.load(Ordering::Relaxed),
            heapfile.write_count.load(Ordering::Relaxed),
        )
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }
}
// TODO:
// clean up code: comments, print statements, etc. (in this file and heapfile)
// run cargo fmt --check and cargo clippy
// run tests in gradescope again just to make sure

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_dir as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_dir for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_dir: &Path) -> Self {
        let mut container_to_hf = Arc::new(RwLock::new(HashMap::new()));
        if !storage_dir.exists() {
            // let display_path = storage_dir.display();
            // print!("path: {display_path}");
            fs::create_dir_all(storage_dir).unwrap();
        } else {
            let filename = "container_to_hf.json";
            let path = storage_dir.join(filename);
            if path.exists() {
                let file2 = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
                let buffer = std::io::BufReader::new(file2);

                let hm: HashMap<u16, std::borrow::Cow<'_, str>> =
                    serde_json::from_reader(buffer).unwrap();
                let serialized_hm: HashMap<ContainerId, PathBuf> = hm
                    .iter()
                    .map(|(k, v)| (*k, PathBuf::from(v.to_string()).to_path_buf()))
                    .collect();
                container_to_hf = Arc::new(RwLock::new(serialized_hm));
            }
        }
        Self {
            storage_dir: storage_dir.to_path_buf(),
            container_to_hf,
            is_temp: false,
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_dir = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_dir);
        if !storage_dir.exists() {
            fs::create_dir_all(&storage_dir).unwrap();
        }
        Self {
            storage_dir,
            container_to_hf: Arc::new(RwLock::new(HashMap::new())),
            is_temp: true,
        }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        // get heapfile associated with the containerId
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();

        let tot_pages = heapfile.num_pages();
        let mut working_pid = 0;
        let mut potential_add;
        if tot_pages == 0 {
            let mut new_page = Page::new(0);
            potential_add = new_page.add_value(&value);
            heapfile.write_page_to_file(&new_page).unwrap();
        } else {
            let mut page = heapfile.read_page_from_file(working_pid).unwrap();
            potential_add = page.add_value(&value);

            // iterate through the pages to find one that has space
            while potential_add.is_none() && working_pid < tot_pages {
                working_pid += 1;
                match heapfile.read_page_from_file(working_pid) {
                    Ok(p) => {
                        page = p;
                        potential_add = page.add_value(&value);
                    }
                    Err(e) => {
                        break;
                    }
                }
            }
            // if there are no pages with space, create a new page and add the value
            if working_pid == tot_pages {
                let mut new_page = Page::new(working_pid);
                potential_add = new_page.add_value(&value);
                heapfile.write_page_to_file(&new_page).unwrap();
            } else {
                heapfile.write_page_to_file(&page).unwrap();
            }
        }

        // return the valueId
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(working_pid),
            slot_id: potential_add,
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        // find container that holds valueid
        let container_id = id.container_id;

        // find heapfile associated with that container
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();

        let page_id = id.page_id;
        let slot_id = id.slot_id;
        if page_id.is_none() || slot_id.is_none() {
            return Ok(());
        }

        let mut page = heapfile.read_page_from_file(page_id.unwrap())?;
        page.delete_value(slot_id.unwrap());
        heapfile.write_page_to_file(&page)?;
        Ok(())
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        self.delete_value(id, _tid)?;
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        let s1 = "heapfile";
        let name = s1.to_string() + &container_id.to_string();

        let init_path = self.storage_dir.to_path_buf();
        let path = self.storage_dir.join(name);

        match HeapFile::new(path.clone(), container_id) {
            Ok(hf) => {
                let mut container_to_hf = self.container_to_hf.write().unwrap();
                container_to_hf.insert(container_id, path);
                Ok(())
            }
            Err(e) => Err(CrustyError::CrustyError(String::from(
                "Could not create new container",
            ))),
        }
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        // find the path associated with the container/heapfile, delete it
        let mut container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        if hf_name.to_path_buf().exists() {
            fs::remove_file(hf_name)?;
        }
        container_to_hf.remove(&container_id);

        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();

        HeapFileIterator::new(tid, Arc::new(heapfile))
        //iter
    }

    fn get_iterator_from(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
        start: ValueId,
    ) -> Self::ValIterator {
        // call heapfile iterator using new_from
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();

        HeapFileIterator::new_from(tid, Arc::new(heapfile), start)
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let container_id = id.container_id;

        // find heapfile associated with that container
        let container_to_hf = self.container_to_hf.write().unwrap();
        let hf_name = container_to_hf.get(&container_id).unwrap();
        let heapfile = HeapFile::new(hf_name.to_path_buf(), container_id).unwrap();

        let page_id = id.page_id;
        let slot_id = id.slot_id;
        if page_id.is_none() || slot_id.is_none() {
            return Err(CrustyError::CrustyError(String::from(
                "ValueId does not exist",
            )));
        }

        let page = heapfile.read_page_from_file(page_id.unwrap())?;
        match page.get_value(slot_id.unwrap()) {
            Some(vec) => Ok(vec),
            None => Err(CrustyError::CrustyError(String::from(
                "Could not find value at given location",
            ))),
        }
    }

    fn get_storage_path(&self) -> &Path {
        &self.storage_dir
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_dir.clone())?;
        fs::create_dir_all(self.storage_dir.clone()).unwrap();

        let mut container_to_hf = self.container_to_hf.write().unwrap();
        container_to_hf.clear();
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        // THIS IS ME JUST TRYING SOMETHING
        // let storage_dir = self.storage_dir.as_path();
        // fs::create_dir_all(storage_dir).expect("Unable to create dir to store SM");

        let container_to_hf = self.container_to_hf.read().unwrap();

        let serialized_hm: HashMap<u16, std::borrow::Cow<'_, str>> = container_to_hf
            .iter()
            .map(|(k, v)| (*k, v.to_string_lossy()))
            .collect();

        let filename = "container_to_hf.json";
        let path = self.storage_dir.join(filename);
        let file2 = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();

        serde_json::to_writer(file2, &serialized_hm).expect("Failed on persisting container");
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_dir);
            let remove_all = fs::remove_dir_all(self.storage_dir.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();
        let val1 = sm.insert_value(cid, bytes.clone(), tid);

        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);

        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);

        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }

    #[test]
    fn hs_sm_b_iterator_from_large() {
        // BORROWED FROM ED
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals.clone(), tid);
        let mut count = 0;

        let mut start = ValueId::new(cid);
        start.page_id = Some(0);
        start.slot_id = Some(5);
        for _ in sm.get_iterator_from(cid, tid, Permissions::ReadOnly, start) {
            count += 1;
        }
        assert_eq!(995, count);
    }
}
