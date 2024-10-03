use crate::page;
use crate::page::{Offset, Page};
use common::prelude::*;
use common::PAGE_SIZE;
use std::fmt;
use std::fmt::Write;

// Add any other constants, type aliases, or structs, or definitions here
const META_HEADER_SIZE: usize = 8; // size of the page metadata
const SLOT_META_SIZE: usize = 6; // size of the slot metadata (per slot)
const PAGEID_SIZE: usize = std::mem::size_of::<PageId>(); // size of PageId type
const OFFSET_SIZE: usize = std::mem::size_of::<Offset>(); // size of Offset type
const SLOTID_SIZE: usize = std::mem::size_of::<SlotId>(); // size of SlotId type

// locations of the header values (to allow for easy access)
const PAGEID_LOC: usize = 0;
const NUMSLOTS_LOC: usize = PAGEID_LOC + PAGEID_SIZE;
const FIRSTOFFSET_LOC: usize = NUMSLOTS_LOC + std::mem::size_of::<u16>();
const TOTSLOTS_LOC: usize = FIRSTOFFSET_LOC + OFFSET_SIZE;
const SLOTSTART_LOC: usize = TOTSLOTS_LOC + SLOTID_SIZE;

pub trait HeapPage {
    // Do not change these functions signatures (only the function bodies)
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId>;
    fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>>;
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()>;
    fn get_header_size(&self) -> usize;
    fn get_free_space(&self) -> usize;

    //Add function signatures for any helper function you need here

    // Deletion and addition utility functions:
    fn compact(&mut self, del_offset: Offset, del_meta_loc: usize);
    fn get_next_slotid(&self) -> SlotId;

    // Header metadata utility functions:
    fn get_num_slots(&self) -> u16;
    fn update_num_slots(&mut self, num_slots: u16);
    fn get_first_offset(&self) -> Offset;
    fn update_first_offset(&mut self, new_first_offset: Offset);
    fn get_total_slot_headers(&self) -> u16;
    fn update_total_slot_headers(&mut self, new_num: u16);

    // Slot metadata utility functions:
    fn get_slot_meta_loc(&self, slot: SlotId) -> Option<usize>;
    fn get_slot_size(&self, slot: SlotId, slotloc: usize) -> u16;
    fn update_slot_size(&mut self, slot: SlotId, slotsize: u16, slotloc: usize);
    fn get_slot_offset(&self, slot: SlotId, slocloc: usize) -> Offset;
    fn update_slot_offset(&mut self, slot: SlotId, new_offset: Offset, slotloc: usize);

    fn new_iter(self, slot: SlotId) -> HeapPageIntoIter;
}

impl HeapPage for Page {
    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        let entry_size = bytes.len() as Offset;

        if self.get_free_space() >= (entry_size as usize + SLOT_META_SIZE) {
            let slot_id: SlotId = self.get_next_slotid();
            let end_at = self.get_first_offset();

            // Header updates
            self.update_num_slots(self.get_num_slots() + 1);
            self.update_first_offset(end_at - entry_size as Offset);

            // Slot metadata updates
            let slotmetaloc =
                META_HEADER_SIZE + (SLOT_META_SIZE * self.get_total_slot_headers() as usize);

            self.update_total_slot_headers(self.get_total_slot_headers() + 1);
            let slot_endid: usize = slotmetaloc + SLOTID_SIZE;
            let slotid_bytes = slot_id.to_le_bytes();
            self.data[slotmetaloc..slot_endid].clone_from_slice(&slotid_bytes);

            // Inserting data
            self.data[(end_at as usize - entry_size as usize)..(end_at as usize)]
                .clone_from_slice(bytes);

            self.update_slot_offset(slot_id, end_at, slotmetaloc);
            self.update_slot_size(slot_id, entry_size, slotmetaloc);

            return Some(slot_id);
        }
        None
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        match self.get_slot_meta_loc(slot_id) {
            Some(slotmetaloc) => {
                let slot_size = self.get_slot_size(slot_id, slotmetaloc) as usize;
                let offset = self.get_slot_offset(slot_id, slotmetaloc) as usize;
                Some(self.data[(offset - slot_size)..offset].to_vec())
            }
            None => None,
        }
    }

    // Eager compaction function that is called at every deletion. Shifts the
    // slots with offsets smaller than the deleted offset to make room for
    // new data.
    fn compact(&mut self, del_offset: Offset, del_meta_loc: usize) {
        let mut cur_loc = del_meta_loc + SLOT_META_SIZE;
        let total_slot_space =
            META_HEADER_SIZE + (self.get_total_slot_headers() as usize * SLOT_META_SIZE);
        let mut new_starting_idx = del_offset;
        let mut new_offset = del_offset;

        while cur_loc < total_slot_space {
            let slot_id = SlotId::from_le_bytes(
                self.data[cur_loc..cur_loc + SLOTID_SIZE]
                    .try_into()
                    .unwrap(),
            );
            let slots_offset = self.get_slot_offset(slot_id, cur_loc) as usize;
            if slots_offset != 0 {
                // if the slot id is valid
                let slots_size = self.get_slot_size(slot_id, cur_loc) as usize;
                // move the data

                new_starting_idx = new_offset - slots_size as Offset;
                self.data.copy_within(
                    (slots_offset - slots_size)..slots_offset,
                    new_starting_idx as usize,
                );

                // update the slot metadata
                self.update_slot_offset(slot_id, new_offset, cur_loc);
            }
            cur_loc += SLOT_META_SIZE;
            new_offset = new_starting_idx;
        }
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        match self.get_slot_meta_loc(slot_id) {
            Some(slotmetaloc) => {
                let slot_size = self.get_slot_size(slot_id, slotmetaloc) as usize;
                let offset = self.get_slot_offset(slot_id, slotmetaloc) as usize;
                self.data[(offset - slot_size)..offset].fill(0); // zeroing out the data

                self.compact(offset as Offset, slotmetaloc);
                // zeroing the values at the slot header
                self.data[slotmetaloc..slotmetaloc + SLOTID_SIZE].fill(0);
                self.update_slot_size(slot_id, 0, slotmetaloc);
                self.update_slot_offset(slot_id, 0, slotmetaloc);

                self.update_num_slots(self.get_num_slots() - 1);

                self.update_first_offset(self.get_first_offset() + slot_size as Offset);

                Some(())
            }
            None => None,
        }
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests.
    #[allow(dead_code)]
    fn get_header_size(&self) -> usize {
        META_HEADER_SIZE + (SLOT_META_SIZE * (self.get_total_slot_headers() as usize))
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests.
    #[allow(dead_code)]
    fn get_free_space(&self) -> usize {
        let first = self.get_first_offset() as usize;
        let headersize = self.get_header_size();
        if first < headersize {
            return 0;
        }
        first - headersize
    }

    // A utility function that returns the number of slots in use on the page.
    fn get_num_slots(&self) -> u16 {
        u16::from_le_bytes(
            self.data[NUMSLOTS_LOC..(FIRSTOFFSET_LOC)]
                .try_into()
                .unwrap(),
        )
    }

    // A utility function to update the number of slots on the page.
    fn update_num_slots(&mut self, num_slots: u16) {
        let up_num_bytes = num_slots.to_le_bytes();
        self.data[NUMSLOTS_LOC..FIRSTOFFSET_LOC].copy_from_slice(&up_num_bytes);
    }

    // A utility function that returns the offset of the first free space
    // on the page.
    fn get_first_offset(&self) -> Offset {
        let cur_offset =
            Offset::from_le_bytes(self.data[FIRSTOFFSET_LOC..TOTSLOTS_LOC].try_into().unwrap());
        if cur_offset == 0 {
            return PAGE_SIZE as Offset;
        }
        cur_offset
    }

    // A utility function to update the first offset on the page.
    fn update_first_offset(&mut self, new_first_offset: Offset) {
        let new_in_bytes = new_first_offset.to_le_bytes();
        self.data[FIRSTOFFSET_LOC..TOTSLOTS_LOC].copy_from_slice(&new_in_bytes);
    }

    // A utility function to get the total number of slot headers, regardless
    // of whether they are currently in use (helpful for determining where the
    // boundary is between header end and free space).
    fn get_total_slot_headers(&self) -> u16 {
        u16::from_le_bytes(self.data[TOTSLOTS_LOC..SLOTSTART_LOC].try_into().unwrap())
    }

    // A utility function to update the total number of slot headers (empty
    // and in use).
    fn update_total_slot_headers(&mut self, new_num: u16) {
        let new_num_bytes = new_num.to_le_bytes();
        self.data[TOTSLOTS_LOC..SLOTSTART_LOC].copy_from_slice(&new_num_bytes);
    }

    // Returns the smallest available SlotId.
    fn get_next_slotid(&self) -> SlotId {
        let num_slotids = self.get_num_slots();
        let total_slot_headers = self.get_total_slot_headers();

        if total_slot_headers == num_slotids {
            // if all the slotids from 0 to num are in use
            return num_slotids as SlotId;
        }

        let mut cur_loc = META_HEADER_SIZE;
        let total_space = cur_loc + (total_slot_headers as usize * SLOT_META_SIZE);

        let mut slot_vec = Vec::new();

        while cur_loc < total_space {
            let cur_slotid = SlotId::from_le_bytes(
                self.data[cur_loc..cur_loc + SLOTID_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if cur_slotid == 0 && self.get_slot_offset(cur_slotid, cur_loc) == 0 {
                cur_loc += SLOT_META_SIZE;
                continue;
            }
            slot_vec.push(cur_slotid);
            cur_loc += SLOT_META_SIZE;
        }
        slot_vec.sort();

        let mut min_missing = num_slotids;
        let mut exp_id = 0;

        for id in slot_vec {
            if id != exp_id {
                min_missing = exp_id;
                break;
            }
            exp_id = id + 1;
        }
        min_missing as SlotId
    }

    // A utility function that returns the location of the metadata for
    // the given slot.
    fn get_slot_meta_loc(&self, slot: SlotId) -> Option<usize> {
        let mut cur_loc = META_HEADER_SIZE;
        let total_space = cur_loc + (self.get_total_slot_headers() as usize * SLOT_META_SIZE);

        while cur_loc < total_space {
            let cur_slotid = SlotId::from_le_bytes(
                self.data[cur_loc..cur_loc + SLOTID_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if cur_slotid == slot {
                if cur_slotid == 0 && self.get_slot_offset(cur_slotid, cur_loc) == 0 {
                    cur_loc += SLOT_META_SIZE;
                    continue;
                }

                return Some(cur_loc);
            }
            cur_loc += SLOT_META_SIZE;
        }
        None
    }

    // Returns the size of the given slot.
    fn get_slot_size(&self, _slot: SlotId, loc: usize) -> u16 {
        let loc_slot_size = loc + SLOTID_SIZE;
        let loc_endof_slot_size = loc_slot_size + OFFSET_SIZE;

        u16::from_le_bytes(
            self.data[loc_slot_size..loc_endof_slot_size]
                .try_into()
                .unwrap(),
        )
    }

    // Updates the size of the given slot.
    fn update_slot_size(&mut self, _slot: SlotId, slotsize: u16, loc: usize) {
        let loc_slot_size = loc + SLOTID_SIZE;
        let loc_endof_slot_size = loc_slot_size + OFFSET_SIZE;

        let size_bytes = slotsize.to_le_bytes();
        self.data[loc_slot_size..loc_endof_slot_size].copy_from_slice(&size_bytes);
    }

    // Returns the offset of the given slot (where its offset is the location
    // of the slot's last byte).
    fn get_slot_offset(&self, _slot: SlotId, loc: usize) -> Offset {
        let loc_slot_offset = loc + SLOTID_SIZE + OFFSET_SIZE;
        let loc_endof_slot_offset = loc_slot_offset + OFFSET_SIZE;
        Offset::from_le_bytes(
            self.data[loc_slot_offset..loc_endof_slot_offset]
                .try_into()
                .unwrap(),
        )
    }

    // Updates the offset of the given slot in the slot metadata.
    fn update_slot_offset(&mut self, _slot: SlotId, new_offset: Offset, loc: usize) {
        let loc_slot_offset = loc + SLOTID_SIZE + OFFSET_SIZE;
        let loc_endof_slot_offset = loc_slot_offset + OFFSET_SIZE;
        let offset_bytes = new_offset.to_le_bytes();
        self.data[loc_slot_offset..loc_endof_slot_offset].copy_from_slice(&offset_bytes);
    }

    fn new_iter(self, slot: SlotId) -> HeapPageIntoIter {
        HeapPageIntoIter {
            page: self,
            next_slot: slot,
        }
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
pub struct HeapPageIntoIter {
    page: Page,
    next_slot: SlotId,
}

/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for HeapPageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot >= self.page.get_total_slot_headers() {
            return None;
        }

        match self.page.get_value(self.next_slot) {
            Some(vec) => {
                let myitem: Self::Item = (vec, self.next_slot);
                self.next_slot += 1;
                Some(myitem)
            }
            None => {
                self.next_slot += 1;
                self.next()
            }
        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = HeapPageIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        // creates the iterator from a page
        Self::IntoIter {
            page: self,
            next_slot: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_sizes_header_free_space() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());

        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn hs_page_debug_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(5);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);

        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);

        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        dbg!(p.clone());
        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let mut p2 = p.clone();
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.to_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_serialization_is_deterministic() {
        init();

        // Create a page and serialize it
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        // Reconstruct the page
        let p1 = Page::from_bytes(*p0_bytes);
        let p1_bytes = p1.to_bytes();

        // Enforce that the two pages serialize determinestically
        assert_eq!(p0_bytes, p1_bytes);
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = *p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = *p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let mut p2 = p.clone();
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let mut p3 = p2.clone();
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let p4 = p3.clone();
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();

        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }

        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let p_clone = p.clone();
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);

                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let p_clone = p.clone();

                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }
}
