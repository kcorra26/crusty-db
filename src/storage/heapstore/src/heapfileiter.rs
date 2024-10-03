use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    heapfile: Arc<HeapFile>,
    tid: TransactionId,
    cur_iter: Option<HeapPageIntoIter>,
    cur_pageid: PageId,
    cur_slotid: SlotId,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        Self {
            heapfile: hf,
            tid,
            cur_iter: None,
            cur_pageid: 0,
            cur_slotid: 0,
        }
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        Self {
            heapfile: hf,
            tid,
            cur_iter: None,
            cur_pageid: value_id.page_id.unwrap(),
            cur_slotid: value_id.slot_id.unwrap(),
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pageid >= self.heapfile.num_pages() {
            return None;
        }

        let page = self.heapfile.read_page_from_file(self.cur_pageid).unwrap();
        let clone = page.clone();

        // if it's a new page or a first call, check if new_from or new
        if self.cur_slotid == 0 {
            self.cur_iter = Some(page.into_iter());
        } else if self.cur_iter.is_none() {
            self.cur_iter = Some(page.new_iter(self.cur_slotid));
        }
        // open the iterator and evaluate next()
        if let Some(ref mut iter) = self.cur_iter {
            let potential = iter.next();
            if potential.is_none() {
                self.cur_pageid += 1;
                self.cur_slotid = 0;
                return self.next();
            }
            let result = potential.unwrap();
            // get information for ValueId struct
            let vec = result.0;
            let slotid = result.1;
            let our_val = ValueId {
                container_id: self.heapfile.container_id,
                segment_id: None,
                page_id: Some(self.cur_pageid),
                slot_id: Some(self.cur_slotid),
            };
            let myitem: Self::Item = (vec, our_val);

            if self.cur_slotid + 1 == clone.get_num_slots() {
                self.cur_pageid += 1;
                self.cur_slotid = 0;
            } else {
                self.cur_slotid += 1;
            }
            Some(myitem)
        } else {
            None
        }
    }
}
