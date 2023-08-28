use std::path::PathBuf;

use crate::FileMmap;

pub struct SequenceNumber {
    filemmap: FileMmap,
}
impl SequenceNumber {
    pub fn new(path: PathBuf) -> Self {
        let mut filemmap = FileMmap::new(path).unwrap();
        let init_size = std::mem::size_of::<usize>() as u64 * 2;
        if filemmap.len() == 0 {
            filemmap.set_len(init_size).unwrap();
        }
        Self { filemmap }
    }
    pub fn next(&mut self) -> usize {
        let current = self.filemmap.as_ptr() as *mut usize;
        let max = unsafe { current.offset(1) };
        unsafe {
            *current += 1;
            *max = *current;
            *current
        }
    }
    pub fn current(&self) -> usize {
        unsafe { *(self.filemmap.as_ptr() as *mut usize) }
    }
    pub fn set_current(&mut self, current: usize) {
        let max = self.max();
        unsafe {
            *(self.filemmap.as_ptr() as *mut usize) = if max < current { max } else { current };
        }
    }
    pub fn max(&self) -> usize {
        unsafe { *(self.filemmap.as_ptr() as *mut usize).offset(1) }
    }
}

pub struct SequenceCursor {
    pub max: usize,
    pub current: usize,
}
