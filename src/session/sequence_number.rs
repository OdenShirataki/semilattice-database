use std::path::PathBuf;

use file_mmap::FileMmap;

pub struct SequenceNumber {
    filemmap: FileMmap,
}
impl SequenceNumber {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        let init_size = std::mem::size_of::<usize>() as u64 * 2;
        if filemmap.len()? == 0 {
            filemmap.set_len(init_size)?;
        }
        Ok(Self { filemmap })
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
            if max < current {
                *(self.filemmap.as_ptr() as *mut usize) = max;
            } else {
                *(self.filemmap.as_ptr() as *mut usize) = current;
            }
        }
    }
    pub fn max(&self) -> usize {
        unsafe { *(self.filemmap.as_ptr() as *mut usize).offset(1) }
    }
}
