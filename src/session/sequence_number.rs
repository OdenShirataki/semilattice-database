use std::path::PathBuf;

use file_mmap::FileMmap;

pub struct SequenceNumber {
    filemmap: FileMmap,
}
impl SequenceNumber {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(std::mem::size_of::<usize>() as u64)?;
        }
        Ok(Self { filemmap })
    }
    pub fn next(&mut self) -> usize {
        let sequence_number = self.filemmap.as_ptr() as *mut usize;
        unsafe {
            *sequence_number += 1;
            *sequence_number
        }
    }
}
