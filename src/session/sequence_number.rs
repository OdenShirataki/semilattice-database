use file_mmap::FileMmap;

pub struct SequenceNumber {
    #[allow(dead_code)]
    filemmap: FileMmap,
    sequence_number: Vec<usize>,
}
impl SequenceNumber {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(std::mem::size_of::<usize>() as u64)?;
        }
        let ptr = filemmap.as_ptr() as *mut usize;
        Ok(Self {
            filemmap,
            sequence_number: unsafe { Vec::from_raw_parts(ptr, 1, 0) },
        })
    }
    pub fn next(&mut self) -> usize {
        self.sequence_number[0] += 1;
        self.sequence_number[0]
    }
}
