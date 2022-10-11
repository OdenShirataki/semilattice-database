use file_mmap::FileMmap;

pub struct SequenceNumber{
    #[allow(dead_code)]
    filemmap:FileMmap
    ,sequence_number: Vec<usize>
}
impl SequenceNumber{
    pub fn new(path:&str)->Result<SequenceNumber,std::io::Error>{
        let filemmap=FileMmap::new(path,std::mem::size_of::<usize>() as u64)?;
        let ptr=filemmap.as_ptr() as *mut usize;
        Ok(SequenceNumber{
            filemmap
            ,sequence_number:unsafe {Vec::from_raw_parts(ptr,1,0)}
        })
    }
    pub fn next(&mut self)->usize{
        self.sequence_number[0]+=1;
        self.sequence_number[0]
    }
}