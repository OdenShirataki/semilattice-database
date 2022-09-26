use versatile_data::Data;

pub struct Collection{
    id:u32
    ,data:Data
}
impl Collection{
    pub fn new(id:u32,data:Data)->Collection{
        Collection{
            id
            ,data
        }
    }
    pub fn data(&self)->&Data{
        &self.data
    }
    pub fn data_mut(&mut self)->&mut Data{
        &mut self.data
    }
}