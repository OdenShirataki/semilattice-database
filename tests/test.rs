#[cfg(test)]
use versatile_data::prelude::*;
use semilattice_database::Database;

#[test]
fn it_works() {
    let dir="D:/sl-test/";

    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    }else{
        std::fs::create_dir_all(dir).unwrap();
    }
    let mut database=Database::new(dir);
    if let Some(testd)=database.create_collection("test"){
        let range=1..=10;
        for i in range.clone(){
            if let Some(row)=testd.insert(Activity::Active,0,0){
                testd.update_field(row,"num",i.to_string());
                testd.update_field(row,"num_by3",(i*3).to_string());
            }
        }
        testd.update(3,Activity::Inactive,0,0);
        testd.load_fields();
        let mut sam=0.0;
        for i in range.clone(){
            sam+=testd.field_num(i,"num");
            println!(
                "{},{},{},{},{},{},{},{}"
                ,testd.serial(i)
                ,if testd.activity(i)==Activity::Active{
                    "Active"
                }else{
                    "Inactive"
                }
                ,testd.uuid_str(i)
                ,testd.last_updated(i)
                ,testd.term_begin(i)
                ,testd.term_end(i)
                ,testd.field_str(i,"num")
                ,testd.field_str(i,"num_by3")
            );
        }
        assert_eq!(sam,55.0);

        let r=testd
            .search(&Condition::Field("num".to_string(),Field::Range(b"3".to_vec(),b"8".to_vec())))
            .search_default()   //Automatic execution of the following two lines
            //.search(SearchCondition::Term(Term::In(chrono::Local::now().timestamp())))
            //.search(SearchCondition::Activity(Activity::Active))
            .result()
        ;
        println!("{:?}",r);
    }
}
