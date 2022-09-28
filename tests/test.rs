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
    
    let range=1..=10;
    if let Some(mut t1)=database.transaction("test1"){
        for i in range.clone(){
            t1.insert(Activity::Active,0,0,vec![
                ("num".to_string(),i.to_string())
                ,("num_by3".to_string(),(i*3).to_string())
            ]);
        }
        t1.update(3,Activity::Inactive,0,0,vec![]);
        t1.commit();
    }
    if let Some(t1)=database.collection("test1"){
        let t1=t1.data();
        let mut sum=0.0;
        for i in range.clone(){
            sum+=t1.field_num(i,"num");
            println!(
                "{},{},{},{},{},{},{},{}"
                ,t1.serial(i)
                ,if t1.activity(i)==Activity::Active{
                    "Active"
                }else{
                    "Inactive"
                }
                ,t1.uuid_str(i)
                ,t1.last_updated(i)
                ,t1.term_begin(i)
                ,t1.term_end(i)
                ,t1.field_str(i,"num")
                ,t1.field_str(i,"num_by3")
            );
        }
        assert_eq!(sum,55.0);

        let r=t1
            .search(&Condition::Field("num".to_string(),Field::Range(b"3".to_vec(),b"8".to_vec())))
            .search_default()   //Automatic execution of the following two lines
            //.search(SearchCondition::Term(Term::In(chrono::Local::now().timestamp())))
            //.search(SearchCondition::Activity(Activity::Active))
            .result()
        ;
        println!("{:?}",r);
    }
}
