use crate::structs;

use structs::directory::*;

use firestore::paths;
use firestore::*;


pub const MASTER_DIRECTORY_COLLECTION_NAME: &'static str = "master_directory";

pub async fn add_to_database(db: FirestoreDb, name: String, part: i32, attachment_url: String) {
    let master_directory_child: MasterDirectoryChildPart = MasterDirectoryChildPart {
        name: name.clone(),
        id: format!("{}.part{}", &name, part),
        part: part,
        parent: name,
        url: attachment_url,
    };

    let mut transaction = db.begin_transaction().await.unwrap();

    let _added = db
        .fluent()
        .update()
        .fields(paths!(MasterDirectoryChild::parts))
        .in_col(MASTER_DIRECTORY_COLLECTION_NAME)
        .document_id(&master_directory_child.name)
        .transforms(|transform_builder| {
            vec![transform_builder
                .field(path!(MasterDirectoryChild::parts))
                .append_missing_elements([&master_directory_child])
                .unwrap()]
        })
        .only_transform()
        .add_to_transaction(&mut transaction)
        .unwrap();

    transaction.commit().await.unwrap();

    println!("Added to database");
}
