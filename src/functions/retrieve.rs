use firestore::FirestoreDb;
use futures::future::join_all;
use std::fs::File;
use std::io::BufWriter;

use std::{
    io::BufReader,
    sync::{Arc},
    vec,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use crate::structs::directory::MasterDirectoryChild;

use super::database::MASTER_DIRECTORY_COLLECTION_NAME;

fn merge_files(files: Vec<String>, name: &str) {
    let output_file = File::create(name).unwrap();
    // 500MB capacity
    let mut writer = BufWriter::with_capacity(1024 * 512 * 500, output_file);

    for file in files {
        let input_file = File::open(format!("chunks/{}", file)).unwrap();
        let mut reader = BufReader::new(input_file);

        // Copy the contents of the input file to the output file
        std::io::copy(&mut reader, &mut writer).unwrap();

        // Delete the input file
        std::fs::remove_file(format!("chunks/{}", file)).unwrap();
    }

    println!("Successfully Merged files!");
}

pub async fn retrieve_and_save(db: FirestoreDb, name: &str) {
    let semaphore = Arc::new(Semaphore::new(24));

    let master_directory: MasterDirectoryChild = db
        .fluent()
        .select()
        .by_id_in(MASTER_DIRECTORY_COLLECTION_NAME)
        .obj()
        .one(name)
        .await
        .unwrap()
        .unwrap();

    // add all the part IDs to a vector and sort ascending
    let mut part_ids_children = master_directory.parts.to_vec();
    part_ids_children.sort_by(|a, b| a.part.cmp(&b.part));
    let part_ids = part_ids_children
        .iter()
        .map(|part| part.id.clone())
        .collect::<Vec<String>>();

    let mut handles = vec![];

    for part in master_directory.parts {
        let semaphore = semaphore.clone();
        let handle = tokio::spawn(async move {
            let permit = semaphore.acquire().await.unwrap();
            let response = reqwest::get(&part.url)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let mut file = tokio::fs::File::create(format!("chunks/{}", part.id))
                .await
                .unwrap();

            file.write_all(&response)
                .await
                .expect("Couldn't write to file");
            file.flush().await.unwrap();
            drop(permit);
            drop(response);
        });
        handles.push(handle);
    }

    join_all(handles).await;

    merge_files(part_ids, name);
}
