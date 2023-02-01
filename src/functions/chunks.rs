use std::{
    borrow::Cow,
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

use firestore::FirestoreDb;
use futures::future::join_all;
use tokio::sync::Semaphore;

use crate::{
    functions::database::{add_to_database, MASTER_DIRECTORY_COLLECTION_NAME},
    structs::{directory::MasterDirectoryChild, drive_config::WebhookData},
};

async fn upload_chunk(
    webhook_data: WebhookData,
    db: FirestoreDb,
    index: i32,
    file_name: String,
    chunk: Vec<u8>,
) {
    let http = webhook_data.http.clone();
    let webhook = webhook_data.webhook.clone();
    let cow = Cow::from(&chunk);

    let res = webhook
        .execute(http, false, |w| {
            w.add_file((
                cow.as_ref(),
                format!("{}.part{}", file_name, index).as_str(),
            ));

            w
        })
        .await
        .expect("Failed to upload chunk");

    match res {
        Some(message) => {
            let attachment_url = message
                .attachments
                .first()
                .expect("No attachments found")
                .url
                .clone();

            add_to_database(db, file_name.to_string(), index, attachment_url).await;

            println!("chunk number {} successfully uploaded", index,);
            return;
        }
        None => {
            println!("Error: couldnt upload chunk {}", index);
            return;
        }
    }
}

pub async fn chunk_file(file_path: PathBuf, webhook_data: Vec<WebhookData>, db: FirestoreDb) {
    let semaphore = Arc::new(Semaphore::new(24));

    let file = std::fs::File::open(&file_path).unwrap();
    let metadata = file.metadata().unwrap();
    let file_size = metadata.len() as usize;

    let mut reader = BufReader::new(file);
    let buffer_size = 7 * 1024 * 1024; // 8MB buffer
    let mut buffer = vec![0; buffer_size];
    let mut i = 0;
    let mut webhook_index = 0;
    let mut handles = vec![];

    let master_directory: MasterDirectoryChild = MasterDirectoryChild {
        name: file_path.file_name().unwrap().to_str().unwrap().to_string(),
        parts: vec![],
    };

    let _master_directory: MasterDirectoryChild = db
        .fluent()
        .insert()
        .into(MASTER_DIRECTORY_COLLECTION_NAME)
        .document_id(&master_directory.name)
        .object(&master_directory)
        .execute()
        .await
        .unwrap();

    loop {
        let bytes_read = reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            break;
        }
        let data_size_mb = bytes_read as f64 / (1024.0 * 1024.0);
        println!("Read {} MB", data_size_mb);
        let data = (&buffer[..bytes_read]).to_vec();
        let semaphore_handle = semaphore.clone();
        let webhook = webhook_data[webhook_index].clone();
        let db = db.clone();
        let file_name = (&file_path)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        let handle = tokio::spawn(async move {
            let _permit = semaphore_handle.acquire().await.unwrap();

            upload_chunk(webhook, db, i, file_name, data).await;
            drop(_permit);
        });
        handles.push(handle);
        i += 1;
        if webhook_index >= 11 {
            webhook_index = 0;
        } else {
            webhook_index += 1;
        }
    }

    join_all(handles).await;

    let total_chunks = (file_size + buffer_size - 1) / buffer_size;
    print!("Total chunks (Calculation): {}", total_chunks);
}
