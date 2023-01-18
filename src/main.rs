use confy;
use firestore::*;
use firestore::{paths, FirestoreDb};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serenity::{http::Http, model::webhook::Webhook};
use std::{
    borrow::Cow,
    env::set_var,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};
use tokio::sync::Semaphore;

const MASTER_DIRECTORY_COLLECTION_NAME: &'static str = "master_directory";

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MasterDirectoryChildPart {
    name: String,
    id: String,
    part: i32,
    parent: String,
    url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MasterDirectoryChild {
    name: String,
    parts: Vec<MasterDirectoryChildPart>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct DriveConfig {
    webhooks: Vec<String>,
    service_account_file_location: String,
}

struct WebhookData {
    http: Arc<Http>,
    webhook: Webhook,
}

impl Clone for WebhookData {
    fn clone(&self) -> Self {
        WebhookData {
            http: self.http.clone(),
            webhook: self.webhook.clone(),
        }
    }
}

async fn add_to_database(db: FirestoreDb, name: String, part: i32, attachment_url: String) {
    let master_directory_child: MasterDirectoryChildPart = MasterDirectoryChildPart {
        name: name.clone(),
        id: format!("{}.part{}", &name, part),
        part: part,
        parent: name,
        url: attachment_url,
    };

    let added = db
        .fluent()
        .update()
        .fields(paths!(MasterDirectoryChild::parts))
        .in_col(MASTER_DIRECTORY_COLLECTION_NAME)
        .document_id(&master_directory_child.name)
        .transforms(|transform_builder| {
            vec![transform_builder
                .field("parts")
                .append_missing_elements(vec![&master_directory_child])
                .unwrap()]
        });

    println!("Added to database: {:?}", added);
}

async fn upload_chunk(
    webhook_data: WebhookData,
    db: FirestoreDb,
    index: i32,
    file_name: &str,
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

async fn chunk_file(webhook_data: Vec<WebhookData>, db: FirestoreDb) {
    let semaphore = Arc::new(Semaphore::new(24));

    let path = Path::new("trial/rq.rar");
    let file = File::open(path).unwrap();
    let metadata = file.metadata().unwrap();
    let file_size = metadata.len() as usize;

    let mut reader = BufReader::new(file);
    let buffer_size = 7 * 1024 * 1024; // 8MB buffer
    let mut buffer = vec![0; buffer_size];
    let mut i = 0;
    let mut webhook_index = 0;
    let mut handles = vec![];

    let master_directory: MasterDirectoryChild = MasterDirectoryChild {
        name: path.file_name().unwrap().to_str().unwrap().to_string(),
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
        let file_name = path.file_name().unwrap().to_str().unwrap();
        print!("{} ", file_name);

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

#[tokio::main]
async fn main() {
    let start = Instant::now();
    println!("Hello, world!");
    let config_path = Path::new("config/config.toml");
    let cfg: DriveConfig = confy::load_path(config_path).unwrap();

    set_var(
        "GOOGLE_APPLICATION_CREDENTIALS",
        cfg.service_account_file_location,
    );

    let db = FirestoreDb::new("discord-ddrive").await.unwrap();

    if cfg.webhooks.len() <= 0 {
        panic!("No webhooks found in config file")
    }

    let webhooks: Arc<Mutex<Vec<WebhookData>>> = Arc::new(Mutex::new(vec![]));

    let http_one = Arc::new(Http::new(""));
    let http_two = Arc::new(Http::new(""));

    let mut webhook_handles = vec![];

    let mut webhook_index = 1;
    for webhook in cfg.webhooks {
        let http = if webhook_index % 2 != 0 {
            http_one.clone()
        } else {
            http_two.clone()
        };

        let webhook_clone = webhooks.clone();
        let handle = tokio::spawn(async move {
            let webhook = Webhook::from_url(http.as_ref(), &webhook)
                .await
                .expect(&format!("Failed to get webhook from url: {}", webhook));
            let mut webhook_vec = webhook_clone.lock().unwrap();
            webhook_vec.push({
                WebhookData {
                    http: http.clone(),
                    webhook: webhook.clone(),
                }
            });
        });
        webhook_handles.push(handle);
        webhook_index += 1;
    }

    join_all(webhook_handles).await;

    print!("Webhooks: {} ", webhooks.lock().unwrap().len());

    let webhooks_clone = webhooks.clone().lock().unwrap().to_vec();

    // let db_clone = Arc::new(db);
    chunk_file(webhooks_clone, db).await;
    let duration = start.elapsed();
    println!("Time taken: {:?}s", duration);
}
