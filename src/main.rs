use confy;
use firestore::*;
use firestore::{paths, FirestoreDb};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serenity::{http::Http, model::webhook::Webhook};
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::{
    borrow::Cow,
    env::set_var,
    io::{BufReader, Read},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

const MASTER_DIRECTORY_COLLECTION_NAME: &'static str = "master_directory";

#[derive(StructOpt, Debug)]
#[structopt(name = "discord_drive")]
struct Opts {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "store")]
    Store {
        #[structopt(name = "file", parse(from_os_str))]
        file: std::path::PathBuf,
    },

    #[structopt(name = "retrieve")]
    Retrieve {
        #[structopt(name = "word")]
        word: String,
    },
}

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

async fn chunk_file(file_path: PathBuf, webhook_data: Vec<WebhookData>, db: FirestoreDb) {
    let semaphore = Arc::new(Semaphore::new(24));

    let file = std::fs::File::open(file_path).unwrap();
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
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

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

async fn retrieve_and_save(db: FirestoreDb, name: &str) {
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

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

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

    match opts.command {
        Command::Store { file } => {
            // check if file is a directory
            if file.is_dir() {
                eprintln!("Error: {} is a directory", file.display());
                std::process::exit(1);
            }

            // store the file
            println!("Storing file: {}", file.display());
            chunk_file(file, webhooks_clone, db).await;
        }
        Command::Retrieve { word } => {
            println!("Retrieving word: {}", word);
            retrieve_and_save(db, "rq.rar").await;
        }
    }

    let duration = start.elapsed();
    println!("Time taken: {:?}s", duration);
}
