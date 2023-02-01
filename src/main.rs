mod functions;
mod structs;

use confy;
use firestore::FirestoreDb;
use functions::chunks::chunk_file;
use functions::database::MASTER_DIRECTORY_COLLECTION_NAME;
use futures::future::join_all;
use serenity::{http::Http, model::webhook::Webhook};
use std::fs::File;
use std::io::BufWriter;

use std::{
    env::set_var,
    io::BufReader,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};
use structs::directory::*;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

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
    let opts = <structs::cli::Opts as structopt::StructOpt>::from_args();

    let start = Instant::now();
    println!("Hello, world!");
    let config_path = Path::new("config/config.toml");
    let cfg: structs::drive_config::DriveConfig = confy::load_path(config_path).unwrap();

    set_var(
        "GOOGLE_APPLICATION_CREDENTIALS",
        cfg.service_account_file_location,
    );

    let db = FirestoreDb::new("discord-ddrive").await.unwrap();

    if cfg.webhooks.len() <= 0 {
        panic!("No webhooks found in config file")
    }

    let webhooks: Arc<Mutex<Vec<structs::drive_config::WebhookData>>> =
        Arc::new(Mutex::new(vec![]));

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
                structs::drive_config::WebhookData {
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
        structs::cli::Command::Store { file } => {
            // check if file is a directory
            if file.is_dir() {
                eprintln!("Error: {} is a directory", file.display());
                std::process::exit(1);
            }

            // store the file
            println!("Storing file: {}", file.display());
            chunk_file(file, webhooks_clone, db).await;
        }
        structs::cli::Command::Retrieve { word } => {
            println!("Retrieving word: {}", word);
            retrieve_and_save(db, "rq.rar").await;
        }
    }

    let duration = start.elapsed();
    println!("Time taken: {:?}s", duration);
}
