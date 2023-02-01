mod functions;
mod structs;

use confy;
use firestore::FirestoreDb;
use functions::chunks::chunk_file;
use functions::retrieve::retrieve_and_save;

use futures::future::join_all;
use serenity::{http::Http, model::webhook::Webhook};
use std::{
    env::set_var,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};

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
