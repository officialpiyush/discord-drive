use confy;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serenity::{http::Http, model::webhook::Webhook};
use std::{
    borrow::Cow,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};
use tokio::sync::Semaphore;

#[derive(Default, Debug, Serialize, Deserialize)]
struct DriveConfig {
    webhooks: Vec<String>,
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

async fn upload_chunk(webhook_data: WebhookData, index: i32, file_name: &str, chunk: Vec<u8>) {
    let http = webhook_data.http.clone();
    let webhook = webhook_data.webhook.clone();
    let cow = Cow::from(&chunk);

    webhook
        .execute(http, false, |w| {
            w.add_file((
                cow.as_ref(),
                format!("{}.part{}", file_name, index).as_str(),
            ));

            w
        })
        .await
        .expect("Failed to upload chunk");
    println!(
        "chunk number {} successfully uploaded | {} size",
        index,
        chunk.len()
    );
    return;
}

async fn chunk_file(webhook_data: Vec<WebhookData>) {
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
        let file_name = path.file_name().unwrap().to_str().unwrap();
        print!("{} ", file_name);

        let handle = tokio::spawn(async move {
            let _permit = semaphore_handle.acquire().await.unwrap();

            upload_chunk(webhook, i, file_name, data).await;
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

    chunk_file(webhooks_clone).await;
    let duration = start.elapsed();
    println!("Time taken: {:?}s", duration);
}
