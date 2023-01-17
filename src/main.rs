use confy;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
    time::{Duration, Instant},
};
use tokio::time;

#[derive(Default, Debug, Serialize, Deserialize)]
struct DriveConfig {
    webhooks: Vec<String>,
}

async fn upload_chunk(index: i32, chunk: Vec<u8>) {
    time::sleep(Duration::from_secs(4)).await;
    println!(
        "chunk number {} successfully uploaded | {} size",
        index,
        chunk.len()
    );
}

async fn open_file() {
    let file = File::open("trial/rq.rar").unwrap();
    let metadata = file.metadata().unwrap();
    let file_size = metadata.len() as usize;
    let mut reader = io::BufReader::new(file);
    let buffer_size = 7 * 1024 * 1024; // 8MB buffer
    let mut buffer = vec![0; buffer_size];
    let mut i = 0;
    let mut handles = vec![];

    loop {
        let bytes_read = reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            break;
        }
        let data_size_mb = bytes_read as f64 / (1024.0 * 1024.0);
        println!("Read {} MB", data_size_mb);
        let data = &buffer[..bytes_read];
        let vec_data = data.to_vec();
        let handle = tokio::spawn(upload_chunk(i, vec_data));
        handles.push(handle);
        i += 1;
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

    println!("Webhooks: {:?}", cfg.webhooks);

    open_file().await;
    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);
}
