use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct DriveConfig {
    pub webhooks: Vec<String>,
    pub service_account_file_location: String,
}