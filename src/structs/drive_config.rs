use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serenity::{http::Http, model::webhook::Webhook};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct DriveConfig {
    pub webhooks: Vec<String>,
    pub service_account_file_location: String,
}

pub struct WebhookData {
    pub http: Arc<Http>,
    pub webhook: Webhook,
}

impl Clone for WebhookData {
    fn clone(&self) -> Self {
        WebhookData {
            http: self.http.clone(),
            webhook: self.webhook.clone(),
        }
    }
}