use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MasterDirectoryChildPart {
    pub name: String,
    pub id: String,
    pub part: i32,
    pub parent: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MasterDirectoryChild {
    pub name: String,
    pub parts: Vec<MasterDirectoryChildPart>,
}