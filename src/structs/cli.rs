use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "discord_drive")]
pub struct Opts {
    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
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