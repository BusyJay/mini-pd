use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct Config {
    pub my_id: u64,
    pub address: String,
    pub advertise_address: String,
    pub data_dir: PathBuf,
    pub initial_peers: Vec<u64>,
    pub initial_address_book: HashMap<u64, String>,
    pub log_file: String,
    // Force user to use ..Default::default().
    _preserved: PhantomData<()>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            my_id: rand::random(),
            address: "127.0.0.1:2379".to_owned(),
            advertise_address: String::new(),
            data_dir: Path::new("pd").to_path_buf(),
            initial_peers: vec![],
            initial_address_book: HashMap::new(),
            log_file: "pd.log".to_owned(),
            _preserved: PhantomData,
        }
    }
}
