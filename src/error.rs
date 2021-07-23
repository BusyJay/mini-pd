use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Storage error {0}")]
    Storage(String),
    #[error("Codec error {0}")]
    Codec(#[from] protobuf::ProtobufError),
    #[error("Raft error {0}")]
    Raft(#[from] raft::Error),
    #[error("Rpc error {0}")]
    Rpc(#[from] grpcio::Error),
    #[error("Other error {0}")]
    Other(String),
}

#[macro_export]
macro_rules! r {
    ($res:expr) => {
        match $res {
            Ok(r) => r,
            Err(e) => return Err(crate::Error::Storage(e)),
        }
    };
}

pub type Result<T> = std::result::Result<T, Error>;
