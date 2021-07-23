use mini_pd::*;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;
use tempdir::TempDir;

#[test]
fn test_smoke() {
    let tmp_dir = TempDir::new("test_single_node").unwrap();
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    let address_map = AddressMap::default();
    let my_addr = "127.0.0.1:1234";
    let mut config = Config::default();
    config.my_id = 1;
    config.address = my_addr.to_owned();
    config.advertise_address = my_addr.to_owned();
    config.data_dir = tmp_dir.path().to_path_buf();
    config.initial_peers = vec![1];
    config.initial_address_book.insert(1, my_addr.to_owned());
    let mut server = Server::new(address_map.clone(), config, logger);
    server.start().unwrap();
    let sender = server.sender();
    sender
        .send(Msg::Command(Command::Put {
            key: "dk1".into(),
            value: "dv1".into(),
        }))
        .unwrap();
    server.shutdown();
}
