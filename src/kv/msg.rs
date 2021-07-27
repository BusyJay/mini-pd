use super::storage::RockSnapshot;
use bytes::Bytes;
use futures::channel::mpsc::Sender;
use protobuf::{CodedInputStream, CodedOutputStream};
use raft::eraftpb::Message;
use std::fmt::{self, Debug};

pub enum Command {
    Put { key: Bytes, value: Bytes },
    UpdateAddress { id: u64, address: String },
    BatchPut { kvs: Vec<(Bytes, Bytes)> },
}

impl Command {
    const PUT_SHORT_KEY: u8 = 0x01;
    const UPDATE_ADDRESS: u8 = 0x02;
    const BATCH_PUT_KEY: u8 = 0x03;

    pub fn put(key: Bytes, value: Bytes) -> Command {
        Command::Put { key, value }
    }

    pub fn batch_put(kvs: Vec<(Bytes, Bytes)>) -> Command {
        Command::BatchPut { kvs }
    }

    pub fn into_proposal(self) -> (Vec<u8>, Vec<u8>) {
        match self {
            Command::Put { key, value } => {
                let p = put_proposal(&key, value);
                (vec![], p)
            }
            Command::UpdateAddress { id, address } => {
                let mut p = Vec::with_capacity(9 + address.len());
                p.extend_from_slice(address.as_bytes());
                p.extend_from_slice(&id.to_le_bytes());
                p.push(Command::UPDATE_ADDRESS);
                (vec![], p)
            }
            Command::BatchPut { kvs } => {
                let p = batch_put_proposal(&kvs);
                (vec![], p)
            }
        }
    }

    pub fn from_proposal(_context: Bytes, mut proposal: Bytes) -> Option<Command> {
        if proposal.is_empty() {
            return None;
        }
        match proposal[proposal.len() - 1] {
            Command::PUT_SHORT_KEY => {
                let key_len = proposal[proposal.len() - 2] as usize;
                let key = proposal.slice(proposal.len() - 2 - key_len..proposal.len() - 2);
                proposal.truncate(proposal.len() - 2 - key_len);
                Some(Command::Put {
                    key,
                    value: proposal,
                })
            }
            Command::UPDATE_ADDRESS => {
                let mut id_bytes = [0; 8];
                id_bytes.copy_from_slice(&proposal[proposal.len() - 9..proposal.len() - 1]);
                proposal.truncate(proposal.len() - 9);
                let id = u64::from_le_bytes(id_bytes);
                let address = String::from_utf8(proposal.to_vec()).unwrap();
                Some(Command::UpdateAddress { id, address })
            }
            Command::BATCH_PUT_KEY => {
                let bytes = proposal.slice(..proposal.len() - 1);
                let mut input = CodedInputStream::from_carllerche_bytes(&bytes);
                let mut kvs = Vec::new();
                while !input.eof().unwrap() {
                    let key = input.read_carllerche_bytes().unwrap();
                    let value = input.read_carllerche_bytes().unwrap();
                    kvs.push((key, value));
                }
                Some(Command::BatchPut { kvs })
            }
            prefix => panic!("unrecognize command type {}", prefix),
        }
    }
}

pub enum Res {
    Success,
    Snapshot(RockSnapshot),
    RoleInfo { term: u64, leader: u64, my_id: u64 },
    Fail(String),
}

impl Debug for Res {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Res::Success => write!(formatter, "Res::Success"),
            Res::Snapshot(_) => write!(formatter, "Res::Snapshot"),
            Res::RoleInfo {
                term,
                leader,
                my_id,
            } => {
                write!(
                    formatter,
                    "Res::RoleInfo {{ term: {}, leader: {}, my_id: {} }}",
                    term, leader, my_id
                )
            }
            Res::Fail(s) => write!(formatter, "Res::Fail({:?})", s),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub enum Event {
    Elected,
    BecameLeader,
    CommittedToCurrentTerm,
    CommittedToCurrentTermAsLeader,
}

pub enum Msg {
    Command {
        cmd: Command,
        term: Option<u64>,
        notifier: Option<Sender<Res>>,
    },
    Snapshot {
        term: Option<u64>,
        notifier: Sender<Res>,
    },
    WaitEvent {
        event: Event,
        notifier: Sender<Res>,
    },
    RaftMessage(Message),
    Tick,
    Stop,
}

impl Msg {
    pub fn command(cmd: Command, notifier: Option<Sender<Res>>) -> Msg {
        Msg::Command {
            cmd,
            term: None,
            notifier,
        }
    }

    pub fn check_term_command(cmd: Command, term: u64, notifier: Option<Sender<Res>>) -> Msg {
        Msg::Command {
            cmd,
            term: Some(term),
            notifier,
        }
    }

    pub fn snapshot(notifier: Sender<Res>) -> Msg {
        Msg::Snapshot {
            term: None,
            notifier,
        }
    }

    pub fn check_snapshot(term: u64, notifier: Sender<Res>) -> Msg {
        Msg::Snapshot {
            term: Some(term),
            notifier,
        }
    }
}

fn batch_put_proposal(kvs: &[(Bytes, Bytes)]) -> Vec<u8> {
    let mut res = Vec::new();
    let mut s = CodedOutputStream::new(&mut res);
    for (k, v) in kvs {
        s.write_bytes_no_tag(k).unwrap();
        s.write_bytes_no_tag(v).unwrap();
    }
    res.push(Command::BATCH_PUT_KEY);
    res
}

fn put_proposal(key: &[u8], val: Bytes) -> Vec<u8> {
    let mut res = Vec::with_capacity(val.len() + key.len() + 1);
    res.extend_from_slice(&val);
    res.extend_from_slice(key);
    if key.len() < 128 {
        res.push(key.len() as u8);
        res.push(Command::PUT_SHORT_KEY);
    } else {
        unimplemented!()
    }
    res
}
