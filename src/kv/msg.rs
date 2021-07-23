use bytes::Bytes;
use raft::eraftpb::Message;

pub enum Command {
    Put { key: Bytes, value: Bytes },
    UpdateAddress { id: u64, address: String },
}

impl Command {
    const PUT_SHORT_KEY: u8 = 0x01;
    const UPDATE_ADDRESS: u8 = 0x02;

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
            prefix => panic!("unrecognize command type {}", prefix),
        }
    }
}

pub enum Msg {
    Command(Command),
    RaftMessage(Message),
    Tick,
    Stop,
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
