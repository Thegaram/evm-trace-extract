use regex::Regex;
use std::collections::HashSet;

lazy_static! {
    static ref RE: Regex =
        Regex::new(r"^.*?-(?P<hash>0x[a-fA-F0-9]+)$",).expect("Regex RE is correct");
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Target {
    Balance(String),
    Storage(String, String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Access {
    pub target: Target,
    pub mode: AccessMode,
}

impl Access {
    fn from_string(raw: &str) -> Self {
        let mode = match &raw[0..1] {
            "R" => AccessMode::Read,
            "W" => AccessMode::Write,
            x => panic!("Unknown Access type: {}", x),
        };

        let target = match &raw[1..2] {
            "B" => {
                let address = raw[3..45].to_owned();
                Target::Balance(address)
            }
            "S" => {
                let address = raw[3..45].to_owned();
                let entry = raw[47..113].to_owned();
                Target::Storage(address, entry)
            }
            x => panic!("Unknown Target type: {}", x),
        };

        Access { target, mode }
    }

    pub fn storage_write(addr: &String, entry: &String) -> Access {
        Access {
            target: Target::Storage(addr.clone(), entry.clone()),
            mode: AccessMode::Write,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub tx_hash: String,
    pub accesses: HashSet<Access>,
}

pub fn parse_tx_hash(raw: &str) -> &str {
    RE.captures(raw)
        .expect(&format!("Expected to tx hash in {}", raw))
        .name("hash")
        .map_or("", |m| m.as_str())
}

pub fn parse_accesses(raw: &str) -> HashSet<Access> {
    raw.trim_matches(|ch| ch == '{' || ch == '}')
        .split(", ")
        .map(Access::from_string)
        .collect()
}
