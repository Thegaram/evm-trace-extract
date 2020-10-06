use crate::transaction_info::{parse_accesses, parse_tx_hash, TransactionInfo};
use rocksdb::{Options, SliceTransform, DB};
use crate::rpc;
use std::collections::HashMap;

pub fn open(path: &str) -> DB {
    let prefix_extractor = SliceTransform::create_fixed_prefix(8);

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_prefix_extractor(prefix_extractor);

    DB::open(&opts, path).expect("can open db")
}

pub fn tx_infos(db: &DB, block: u64) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.status().unwrap();

    iter.map(|(key, value)| {
        let key = std::str::from_utf8(&*key).expect("key read is valid string");
        let value = std::str::from_utf8(&*value).expect("value read is valid string");

        TransactionInfo {
            tx_hash: parse_tx_hash(key).to_owned(),
            accesses: parse_accesses(value).to_owned(),
        }
    })
    .collect()
}


pub fn tx_infos_2(db: &DB, block: u64, infos: &Vec<rpc::TxInfo>) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.status().unwrap();

    let mut x = iter.map(|(key, value)| {
        let key = std::str::from_utf8(&*key).expect("key read is valid string");
        let value = std::str::from_utf8(&*value).expect("value read is valid string");

        let tx_hash = parse_tx_hash(key).to_owned();

        (tx_hash, TransactionInfo {
            tx_hash: parse_tx_hash(key).to_owned(),
            accesses: parse_accesses(value).to_owned(),
        })
    })
    .collect::<HashMap<String, TransactionInfo>>();

    let mut result = vec![];

    assert_eq!(infos.len(), x.len());

    for hash in infos.iter().map(|i| i.hash) {
        let y = x.remove(&format!("{:?}", hash))
            .expect("hash should exist");

        result.push(y);
    }

    result
}
