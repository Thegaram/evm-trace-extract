use crate::rpc;
use crate::transaction_info::{parse_accesses, parse_tx_hash, TransactionInfo};
use rocksdb::{Options, SliceTransform, DB};
use std::collections::HashMap;
use web3::types::{Transaction, TransactionReceipt};

pub fn open_traces(path: &str) -> DB {
    let prefix_extractor = SliceTransform::create_fixed_prefix(8);

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_prefix_extractor(prefix_extractor);

    DB::open(&opts, path).expect("db open should succeed")
}

// note: this will get tx infos in the wrong order!
pub fn tx_infos_deprecated(db: &DB, block: u64) -> Vec<TransactionInfo> {
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

pub fn tx_infos(db: &DB, block: u64, infos: &Vec<rpc::TxInfo>) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.status().unwrap();

    // get results from db
    let mut txs_unordered: HashMap<String, TransactionInfo> = iter
        .map(|(key, value)| {
            let key = std::str::from_utf8(&*key).expect("key read is valid string");
            let value = std::str::from_utf8(&*value).expect("value read is valid string");

            let tx_hash = parse_tx_hash(key).to_owned();

            let tx = TransactionInfo {
                tx_hash: tx_hash.clone(),
                accesses: parse_accesses(value).to_owned(),
            };

            (tx_hash, tx)
        })
        .collect();

    // order results based on `infos`
    assert_eq!(infos.len(), txs_unordered.len());
    let mut res = vec![];

    for hash in infos.iter().map(|i| i.hash) {
        let tx = txs_unordered
            .remove(&format!("{:?}", hash))
            .expect("hash should exist");

        res.push(tx);
    }

    res
}

pub struct RpcDb {
    db: DB,
}

impl RpcDb {
    pub fn open(path: &str) -> Result<RpcDb, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(RpcDb { db })
    }

    fn txs_key(block: u64) -> Vec<u8> {
        format!("{:0>8}-txs", block).as_bytes().to_vec()
    }

    fn receipts_key(block: u64) -> Vec<u8> {
        format!("{:0>8}-rec", block).as_bytes().to_vec()
    }

    pub fn put_txs(&mut self, block: u64, txs: Vec<Transaction>) -> Result<(), rocksdb::Error> {
        let key = RpcDb::txs_key(block);
        let value = rmp_serde::to_vec(&txs).expect("serialize should succeed");
        self.db.put(key, value)
    }

    pub fn get_txs(&self, block: u64) -> Result<Option<Vec<Transaction>>, rocksdb::Error> {
        let key = RpcDb::txs_key(block);

        match self.db.get(&key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(rmp_serde::from_slice(&raw[..]).unwrap())),
        }
    }

    pub fn put_receipts(
        &mut self,
        block: u64,
        receipts: Vec<TransactionReceipt>,
    ) -> Result<(), rocksdb::Error> {
        let key = RpcDb::receipts_key(block);
        let value = rmp_serde::to_vec(&receipts).expect("serialize should succeed");
        self.db.put(key, value)
    }

    pub fn get_receipts(
        &self,
        block: u64,
    ) -> Result<Option<Vec<TransactionReceipt>>, rocksdb::Error> {
        let key = RpcDb::receipts_key(block);

        match self.db.get(&key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(rmp_serde::from_slice(&raw[..]).unwrap())),
        }
    }
}
