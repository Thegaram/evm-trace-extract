use crate::transaction_info::{parse_accesses, parse_tx_hash, TransactionInfo};
use rocksdb::{Options, SliceTransform, DB};

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
