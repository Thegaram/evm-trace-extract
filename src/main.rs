extern crate regex;
extern crate rocksdb;
#[macro_use]
extern crate lazy_static;

mod stats;
mod transaction_info;

use rocksdb::{Options, SliceTransform, DB};
use stats::{BlockStats, TxPairStats};
use std::collections::HashMap;
use std::env;
use transaction_info::{Access, AccessMode, Target, TransactionInfo};

#[derive(Clone, Copy, Eq, PartialEq)]
enum OutputMode {
    Normal,
    Detailed,
    Csv,
}

impl OutputMode {
    fn from_str(raw: &str) -> OutputMode {
        match raw {
            "normal" => OutputMode::Normal,
            "detailed" => OutputMode::Detailed,
            "csv" => OutputMode::Csv,
            x => panic!("Unknown OutputMode type: {}", x),
        }
    }
}

fn tx_infos_from_db(db: &DB, block: u64) -> Vec<TransactionInfo> {
    use transaction_info::{parse_accesses, parse_tx_hash};

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

fn into_pairwise_iter<'a>(
    txs: &'a Vec<TransactionInfo>,
) -> impl Iterator<Item = (&'a TransactionInfo, &'a TransactionInfo)> {
    (0..(txs.len() - 1))
        .flat_map(move |ii| ((ii + 1)..txs.len()).map(move |jj| (ii, jj)))
        .map(move |(ii, jj)| (&txs[ii], &txs[jj]))
}

fn extract_tx_stats<'a>(pair: (&'a TransactionInfo, &'a TransactionInfo)) -> TxPairStats<'a> {
    let (tx_a, tx_b) = pair;
    let mut stats = TxPairStats::new(&tx_a.tx_hash, &tx_b.tx_hash);

    for access in &tx_a.accesses {
        match access {
            Access {
                target: Target::Balance(addr),
                mode: AccessMode::Read,
            } => {
                if tx_b.accesses.contains(&Access {
                    target: Target::Balance(addr.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.balance_rw += 1;
                }
            }
            Access {
                target: Target::Balance(addr),
                mode: AccessMode::Write,
            } => {
                if tx_b.accesses.contains(&Access {
                    target: Target::Balance(addr.clone()),
                    mode: AccessMode::Read,
                }) {
                    stats.balance_rw += 1;
                }

                if tx_b.accesses.contains(&Access {
                    target: Target::Balance(addr.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.balance_ww += 1;
                }
            }
            Access {
                target: Target::Storage(addr, entry),
                mode: AccessMode::Read,
            } => {
                if tx_b.accesses.contains(&Access {
                    target: Target::Storage(addr.clone(), entry.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.storage_rw += 1;
                }
            }
            Access {
                target: Target::Storage(addr, entry),
                mode: AccessMode::Write,
            } => {
                if tx_b.accesses.contains(&Access {
                    target: Target::Storage(addr.clone(), entry.clone()),
                    mode: AccessMode::Read,
                }) {
                    stats.storage_rw += 1;
                }

                if tx_b.accesses.contains(&Access {
                    target: Target::Storage(addr.clone(), entry.clone()),
                    mode: AccessMode::Write,
                }) {
                    stats.storage_ww += 1;
                }
            }
        }
    }

    stats
}

fn process_txs_pairwise(block: u64, tx_infos: Vec<TransactionInfo>, mode: OutputMode) {
    // collect pairwise stats
    let mut block_stats = BlockStats::new(block);

    for stats in into_pairwise_iter(&tx_infos).map(extract_tx_stats) {
        block_stats.accumulate(&stats);

        if mode == OutputMode::Detailed && stats.has_conflict() {
            println!("    {:?}", stats);
        }
    }

    // print stats
    match mode {
        OutputMode::Normal | OutputMode::Detailed => {
            if !block_stats.has_conflicts() {
                println!("No conflicts in block\n");
                return;
            }

            println!("{:?}\n", block_stats);
        }
        OutputMode::Csv => {
            println!(
                "{},{},{},{}",
                block,
                block_stats.num_conflicting_pairs(),
                block_stats.conflicting_pairs_balance,
                block_stats.conflicting_pairs_storage
            );
        }
    }
}

fn process_pairwise(db: &DB, blocks: impl Iterator<Item = u64>, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,conflicts,balance,storage");
    }

    // process blocks
    for block in blocks {
        let tx_infos = tx_infos_from_db(&db, block);

        if matches!(mode, OutputMode::Normal | OutputMode::Detailed) {
            println!(
                "Checking pairwise conflicts in block #{} ({} txs)...",
                block,
                tx_infos.len(),
            );
        }

        process_txs_pairwise(block, tx_infos, mode);
    }
}

fn process_block_aborts(
    block: u64,
    txs: Vec<TransactionInfo>,
    mode: OutputMode,
    ignore_balance: bool,
    abort_counts: &mut HashMap<String, u64>,
    filter_addr: Option<String>,
) {
    let mut balances = HashMap::new();
    let mut storages = HashMap::new();

    let mut num_aborts = 0;

    for tx in txs {
        let TransactionInfo { tx_hash, accesses } = tx;

        let (reads, writes): (Vec<_>, Vec<_>) = accesses
            .into_iter()
            .partition(|a| a.mode == AccessMode::Read);

        let mut aborted = false;

        // we process reads first so that a tx does not "abort itsself"
        for access in reads.into_iter().map(|a| a.target) {
            match access {
                Target::Balance(addr) => {
                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if &addr != a {
                            continue;
                        }
                    }

                    if balances.contains_key(&addr) && !ignore_balance {
                        aborted = true;
                        *abort_counts.entry(addr.clone()).or_insert(0) += 1;

                        if mode == OutputMode::Detailed {
                            println!("    abort on read balance({:?})", addr);
                            println!("        1st: {:?}", balances[&addr]);
                            println!("        2nd: {:?}", tx_hash);
                        }

                        break;
                    }
                }
                Target::Storage(addr, entry) => {
                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if &addr != a {
                            continue;
                        }
                    }

                    let key = (addr, entry);

                    if storages.contains_key(&key) {
                        aborted = true;
                        *abort_counts.entry(key.0.clone()).or_insert(0) += 1;

                        if mode == OutputMode::Detailed {
                            println!("    abort on read storage({:?}, {:?})", key.0, key.1);
                            println!("        1st: {:?}", storages[&key]);
                            println!("        2nd: {:?}", tx_hash);
                        }

                        break;
                    }
                }
            }
        }

        // then, we process writes, checking for aborts and enacting updates
        for access in writes.into_iter().map(|a| a.target) {
            match access {
                Target::Balance(addr) => {
                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if &addr != a {
                            continue;
                        }
                    }

                    if balances.contains_key(&addr) && !ignore_balance {
                        aborted = true;
                        *abort_counts.entry(addr.clone()).or_insert(0) += 1;

                        if mode == OutputMode::Detailed {
                            println!("    abort on write balance({:?})", addr);
                            println!("        1st: {:?}", balances[&addr]);
                            println!("        2nd: {:?}", tx_hash);
                        }
                    }

                    balances.insert(addr, tx_hash.clone());
                }
                Target::Storage(addr, entry) => {
                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if &addr != a {
                            continue;
                        }
                    }

                    let key = (addr, entry);

                    if storages.contains_key(&key) {
                        aborted = true;
                        *abort_counts.entry(key.0.clone()).or_insert(0) += 1;

                        if mode == OutputMode::Detailed {
                            println!("    abort on write storage({:?}, {:?})", key.0, key.1);
                            println!("        1st: {:?}", storages[&key]);
                            println!("        2nd: {:?}", tx_hash);
                        }
                    }

                    storages.insert(key, tx_hash.clone());
                }
            }
        }

        if aborted {
            num_aborts += 1;
        }
    }

    match mode {
        OutputMode::Normal | OutputMode::Detailed => {
            println!("Num aborts in block #{}: {}\n", block, num_aborts);
        }
        OutputMode::Csv => {
            println!("{},{}", block, num_aborts);
        }
    }
}

fn process_aborts(db: &DB, blocks: impl Iterator<Item = u64>, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,aborts");
    }

    let mut abort_counts = HashMap::new();

    for block in blocks {
        let tx_infos = tx_infos_from_db(&db, block);

        if matches!(mode, OutputMode::Normal | OutputMode::Detailed) {
            println!(
                "Checking aborts in block #{} ({} txs)...",
                block,
                tx_infos.len(),
            );
        }

        process_block_aborts(
            block,
            tx_infos,
            mode,
            /* ignore_balance = */ true,
            &mut abort_counts,
            /* filter_addr = */ None,
        );
    }

    // let mut counts = abort_counts.into_iter().collect::<Vec<_>>();
    // counts.sort_by(|&(_, a), &(_, b)| a.cmp(&b).reverse());

    // for ii in 0..20 {
    //     if ii >= counts.len() {
    //         break;
    //     }

    //     println!("#{}: {} ({} aborts)", ii, counts[ii].0, counts[ii].1);
    // }
}

fn main() {
    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 6 {
        println!("Usage: evm-trace-extract [db-path:str] [from-block:int] [to-block:int] [mode:pairwise|aborts] [output:normal|detailed|csv]");
        return;
    }

    let path = &args[1][..];

    let from = args[2]
        .parse::<u64>()
        .expect("from-block should be a number");

    let to = args[3].parse::<u64>().expect("to-block should be a number");
    let mode = &args[4][..];
    let output = OutputMode::from_str(&args[5][..]);

    // open db
    let prefix_extractor = SliceTransform::create_fixed_prefix(8);

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_prefix_extractor(prefix_extractor);

    let db = DB::open(&opts, path).expect("can open db");

    // check range
    let latest_raw = db
        .get(b"latest")
        .expect("get latest should succeed")
        .expect("latest should exist");

    let latest = std::str::from_utf8(&latest_raw[..])
        .expect("parse to string succeed")
        .parse::<u64>()
        .expect("parse to int should succees");

    if to > latest {
        println!("Latest header in trace db: #{}", latest);
        return;
    }

    // process
    match mode {
        "pairwise" => process_pairwise(&db, from..=to, output),
        "aborts" => process_aborts(&db, from..=to, output),
        _ => {
            println!("mode should be one of: pairwise, aborts");
            return;
        }
    }
}
