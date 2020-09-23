#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate rocksdb;
extern crate web3;

mod rpc;
mod stats;
mod transaction_info;

use rocksdb::{Options, SliceTransform, DB};
use rpc::{retrieve_gas, retrieve_gas_parity};
use stats::{BlockStats, TxPairStats};
use std::collections::{HashMap, HashSet};
use std::env;
use transaction_info::{Access, AccessMode, Target, TransactionInfo};
use web3::{transports, types::U256, Web3 as Web3Generic};

type Web3 = Web3Generic<transports::Http>;

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

async fn process_block_aborts(
    web3: &Web3,
    block: u64,
    txs: Vec<TransactionInfo>,
    mode: OutputMode,
    ignore_balance: bool,
    abort_stats: &mut HashMap<String, U256>,
    filter_addr: Option<String>,
) {
    let mut balances = HashMap::new();
    let mut storages = HashMap::new();

    let mut num_aborted_txs_in_block = 0;

    for tx in txs {
        let TransactionInfo { tx_hash, accesses } = tx;

        let mut tx_aborted = false;
        let mut tx_aborted_by = HashSet::new();

        // go through accesses without enacting the changes,
        // just checking conflicts
        for access in &accesses {
            match &access.target {
                Target::Balance(addr) => {
                    // ignore balance conflicts
                    if ignore_balance {
                        continue;
                    }

                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if addr != a {
                            continue;
                        }
                    }

                    // no conflict
                    if !balances.contains_key(addr) {
                        continue;
                    }

                    tx_aborted = true;
                    tx_aborted_by.insert(addr.clone());

                    if mode == OutputMode::Detailed {
                        let mode = match access.mode {
                            AccessMode::Read => "read",
                            AccessMode::Write => "write",
                        };

                        println!("    abort on {} balance({:?})", mode, addr);
                        println!("        1st: {:?}", balances[addr]);
                        println!("        2nd: {:?}", tx_hash);
                    }
                }
                Target::Storage(addr, entry) => {
                    // skip if we filter for a different address
                    if let Some(ref a) = filter_addr {
                        if addr != a {
                            continue;
                        }
                    }

                    let key = (addr.clone(), entry.clone());

                    // no conflict
                    if !storages.contains_key(&key) {
                        continue;
                    }

                    tx_aborted = true;
                    tx_aborted_by.insert(addr.clone());

                    if mode == OutputMode::Detailed {
                        let mode = match access.mode {
                            AccessMode::Read => "read",
                            AccessMode::Write => "write",
                        };

                        println!("    abort on {} storage({:?}, {:?})", mode, key.0, key.1);
                        println!("        1st: {:?}", storages[&key]);
                        println!("        2nd: {:?}", tx_hash);
                    }
                }
            }
        }

        // enact changes
        for access in accesses.into_iter().filter(|a| a.mode == AccessMode::Write) {
            match access.target {
                Target::Balance(addr) => {
                    balances.insert(addr, tx_hash.clone());
                }
                Target::Storage(addr, entry) => {
                    storages.insert((addr, entry), tx_hash.clone());
                }
            }
        }

        if tx_aborted {
            num_aborted_txs_in_block += 1;

            // TODO: get gas for the whole block?
            let gas = retrieve_gas(web3, &tx_hash[..])
                .await
                .expect(&format!("Unable to retrieve gas (1) {}", tx_hash)[..])
                .expect(&format!("Unable to retrieve gas (2) {}", tx_hash)[..]);

            for addr in tx_aborted_by {
                let entry = abort_stats.entry(addr).or_insert(0.into());
                *entry = entry.saturating_add(gas);
            }
        }
    }

    match mode {
        OutputMode::Normal | OutputMode::Detailed => {
            println!(
                "Num aborts in block #{}: {}\n",
                block, num_aborted_txs_in_block
            );
        }
        OutputMode::Csv => {
            println!("{},{}", block, num_aborted_txs_in_block);
        }
    }
}

async fn process_block_aborts2(
    web3: &Web3,
    block: u64,
    txs: Vec<TransactionInfo>,
    mode: OutputMode,
) {
    let mut storages = HashMap::new();
    let mut serial_gas_cost = U256::from(0);
    let mut parallel_gas_cost = U256::from(0);
    let mut max_gas_cost_of_parallel_txs = U256::from(0);

    let mut tx_aborted = false;

    // retrieve all gas costs  before processing block
    let tx_gas = retrieve_gas_parity(web3, block)
        // let gas = retrieve_gas_parallel(web3, txs.iter().map(|tx| tx.tx_hash.clone()))
        .await
        .expect(&format!("Error while collecting gas for block #{}", block)[..]);

    assert_eq!(txs.len(), tx_gas.len());

    for (id, tx) in txs.iter().enumerate() {
        let TransactionInfo { tx_hash, accesses } = tx;
        let gas = tx_gas[id];

        // for serial execution, we simply add up all gas costs
        serial_gas_cost += gas;

        // go through accesses without enacting the changes,
        // just checking conflicts
        for acc in accesses {
            // ignore balance for now
            if let Target::Storage(addr, entry) = &acc.target {
                if storages.contains_key(&(addr, entry)) {
                    tx_aborted = true;
                    break;
                }
            }
        }

        // enact changes
        for acc in accesses {
            // ignore balance for now
            if let Target::Storage(addr, entry) = &acc.target {
                storages.insert((addr, entry), tx_hash);
            }
        }

        if tx_aborted {
            // gas contributes to cost serial execution after parallel one
            parallel_gas_cost += gas;
        } else {
            // gas contributed to cost of parallel execution
            if gas > max_gas_cost_of_parallel_txs {
                max_gas_cost_of_parallel_txs = gas;
            }
        }
    }

    parallel_gas_cost += max_gas_cost_of_parallel_txs;

    match mode {
        OutputMode::Normal | OutputMode::Detailed => {
            println!(
                "Gas cost of block #{}: {} --> {}\n",
                block, serial_gas_cost, parallel_gas_cost,
            );
        }
        OutputMode::Csv => {
            println!("{},{},{}", block, serial_gas_cost, parallel_gas_cost);
        }
    }
}

async fn process_block_aborts3(web3: &'static Web3, block: u64, txs: Vec<TransactionInfo>) -> U256 {
    // retrieve all gas costs  before processing block
    let gas = retrieve_gas_parity(web3, block)
        // let gas = retrieve_gas_parallel(web3, txs.iter().map(|tx| tx.tx_hash.clone()))
        .await
        .expect(&format!("Error while collecting gas for block #{}", block)[..]);

    assert_eq!(txs.len(), gas.len());

    const BATCH_SIZE: usize = 4;

    let mut next_to_process = std::cmp::min(BATCH_SIZE, txs.len()); // 4
    let mut batch = (0..next_to_process).collect::<Vec<_>>(); // [0, 1, 2, 3]
    let mut gas_cost = U256::from(0);

    loop {
        // exit condition: nothing left to process
        if batch.is_empty() {
            assert!(next_to_process == txs.len());
            break;
        }

        // cost of batch is the maximum gas cost in this batch
        let cost_of_batch = batch
            .iter()
            .map(|id| gas[*id])
            .max()
            .expect("batch not empty");

        gas_cost += cost_of_batch;

        // process batch
        let mut storages = HashMap::new(); // start with clear storage (!)
        let mut aborted: Vec<usize> = vec![];

        for id in batch {
            let TransactionInfo { tx_hash, accesses } = &txs[id];

            // detect aborts
            for acc in accesses {
                // ignore balance for now
                if let Target::Storage(addr, entry) = &acc.target {
                    if storages.contains_key(&(addr, entry)) {
                        aborted.push(id);
                    }
                }
            }

            // enact updates
            for acc in accesses {
                // ignore balance for now
                if let Target::Storage(addr, entry) = &acc.target {
                    storages.insert((addr, entry), tx_hash);
                }
            }
        }

        while aborted.len() < BATCH_SIZE && next_to_process < txs.len() {
            aborted.push(next_to_process);
            next_to_process += 1;
        }

        batch = aborted;
    }

    gas_cost
}

async fn process_aborts(db: &DB, web3: &Web3, blocks: impl Iterator<Item = u64>, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,aborts");
    }

    let mut abort_stats = HashMap::new();

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
            web3,
            block,
            tx_infos,
            mode,
            /* ignore_balance = */ true,
            &mut abort_stats,
            /* filter_addr = */ None,
        )
        .await;
    }

    // let mut counts = abort_stats.into_iter().collect::<Vec<_>>();
    // counts.sort_by(|&(_, a), &(_, b)| a.cmp(&b).reverse());

    // for ii in 0..20 {
    //     if ii >= counts.len() {
    //         break;
    //     }

    //     println!("#{}: {} ({} aborts)", ii, counts[ii].0, counts[ii].1);
    // }
}

async fn process_aborts2(
    db: &DB,
    web3: &Web3,
    blocks: impl Iterator<Item = u64>,
    mode: OutputMode,
) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,serial_gas_cost,parallel_gas_cost");
    }

    for block in blocks {
        let tx_infos = tx_infos_from_db(&db, block);

        if matches!(mode, OutputMode::Normal | OutputMode::Detailed) {
            println!(
                "Checking aborts in block #{} ({} txs)...",
                block,
                tx_infos.len(),
            );
        }

        process_block_aborts2(web3, block, tx_infos, mode).await;
    }
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    let transport = web3::transports::Http::new("http://localhost:8545")?;
    // let transport = web3::transports::Http::new(
    //     "https://mainnet.infura.io/v3/c15ab95c12d441d19702cb4a0d1313e7",
    // )?;
    let web3 = web3::Web3::new(transport);

    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 6 {
        println!("Usage: evm-trace-extract [db-path:str] [from-block:int] [to-block:int] [mode:pairwise|aborts] [output:normal|detailed|csv]");
        return Ok(());
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
        return Ok(());
    }

    // process
    match mode {
        "pairwise" => process_pairwise(&db, from..=to, output),
        "aborts" => process_aborts2(&db, &web3, from..=to, output).await,
        _ => {
            println!("mode should be one of: pairwise, aborts");
            return Ok(());
        }
    }

    Ok(())
}
