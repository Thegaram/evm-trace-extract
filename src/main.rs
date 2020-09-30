#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate rocksdb;
extern crate web3;

mod db;
mod occ;
mod output_mode;
mod pairwise;
mod rpc;
mod stats;
mod transaction_info;

use futures::{stream, StreamExt};
use output_mode::OutputMode;
use rocksdb::DB;
use std::collections::{HashMap, HashSet};
use std::env;
use transaction_info::{AccessMode, Target, TransactionInfo};
use web3::{transports, types::U256, Web3 as Web3Generic};

type Web3 = Web3Generic<transports::Http>;

fn process_pairwise(db: &DB, blocks: impl Iterator<Item = u64>, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,conflicts,balance,storage");
    }

    // process blocks
    for block in blocks {
        let tx_infos = db::tx_infos(&db, block);

        if matches!(mode, OutputMode::Normal | OutputMode::Detailed) {
            println!(
                "Checking pairwise conflicts in block #{} ({} txs)...",
                block,
                tx_infos.len(),
            );
        }

        pairwise::process(block, tx_infos, mode);
    }
}

#[allow(dead_code)]
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
            let gas = rpc::gas(web3, &tx_hash[..])
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
        _ => {}
    }
}

#[allow(dead_code)]
async fn process_aborts(db: &DB, web3: &Web3, blocks: impl Iterator<Item = u64>, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,aborts");
    }

    let mut abort_stats = HashMap::new();

    for block in blocks {
        let tx_infos = db::tx_infos(&db, block);

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

async fn occ_detailed_stats(db: &DB, web3: &Web3, from: u64, to: u64, mode: OutputMode) {
    // print csv header if necessary
    if mode == OutputMode::Csv {
        println!("block,num_txs,num_aborted,serial_gas_cost,parallel_gas_cost,batch_2,batch_4,batch_8,batch_16,batch_all,pool_2,pool_4,pool_8,pool_16,pool_all");
    }

    // construct async streams for blocks and tx receipts
    let blocks = stream::iter(from..=to);
    let gases = rpc::gas_parity_parallel(&web3, from..=to);
    let mut both = blocks.zip(gases);

    let mut stats: HashMap<&str, HashMap<String, (u64, U256)>> = Default::default();

    let stat_targets = vec!["batch-2", "batch-4", "batch-8", "batch-16", "batch-all", "pool-2", "pool-4", "pool-8", "pool-16", "pool-all"];

    for target in &stat_targets {
        stats.insert(target, Default::default());
    }

    // process blocks one by one
    while let Some((block, gas)) = both.next().await {
        let txs = db::tx_infos(&db, block);
        assert_eq!(txs.len(), gas.len());

        let num_txs = txs.len();
        let serial = gas.iter().fold(U256::from(0), |acc, item| acc + item);
        let num_aborted = occ::num_aborts(&txs);

        let parallel = occ::parallel_then_serial(&txs, &gas);

        let batch_2 = occ::batches(&txs, &gas, 2, &mut stats.get_mut("batch-2").unwrap());
        let batch_4 = occ::batches(&txs, &gas, 4, &mut stats.get_mut("batch-4").unwrap());
        let batch_8 = occ::batches(&txs, &gas, 8, &mut stats.get_mut("batch-8").unwrap());
        let batch_16 = occ::batches(&txs, &gas, 16, &mut stats.get_mut("batch-16").unwrap());
        let batch_all = occ::batches(&txs, &gas, txs.len(), &mut stats.get_mut("batch-all").unwrap());

        let pool_2 = occ::thread_pool(&txs, &gas, 2, &mut stats.get_mut("pool-2").unwrap());
        let pool_4 = occ::thread_pool(&txs, &gas, 4, &mut stats.get_mut("pool-4").unwrap());
        let pool_8 = occ::thread_pool(&txs, &gas, 8, &mut stats.get_mut("pool-8").unwrap());
        let pool_16 = occ::thread_pool(&txs, &gas, 16, &mut stats.get_mut("pool-16").unwrap());
        let pool_all = occ::thread_pool(&txs, &gas, txs.len(), &mut stats.get_mut("pool-all").unwrap());

        if mode == OutputMode::Csv {
            println!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                block,
                num_txs,
                num_aborted,
                serial,
                parallel,
                batch_2,
                batch_4,
                batch_8,
                batch_16,
                batch_all,
                pool_2,
                pool_4,
                pool_8,
                pool_16,
                pool_all,
            );
        }
    }

    // print overall stats
    for target in stat_targets {
        let stats = &stats[target];

        let mut counts = stats.iter().collect::<Vec<_>>();

        // sort based on number of aborts
        counts.sort_by(|&(_, (n_a, _)), &(_, (n_b, _))| n_a.cmp(&n_b).reverse());

        println!("\n\n{}, number of aborts:", target);

        for ii in 0..11 {
            if ii >= counts.len() {
                break;
            }

            println!("    #{}: {} ({} aborts, ~{:.2}%)", ii, counts[ii].0, (counts[ii].1).0, 100.0 * ((counts[ii].1).0 as f64 / stats["total"].0 as f64));
        }

        // sort based on weighted aborts
        counts.sort_by(|&(_, (_, g_a)), &(_, (_, g_b))| g_a.cmp(&g_b).reverse());

        println!("\n{}, aborted gas:", target);

        for ii in 0..11 {
            if ii >= counts.len() {
                break;
            }

            println!("    #{}: {} ({} gas)", ii, counts[ii].0, (counts[ii].1).1);
        }
    }
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    let transport = web3::transports::Http::new("http://localhost:8545")?;
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
    let db = db::open(path);

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
        "aborts" => occ_detailed_stats(&db, &web3, from, to, output).await,
        _ => {
            println!("mode should be one of: pairwise, aborts");
            return Ok(());
        }
    }

    Ok(())
}
