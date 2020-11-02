extern crate regex;
extern crate rocksdb;
extern crate web3;

use common::*;

mod depgraph;
mod occ;

use futures::{stream, StreamExt};
use rocksdb::DB;
use std::env;
use web3::{transports, types::U256, Web3 as Web3Generic};

type Web3 = Web3Generic<transports::Http>;

async fn occ_detailed_stats(db: &DB, _web3: &Web3, from: u64, to: u64) {
    // print csv header
    println!("block,num_txs,num_aborted,serial_gas_cost,pool_t_2,pool_t_4,pool_t_8,pool_t_16,pool_t_all,optimal_t_2,optimal_t_4,optimal_t_8,optimal_t_16,optimal_t_all");

    // stream RPC results
    // let others = stream::iter(from..=to)
    //     .map(|b| {
    //         let web3_clone = web3.clone();

    //         let a = tokio::spawn(async move {
    //             rpc::gas_parity(&web3_clone, b)
    //                 .await
    //                 .expect("parity_getBlockReceipts RPC should succeed")
    //         });

    //         let web3_clone = web3.clone();

    //         let b = tokio::spawn(async move {
    //             rpc::tx_infos(&web3_clone, b)
    //                 .await
    //                 .expect("eth_getBlock RPC should succeed")
    //                 .expect("block should exist")
    //         });

    //         future::join(a, b).map(|(a, b)| (a.expect("future OK"), b.expect("future OK")))
    //     })
    //     .buffered(10);

    let rpc_db = db::RpcDb::open("./_rpc_db").expect("db open succeeds");

    let others = stream::iter(from..=to).map(|block| {
        let gas = rpc_db
            .gas_used(block)
            .expect("get from db succeeds")
            .expect("block exists in db");

        let info = rpc_db
            .tx_infos(block)
            .expect("get from db succeeds")
            .expect("block exists in db");

        (gas, info)
    });

    let blocks = stream::iter(from..=to);
    let mut it = blocks.zip(others);

    // simulate OCC for each block
    while let Some((block, (gas, info))) = it.next().await {
        let txs = db::tx_infos(&db, block, &info);

        assert_eq!(txs.len(), gas.len());
        assert_eq!(txs.len(), info.len());

        let serial = gas.iter().fold(U256::from(0), |acc, item| acc + item);
        let num_txs = txs.len();
        let num_aborted = occ::num_aborts(&txs);

        let simulate = |num_threads| {
            occ::thread_pool(
                &txs,
                &gas,
                &info,
                num_threads,
                false, // allow_ignore_slots
                false, // allow_avoid_conflicts_during_scheduling
                false, // allow_read_from_uncommitted
            )
        };

        let pool_t_2_q_0 = simulate(2);
        let pool_t_4_q_0 = simulate(4);
        let pool_t_8_q_0 = simulate(8);
        let pool_t_16_q_0 = simulate(16);
        let pool_t_all_q_0 = simulate(txs.len());

        let optimal_t_2 = depgraph::cost(&txs, &gas, 2);
        let optimal_t_4 = depgraph::cost(&txs, &gas, 4);
        let optimal_t_8 = depgraph::cost(&txs, &gas, 8);
        let optimal_t_16 = depgraph::cost(&txs, &gas, 16);
        let optimal_t_all = depgraph::cost(&txs, &gas, txs.len());

        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            block,
            num_txs,
            num_aborted,
            serial,
            pool_t_2_q_0,
            pool_t_4_q_0,
            pool_t_8_q_0,
            pool_t_16_q_0,
            pool_t_all_q_0,
            optimal_t_2,
            optimal_t_4,
            optimal_t_8,
            optimal_t_16,
            optimal_t_all,
        );
    }
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    env_logger::builder()
        .format_timestamp(None)
        .format_level(false)
        .format_module_path(false)
        .init();

    let transport = web3::transports::Http::new("http://localhost:8545")?;
    let web3 = web3::Web3::new(transport);

    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        println!("Usage: evm-trace-extract [db-path:str] [from-block:int] [to-block:int]");
        return Ok(());
    }

    let path = &args[1][..];

    let from = args[2]
        .parse::<u64>()
        .expect("from-block should be a number");

    let to = args[3].parse::<u64>().expect("to-block should be a number");

    // open db
    let db = db::open_traces(path);

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
    occ_detailed_stats(&db, &web3, from, to).await;

    Ok(())
}
