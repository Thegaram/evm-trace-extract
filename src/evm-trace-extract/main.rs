extern crate regex;
extern crate rocksdb;
extern crate web3;

use common::*;

mod depgraph;
mod occ;

use futures::{future, stream, FutureExt, StreamExt};
use rocksdb::DB;
use std::env;
use web3::types::U256;

// define a "trait alias" (see https://www.worthe-it.co.za/blog/2017-01-15-aliasing-traits-in-rust.html)
trait BlockDataStream: stream::Stream<Item = (u64, (Vec<U256>, Vec<rpc::TxInfo>))> {}
impl<T> BlockDataStream for T where T: stream::Stream<Item = (u64, (Vec<U256>, Vec<rpc::TxInfo>))> {}

async fn occ_detailed_stats(trace_db: &DB, mut stream: impl BlockDataStream + Unpin) {
    // print csv header
    println!("block,num_txs,num_conflicts,serial_gas_cost,pool_t_2,pool_t_4,pool_t_8,pool_t_16,pool_t_all,optimal_t_2,optimal_t_4,optimal_t_8,optimal_t_16,optimal_t_all");

    // simulate OCC for each block
    while let Some((block, (gas, info))) = stream.next().await {
        let txs = db::tx_infos(&trace_db, block, &info);

        assert_eq!(txs.len(), gas.len());
        assert_eq!(txs.len(), info.len());

        let serial = gas.iter().fold(U256::from(0), |acc, item| acc + item);
        let num_txs = txs.len();
        let num_conflicts = occ::num_conflicts(&txs);

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
            num_conflicts,
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

#[allow(dead_code)]
fn stream_from_rpc(provider: &str, from: u64, to: u64) -> web3::Result<impl BlockDataStream> {
    // connect to node
    let transport = web3::transports::Http::new(provider)?;
    let web3 = web3::Web3::new(transport);

    // stream RPC results
    let gas_and_infos = stream::iter(from..=to)
        .map(move |b| {
            let web3_clone = web3.clone();

            let gas = tokio::spawn(async move {
                rpc::gas_parity(&web3_clone, b)
                    .await
                    .expect("parity_getBlockReceipts RPC should succeed")
            });

            let web3_clone = web3.clone();

            let infos = tokio::spawn(async move {
                rpc::tx_infos(&web3_clone, b)
                    .await
                    .expect("eth_getBlock RPC should succeed")
                    .expect("block should exist")
            });

            future::join(gas, infos)
                .map(|(gas, infos)| (gas.expect("future OK"), infos.expect("future OK")))
        })
        .buffered(10);

    let blocks = stream::iter(from..=to);
    let stream = blocks.zip(gas_and_infos);
    Ok(stream)
}

#[allow(dead_code)]
fn stream_from_db(db_path: &str, from: u64, to: u64) -> impl BlockDataStream {
    let rpc_db = db::RpcDb::open(db_path).expect("db open succeeds");

    let gas_and_infos = stream::iter(from..=to).map(move |block| {
        let gas = rpc_db
            .gas_used(block)
            .expect(&format!("get gas #{} failed", block)[..])
            .expect(&format!("#{} not found in db", block)[..]);

        let info = rpc_db
            .tx_infos(block)
            .expect(&format!("get infos #{} failed", block)[..])
            .expect(&format!("#{} not found in db", block)[..]);

        (gas, info)
    });

    let blocks = stream::iter(from..=to);
    let stream = blocks.zip(gas_and_infos);
    stream
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    env_logger::builder()
        .format_timestamp(None)
        .format_level(false)
        .format_module_path(false)
        .init();

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
    let stream = stream_from_db("./_rpc_db", from, to);
    // let stream = stream_from_rpc("http://localhost:8545", from, to)?;

    occ_detailed_stats(&db, stream).await;

    Ok(())
}
