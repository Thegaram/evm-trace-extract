use common::*;

mod depgraph;
mod occ;

use futures::{future, stream, FutureExt, StreamExt};
use rocksdb::DB;
use rustop::opts;
use web3::types::U256;

// define a "trait alias" (see https://www.worthe-it.co.za/blog/2017-01-15-aliasing-traits-in-rust.html)
trait BlockDataStream: stream::Stream<Item = (u64, (Vec<U256>, Vec<rpc::TxInfo>))> {}
impl<T> BlockDataStream for T where T: stream::Stream<Item = (u64, (Vec<U256>, Vec<rpc::TxInfo>))> {}

async fn occ_detailed_stats(
    trace_db: &DB,
    batch_size: usize,
    stream: impl BlockDataStream + Unpin,
) {
    println!(
        "block,num_txs,num_conflicts,serial_gas_cost,optimal_t_32_no_deps,optimal_t_32_counter_1,optimal_t_32_counter_10,optimal_t_32_counter_30"
    );

    let mut stream = stream.chunks(batch_size);

    while let Some(batch) = stream.next().await {
        let mut blocks = vec![];
        let mut txs = vec![];
        let mut gas = vec![];
        let mut info = vec![];

        for (block, (batch_gas, batch_info)) in batch {
            blocks.push(block);
            txs.extend(db::tx_infos(&trace_db, block, &batch_info).into_iter());
            gas.extend(batch_gas.into_iter());
            info.extend(batch_info.into_iter());
        }

        assert_eq!(txs.len(), gas.len());
        assert_eq!(txs.len(), info.len());

        let num_txs = txs.len();
        let num_conflicts = occ::num_conflicts(&txs);
        let serial = gas.iter().fold(U256::from(0), |acc, item| acc + item);

        let graph = depgraph::DependencyGraph::no_deps();
        let optimal_t_32_no_deps = graph.cost(&gas, 32);

        let graph = depgraph::DependencyGraph::simple(&txs, &info);
        let optimal_t_32_counter_1 = graph.cost(&gas, 32);

        let graph = depgraph::DependencyGraph::with_sharding(&txs, &info, 10);
        let optimal_t_32_counter_10 = graph.cost(&gas, 32);

        let graph = depgraph::DependencyGraph::with_sharding(&txs, &info, 30);
        let optimal_t_32_counter_30 = graph.cost(&gas, 32);

        let block = blocks
            .into_iter()
            .map(|b| b.to_string())
            .collect::<Vec<String>>()
            .join("-");

        println!(
            "{},{},{},{},{},{},{},{}",
            block,
            num_txs,
            num_conflicts,
            serial,
            optimal_t_32_no_deps,
            optimal_t_32_counter_1,
            optimal_t_32_counter_10,
            optimal_t_32_counter_30,
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
    let rpc_db = db::RpcDb::open_for_read_only(db_path).expect("db open succeeds");

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
    // parse args
    let (args, _) = opts! {
        opt from:u64, desc:"Process from this block number.";
        opt to:u64, desc:"Process up to (and including) this block number.";
        opt traces:String, desc:"Path to trace DB.";
        opt rpc_db:Option<String>, desc:"Path to RPC DB (optional).";
        opt rpc_provider:Option<String>, desc:"RPC provider URL (optional).";
        opt batch_size:usize=1, desc:"Size of block batch (optional).";
    }
    .parse_or_exit();

    if args.rpc_db.is_none() && args.rpc_provider.is_none() {
        println!("Error: you need to specify one of '--rpc-db' and '--rpc-provider'.");
        println!("Try --help for help.");
        return Ok(());
    }

    // open db and validate args
    let trace_db = db::open_traces(&args.traces);

    let latest_raw = trace_db
        .get(b"latest")
        .expect("get latest should succeed")
        .expect("latest should exist");

    let latest = std::str::from_utf8(&latest_raw[..])
        .expect("parse to string succeed")
        .parse::<u64>()
        .expect("parse to int should succees");

    if args.to > latest {
        println!("Latest header in trace db: #{}", latest);
        return Ok(());
    }

    // initialize logger
    env_logger::builder()
        .format_timestamp(None)
        .format_level(false)
        .format_module_path(false)
        .init();

    // process all blocks in range
    match (args.rpc_db, args.rpc_provider) {
        (Some(rpc_db), _) => {
            let stream = stream_from_db(&rpc_db, args.from, args.to);
            occ_detailed_stats(&trace_db, args.batch_size, stream).await;
        }
        (_, Some(rpc_provider)) => {
            let stream = stream_from_rpc(&rpc_provider, args.from, args.to)?;
            occ_detailed_stats(&trace_db, args.batch_size, stream).await;
        }
        _ => unreachable!(),
    };

    Ok(())
}
