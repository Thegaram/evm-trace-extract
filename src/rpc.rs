use futures::{stream, StreamExt};
use tokio::task::JoinError;
use web3::{transports, types::TransactionReceipt, types::U256, Transport, Web3 as Web3Generic};

type Web3 = Web3Generic<transports::Http>;

// retrieve tx gas using `eth_getTransactionReceipt`
pub async fn retrieve_gas(web3: &Web3, tx_hash: &str) -> Result<Option<U256>, web3::Error> {
    let tx_hash = tx_hash
        .trim_start_matches("0x")
        .parse()
        .expect("Unable to parse tx-hash");

    let gas = web3
        .eth()
        .transaction_receipt(tx_hash)
        .await?
        .and_then(|tx| tx.gas_used);

    Ok(gas)
}

// retrieve block tx gases using `eth_getTransactionReceipt`
#[allow(dead_code)]
pub async fn retrieve_gas_parallel(
    web3: &Web3,
    hashes: impl Iterator<Item = String>,
) -> Result<Vec<U256>, JoinError> {
    // create async tasks, one for each tx hash
    let tasks = hashes.map(|tx| {
        // clone so that we can move into async block
        // this should not be expensive
        let web3 = web3.clone();

        let fut = async move {
            match retrieve_gas(&web3, &tx[..]).await {
                Err(e) => panic!(format!("Failed to retrieve gas for {}: {}", tx, e)),
                Ok(None) => panic!(format!("Failed to retrieve gas for {}: None", tx)),
                Ok(Some(g)) => g,
            }
        };

        tokio::spawn(fut)
    });

    stream::iter(tasks)
        .buffered(20) // execute in parallel in batches of 8
        .collect::<Vec<_>>()
        .await // wait for all requests to complete
        .into_iter()
        .collect() // convert Vec<Result<_>> to Result<Vec<_>>
}

// retrieve block tx gases using `parity_getBlockReceipts`
// this should be faster than `retrieve_gas_parallel`
pub async fn retrieve_gas_parity(web3: &Web3, block: u64) -> web3::Result<Vec<U256>> {
    // convert block number to hex
    let block = format!("0x{:x}", block).into();

    // call RPC
    let raw = web3
        .transport()
        .execute("parity_getBlockReceipts", vec![block])
        .await?;

    // parse response
    let gas = serde_json::from_value::<Vec<TransactionReceipt>>(raw)?
        .into_iter()
        .map(|r| r.gas_used.expect("Receipt should contain `gas_used`"))
        .collect();

    Ok(gas)
}
