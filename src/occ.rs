use crate::transaction_info::{AccessMode, Target, TransactionInfo};
use std::{cmp::min, collections::HashSet};
use web3::types::U256;

// Estimate number of aborts (due to conflicts) in block.
// The actual number can be lower if we process transactions in batches,
//      e.g. with batches of size 2, tx-1's write will not affect tx-3's read.
// The actual number can also be higher because the same transaction could be aborted multiple times,
//      e.g. with batch [tx-1, tx-2, tx-3], tx-2's abort will make tx-3 abort as well,
//      then in the next batch [tx-2, tx-3, tx-4] tx-3 might be aborted again if it reads a slot written by tx-2.
pub fn num_aborts(txs: &Vec<TransactionInfo>) -> u64 {
    let mut num_aborted = 0;

    // keep track of which storage entries were written
    let mut storages = HashSet::new();

    for tx in txs {
        let TransactionInfo { accesses, .. } = tx;

        // check for conflicts without committing changes
        // a conflict is when tx-b reads a storage entry written by tx-a
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
            if let Target::Storage(addr, entry) = &acc.target {
                if storages.contains(&(addr, entry)) {
                    num_aborted += 1;
                    break;
                }
            }
        }

        // commit changes
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
            if let Target::Storage(addr, entry) = &acc.target {
                storages.insert((addr, entry));
            }
        }
    }

    num_aborted
}

// First execute all transaction in parallel (infinite threads), then re-execute aborted ones serially.
// Note that this is inaccuare in practice: if tx-1 and tx-3 succeed and tx-2 aborts, we will have to
// re-execute both tx-2 and tx-3, as tx-2's new storage access patterns might make tx-3 abort this time.
pub fn parallel_then_serial(txs: &Vec<TransactionInfo>, gas: &Vec<U256>) -> U256 {
    assert_eq!(txs.len(), gas.len());

    // keep track of which storage entries were written
    let mut storages = HashSet::new();

    // parallel gas cost is the cost of parallel execution (max gas cost)
    // + sum of gas costs for aborted txs
    let parallel_cost = gas.iter().max().cloned().unwrap_or(U256::from(0));
    let mut serial_cost = U256::from(0);

    for (id, tx) in txs.iter().enumerate() {
        let TransactionInfo { accesses, .. } = tx;

        // check for conflicts without committing changes
        // a conflict is when tx-b reads a storage entry written by tx-a
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
            if let Target::Storage(addr, entry) = &acc.target {
                if storages.contains(&(addr, entry)) {
                    serial_cost += gas[id];
                    break;
                }
            }
        }

        // commit changes
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
            if let Target::Storage(addr, entry) = &acc.target {
                storages.insert((addr, entry));
            }
        }
    }

    parallel_cost + serial_cost
}

// Process transactions in fixed-size batches.
// We assume a batch's execution cost is (proportional to) the largest gas cost in that batch.
// If the n'th transaction in a batch aborts (detected on commit), we will re-execute all transactions after (and including) n.
// Note that in this scheme, we wait for all txs in a batch before starting the next one, resulting in thread under-utilization.
pub fn batches(txs: &Vec<TransactionInfo>, gas: &Vec<U256>, batch_size: usize) -> U256 {
    assert_eq!(txs.len(), gas.len());

    let mut next = min(batch_size, txs.len()); // e.g. 4
    let mut batch = (0..next).collect::<Vec<_>>(); // e.g. [0, 1, 2, 3]
    let mut cost = U256::from(0);

    loop {
        // exit condition: nothing left to process
        if batch.is_empty() {
            assert_eq!(next, txs.len());
            break;
        }

        // cost of batch is the maximum gas cost in this batch
        let cost_of_batch = batch
            .iter()
            .map(|id| gas[*id])
            .max()
            .expect("batch not empty");

        cost += cost_of_batch;

        // keep track of which storage entries were written
        // start with clear storage for each batch!
        // i.e. txs will not abort due to writes in previous batches
        let mut storages = HashSet::new();

        // process batch
        'outer: for id in batch {
            let TransactionInfo { accesses, .. } = &txs[id];

            // check for conflicts without committing changes
            // a conflict is when tx-b reads a storage entry written by tx-a
            for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
                if let Target::Storage(addr, entry) = &acc.target {
                    if storages.contains(&(addr, entry)) {
                        // e.g. if our batch is [0, 1, 2, 3]
                        // and we detect a conflict while committing `2`,
                        // then the next batch is [2, 3, 4, 5]
                        // because the outdated value read by `2` might affect `3`

                        next = id;
                        break 'outer;
                    }
                }
            }

            // commit updates
            for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
                if let Target::Storage(addr, entry) = &acc.target {
                    storages.insert((addr, entry));
                }
            }
        }

        // prepare next batch
        batch = vec![];

        while batch.len() < batch_size && next < txs.len() {
            batch.push(next);
            next += 1;
        }
    }

    cost
}
