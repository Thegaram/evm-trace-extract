use crate::transaction_info::{AccessMode, Target, TransactionInfo};
use std::cmp::min;
use std::collections::HashSet;
use web3::types::U256;

pub fn occ_num_aborts(txs: &Vec<TransactionInfo>) -> u64 {
    let mut storages = HashSet::new();
    let mut num_aborted = 0;

    for tx in txs.iter() {
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

pub fn occ_parallel_then_serial(txs: &Vec<TransactionInfo>, gas: &Vec<U256>) -> U256 {
    assert_eq!(txs.len(), gas.len());

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

pub fn occ_batches(txs: &Vec<TransactionInfo>, gas: &Vec<U256>, batch_size: usize) -> U256 {
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

        // process batch
        // start with clear storage for each batch!
        let mut storages = HashSet::new();

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
