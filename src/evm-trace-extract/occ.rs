use crate::rpc;
use crate::transaction_info::{Access, AccessMode, Target, TransactionInfo};
use std::cmp::{min, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::convert::TryFrom;
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

pub fn thread_pool(
    txs: &Vec<TransactionInfo>,
    gas: &Vec<U256>,
    info: &Vec<rpc::TxInfo>,
    num_threads: usize,
    max_queued_per_thread: usize,
    min_gas_for_queue: U256,
) -> U256 {
    assert_eq!(txs.len(), gas.len());

    #[allow(unused_mut)]
    let mut ignored_slots: HashSet<&str> = Default::default();
    // ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x000000000000000000000000000000000000000000000000000000000000000f");
    // ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x0000000000000000000000000000000000000000000000000000000000000006");
    // ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182");

    type MinHeap<T> = BinaryHeap<Reverse<T>>;

    // transaction queue: transactions waiting to be executed
    // item: <transaction-id>
    // pop() always returns the lowest transaction-id
    let mut tx_queue: MinHeap<usize> = (0..txs.len()).map(Reverse).collect();

    // per thread transaction queue: transactions schedule for specific threads waiting to be executed
    // item in each queue: <transaction-id>
    // pop() always returns the lowest transaction-id
    let mut per_thread_tx_queue: Vec<MinHeap<usize>> = vec![Default::default(); num_threads];

    // commit queue: txs that finished execution and are waiting to commit
    // item: <transaction-id, storage-version>
    // pop() always returns the lowest transaction-id
    // storage-version is the highest committed transaction-id before the tx's execution started
    let mut commit_queue: MinHeap<(usize, i32)> = Default::default();

    // next transaction-id to commit
    let mut next_to_commit = 0;

    // thread pool: information on current execution on each thread
    // item: <transaction-id, gas-left, storage-version> or None if idle
    let mut threads: Vec<Option<(usize, U256, i32)>> = vec![None; num_threads];

    // overall cost of execution
    let mut cost = U256::from(0);

    // let mut num_iteration = 0;

    loop {
        // exit condition: we have committed all transactions
        if next_to_commit == txs.len() {
            // nothing left to execute or commit
            assert!(tx_queue.is_empty(), "tx queue not empty");
            assert!(commit_queue.is_empty(), "commit queue not empty");

            // all threads are idle
            assert!(
                threads.iter().all(|opt| opt.is_none()),
                "some threads are not idle"
            );

            break;
        }

        // scheduling: run transactions on idle threads
        let idle_threads = threads
            .iter()
            .enumerate()
            .filter(|(_, opt)| opt.is_none())
            .map(|(id, _)| id)
            .collect::<Vec<_>>();

        for thread_id in idle_threads {
            // try to schedule from its own queue first
            if let Some(Reverse(tx_id)) = per_thread_tx_queue[thread_id].pop() {
                let gas_left = gas[tx_id];
                let sv = next_to_commit as i32 - 1;
                threads[thread_id] = Some((tx_id, gas_left, sv));
                continue;
            }

            'assign_to_thread: loop {
                // otherwise, get tx from tx_queue
                let tx_id = match tx_queue.pop() {
                    Some(Reverse(id)) => id,
                    None => break,
                };

                // cheap transactions are scheduled directly on the current thread
                if info[tx_id].gas_limit < min_gas_for_queue {
                    let gas_left = gas[tx_id];
                    let sv = next_to_commit as i32 - 1;
                    threads[thread_id] = Some((tx_id, gas_left, sv));
                    break;
                }

                // check if there's any potentially conflicting tx running
                let receiver_matches =
                    |info0: &rpc::TxInfo, info1: &rpc::TxInfo| match (info0.to, info1.to) {
                        (Some(to0), Some(to1)) => to0 == to1,
                        _ => false,
                    };

                let conflicting_threads = threads
                    .iter()
                    .enumerate()
                    .filter(|(_, opt)| opt.is_some())
                    .map(|(thread_id, opt)| (thread_id, opt.unwrap()))
                    .filter(|(_, (other_tx, _, _))| {
                        receiver_matches(&info[tx_id], &info[*other_tx])
                    })
                    .map(|(thread_id, _)| thread_id);

                // and add this tx to the corresponding thread's queue
                for other_thread in conflicting_threads {
                    if per_thread_tx_queue[other_thread].len() < max_queued_per_thread {
                        per_thread_tx_queue[other_thread].push(Reverse(tx_id));
                        continue 'assign_to_thread;
                    }
                }

                // if there are no conflicting transactions running or all threads'
                // queues are full, we schedule the current transaction directly
                let gas_left = gas[tx_id];
                let sv = next_to_commit as i32 - 1;
                threads[thread_id] = Some((tx_id, gas_left, sv));
                break;
            }
        }

        // find transaction that finishes execution next
        let (thread_id, (tx_id, gas_step, sv)) = threads
            .iter()
            .enumerate()
            .filter(|(_, opt)| opt.is_some())
            .map(|(id, opt)| (id, opt.unwrap()))
            .min_by_key(|(_, (_, gas_left, _))| *gas_left)
            .expect("not all threads are idle");

        // finish executing tx, update thread states
        threads[thread_id] = None;
        commit_queue.push(Reverse((tx_id, sv)));
        cost += gas_step;

        for ii in 0..threads.len() {
            if let Some((_, gas_left, _)) = &mut threads[ii] {
                *gas_left -= gas_step;
            }
        }

        // process commits/aborts
        while let Some(Reverse((tx_id, _))) = commit_queue.peek() {
            // we must commit transactions in order
            if *tx_id != next_to_commit {
                assert!(*tx_id > next_to_commit);
                break;
            }

            let Reverse((tx_id, sv)) = commit_queue.pop().unwrap();

            // check all potentially conflicting transactions
            // e.g. if tx-3 was executed with sv = -1, it means that it cannot see writes by tx-0, tx-1, tx-2
            let conflict_from = usize::try_from(sv + 1).expect("sv + 1 should be non-negative");
            let conflict_to = tx_id;

            let accesses = &txs[tx_id].accesses;
            let mut aborted = false;

            'outer: for prev_tx in conflict_from..conflict_to {
                let concurrent = &txs[prev_tx].accesses;

                for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
                    if let Target::Storage(addr, entry) = &acc.target {
                        if concurrent.contains(&Access::storage_write(addr, entry)) {
                            if !ignored_slots.contains(&format!("{}-{}", addr, entry)[..]) {
                                aborted = true;
                                break 'outer;
                            }
                        }
                    }
                }
            }

            // commit transaction
            if !aborted {
                next_to_commit += 1;
                continue;
            }

            // re-schedule aborted tx
            tx_queue.push(Reverse(tx_id));
        }
    }

    cost
}
