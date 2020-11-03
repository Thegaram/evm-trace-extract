use crate::transaction_info::{Access, AccessMode, Target, TransactionInfo};
use std::collections::{HashMap, HashSet};
use web3::types::U256;

fn is_wr_conflict(first: &TransactionInfo, second: &TransactionInfo) -> bool {
    for acc in second
        .accesses
        .iter()
        .filter(|a| a.mode == AccessMode::Read)
    {
        if let Target::Storage(addr, entry) = &acc.target {
            if first.accesses.contains(&Access::storage_write(addr, entry)) {
                return true;
            }
        }
    }

    false
}

#[derive(Default)]
struct DependencyGraph {
    pub predecessors_of: HashMap<usize, Vec<usize>>,
    pub successors_of: HashMap<usize, Vec<usize>>,
}

impl DependencyGraph {
    pub fn from(txs: &Vec<TransactionInfo>) -> DependencyGraph {
        let mut predecessors_of = HashMap::<usize, Vec<usize>>::new();
        let mut successors_of = HashMap::<usize, Vec<usize>>::new();

        for first in 0..(txs.len().saturating_sub(1)) {
            for second in (first + 1)..txs.len() {
                if is_wr_conflict(&txs[first], &txs[second]) {
                    predecessors_of.entry(second).or_insert(vec![]).push(first);
                    successors_of.entry(first).or_insert(vec![]).push(second);
                }
            }
        }

        DependencyGraph {
            predecessors_of,
            successors_of,
        }
    }

    fn max_cost_from(&self, tx: usize, gas: &Vec<U256>, memo: &mut HashMap<usize, U256>) -> U256 {
        if let Some(result) = memo.get(&tx) {
            return result.clone();
        }

        if !self.successors_of.contains_key(&tx) {
            return gas[tx];
        }

        let max_of_successors = self.successors_of[&tx]
            .iter()
            .map(|succ| self.max_cost_from(*succ, gas, memo))
            .max()
            .unwrap_or(U256::from(0));

        let result = max_of_successors + gas[tx];
        memo.insert(tx, result);
        result
    }

    pub fn max_costs(&self, gas: &Vec<U256>) -> HashMap<usize, U256> {
        let mut memo = HashMap::new();

        (0..gas.len())
            .map(|tx| (tx, self.max_cost_from(tx, gas, &mut memo)))
            .collect()
    }

    pub fn cost(&self, gas: &Vec<U256>, num_threads: usize) -> U256 {
        let num_txs = gas.len();
        let max_cost_from = self.max_costs(gas);

        let mut threads: Vec<Option<(usize, U256)>> = vec![None; num_threads];
        let mut finished: HashSet<usize> = Default::default();

        let is_executing = |tx0: &usize, threads: &Vec<Option<_>>| {
            threads
                .iter()
                .filter_map(|opt| opt.map(|(tx1, _)| tx1))
                .find(|tx1| tx1 == tx0)
                .is_some()
        };

        let is_ready = |tx: &usize, finished: &HashSet<usize>| {
            self.predecessors_of
                .get(tx)
                .unwrap_or(&vec![])
                .iter()
                .all(|tx0| finished.contains(tx0))
        };

        let mut cost = U256::from(0);

        loop {
            // exit condition
            if finished.len() == num_txs {
                // all threads are idle
                assert!(threads.iter().all(Option::is_none));

                break;
            }

            // schedule txs on idle threads
            for thread_id in 0..threads.len() {
                if threads[thread_id].is_some() {
                    continue;
                }

                let maybe_tx = (0..num_txs)
                    .filter(|tx| !is_executing(tx, &threads))
                    .filter(|tx| !finished.contains(tx))
                    .filter(|tx| is_ready(tx, &finished))
                    .max_by_key(|tx| max_cost_from[tx]);

                let tx = match maybe_tx {
                    Some(tx) => tx,
                    None => break,
                };

                // println!("scheduling tx-{} on thread-{}", tx, thread_id);
                threads[thread_id] = Some((tx, gas[tx]));
            }

            // execute transaction
            let (thread_id, (tx, gas_step)) = threads
                .iter()
                .enumerate()
                .filter(|(_, opt)| opt.is_some())
                .map(|(id, opt)| (id, opt.unwrap()))
                .min_by_key(|(_, (_, gas))| gas.clone())
                .unwrap();

            // println!("finish executing tx-{} on thread-{}", tx, thread_id);

            threads[thread_id] = None;
            finished.insert(tx);
            cost += gas_step;

            // update gas costs
            for ii in 0..threads.len() {
                if let Some((_, gas_left)) = &mut threads[ii] {
                    *gas_left -= gas_step;
                }
            }
        }

        cost
    }
}

pub fn cost(txs: &Vec<TransactionInfo>, gas: &Vec<U256>, num_threads: usize) -> U256 {
    DependencyGraph::from(txs).cost(gas, num_threads)
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::{convert_args, hashmap};

    #[rustfmt::skip]
    #[test]
    fn test_cost() {
        let mut graph = DependencyGraph::default();

        // 0 (1) - 1 (1)
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (1)

        graph.predecessors_of = hashmap! { 1 => vec![0], 3 => vec![2], 4 => vec![3], 5 => vec![4], 7 => vec![6] };
        graph.successors_of = hashmap! { 0 => vec![1], 2 => vec![3], 3 => vec![4], 4 => vec![5], 6 => vec![7] };

        let gas = vec![1.into(); 8];
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 2, 1 => 1, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 2, 7 => 1 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 1 6 7
        // 2 3 4 5

        assert_eq!(graph.cost(&gas, 2), U256::from(4));

        // ----------------------------------------

        // 0 (1) - 1 (3)
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (3)

        let gas = vec![1, 3, 1, 1, 1, 1, 1, 3].into_iter().map(Into::into).collect();
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 4, 1 => 3, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 4, 7 => 3 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 1 1 1 2 4
        // 6 7 7 7 3 5

        assert_eq!(graph.cost(&gas, 2), U256::from(6));

        // ----------------------------------------

        // 0 (1) - 1 (1) \
        // 2 (1) - 3 (1) - 4 (1) - 5 (1)
        // 6 (1) - 7 (1) /

        graph.predecessors_of.insert(4, vec![1, 3, 7]);

        graph.successors_of.insert(1, vec![4]);
        graph.successors_of.insert(7, vec![4]);

        let gas = vec![U256::from(1); 8];
        let expected = convert_args!(values=Into::into, hashmap! ( 0 => 4, 1 => 3, 2 => 4, 3 => 3, 4 => 2, 5 => 1, 6 => 4, 7 => 3 ));

        assert_eq!(graph.max_costs(&gas), expected);

        // 0 6 1 4 5
        // 2 3 7

        assert_eq!(graph.cost(&gas, 2), U256::from(5));

        // ----------------------------------------

        let graph = DependencyGraph::default();

        // 0 (1)
        // 1 (1)
        // 2 (1)
        // 3 (1)
        // 5 (1)

        let gas = vec![U256::from(1); 5];

        assert_eq!(graph.cost(&gas, 4), U256::from(2));
    }
}
