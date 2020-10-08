#[derive(Default)]
pub struct TxPairStats<'a> {
    pub tx_a_hash: &'a str,
    pub tx_b_hash: &'a str,
    pub balance_rw: i32,
    pub balance_ww: i32,
    pub storage_rw: i32,
    pub storage_ww: i32,
}

impl<'a> TxPairStats<'a> {
    pub fn new<'b>(tx_a_hash: &'b str, tx_b_hash: &'b str) -> TxPairStats<'b> {
        let mut stats = TxPairStats::default();
        stats.tx_a_hash = tx_a_hash;
        stats.tx_b_hash = tx_b_hash;
        stats
    }

    pub fn num_balance_conflicts(&self) -> i32 {
        self.balance_rw + self.balance_ww
    }

    pub fn num_storage_conflicts(&self) -> i32 {
        self.storage_rw + self.storage_ww
    }

    pub fn num_conflicts(&self) -> i32 {
        self.num_balance_conflicts() + self.num_storage_conflicts()
    }

    pub fn has_balance_conflict(&self) -> bool {
        self.num_balance_conflicts() > 0
    }

    pub fn has_storage_conflict(&self) -> bool {
        self.num_storage_conflicts() > 0
    }

    pub fn has_conflict(&self) -> bool {
        self.num_conflicts() > 0
    }
}

impl<'a> std::fmt::Debug for TxPairStats<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{} - {}: b = {} ({}/{}), s = {} ({}/{})",
            self.tx_a_hash,
            self.tx_b_hash,
            self.num_balance_conflicts(),
            self.balance_rw,
            self.balance_ww,
            self.num_storage_conflicts(),
            self.storage_rw,
            self.storage_ww,
        ))
    }
}

#[derive(Default)]
pub struct BlockStats {
    pub block_number: u64,
    pub conflicting_pairs_balance: i32,
    pub conflicting_pairs_storage: i32,
    pub balance_conflicts: i32,
    pub storage_conflicts: i32,
}

impl BlockStats {
    pub fn new(block_number: u64) -> BlockStats {
        let mut stats = BlockStats::default();
        stats.block_number = block_number;
        stats
    }

    pub fn accumulate(&mut self, stats: &TxPairStats) {
        self.conflicting_pairs_balance += stats.has_balance_conflict() as i32;
        self.conflicting_pairs_storage += stats.has_storage_conflict() as i32;
        self.balance_conflicts += stats.num_balance_conflicts();
        self.storage_conflicts += stats.num_storage_conflicts();
    }

    pub fn num_conflicting_pairs(&self) -> i32 {
        self.conflicting_pairs_balance + self.conflicting_pairs_storage
    }

    pub fn num_conflicts(&self) -> i32 {
        self.balance_conflicts + self.storage_conflicts
    }

    pub fn has_conflicts(&self) -> bool {
        self.num_conflicts() > 0
    }
}

impl std::fmt::Debug for BlockStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "number of conflicting tx pairs in block #{}: {} ({}/{}) (#conflicts = {} ({}/{}))",
            self.block_number,
            self.num_conflicting_pairs(),
            self.conflicting_pairs_balance,
            self.conflicting_pairs_storage,
            self.num_conflicts(),
            self.balance_conflicts,
            self.storage_conflicts,
        ))
    }
}
