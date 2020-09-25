#[derive(Clone, Copy, Eq, PartialEq)]
pub enum OutputMode {
    Normal,
    Detailed,
    Csv,
    Unknown,
}

impl OutputMode {
    pub fn from_str(raw: &str) -> OutputMode {
        match raw {
            "normal" => OutputMode::Normal,
            "detailed" => OutputMode::Detailed,
            "csv" => OutputMode::Csv,
            x => {
                println!("Warning: Unknown output mode {}", x);
                OutputMode::Unknown
            }
        }
    }
}
