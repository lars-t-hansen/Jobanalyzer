use std::collections::HashMap;

pub struct System {
    pub hostname: String,
    pub description: String,
    pub cpu_cores: usize,       // Accounts for hyperthreads, ie, a 4-core system w/ hyperthreads has the value 8 here
    pub mem_gb: usize,
    pub gpu_cards: usize,
    pub gpu_mem_gb: usize       // Total across all cards
}

// Returns a map from host name to config info, or an error message

pub fn read_from_json(_filename: &str) -> Result<HashMap<String, System>, String> {
    let mut m = HashMap::new();
    m.insert("ml8.hpc.uio.no".to_string(), 
             System {
                 hostname: "ml8.hpc.uio.no".to_string(),
                 description: "2x48 AMD EPYC 7642 (hyperthreaded), 4xNVIDIA A100 @ 40GB".to_string(),
                 cpu_cores: 192,
                 mem_gb: 1024,
                 gpu_cards: 4,
                 gpu_mem_gb: 40 * 4,
             });
    Ok(m)
}
