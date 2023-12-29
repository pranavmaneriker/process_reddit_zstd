mod constants;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use serde_json::{Value, Map};
use std::collections::HashMap;

fn compute_author_counts_simd(path: &str, progress: Option<bool>) -> HashMap<String, i64> {
    let mut author_counts: HashMap<String, i64> = HashMap::new(); 
    let print_progress = if progress.is_some() { progress.unwrap() } else { true };

    let file = File::open(path).expect("file not found");
    let mut zstd_reader = zstd::stream::read::Decoder::new(file).expect("zstd parsing error");
    let _ = zstd_reader.window_log_max(31).expect("Window log max too high");
    let zstd_bufreader = BufReader::new(zstd_reader);

    let mut linecount = 0;
    let start = Instant::now();

    for line in zstd_bufreader.lines() {
        let mut lw = line.unwrap();
        linecount += 1;
        //let _parsed_line: Map<String, Value> = serde_json::from_str(&lw).unwrap();
        let _parsed_line: Map<String, Value> = simd_json::serde::from_slice(unsafe { lw.as_bytes_mut() }).unwrap();
        match _parsed_line.get(constants::AUTHOR) {
            Some(author) => {
                let author_str = author.as_str().unwrap();
                let count = author_counts.entry(author_str.to_string()).or_insert(0);
                *count += 1;
            },
            None => {}
        }
        if linecount % 1_000_000 == 0 && print_progress {
            println!("{} lines read, time taken: {}, rate: {}", linecount, start.elapsed().as_secs_f64(), linecount as f64 / start.elapsed().as_secs_f64());
        }
        if linecount == 10_000_000 {
            break;
        }
    }
    if print_progress {println!("{} lines read, time taken: {}, rate: {}", linecount, start.elapsed().as_secs_f64(), linecount as f64 / start.elapsed().as_secs_f64());}
    author_counts
}

fn main() {
    let path = "/local/storage/maneriker.1/reddit-sl-subset/expt/RC_2021-01.zst";
    let author_counts = compute_author_counts_simd(path, None);
    println!("Num authors: {}", author_counts.len());
}
