use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use std::collections::HashMap;

#[derive(serde::Deserialize)]
struct RedditRow {
    author: String
}

pub fn compute_post_counts_hashmap(path: &str, progress: Option<bool>) -> HashMap<String, i64> {
    let mut author_counts: HashMap<String, i64> = HashMap::new(); 
    let print_progress = if progress.is_some() { progress.unwrap() } else { true };

    let file = File::open(path).expect("file not found");
    let mut zstd_reader = zstd::stream::read::Decoder::new(file).expect("zstd parsing error");
    let _ = zstd_reader.window_log_max(31).expect("Window log max too high");
    let zstd_bufreader = BufReader::new(zstd_reader);

    let mut linecount = 0;
    let start = Instant::now();

    for line in zstd_bufreader.lines() {
        let lw = line.unwrap();
        linecount += 1;
        let parsed_row: RedditRow = serde_json::from_str(&lw).unwrap();
        let author = parsed_row.author;
        let count = author_counts.entry(author).or_insert(0);
        *count += 1;

        if linecount % 1_000_000 == 0 && print_progress {
            println!("{} lines read, time taken: {}, rate: {}", linecount, start.elapsed().as_secs_f64(), linecount as f64 / start.elapsed().as_secs_f64());
        }
    }
    if print_progress {
        println!("{} lines read, time taken: {}, rate: {}", linecount, start.elapsed().as_secs_f64(), linecount as f64 / start.elapsed().as_secs_f64());
        println!("Num authors: {}", author_counts.len());
    }
    author_counts
}

pub fn output_counts_hashmap_to_file(author_counts: &HashMap<String, i64>, path: &str) -> Result<(), serde_json::Error> {
    let mut output_file = File::create(path).expect("Could not create output file");
    // serialze the hashmap and write the json to the output file
    serde_json::to_writer(&mut output_file, author_counts)
}