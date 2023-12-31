use std::path::PathBuf;
use clap::{Parser, Subcommand, command};

mod subcmds;
use subcmds::counts::{compute_post_counts_hashmap, output_counts_hashmap_to_file};

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[clap(subcommand)]
    cmd: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Compute author counts from a zstd compressed file
    PostCounts {
        /// Input file
        #[arg(short, long, value_name = "FILE")]
        input: PathBuf,
        
        /// Output file
        #[arg(short, long, value_name = "FILE")]
        output: PathBuf,
        
        /// Print progress
        #[arg(short, long)]
        progress: bool,
    },
}
fn main() {
    let cli = Cli::parse();

    match cli.cmd {
        Some(Commands::PostCounts { input, output, progress }) => {
            let author_counts = compute_post_counts_hashmap(input.to_str().unwrap(), Some(progress));
            output_counts_hashmap_to_file(&author_counts, output.to_str().unwrap()).expect("Could not write to output file");
        }
        None => {
            println!("No subcommand specified");
        }
    }
}
