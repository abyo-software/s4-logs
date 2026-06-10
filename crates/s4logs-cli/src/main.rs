//! s4logs — CLI entrypoint (DESIGN.md §9). Thin shell: parse args, init
//! tracing (stderr — stdout carries data), dispatch to `commands::*`, map
//! errors to exit codes: 0 ok, 1 runtime error, 2 usage error.

mod aws;
mod cli;
mod commands;
mod scan;
mod timearg;

use clap::Parser;

use crate::cli::{Cli, Cmd, LogFormat, UsageError};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(&cli.global);
    if let Err(err) = run(cli).await {
        if let Some(usage) = err.downcast_ref::<UsageError>() {
            eprintln!("s4logs: usage error: {usage}");
            std::process::exit(2);
        }
        eprintln!("s4logs: error: {err:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        Cmd::Drain(args) => commands::drain::run(&cli.global, &args).await,
        Cmd::Grep(args) => commands::grep::run(&cli.global, &args).await,
        Cmd::Restore(args) => commands::restore::run(&cli.global, &args).await,
        Cmd::Serve(args) => commands::serve::run(&cli.global, &args).await,
        Cmd::Version => {
            println!("s4logs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn init_tracing(global: &cli::GlobalArgs) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_new(&global.log_level).unwrap_or_else(|err| {
        eprintln!(
            "s4logs: bad --log-level {:?} ({err}); falling back to \"info\"",
            global.log_level
        );
        EnvFilter::new("info")
    });
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match global.log_format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Pretty => builder.init(),
    }
}
