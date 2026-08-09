//! Point kaas-lib at a real Kafka cluster.
//!
//! Not a replacement for the container acceptance suite, which owns the cases
//! that need a broker configured a particular way, killed mid-request, or fed a
//! damaged log segment. This is the other half: what happens against a cluster
//! that is *shared, long-lived and not ours*, running a Kafka build we did not
//! choose, holding data produced by clients we did not write.
//!
//! Everything here is namespaced under a prefix and swept up afterwards. See
//! the `live-cluster` skill for how the addresses and credentials are resolved.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod probe;
mod produce;
mod read;
mod report;
mod smoke;
mod sweep;
mod target;

use anyhow::{Result, bail};

use crate::target::Target;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_owned());
    let rest: Vec<String> = args.collect();

    let target = match command.as_str() {
        "help" | "--help" | "-h" => {
            print_help();
            return Ok(());
        }
        _ => Target::from_env()?,
    };

    match command.as_str() {
        // Notes to stderr, facts to stdout, so `livetest probe > out.txt`
        // captures exactly the diffable body and nothing else.
        "probe" => emit(report::Outcome::ok(probe::probe(&target).await?)),
        "smoke" => emit(smoke::smoke(&target).await?),
        "produce" => {
            let options = produce::Options::parse(&rest)?;
            emit(produce::produce(&target, &options).await?)
        }
        "read" => {
            let options = read::Options::parse(&rest)?;
            emit(read::read(&target, &options).await?)
        }
        "sweep" => {
            let removed = sweep::sweep(&target).await?;
            for name in &removed {
                println!("removed {name}");
            }
            eprintln!("swept {} leftover resource(s)", removed.len());
            Ok(())
        }
        other => {
            print_help();
            bail!("unknown command {other:?}");
        }
    }
}

/// Print what was learned, then report whether the run passed.
///
/// The report comes first even when the run failed: a partial report says how
/// far it got and what the cluster looked like on the way, which is the whole
/// diagnostic.
fn emit(outcome: report::Outcome) -> Result<()> {
    eprint!("{}", outcome.report.render_notes());
    eprintln!("# {} fact(s)", outcome.report.len());
    print!("{}", outcome.report.render());
    outcome.result
}

fn print_help() {
    eprintln!(
        "livetest — run kaas-lib against a real Kafka cluster

USAGE
    livetest <probe|smoke|produce|read|sweep> [options]

COMMANDS
    probe    Read-only inventory and negotiated version table. Touches
             nothing. Output is a sorted, diffable report — run it against two
             clusters and diff the results for a conformance check.
    smoke    Admin round trip: create, describe, alter, verify, delete. Creates
             only prefixed resources and removes them again.
    produce  Write path round trip: produce with every codec, an explicit
             partition, a tombstone and a keyed spread, then read it all back
             through kafka-read. Creates only prefixed resources.
               --topic <name>      use an existing topic instead of creating
                                   one, so its leader map can be diffed
                                   against another client's view of it
    read     Scan and tail topics, asserting the decoder against real data
             written by clients we did not write.
               --topic <name>      read this topic (repeatable)
               --expect <n>        require exactly n records from --topic
               --limit <n>         cap records scanned per topic (default 20000)
               --max-topics <n>    when no --topic is given (default 5)
    sweep    Delete anything this tool left behind. Refuses to touch a name
             without the configured prefix.

ENVIRONMENT
    KAAS_TEST_BOOTSTRAP      required, comma-separated host:port
    KAAS_TEST_LABEL          report label, defaults to the first hostname
    KAAS_TEST_PREFIX         prefix for created resources (default kaaslib-live)
    KAAS_TEST_READ_ONLY      1 to refuse every mutating api key
    KAAS_TEST_CA_PEM         inline PEM bundle to trust
    KAAS_TEST_CA_FILE        path to a PEM bundle to trust
    KAAS_TEST_TLS_SERVER_NAME  name to verify the broker certificate against
    KAAS_TEST_CLIENT_CERT_PEM  inline PEM client certificate chain, mutual TLS
    KAAS_TEST_CLIENT_CERT_FILE path to one
    KAAS_TEST_CLIENT_KEY_PEM   inline PEM key for that chain
    KAAS_TEST_CLIENT_KEY_FILE  path to one — both halves or neither
    KAAS_TEST_SASL_MECHANISM   PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 |
                               OAUTHBEARER
    KAAS_TEST_SASL_USERNAME
    KAAS_TEST_SASL_PASSWORD

The `live-cluster` skill resolves all of these from Kubernetes."
    );
}
