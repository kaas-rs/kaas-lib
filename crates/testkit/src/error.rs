//! Fixture errors.
//!
//! Deliberately small: a fixture either comes up or it doesn't, and when it
//! doesn't the useful information is the container's own output, not a
//! taxonomy. The real error taxonomy is `kafka-meta`'s, and it describes the
//! broker's answers rather than the harness's.

use std::fmt;

/// Result alias for fixture operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Something went wrong bringing a fixture up, or talking to one.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The container runtime rejected an operation.
    #[error("container runtime: {0}")]
    Container(#[from] testcontainers::TestcontainersError),

    /// A command run inside a container exited non-zero.
    #[error("`{argv}` in node {node} exited with {code:?}\nstdout:\n{stdout}\nstderr:\n{stderr}")]
    Exec {
        /// The command that failed, joined for display.
        argv: String,
        /// Index of the node the command ran on.
        node: usize,
        /// Exit status, if the runtime reported one.
        code: Option<i64>,
        /// Captured stdout.
        stdout: String,
        /// Captured stderr.
        stderr: String,
    },

    /// A node index was out of range for this cluster.
    #[error("no node {index} in a {size}-node cluster")]
    NoSuchNode {
        /// The requested index.
        index: usize,
        /// How many nodes the cluster actually has.
        size: usize,
    },

    /// The fixture was asked for something it cannot provide — an external
    /// cluster has no container to exec into, for instance.
    #[error("fixture does not support this operation: {0}")]
    Unsupported(&'static str),

    /// A configuration the harness cannot express.
    #[error("invalid fixture configuration: {0}")]
    Config(String),
}

impl Error {
    pub(crate) fn config(msg: impl fmt::Display) -> Self {
        Self::Config(msg.to_string())
    }
}
