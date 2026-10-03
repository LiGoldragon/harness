//! Live witness of the usage snapshot against this host's real providers.
//! Ignored by default: it reaches the network and the user's live sessions.
//! Run with `cargo test --test usage_live -- --ignored --nocapture`.

use datom_codec::Datomizable;
use harness::usage::{UsageHome, UsageSnapshotReader, UsageSnapshotReading};
use protos::{Protosizable, Textualizable};

#[test]
#[ignore = "reads live provider state"]
fn live_snapshot_prints_as_datom() {
    let home = UsageHome::from_process().expect("HOME is set");
    let snapshot = UsageSnapshotReader::for_home(home).read_snapshot();
    let response = usage_contract::Response::UsageSnapshot(snapshot);
    println!("{}", response.datomize(vec![]).protosize().textualize());
}
