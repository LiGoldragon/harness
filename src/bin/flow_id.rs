use std::{env, path::PathBuf, process};

use harness::flow_id::{HarnessKind, claim};

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("flow-id: {error}");
        process::exit(2);
    }
}

fn run(arguments: Vec<String>) -> harness::flow_id::Result<()> {
    let Some((harness, rest)) = arguments.split_first() else {
        return Err(harness::flow_id::Error::Argument(usage()));
    };
    let harness = HarnessKind::parse(harness)?;
    let mut root = None;
    let mut parent_session = None;
    let mut arguments = rest.iter();
    while let Some(argument) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| harness::flow_id::Error::Argument(usage()))?;
        match argument.as_str() {
            "--flows-root" if root.is_none() => root = Some(PathBuf::from(value)),
            "--parent-session" if parent_session.is_none() => parent_session = Some(value.clone()),
            _ => return Err(harness::flow_id::Error::Argument(usage())),
        }
    }
    let root = root.ok_or_else(|| harness::flow_id::Error::Argument(usage()))?;
    let identity = match harness {
        HarnessKind::Codex if parent_session.is_none() => {
            env::var("CODEX_SESSION_ID").map_err(|_| {
                harness::flow_id::Error::Argument("CODEX_SESSION_ID is required for codex".into())
            })?
        }
        HarnessKind::Claude if parent_session.is_some() => parent_session.expect("checked"),
        _ => return Err(harness::flow_id::Error::Argument(usage())),
    };
    println!("{}", claim(harness, &root, &identity)?);
    Ok(())
}

fn usage() -> String {
    "usage: flow-id codex --flows-root ABSOLUTE_DIRECTORY | flow-id claude --flows-root ABSOLUTE_DIRECTORY --parent-session UUID".into()
}
