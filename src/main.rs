use harness::{HarnessDaemonCommand, Result};

fn main() -> Result<()> {
    HarnessDaemonCommand::from_environment().run()
}
