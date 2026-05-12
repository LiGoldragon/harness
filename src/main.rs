use persona_harness::{HarnessCommandLine, Result};

fn main() -> Result<()> {
    HarnessCommandLine::from_environment().run()
}
