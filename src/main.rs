use nota_config::ConfigurationSource;
use persona_harness::{Result, daemon::HarnessDaemon};
use signal_persona_harness::HarnessDaemonConfiguration;

fn main() -> Result<()> {
    let configuration: HarnessDaemonConfiguration = ConfigurationSource::from_argv()?.decode()?;
    HarnessDaemon::from_configuration(configuration).run()
}
