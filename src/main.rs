use harness::{Result, daemon::HarnessDaemon};
use nota_config::ConfigurationSource;
use signal_harness::HarnessDaemonConfiguration;

fn main() -> Result<()> {
    let configuration: HarnessDaemonConfiguration = ConfigurationSource::from_argv()?.decode()?;
    HarnessDaemon::from_configuration(configuration).run()
}
