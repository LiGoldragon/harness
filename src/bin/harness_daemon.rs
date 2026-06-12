use harness::HarnessProcessDaemon;
use harness::schema::daemon::DaemonEntry;

fn main() -> std::process::ExitCode {
    <HarnessProcessDaemon as DaemonEntry>::run_to_exit_code()
}
