use harness::launch_user::UserServiceLaunch;

fn main() -> std::process::ExitCode {
    if std::env::args().len() > 1 {
        eprintln!("harness-daemon-launch: takes no argument");
        return std::process::ExitCode::FAILURE;
    }
    let error = match UserServiceLaunch::from_service() {
        Ok(launch) => launch.exec(),
        Err(error) => error,
    };
    eprintln!("harness-daemon-launch: {error}");
    std::process::ExitCode::FAILURE
}
