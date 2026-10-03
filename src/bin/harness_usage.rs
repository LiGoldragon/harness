use harness::UsageCommandLine;

fn main() {
    if let Err(error) = UsageCommandLine::from_env().run(std::io::stdout().lock()) {
        eprintln!("harness-usage: {error}");
        std::process::exit(1);
    }
}
