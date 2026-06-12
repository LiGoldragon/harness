use harness::HarnessCommandLine;

fn main() {
    if let Err(error) = HarnessCommandLine::from_env().run(std::io::stdout().lock()) {
        eprintln!("harness: {error}");
        std::process::exit(1);
    }
}
