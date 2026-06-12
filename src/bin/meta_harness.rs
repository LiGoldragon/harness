use harness::MetaHarnessCommandLine;

fn main() {
    if let Err(error) = MetaHarnessCommandLine::from_env().run(std::io::stdout().lock()) {
        eprintln!("meta-harness: {error}");
        std::process::exit(1);
    }
}
