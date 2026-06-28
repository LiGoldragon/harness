use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use harness::{ClaudeArtifactObserver, Error, Result};

fn main() {
    if let Err(error) = ObserverCommandLine::from_environment()
        .and_then(|command| command.run(std::io::stdout().lock()))
    {
        eprintln!("harness-claude-artifact-observer-test: {error}");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObserverCommandLine {
    home_directory: PathBuf,
    current_working_directory: PathBuf,
    session_identifier: Option<String>,
    prompt_marker: String,
    final_marker: String,
    tool_marker: String,
    timeout: Duration,
    poll_interval: Duration,
}

impl ObserverCommandLine {
    fn from_environment() -> Result<Self> {
        let arguments = std::env::args().skip(1).collect::<Vec<_>>();
        let home_directory = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self::from_arguments(arguments, home_directory, std::env::current_dir()?)
    }

    fn from_arguments(
        arguments: Vec<String>,
        default_home_directory: PathBuf,
        default_current_working_directory: PathBuf,
    ) -> Result<Self> {
        let mut parser = ObserverArgumentParser::new(arguments);
        let home_directory = parser
            .optional_value("--home")?
            .map(PathBuf::from)
            .unwrap_or(default_home_directory);
        let current_working_directory = parser
            .optional_value("--cwd")?
            .map(PathBuf::from)
            .unwrap_or(default_current_working_directory);
        let session_identifier = parser.optional_value("--session")?;
        let prompt_marker = parser
            .optional_value("--prompt-marker")?
            .unwrap_or_else(|| "PROMPT_MARKER".to_string());
        let final_marker = parser
            .optional_value("--final-marker")?
            .unwrap_or_else(|| "FINAL_MARKER".to_string());
        let tool_marker = parser
            .optional_value("--tool-marker")?
            .unwrap_or_else(|| "TOOL_MARKER".to_string());
        let timeout = Duration::from_secs(parser.optional_u64("--timeout-seconds")?.unwrap_or(180));
        let poll_interval =
            Duration::from_millis(parser.optional_u64("--poll-millis")?.unwrap_or(250));
        parser.reject_remaining()?;
        Ok(Self {
            home_directory,
            current_working_directory,
            session_identifier,
            prompt_marker,
            final_marker,
            tool_marker,
            timeout,
            poll_interval,
        })
    }

    fn run(&self, mut output: impl Write) -> Result<()> {
        let mut observer = ClaudeArtifactObserver::with_home(
            &self.home_directory,
            &self.current_working_directory,
        )
        .with_poll_interval(self.poll_interval);
        if let Some(session_identifier) = &self.session_identifier {
            observer = observer.with_session_identifier(session_identifier);
        }
        let snapshot =
            observer.wait_for_markers(&self.prompt_marker, &self.final_marker, self.timeout)?;
        writeln!(
            output,
            "{}",
            snapshot.summary_json(&self.prompt_marker, &self.final_marker, &self.tool_marker)
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObserverArgumentParser {
    arguments: Vec<String>,
}

impl ObserverArgumentParser {
    fn new(arguments: Vec<String>) -> Self {
        Self { arguments }
    }

    fn optional_value(&mut self, name: &str) -> Result<Option<String>> {
        if let Some(index) = self.arguments.iter().position(|argument| argument == name) {
            self.arguments.remove(index);
            if index >= self.arguments.len() {
                return Err(Error::ClaudeObserverArgument {
                    message: format!("missing value for {name}"),
                });
            }
            Ok(Some(self.arguments.remove(index)))
        } else {
            Ok(None)
        }
    }

    fn optional_u64(&mut self, name: &str) -> Result<Option<u64>> {
        self.optional_value(name)?
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| Error::ClaudeObserverArgument {
                        message: format!("invalid integer for {name}: {value}"),
                    })
            })
            .transpose()
    }

    fn reject_remaining(&self) -> Result<()> {
        if self.arguments.is_empty() {
            Ok(())
        } else {
            Err(Error::ClaudeObserverArgument {
                message: format!("unknown arguments: {}", self.arguments.join(" ")),
            })
        }
    }
}
