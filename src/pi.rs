use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Value, json};

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PiRpcDeliveryCommand {
    Prompt,
    Steer,
    FollowUp,
}

impl PiRpcDeliveryCommand {
    pub fn rpc_type(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Steer => "steer",
            Self::FollowUp => "follow_up",
        }
    }

    pub fn response_command(self) -> &'static str {
        self.rpc_type()
    }
}

impl From<signal_harness::PiRpcDeliveryMode> for PiRpcDeliveryCommand {
    fn from(value: signal_harness::PiRpcDeliveryMode) -> Self {
        match value {
            signal_harness::PiRpcDeliveryMode::Prompt => Self::Prompt,
            signal_harness::PiRpcDeliveryMode::Steer => Self::Steer,
            signal_harness::PiRpcDeliveryMode::FollowUp => Self::FollowUp,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRpcProcessConfiguration {
    command_path: PathBuf,
    command_arguments: Vec<String>,
    session_directory_path: PathBuf,
    session_name: String,
    model_pattern: Option<String>,
    delivery_command: PiRpcDeliveryCommand,
    response_timeout: Duration,
}

impl PiRpcProcessConfiguration {
    pub fn new(
        command_path: impl Into<PathBuf>,
        session_directory_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            command_path: command_path.into(),
            command_arguments: Vec::new(),
            session_directory_path: session_directory_path.into(),
            session_name: "harness".to_string(),
            model_pattern: None,
            delivery_command: PiRpcDeliveryCommand::FollowUp,
            response_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_command_arguments(mut self, command_arguments: Vec<String>) -> Self {
        self.command_arguments = command_arguments;
        self
    }

    pub fn with_session_name(mut self, session_name: impl Into<String>) -> Self {
        self.session_name = session_name.into();
        self
    }

    pub fn with_model_pattern(mut self, model_pattern: impl Into<String>) -> Self {
        self.model_pattern = Some(model_pattern.into());
        self
    }

    pub fn with_delivery_command(mut self, delivery_command: PiRpcDeliveryCommand) -> Self {
        self.delivery_command = delivery_command;
        self
    }

    pub fn with_response_timeout(mut self, response_timeout: Duration) -> Self {
        self.response_timeout = response_timeout;
        self
    }

    pub fn command_path(&self) -> &Path {
        &self.command_path
    }

    pub fn session_directory_path(&self) -> &Path {
        &self.session_directory_path
    }

    pub fn session_name(&self) -> &str {
        &self.session_name
    }

    pub fn model_pattern(&self) -> Option<&str> {
        self.model_pattern.as_deref()
    }

    pub fn delivery_command(&self) -> PiRpcDeliveryCommand {
        self.delivery_command
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.command_path);
        command.args(&self.command_arguments);
        command.arg("--mode").arg("rpc");
        command
            .arg("--session-dir")
            .arg(&self.session_directory_path);
        command.arg("--name").arg(&self.session_name);
        if let Some(model_pattern) = &self.model_pattern {
            command.arg("--model").arg(model_pattern);
        }
        command
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRpcDeliveryReceipt {
    command_identifier: String,
    command: PiRpcDeliveryCommand,
}

impl PiRpcDeliveryReceipt {
    fn accepted(command_identifier: String, command: PiRpcDeliveryCommand) -> Self {
        Self {
            command_identifier,
            command,
        }
    }

    pub fn command_identifier(&self) -> &str {
        &self.command_identifier
    }

    pub fn command(&self) -> PiRpcDeliveryCommand {
        self.command
    }
}

#[derive(Debug)]
pub struct PiRpcSession {
    process: Child,
    input: ChildStdin,
    output: Receiver<PiRpcOutputLine>,
    output_thread: Option<JoinHandle<()>>,
    next_identifier: u64,
    delivery_command: PiRpcDeliveryCommand,
    response_timeout: Duration,
}

impl PiRpcSession {
    pub fn spawn(configuration: PiRpcProcessConfiguration) -> Result<Self> {
        std::fs::create_dir_all(configuration.session_directory_path())?;
        let mut command = configuration.command();
        let mut process = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = process.stdin.take().ok_or(Error::PiRpcInputUnavailable)?;
        let output = process.stdout.take().ok_or(Error::PiRpcOutputUnavailable)?;
        let reader = PiRpcOutputReader::new(output);
        let (output, output_thread) = reader.spawn();
        Ok(Self {
            process,
            input,
            output,
            output_thread: Some(output_thread),
            next_identifier: 1,
            delivery_command: configuration.delivery_command(),
            response_timeout: configuration.response_timeout,
        })
    }

    pub fn deliver_text(&mut self, text: &str) -> Result<PiRpcDeliveryReceipt> {
        let command_identifier = self.next_command_identifier();
        let command = PiRpcCommand::delivery(command_identifier, self.delivery_command, text);
        let command_identifier = command.identifier().to_string();
        let delivery_command = command.delivery_command();
        self.write_command(command)?;
        self.wait_for_response(command_identifier, delivery_command)
    }

    pub fn stop(mut self) -> Result<()> {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Some(output_thread) = self.output_thread.take() {
            let _ = output_thread.join();
        }
        Ok(())
    }

    fn next_command_identifier(&mut self) -> String {
        let identifier = format!("harness-{}", self.next_identifier);
        self.next_identifier = self.next_identifier.saturating_add(1);
        identifier
    }

    fn write_command(&mut self, command: PiRpcCommand) -> Result<()> {
        let value = command.into_value();
        writeln!(self.input, "{value}")?;
        self.input.flush()?;
        Ok(())
    }

    fn wait_for_response(
        &self,
        command_identifier: String,
        delivery_command: PiRpcDeliveryCommand,
    ) -> Result<PiRpcDeliveryReceipt> {
        loop {
            let line = self
                .output
                .recv_timeout(self.response_timeout)
                .map_err(|_| Error::PiRpcResponseTimeout {
                    command_identifier: command_identifier.clone(),
                })?;
            let Some(response) = line.response_for(&command_identifier)? else {
                continue;
            };
            return response.accepted_receipt(delivery_command);
        }
    }
}

impl Drop for PiRpcSession {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Some(output_thread) = self.output_thread.take() {
            let _ = output_thread.join();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRpcCommand {
    identifier: String,
    delivery_command: PiRpcDeliveryCommand,
    message: String,
}

impl PiRpcCommand {
    fn delivery(identifier: String, delivery_command: PiRpcDeliveryCommand, message: &str) -> Self {
        Self {
            identifier,
            delivery_command,
            message: message.to_string(),
        }
    }

    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn delivery_command(&self) -> PiRpcDeliveryCommand {
        self.delivery_command
    }

    fn into_value(self) -> Value {
        json!({
            "id": self.identifier,
            "type": self.delivery_command.rpc_type(),
            "message": self.message,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRpcCommandResponse {
    command_identifier: String,
    command: String,
    success: bool,
    error: Option<String>,
}

impl PiRpcCommandResponse {
    fn accepted_receipt(
        self,
        delivery_command: PiRpcDeliveryCommand,
    ) -> Result<PiRpcDeliveryReceipt> {
        if self.command != delivery_command.response_command() {
            return Err(Error::PiRpcUnexpectedResponse {
                got: format!(
                    "response for {} had command {}",
                    self.command_identifier, self.command
                ),
            });
        }
        if self.success {
            Ok(PiRpcDeliveryReceipt::accepted(
                self.command_identifier,
                delivery_command,
            ))
        } else {
            Err(Error::PiRpcRejected {
                command_identifier: self.command_identifier,
                error: self.error.unwrap_or_else(|| "missing error".to_string()),
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiRpcOutputLine {
    text: String,
}

impl PiRpcOutputLine {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn response_for(&self, command_identifier: &str) -> Result<Option<PiRpcCommandResponse>> {
        let value: Value = serde_json::from_str(self.text.trim_end())?;
        if value.get("type").and_then(Value::as_str) != Some("response") {
            return Ok(None);
        }
        if value.get("id").and_then(Value::as_str) != Some(command_identifier) {
            return Ok(None);
        }
        let command = value
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::PiRpcUnexpectedResponse {
                got: self.text.clone(),
            })?;
        let success = value
            .get("success")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::PiRpcUnexpectedResponse {
                got: self.text.clone(),
            })?;
        let error = value
            .get("error")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        Ok(Some(PiRpcCommandResponse {
            command_identifier: command_identifier.to_string(),
            command: command.to_string(),
            success,
            error,
        }))
    }
}

#[derive(Debug)]
pub struct PiRpcOutputReader {
    output: BufReader<ChildStdout>,
}

impl PiRpcOutputReader {
    fn new(output: ChildStdout) -> Self {
        Self {
            output: BufReader::new(output),
        }
    }

    fn spawn(self) -> (Receiver<PiRpcOutputLine>, JoinHandle<()>) {
        let (sender, receiver) = channel();
        let handle = thread::spawn(move || {
            let mut reader = self;
            reader.read_until_closed(sender);
        });
        (receiver, handle)
    }

    fn read_until_closed(&mut self, sender: Sender<PiRpcOutputLine>) {
        let mut line = String::new();
        loop {
            line.clear();
            match self.output.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    let _ = sender.send(PiRpcOutputLine::new(line.clone()));
                }
            }
        }
    }
}
