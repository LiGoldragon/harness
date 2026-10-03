//! The Datom text the component CLIs take and print.
//!
//! A CLI takes exactly one inline Datom value and prints one Datom value. The
//! descent from text to a contract value is `actualize`; the ascent back to
//! text is `textualize`, which cannot fail.

use datom_codec::{Actualizing, Budget, Composing, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};

use crate::{Error, Result};

/// How much text, and how deep a structure, one CLI argument may carry.
const ARGUMENT_BUDGET: i64 = 1 << 20;
const MAXIMUM_DEPTH: i64 = 1024;

/// The single inline Datom argument a component CLI received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatomArgument {
    text: String,
}

/// The Datom text of one contract value a CLI prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatomPrint {
    text: String,
}

impl DatomArgument {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    /// The one operand of a CLI invocation; any other count is refused.
    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Result<Self>
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        let mut operands: Vec<String> = arguments.into_iter().map(Into::into).collect();
        match operands.len() {
            1 => Ok(Self::new(operands.remove(0))),
            count => Err(Error::ArgumentCount { count }),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Read one contract value from the argument's Datom text.
    pub fn actualize<Value: Composing>(&self) -> Result<Value> {
        Potential::<Value>::from(self.text.as_str())
            .actualize(&mut Self::budget())
            .map_err(|error| Error::DatomText {
                detail: format!("{error:?}"),
            })
    }

    fn budget() -> Budget {
        Budget {
            remaining: ARGUMENT_BUDGET,
            reader: ReaderBudget {
                remaining: ARGUMENT_BUDGET as usize,
            },
            depth: 0,
            maximum_depth: MAXIMUM_DEPTH,
        }
    }
}

impl DatomPrint {
    /// Project one contract value into its canonical Datom text.
    pub fn of<Value>(value: &Value) -> Self
    where
        Value: Datomizable,
        Value::Output: Protosizable,
        <Value::Output as Protosizable>::Output: Textualizable,
    {
        Self {
            text: value.datomize(Vec::new()).protosize().textualize(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}
