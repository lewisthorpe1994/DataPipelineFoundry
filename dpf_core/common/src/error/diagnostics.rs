use std::{borrow::Cow, fmt, panic::Location};

/// Human-friendly error message that automatically records the call-site.
///
/// Call [`DiagnosticMessage::new`] or the [`diag!`] macro to create an
/// instance; the macro allows inline formatting (e.g. `diag!("missing {}", x)`)
/// while capturing `file!()`/`line!()` when the error was constructed.
#[derive(Clone, Debug)]
pub struct DiagnosticMessage {
    message: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl DiagnosticMessage {
    /// Create a message and record the caller location.
    #[track_caller]
    pub fn new(message: impl Into<Cow<'static, str>>) -> Self {
        let location = Location::caller();
        Self {
            message: message.into(),
            location,
        }
    }

    /// Access the original, human readable message.
    pub fn message(&self) -> &str {
        self.message.as_ref()
    }

    /// Access the location where the diagnostic was created.
    pub fn location(&self) -> &'static Location<'static> {
        self.location
    }
}

impl fmt::Display for DiagnosticMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (at {}:{})",
            self.message,
            self.location.file(),
            self.location.line()
        )
    }
}

/// Convenience macro for creating [`DiagnosticMessage`] values with `format!`
/// style syntax while capturing the file/line automatically.
#[macro_export]
macro_rules! diag {
    ($msg:literal $(,)?) => {
        $crate::error::diagnostics::DiagnosticMessage::new($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::diagnostics::DiagnosticMessage::new(format!($fmt, $($arg)*))
    };
}
