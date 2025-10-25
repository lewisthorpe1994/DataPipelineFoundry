pub mod errors;
pub mod transforms;
pub mod utils;

use crate::smt::transforms::custom::Custom;
use crate::smt::transforms::debezium::{ByLogicalTableRouter, ExtractNewRecordState};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub enum SmtKind {
    ExtractNewRecordState(ExtractNewRecordState),
    ByLogicalTableRouter(ByLogicalTableRouter),
    Custom(Custom),
    // You can add more SMTs here as needed
}

impl SmtKind {
    pub fn class_name(&self) -> Cow<'_, str> {
        match self {
            SmtKind::ExtractNewRecordState { .. } => {
                Cow::Borrowed("io.debezium.transforms.ExtractNewRecordState")
            }
            SmtKind::ByLogicalTableRouter { .. } => {
                Cow::Borrowed("io.debezium.transforms.ByLogicalTableRouter")
            }
            SmtKind::Custom(c) => Cow::Owned(c.class.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SmtClass {
    ExtractNewRecordState,
    ByLogicalTableRouter,
    Custom(String),
}

impl TryFrom<&str> for SmtClass {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "io.debezium.transforms.ExtractNewRecordState" => Ok(SmtClass::ExtractNewRecordState),
            "io.debezium.transforms.ByLogicalTableRouter" => Ok(SmtClass::ByLogicalTableRouter),
            _ => Ok(SmtClass::Custom(value.to_string())),
        }
    }
}

impl SmtClass {
    pub fn class_name(&self) -> Cow<'_, str> {
        match self {
            SmtClass::ExtractNewRecordState => {
                Cow::Borrowed("io.debezium.transforms.ExtractNewRecordState")
            }
            SmtClass::ByLogicalTableRouter => {
                Cow::Borrowed("io.debezium.transforms.ByLogicalTableRouter")
            }
            SmtClass::Custom(class) => Cow::Owned(class.clone()),
        }
    }
}
/* ---------- One transform = name + kind ---------- */
#[derive(Debug, Clone)]
pub struct Transform {
    pub name: String, // e.g. "unwrap", "route"
    pub kind: SmtKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SmtPreset {
    DebeziumExtractNewRecordState,
    DebeziumByLogicalTableRouter,
}

impl SmtPreset {
    fn aliases(self) -> &'static [&'static str] {
        match self {
            SmtPreset::DebeziumExtractNewRecordState => &[
                "debezium.unwrap_default",
                "debezium.extract_new_record_state",
            ],
            SmtPreset::DebeziumByLogicalTableRouter => &[
                "debezium.route_by_field",
                "debezium.by_logical_table_router",
            ],
        }
    }
}

impl TryFrom<&str> for SmtPreset {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if SmtPreset::DebeziumExtractNewRecordState
            .aliases()
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(value))
        {
            return Ok(SmtPreset::DebeziumExtractNewRecordState);
        }

        if SmtPreset::DebeziumByLogicalTableRouter
            .aliases()
            .iter()
            .any(|alias| alias.eq_ignore_ascii_case(value))
        {
            return Ok(SmtPreset::DebeziumByLogicalTableRouter);
        }

        Err(())
    }
}
