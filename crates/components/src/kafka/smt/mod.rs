pub mod errors;
pub mod transforms;
pub mod utils;

use crate::smt::transforms::custom::Custom;
use crate::smt::transforms::debezium::{
    ByLogicalTableRouter, ContentBasedRouter, DecodeLogicalDecodingMessageContent,
    ExtractNewRecordState, Filter, HeaderToValue, OutboxEventRouter, PartitionRouting,
    TimezoneConverter,
};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub enum SmtKind {
    ExtractNewRecordState(ExtractNewRecordState),
    ByLogicalTableRouter(ByLogicalTableRouter),
    ContentBasedRouter(ContentBasedRouter),
    DecodeLogicalMessageContent(DecodeLogicalDecodingMessageContent),
    Filter(Filter),
    HeaderToValue(HeaderToValue),
    OutboxEventRouter(OutboxEventRouter),
    PartitionRouting(PartitionRouting),
    TimezoneConverter(TimezoneConverter),
    Custom(Custom),
    // You can add more SMTs here as needed
}

impl SmtKind {
    pub fn class_name(&self) -> Cow<'_, str> {
        match self {
            SmtKind::ExtractNewRecordState(_) => {
                Cow::Borrowed("io.debezium.transforms.ExtractNewRecordState")
            }
            SmtKind::ByLogicalTableRouter(_) => {
                Cow::Borrowed("io.debezium.transforms.ByLogicalTableRouter")
            }
            SmtKind::ContentBasedRouter(_) => {
                Cow::Borrowed("io.debezium.transforms.ContentBasedRouter")
            }
            SmtKind::DecodeLogicalMessageContent(_) => {
                Cow::Borrowed("io.debezium.transforms.DecodeLogicalDecodingMessageContent")
            }
            SmtKind::Filter(_) => Cow::Borrowed("io.debezium.transforms.Filter"),
            SmtKind::HeaderToValue(_) => Cow::Borrowed("io.debezium.transforms.HeaderToValue"),
            SmtKind::OutboxEventRouter(_) => {
                Cow::Borrowed("io.debezium.transforms.OutboxEventRouter")
            }
            SmtKind::PartitionRouting(_) => {
                Cow::Borrowed("io.debezium.transforms.PartitionRouting")
            }
            SmtKind::TimezoneConverter(_) => {
                Cow::Borrowed("io.debezium.transforms.TimezoneConverter")
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
    ContentBasedRouter,
    DecodeLogicalMessageContent,
    Filter,
    HeaderToValue,
    OutboxEventRouter,
    PartitionRouting,
    TimezoneConverter,
}

impl TryFrom<&str> for SmtClass {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "io.debezium.transforms.ExtractNewRecordState" => Ok(SmtClass::ExtractNewRecordState),
            "io.debezium.transforms.ByLogicalTableRouter" => Ok(SmtClass::ByLogicalTableRouter),
            "io.debezium.transforms.ContentBasedRouter" => Ok(SmtClass::ContentBasedRouter),
            "io.debezium.transforms.DecodeLogicalDecodingMessageContent" => {
                Ok(SmtClass::DecodeLogicalMessageContent)
            }
            "io.debezium.transforms.Filter" => Ok(SmtClass::Filter),
            "io.debezium.transforms.HeaderToValue" => Ok(SmtClass::HeaderToValue),
            "io.debezium.transforms.OutboxEventRouter" => Ok(SmtClass::OutboxEventRouter),
            "io.debezium.transforms.PartitionRouting" => Ok(SmtClass::PartitionRouting),
            "io.debezium.transforms.TimezoneConverter" => Ok(SmtClass::TimezoneConverter),
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
            SmtClass::DecodeLogicalMessageContent => {
                Cow::Borrowed("io.debezium.transforms.DecodeLogicalDecodingMessageContent")
            }
            SmtClass::ContentBasedRouter => {
                Cow::Borrowed("io.debezium.transforms.ContentBasedRouter")
            }
            SmtClass::Filter => Cow::Borrowed("io.debezium.transforms.Filter"),
            SmtClass::HeaderToValue => Cow::Borrowed("io.debezium.transforms.HeaderToValue"),
            SmtClass::OutboxEventRouter => {
                Cow::Borrowed("io.debezium.transforms.OutboxEventRouter")
            }
            SmtClass::PartitionRouting => Cow::Borrowed("io.debezium.transforms.PartitionRouting"),
            SmtClass::TimezoneConverter => {
                Cow::Borrowed("io.debezium.transforms.TimezoneConverter")
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
