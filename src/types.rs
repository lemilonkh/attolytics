use std::convert::TryFrom;

use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone};
use postgres::types::ToSql;
use serde::Deserialize;
use std::fmt::Display;
use std::error::Error;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub enum Type {
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "i32")]
    I32,
    #[serde(rename = "i64")]
    I64,
    #[serde(rename = "f32")]
    F32,
    #[serde(rename = "f64")]
    F64,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "timestamp")]
    Timestamp,
}

impl Default for Type {
    fn default() -> Type {
        Type::String
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConversionError {
    MissingValue(String),
    TimestampFormat(chrono::format::ParseError),
    TimestampTooLarge(),
}

impl Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ConversionError::MissingValue(key) => write!(f, "required value \"{}\" was omitted", key),
            ConversionError::TimestampFormat(err) => write!(f, "could not parse timestamp: {}", err),
            ConversionError::TimestampTooLarge() => write!(f, "could not parse timestame: value out of range"),
        }
    }
}

impl Error for ConversionError {}

impl Type {
    pub fn postgres_type_name(&self) -> String {
        self.postgres_type().name().to_string()
    }

    pub fn postgres_type(&self) -> postgres::types::Type {
        match self {
            Type::Bool => postgres::types::Type::BOOL,
            Type::I32 => postgres::types::Type::INT4,
            Type::I64 => postgres::types::Type::INT8,
            Type::F32 => postgres::types::Type::FLOAT4,
            Type::F64 => postgres::types::Type::FLOAT8,
            Type::String => postgres::types::Type::VARCHAR,
            Type::Timestamp => postgres::types::Type::TIMESTAMPTZ,
        }
    }

    pub fn json_to_sql(&self, key: &str, json: &serde_json::Value, required: bool) -> Result<Box<dyn ToSql + Sync>, ConversionError> {
        match self {
            Type::Bool => unwrap_if_required(key, json.as_bool(), required),
            Type::I32 => unwrap_if_required(key, json.as_i64().map(|i| i32::try_from(i).ok()), required),
            Type::I64 => unwrap_if_required(key, json.as_i64(), required),
            Type::F32 => unwrap_if_required(key, json.as_f64().map(|f| f as f32), required),
            Type::F64 => unwrap_if_required(key, json.as_f64(), required),
            Type::String => unwrap_if_required(key, json.as_str().map(|s| s.to_string()), required),
            Type::Timestamp => unwrap_if_required(key, json_to_date_time(json)?, required),
        }
    }
}

pub fn header_to_sql<'a>(key: &str, value: Option<&'a str>, required: bool) -> Result<Box<dyn ToSql + Sync + 'a>, ConversionError> {
    unwrap_if_required(key, value, required)
}

pub fn unwrap_if_required<'a, T>(key: &str, option: Option<T>, required: bool) -> Result<Box<dyn ToSql + Sync + 'a>, ConversionError>
    where T: ToSql + Sync + 'a
{
    if required {
        Ok(Box::new(option.ok_or_else(|| ConversionError::MissingValue(key.to_string()))?))
    } else {
        Ok(Box::new(option))
    }
}

fn json_to_date_time(json: &serde_json::Value) -> Result<Option<DateTime<FixedOffset>>, ConversionError> {
    if json.is_number() {
        let timestamp = json.as_f64().unwrap();
        let naive = NaiveDateTime::from_timestamp_opt(timestamp.floor() as i64, (1e9 * timestamp.fract()) as u32);
        let offset = FixedOffset::west_opt(0).unwrap();
        if naive.is_none() {
            Err(ConversionError::TimestampTooLarge())
        } else {
            Ok(Some(TimeZone::from_utc_datetime(&offset, &naive.unwrap())))
        }
    } else if json.is_string() {
        Ok(Some(DateTime::parse_from_rfc3339(json.as_str().unwrap())
            .map_err(|err| ConversionError::TimestampFormat(err))?))
    } else {
        Ok(None)
    }
}
