use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::error::PlanError;

/// Scalar types supported by the first ColumnScope plan format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl ScalarType {
    pub fn is_numeric(self) -> bool {
        matches!(self, Self::Int64 | Self::Float64)
    }
}

impl std::fmt::Display for ScalarType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Int64 => "Int64",
            Self::Float64 => "Float64",
            Self::Boolean => "Boolean",
            Self::Utf8 => "Utf8",
        };
        formatter.write_str(name)
    }
}

impl From<ScalarType> for DataType {
    fn from(value: ScalarType) -> Self {
        match value {
            ScalarType::Int64 => Self::Int64,
            ScalarType::Float64 => Self::Float64,
            ScalarType::Boolean => Self::Boolean,
            ScalarType::Utf8 => Self::Utf8,
        }
    }
}

/// A named, typed field in a serialized schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDef {
    pub name: String,
    pub data_type: ScalarType,
    pub nullable: bool,
}

/// Stable schema representation used by plans and snapshot descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDef {
    pub fields: Vec<FieldDef>,
}

impl SchemaDef {
    pub fn validate(&self) -> Result<(), PlanError> {
        let mut names = HashSet::with_capacity(self.fields.len());
        for field in &self.fields {
            if field.name.is_empty() {
                return Err(PlanError::EmptyFieldName);
            }
            if !names.insert(field.name.as_str()) {
                return Err(PlanError::DuplicateField(field.name.clone()));
            }
        }
        Ok(())
    }

    pub fn field(&self, name: &str) -> Option<(usize, &FieldDef)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == name)
    }

    pub fn to_arrow(&self) -> Result<Schema, PlanError> {
        self.validate()?;
        Ok(Schema::new(
            self.fields
                .iter()
                .map(|field| {
                    Field::new(&field.name, DataType::from(field.data_type), field.nullable)
                })
                .collect::<Vec<_>>(),
        ))
    }
}
