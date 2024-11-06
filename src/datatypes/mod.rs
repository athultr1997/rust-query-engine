use core::{fmt, panic};
use std::{any::Any, cmp::Ordering, collections::HashSet, fmt::Debug, hash::Hash, sync::Arc};

use arrow::{
    array::{
        Array, ArrayAccessor, ArrayBuilder, BooleanArray, Int16Array, Int16Builder, Int32Array,
        Int8Array, Int8Builder, StringArray, StringBuilder,
    },
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Int8Type, Schema as ArrowSchema},
};

/// Available data types in QE
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    BooleanType,
    Int8Type,
    Int16Type,
    Int32Type,
    Int64Type,
    Float32Type,
    StringType,
}

impl DataType {
    /// Gets Arrow's data type corresponding to QE's data type
    pub fn get_arrow_data_type(arrow_type: &DataType) -> ArrowDataType {
        match arrow_type {
            DataType::BooleanType => ArrowDataType::Boolean,
            DataType::Int8Type => ArrowDataType::Int8,
            DataType::Int16Type => ArrowDataType::Int16,
            DataType::Int32Type => ArrowDataType::Int32,
            DataType::Int64Type => ArrowDataType::Int64,
            DataType::Float32Type => ArrowDataType::Float16,
            DataType::StringType => ArrowDataType::Utf8,
        }
    }

    /// Gets QE's data type corresponding to Arrows's data type
    pub fn get_qe_data_type(arrow_type: &ArrowDataType) -> DataType {
        match arrow_type {
            ArrowDataType::Boolean => DataType::BooleanType,
            ArrowDataType::Int8 => DataType::Int8Type,
            ArrowDataType::Int16 => DataType::Int16Type,
            ArrowDataType::Int32 => DataType::Int32Type,
            ArrowDataType::Int64 => DataType::Int64Type,
            ArrowDataType::Float32 => DataType::Float32Type,
            ArrowDataType::Utf8 => DataType::StringType,
            _ => panic!("Invalid arrow data type {}", arrow_type),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::BooleanType => write!(f, "Boolean"),
            DataType::Int8Type => write!(f, "Int8"),
            DataType::Int16Type => write!(f, "Int16"),
            DataType::Int32Type => write!(f, "Int32"),
            DataType::Int64Type => write!(f, "Int64"),
            DataType::Float32Type => write!(f, "Float32"),
            DataType::StringType => write!(f, "String"),
        }
    }
}

/// Represents a Field in the Table
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
}

impl Field {
    /// Converts the Field into a corresponding [`Field`](ArrowField) in Arrow.
    /// ## Assumptions
    /// * By default all columns are nullable.
    fn to_arrow(&self) -> ArrowField {
        ArrowField::new(
            self.name.clone(),
            DataType::get_arrow_data_type(&self.data_type),
            true,
        )
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    fn to_arrow(&self) -> ArrowSchema {
        let arrow_fields: Vec<_> = self.fields.iter().map(|field| field.to_arrow()).collect();
        ArrowSchema::new(arrow_fields)
    }

    fn project(&self, indices: Vec<usize>) -> Schema {
        let projected_fields: Vec<_> = indices
            .into_iter()
            .map(|i| self.fields[i].clone())
            .collect();
        Schema {
            fields: projected_fields,
        }
    }

    pub fn select(&self, names: Vec<String>) -> Schema {
        let name_set: HashSet<_> = names.into_iter().collect();
        let selected_fields: Vec<_> = self
            .fields
            .iter()
            .filter(|field| name_set.contains(&field.name))
            .cloned()
            .collect();
        Schema {
            fields: selected_fields,
        }
    }
}

struct SchemaConverter {}

impl SchemaConverter {
    fn from_arrow(arrow_schema: ArrowSchema) -> Schema {
        let fields = arrow_schema
            .fields()
            .iter()
            .map(|f| Field {
                name: f.name().to_owned(),
                data_type: DataType::get_qe_data_type(f.data_type()),
            })
            .collect();
        Schema { fields }
    }
}

#[derive(Debug, Clone, Eq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            String(v) => v.hash(state),
            _ => panic!("hash not yet implemented"),
        }
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => Some(Ordering::Equal),
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a.partial_cmp(b),
            (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a.partial_cmp(b),
            (ScalarValue::Int16(a), ScalarValue::Int16(b)) => a.partial_cmp(b),
            (ScalarValue::Int32(a), ScalarValue::Int32(b)) => a.partial_cmp(b),
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a.partial_cmp(b),
            (ScalarValue::String(a), ScalarValue::String(b)) => a.partial_cmp(b),
            _ => panic!("Wrong data types compared"),
        }
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Null comparison returns true if both are Null
            (ScalarValue::Null, ScalarValue::Null) => true,
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a == b,
            (ScalarValue::Int16(a), ScalarValue::Int16(b)) => a == b,
            (ScalarValue::Int32(a), ScalarValue::Int32(b)) => a == b,
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a == b,
            (ScalarValue::String(a), ScalarValue::String(b)) => a == b,
            _ => false,
        }
    }
}

pub trait ColumnVector {
    fn get_type(&self) -> DataType;
    fn get_value(&self, i: usize) -> ScalarValue;
    fn size(&self) -> usize;
}

pub struct FieldVector {
    pub field_vector: Box<dyn Array>,
}

impl ColumnVector for FieldVector {
    fn get_type(&self) -> DataType {
        let data_type = self.field_vector.data_type();
        DataType::get_qe_data_type(data_type)
    }

    fn get_value(&self, i: usize) -> ScalarValue {
        if self.field_vector.len() <= i {
            panic!("invalid index")
        }
        let data_type = self.get_type();
        match data_type {
            DataType::BooleanType => {
                let array = self
                    .field_vector
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                ScalarValue::Boolean(array.value(i))
            }
            DataType::Int8Type => {
                let array = self
                    .field_vector
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                ScalarValue::Int8(array.value(i))
            }
            DataType::Int16Type => {
                let array = self
                    .field_vector
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap();
                ScalarValue::Int16(array.value(i))
            }
            DataType::Int32Type => {
                let array = self
                    .field_vector
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ScalarValue::Int32(array.value(i))
            }
            DataType::StringType => {
                let array = self
                    .field_vector
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                ScalarValue::String(array.value(i).to_string())
            }
            _ => panic!("Unsupported data type"),
        }
    }

    fn size(&self) -> usize {
        self.field_vector.len()
    }
}

pub struct LiteralValueVector {
    pub data_type: DataType,
    pub size: usize,
    pub value: ScalarValue,
}

impl ColumnVector for LiteralValueVector {
    fn get_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get_value(&self, i: usize) -> ScalarValue {
        if i >= self.size {
            panic!("Index out of bounds: {}", i);
        }
        self.value.clone()
    }
}

#[derive(Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub field_vectors: Vec<Arc<dyn ColumnVector>>,
}

impl RecordBatch {
    pub fn field(&self, i: usize) -> Arc<dyn ColumnVector> {
        self.field_vectors.get(i).unwrap().clone()
    }

    pub fn row_count(&self) -> usize {
        self.field_vectors.first().map_or(0, |f| f.size())
    }

    pub fn column_count(&self) -> usize {
        self.field_vectors.len()
    }

    pub fn to_csv(&self) -> String {
        let mut csv_str = String::new();
        let col_count = self.column_count();
        let row_count = self.row_count();

        for row_index in 0..row_count {
            for col_index in 0..col_count {
                if col_index > 0 {
                    csv_str.push(',')
                }
                let value = &self.field_vectors[col_index].get_value(row_index);
                if let ScalarValue::String(s) = value {
                    csv_str.push_str(s);
                } else {
                    panic!("Unsupported condition")
                }
            }
            csv_str.push('\n');
        }
        csv_str
    }
}

/// Responsible for building a [`FieldVector`] incrementally for any [`DataType`]
pub struct FieldVectorBuilder {
    pub data_type: DataType,
    pub builder: Box<dyn ArrayBuilder>,
}

impl FieldVectorBuilder {
    pub fn new(data_type: DataType) -> Self {
        let builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Int8Type => Box::new(Int8Builder::new()),
            DataType::Int16Type => Box::new(Int16Builder::new()),
            DataType::StringType => Box::new(StringBuilder::new()),
            _ => panic!("Not yet implemented"),
        };
        FieldVectorBuilder { data_type, builder }
    }

    pub fn append(&mut self, value: ScalarValue) {
        match (self.data_type.clone(), value) {
            (DataType::Int8Type, ScalarValue::Int8(v)) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<Int8Builder>()
                    .unwrap()
                    .append_value(v);
            }
            (DataType::StringType, ScalarValue::String(v)) => {
                self.builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(v);
            }
            _ => panic!("Not implemented append for FieldVectorBuilder for this data type"),
        }
    }

    pub fn build(&mut self) -> FieldVector {
        let array = self.builder.finish();
        FieldVector {
            field_vector: Box::new(array),
        }
    }
}
