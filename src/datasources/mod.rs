use std::{
    collections::HashMap,
    fs::File,
    sync::{Arc, Mutex},
};

use arrow::array::{
    BooleanBuilder, Float16Builder, Float32Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder,
};
use csv::{Reader, StringRecord};

use crate::datatypes::{ColumnVector, DataType, Field, FieldVector, RecordBatch, Schema};

pub trait DataSource {
    /// Returns the Schema for the underlying data source.
    fn schema(&self) -> Schema;
    // TODO: Move Vec<RecordBatch> to a lazy evaluation
    fn scan(&self, projection: Vec<String>) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>>;
}

pub struct CsvDataSource {
    pub filename: String,
    // schema:
    pub has_headers: bool,
    pub batch_size: usize,
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Schema {
        self.infer_schema()
    }

    fn scan(&self, projection: Vec<String>) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        let file = File::open(self.filename.clone()).unwrap();
        // TODO: move to lazy
        let read_schema = self.infer_schema().select(projection);
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        Arc::new(Mutex::new(ReadIterator::new(
            read_schema,
            reader,
            self.batch_size,
        )))
    }
}

impl CsvDataSource {
    fn infer_schema(&self) -> Schema {
        let file = File::open(self.filename.clone()).unwrap();
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(file);
        if self.has_headers {
            let headers = rdr.headers().unwrap();
            let fields = headers
                .iter()
                .map(|h| Field {
                    name: h.to_string(),
                    data_type: DataType::StringType,
                })
                .collect();
            return Schema { fields };
        }
        panic!("non header csv file is not yet implemented");
    }
}

struct ReadIterator {
    schema: Schema,
    reader: Reader<File>,
    batch_size: usize,
    field_indices: HashMap<String, usize>,
}

impl Iterator for ReadIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut rows = Vec::with_capacity(self.batch_size);
        for result in self.reader.records().take(self.batch_size) {
            match result {
                Ok(record) => rows.push(record),
                Err(err) => panic!("Error reading csv records: {}", err),
            }
        }
        if rows.is_empty() {
            return None;
        }
        Some(self.create_record_batch(rows))
    }
}

impl ReadIterator {
    fn new(schema: Schema, mut reader: Reader<File>, batch_size: usize) -> Self {
        let headers = reader.headers().unwrap();
        let mut field_indices = HashMap::new();
        for (i, header) in headers.iter().enumerate() {
            field_indices.insert(header.to_string(), i);
        }
        Self {
            schema,
            reader,
            batch_size,
            field_indices,
        }
    }

    fn create_record_batch(&self, rows: Vec<StringRecord>) -> RecordBatch {
        let mut arrays: Vec<Arc<dyn ColumnVector>> = Vec::with_capacity(self.schema.fields.len());
        for field in self.schema.fields.iter() {
            let field_name = field.name.as_str();
            let header_index = self.field_indices.get(field_name).unwrap().clone();
            match field.data_type {
                DataType::BooleanType => {
                    let mut builder = BooleanBuilder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row
                            .get(header_index)
                            .unwrap_or(&String::new())
                            .trim()
                            .to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<bool>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::Int8Type => {
                    let mut builder = Int8Builder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row
                            .get(header_index)
                            .unwrap_or(&String::new())
                            .trim()
                            .to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<i8>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::Int16Type => {
                    let mut builder = Int16Builder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row
                            .get(header_index)
                            .unwrap_or(&String::new())
                            .trim()
                            .to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<i16>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::Int32Type => {
                    let mut builder = Int32Builder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row
                            .get(header_index)
                            .unwrap_or(&String::new())
                            .trim()
                            .to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<i32>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::Int64Type => {
                    let mut builder = Int64Builder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row
                            .get(header_index)
                            .unwrap_or(&String::new())
                            .trim()
                            .to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<i64>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::Float32Type => {
                    let mut builder = Float32Builder::with_capacity(rows.len());
                    for row in &rows {
                        let value_str = row.get(header_index).unwrap_or("").trim().to_string();
                        if value_str.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(value_str.parse::<f32>().unwrap());
                        }
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
                DataType::StringType => {
                    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len());
                    for row in &rows {
                        let value_str = row.get(header_index).unwrap_or("").trim().to_string();
                        builder.append_value(value_str);
                    }
                    let array = builder.finish();
                    arrays.push(Arc::new(FieldVector {
                        field_vector: Box::new(array),
                    }) as Arc<dyn ColumnVector>);
                }
            }
        }
        RecordBatch {
            schema: self.schema.clone(),
            field_vectors: arrays,
        }
    }
}

pub struct InMemoryDataSource {
    pub schema: Schema,
    pub data: Vec<RecordBatch>,
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Each scan of an [`InMemoryDataSource`] creates an iterator over the copy of the entire
    /// data.
    fn scan(&self, projection: Vec<String>) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        Arc::new(Mutex::new(InMemoryDataSourceIterator::new(
            projection,
            self.schema.clone(),
            self.data.clone(),
        )))
    }
}

struct InMemoryDataSourceIterator {
    schema: Schema,
    projection_indices: Vec<usize>,
    data_iterator: Box<dyn Iterator<Item = RecordBatch>>,
}

impl Iterator for InMemoryDataSourceIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let rb = self.data_iterator.next().unwrap();
        let field_vectors = self
            .projection_indices
            .iter()
            .map(|pi| rb.field(*pi))
            .collect();
        Some(RecordBatch {
            schema: self.schema.clone(),
            field_vectors,
        })
    }
}

impl InMemoryDataSourceIterator {
    fn new(projection: Vec<String>, schema: Schema, data: Vec<RecordBatch>) -> Self {
        let projection_indices = projection
            .iter()
            .map(|name| {
                schema
                    .fields
                    .iter()
                    .position(|field| field.name == *name)
                    .expect(&format!("Field '{}' not found in schema", name))
            })
            .collect();
        let data_iterator = data.into_iter();
        InMemoryDataSourceIterator {
            schema,
            projection_indices,
            data_iterator: Box::new(data_iterator),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, collections::HashSet};

    use super::*;

    #[test]
    fn test_read_csv_data_source_with_headers() {
        let csv_data_source = CsvDataSource {
            filename: "testdata/test.csv".to_string(),
            has_headers: true,
            batch_size: 10,
        };
        let actual_schema = csv_data_source.schema();
        let expected_schema = Schema {
            fields: vec![
                Field {
                    name: "id".to_string(),
                    data_type: DataType::StringType,
                },
                Field {
                    name: "name".to_string(),
                    data_type: DataType::StringType,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::StringType,
                },
                Field {
                    name: "email".to_string(),
                    data_type: DataType::StringType,
                },
            ],
        };

        assert_eq!(actual_schema, expected_schema);
    }

    #[test]
    fn read_csv_with_projections() {
        let csv_data_source = CsvDataSource {
            filename: "testdata/test.csv".to_string(),
            has_headers: true,
            batch_size: 10,
        };
        let projection: Vec<_> = vec!["id", "name"].into_iter().map(String::from).collect();
        let iterator = csv_data_source.scan(projection.clone());
        let mut iter = iterator.lock().unwrap();
        while let Some(val) = iter.next() {
            let field = val.field(0);
            assert_eq!(field.size(), 5);

            assert_eq!(val.schema.fields.len(), projection.len());

            let actual_field_names: HashSet<_> =
                val.schema.fields.iter().map(|h| h.name.clone()).collect();
            let expected_field_names: HashSet<_> =
                projection.iter().map(|h| h.to_string()).collect();
            assert_eq!(actual_field_names, expected_field_names)
        }
    }
}
