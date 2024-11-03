use arrow::array::{Array, BooleanArray};
use expression::{AggregateExpression, Expression};

use crate::datasources::DataSource;
use crate::physical_plan::accumulator::Accumulator;

use crate::datatypes::{
    ColumnVector, DataType, FieldVector, FieldVectorBuilder, RecordBatch, ScalarValue, Schema,
};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Mutex;
use std::usize;
use std::{fmt::Write, sync::Arc};

pub mod accumulator;
pub mod expression;

/// Executes Logical Plan against data.
pub trait PhysicalPlan: Display {
    /// The output schema from running this Physical Plan
    fn schema(&self) -> Schema;
    /// Executes the physical plan to get results.
    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>>;
    /// Returns the children of the current PhysicalPlan. The children are also the input of the
    /// current PhysicalPlan. Enables the use of Visitor Pattern to walk the Query Tree.
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;
}

/// Helps in printing the physical plan in human readable format. Very helpful for debugging.
pub fn format(plan: &dyn PhysicalPlan, indent: Option<usize>) -> String {
    let indent = indent.unwrap_or(0);
    let mut plan_str: String = String::new();
    for _ in 0..indent {
        write!(plan_str, "\t").unwrap()
    }
    writeln!(plan_str, "{}", plan).unwrap();
    for child in plan.children() {
        plan_str.push_str(&format(child.as_ref(), Some(indent + 1)));
    }
    plan_str
}

/// Scan a [`DataSource`] with optional push-down projection
///
/// Physically reads data from the [`DataSource`] and gives out [`RecordBatch`]es.
pub struct ScanExec {
    pub data_source: Arc<dyn DataSource>,
    pub projection: Vec<String>,
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Schema {
        self.data_source.schema().select(self.projection.clone())
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![]
    }

    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        self.data_source.scan(self.projection.clone())
    }
}

impl Display for ScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let proj_str = self.projection.join(", ");
        write!(
            f,
            "ScanExec: schema={:?}, projection={}",
            self.schema(),
            proj_str
        )
    }
}

/// Executes a Projection
pub struct ProjectionExec {
    pub input: Arc<dyn PhysicalPlan>,
    pub schema: Schema,
    pub expr: Vec<Arc<dyn Expression>>,
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }

    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        Arc::new(Mutex::new(ProjectionIterator::new(
            self.input.clone(),
            self.schema.clone(),
            self.expr.clone(),
        )))
    }
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expr_str = self
            .expr
            .iter()
            .map(|expr| expr.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "ProjectionExec: [{}]", expr_str)
    }
}

struct ProjectionIterator {
    schema: Schema,
    expressions: Vec<Arc<dyn Expression>>,
    input_iter: Arc<Mutex<dyn Iterator<Item = RecordBatch>>>,
}

impl Iterator for ProjectionIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let record_batch = Arc::new(self.input_iter.lock().unwrap().next().unwrap());
        let columns = self
            .expressions
            .iter()
            .map(|e| e.evaluate(record_batch.clone()))
            .collect();
        Some(RecordBatch {
            schema: self.schema.clone(),
            field_vectors: columns,
        })
    }
}

impl ProjectionIterator {
    fn new(
        input: Arc<dyn PhysicalPlan>,
        schema: Schema,
        expressions: Vec<Arc<dyn Expression>>,
    ) -> Self {
        ProjectionIterator {
            schema,
            expressions,
            input_iter: input.execute(),
        }
    }
}

/// Represents the execution of a Filter node which filters the data based on the output of various
/// expressions.
pub struct FilterExec {
    // Represents the underlying input data which will be filtered.
    pub input: Arc<dyn PhysicalPlan>,
    // This expression must evaluate to boolean else this execution will fail.
    pub expression: Arc<dyn Expression>,
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }

    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        Arc::new(Mutex::new(FilterExecIterator {
            input_iter: self.input.execute(),
            expr: self.expression.clone(),
            schema: self.schema(),
        }))
    }
}

impl Display for FilterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

struct FilterExecIterator {
    input_iter: Arc<Mutex<dyn Iterator<Item = RecordBatch>>>,
    expr: Arc<dyn Expression>,
    schema: Schema,
}

impl Iterator for FilterExecIterator {
    type Item = RecordBatch;

    // TODO: Solve the additional cloning of RecordBatch here.
    fn next(&mut self) -> Option<Self::Item> {
        let input_record_batch = self.input_iter.lock().unwrap().next().unwrap();
        let output_column_count = input_record_batch.schema.fields.len();
        let result = self.expr.evaluate(input_record_batch.clone().into());
        let output_schema = self.schema.clone();
        let mut filtered_fields = vec![];
        for index in 0..output_column_count {
            let filtered_field = self.filter_rows(
                input_record_batch.field_vectors[index].clone(),
                result.clone(),
            );
            filtered_fields.push(filtered_field);
        }
        Some(RecordBatch {
            schema: output_schema,
            field_vectors: filtered_fields,
        })
    }
}

impl FilterExecIterator {
    // TODO: Vectorize this.
    fn filter_rows(
        &self,
        value_vector: Arc<dyn ColumnVector>,
        filter: Arc<dyn ColumnVector>,
    ) -> Arc<dyn ColumnVector> {
        assert_eq!(
            filter.get_type(),
            DataType::BooleanType,
            "return type of filter expression should be BooleanType"
        );

        let mut builder = FieldVectorBuilder::new(value_vector.get_type());
        for index in 0..value_vector.size() {
            match filter.get_value(index) {
                ScalarValue::Boolean(v) => {
                    if v {
                        builder.append(value_vector.get_value(index));
                    }
                }
                _ => panic!("inconsistent vector!"),
            }
        }

        Arc::new(builder.build()) as Arc<dyn ColumnVector>
    }
}

pub struct HashAggregateExec {
    pub input: Arc<dyn PhysicalPlan>,
    // the output schema from the hash aggregate plan node
    pub schema: Schema,
    pub group_expr: Vec<Arc<dyn Expression>>,
    pub aggregate_expr: Vec<Arc<dyn AggregateExpression>>,
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }

    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        Arc::new(Mutex::new(HashAggregateExecIterator {
            input_iter: self.input.execute(),
            group_expr: self.group_expr.clone(),
            aggregate_expr: self.aggregate_expr.clone(),
            schema: self.schema.clone(),
        }))
    }
}

impl Display for HashAggregateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group_expr_str = self
            .group_expr
            .iter()
            .map(|expr| expr.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let agg_expr_str = self
            .aggregate_expr
            .iter()
            .map(|e| e.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "HashAggregateExec: group_expr=[{}], aggregate_expr=[{}]",
            group_expr_str, agg_expr_str
        )
    }
}

struct HashAggregateExecIterator {
    input_iter: Arc<Mutex<dyn Iterator<Item = RecordBatch>>>,
    group_expr: Vec<Arc<dyn Expression>>,
    aggregate_expr: Vec<Arc<dyn AggregateExpression>>,
    schema: Schema,
}

impl Iterator for HashAggregateExecIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut map: HashMap<Vec<ScalarValue>, Vec<Arc<Mutex<dyn Accumulator>>>> = HashMap::new();
        let mut input_iterator = self.input_iter.lock().unwrap();
        for batch in input_iterator.into_iter() {
            let batch = Arc::new(batch);

            let group_keys: Vec<Arc<dyn ColumnVector>> = self
                .group_expr
                .iter()
                .map(|ge| ge.evaluate(batch.clone()))
                .collect();

            let agg_input_values: Vec<Arc<dyn ColumnVector>> = self
                .aggregate_expr
                .iter()
                .map(|ae| ae.input_expression().evaluate(batch.clone()))
                .collect();

            // for each row in the record batch
            for row_index in 0..batch.row_count() {
                // create the grouping key for the row
                let row_key: Vec<ScalarValue> = group_keys
                    .iter()
                    .map(|gk| gk.get_value(row_index))
                    .collect();
                println!("row_key = {:?}", row_key);

                // get or create accumulators for this grouping key
                let accumulators = match map.get(&row_key) {
                    Some(accumulator) => accumulator.clone(),
                    None => {
                        let new_accumulators = self
                            .aggregate_expr
                            .iter()
                            .map(|ae| ae.create_accumulator())
                            .collect::<Vec<Arc<Mutex<dyn Accumulator>>>>();
                        map.insert(row_key.clone(), new_accumulators.clone()); // Insert the new accumulator into the map
                        new_accumulators
                    }
                };

                // perform accumulation
                for (acc_index, acc) in accumulators.iter().enumerate() {
                    let value = agg_input_values[acc_index].get_value(row_index);
                    let mut acc = acc.lock().unwrap();
                    acc.accumulate(value);
                }
            }
        }

        let mut builders: Vec<FieldVectorBuilder> = Vec::with_capacity(self.schema.fields.len());

        // no need to use output_row_index because everything is append only. Nothing is getting
        // added out of order.
        for (output_row_index, (grouping_key, accumulators)) in map.iter().enumerate() {
            for (index, ge) in self.group_expr.clone().iter().enumerate() {
                builders[index].append(grouping_key[index].clone());
            }
            for (index, ae) in self.aggregate_expr.clone().iter().enumerate() {
                let acc_final_value = accumulators[index].lock().unwrap().final_value();
                builders[self.group_expr.len() + index].append(acc_final_value);
            }
        }

        let arrays: Vec<Arc<dyn ColumnVector>> = builders
            .iter_mut()
            .map(|b| Arc::new(b.build()) as Arc<dyn ColumnVector>)
            .collect();

        let output_batch = RecordBatch {
            schema: self.schema.clone(),
            field_vectors: arrays,
        };

        Some(output_batch)
    }
}
