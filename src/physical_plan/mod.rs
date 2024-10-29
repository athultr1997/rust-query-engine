use expression::Expression;

use crate::physical_plan::accumulator::Accumulator;

use crate::datatypes::{ColumnVector, RecordBatch, Schema};
use crate::logical_plan::AggregateExpr;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Mutex;
use std::{fmt::Write, sync::Arc};

pub mod accumulator;
pub mod expression;

/// Executes Logical Plan against data.
pub trait PhysicalPlan: Display {
    /// The output schema from running this Physical Plan.s
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

pub struct HashAggregateExec {
    pub input: Arc<dyn PhysicalPlan>,
    pub schema: Schema,
    pub group_expr: Vec<Arc<dyn Expression>>,
    pub aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }

    fn execute(&self) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        todo!()
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

struct HashAggregateIterator {
    input_iter: Arc<Mutex<dyn Iterator<Item = RecordBatch>>>,
    group_expr: Vec<Arc<dyn Expression>>,
    aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
}

impl Iterator for HashAggregateIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let map: HashMap<Arc<Vec<Box<dyn Any>>>, Vec<Arc<dyn Accumulator>>>;
        todo!("")
    }
}
