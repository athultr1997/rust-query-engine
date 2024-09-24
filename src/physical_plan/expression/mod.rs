use core::panic;
use std::{any::Any, fmt::Display, sync::Arc};

use arrow::array::BooleanBuilder;

use crate::datatypes::{ColumnVector, DataType, FieldVector, LiteralValueVector, RecordBatch};

use super::accumulator::Accumulator;

pub trait Expression: Display {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector>;
}

pub trait AggregateExpression: Expression {
    fn input_expression(&self) -> Arc<dyn Expression>;
    fn create_accumulator(&self) -> Arc<dyn Accumulator>;
}

pub trait BooleanExpression: Expression {
    fn left(&self) -> Arc<dyn Expression>;
    fn right(&self) -> Arc<dyn Expression>;
    fn evaluate_boolean(
        &self,
        l: Option<Box<dyn Any>>,
        r: Option<Box<dyn Any>>,
        data_type: &DataType,
    ) -> bool;
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        let l = self.left().evaluate(input.clone());
        let r = self.right().evaluate(input.clone());
        if l.size() != r.size() {
            panic!("left and right expressions output have different sizes");
        }
        if l.get_type() != r.get_type() {
            panic!(
                "left and right expressions have different data types: {}!={}",
                l.get_type(),
                r.get_type()
            );
        }
        // Size of `l` and `r` is the same
        self.compare(l.clone(), r.clone(), l.size(), l.get_type())
    }

    fn compare(
        &self,
        left: Arc<dyn ColumnVector>,
        right: Arc<dyn ColumnVector>,
        size: usize,
        data_type: DataType,
    ) -> Arc<dyn ColumnVector> {
        let mut array_builder = BooleanBuilder::with_capacity(size);
        for i in 0..size {
            let value = self.evaluate_boolean(left.get_value(i), right.get_value(i), &data_type);
            array_builder.append_value(value)
        }
        let array = array_builder.finish();
        Arc::new(FieldVector {
            field_vector: Box::new(array),
        })
    }
}

pub struct AndExpression {
    left: Arc<dyn Expression>,
    right: Arc<dyn Expression>,
}

impl BooleanExpression for AndExpression {
    fn left(&self) -> Arc<dyn Expression> {
        self.left.clone()
    }
    fn right(&self) -> Arc<dyn Expression> {
        self.right.clone()
    }
    fn evaluate_boolean(
        &self,
        l: Option<Box<dyn Any>>,
        r: Option<Box<dyn Any>>,
        data_type: &DataType,
    ) -> bool {
        todo!()
    }
}

impl Expression for AndExpression {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl Display for AndExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("not implemented")
    }
}

pub struct LiteralLongExpression {
    pub value: i64,
}

impl Expression for LiteralLongExpression {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            data_type: DataType::Int64Type,
            value: Some(self.value),
            size: input.row_count(),
        })
    }
}

impl Display for LiteralLongExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Not implemented LiteralLongExpression Display")
    }
}

fn to_bool(value: Arc<dyn Any>) {
    todo!()
}
