use core::panic;
use std::{
    any::Any,
    fmt::Display,
    sync::{Arc, Mutex},
};

use arrow::array::BooleanBuilder;

use crate::datatypes::{
    ColumnVector, DataType, FieldVector, LiteralValueVector, RecordBatch, ScalarValue,
};

use super::accumulator::{Accumulator, MaxAccumulator, SumAccumulator};

pub trait Expression: Display {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector>;
}

/// Interface for aggregate expressions like max, min, avg, and so on.
///
/// Note that, this is not a type of [`Expression`].
pub trait AggregateExpression: Display {
    fn input_expression(&self) -> Arc<dyn Expression>;
    fn create_accumulator(&self) -> Arc<Mutex<dyn Accumulator>>;
}

/// Used for fetching a column by column index
pub struct ColumnExpression {
    pub index: usize,
}

impl Expression for ColumnExpression {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        input.field(self.index)
    }
}

impl Display for ColumnExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.index)
    }
}

pub trait BooleanExpression: Expression {
    fn left(&self) -> Arc<dyn Expression>;
    fn right(&self) -> Arc<dyn Expression>;
    fn evaluate_boolean(&self, l: ScalarValue, r: ScalarValue, data_type: &DataType) -> bool;
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

pub struct EqExpression {
    pub left: Arc<dyn Expression>,
    pub right: Arc<dyn Expression>,
}

impl BooleanExpression for EqExpression {
    fn left(&self) -> Arc<dyn Expression> {
        self.left.clone()
    }

    fn right(&self) -> Arc<dyn Expression> {
        self.right.clone()
    }

    fn evaluate_boolean(&self, l: ScalarValue, r: ScalarValue, _data_type: &DataType) -> bool {
        l == r
    }
}

impl Expression for EqExpression {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        BooleanExpression::evaluate(self, input)
    }
}

impl Display for EqExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.left, self.right)
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
    fn evaluate_boolean(&self, l: ScalarValue, r: ScalarValue, data_type: &DataType) -> bool {
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
            value: ScalarValue::Int64(self.value),
            size: input.row_count(),
        })
    }
}

impl Display for LiteralLongExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Not implemented LiteralLongExpression Display")
    }
}

pub struct LiteralStringExpression {
    pub value: String,
}

impl Expression for LiteralStringExpression {
    fn evaluate(&self, input: Arc<RecordBatch>) -> Arc<dyn ColumnVector> {
        Arc::new(LiteralValueVector {
            data_type: DataType::StringType,
            value: ScalarValue::String(self.value.clone()),
            size: input.row_count(),
        })
    }
}

impl Display for LiteralStringExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

pub struct MaxExpression {
    pub expr: Arc<dyn Expression>,
}

impl AggregateExpression for MaxExpression {
    fn input_expression(&self) -> Arc<dyn Expression> {
        self.expr.clone()
    }

    fn create_accumulator(&self) -> Arc<Mutex<dyn Accumulator>> {
        Arc::new(Mutex::new(MaxAccumulator::new()))
    }
}

impl Display for MaxExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAX({})", self.expr.to_string())
    }
}

pub struct SumExpression {
    pub expr: Arc<dyn Expression>,
}

impl AggregateExpression for SumExpression {
    fn input_expression(&self) -> Arc<dyn Expression> {
        self.expr.clone()
    }

    fn create_accumulator(&self) -> Arc<Mutex<dyn Accumulator>> {
        Arc::new(Mutex::new(SumAccumulator::new()))
    }
}

impl Display for SumExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUM({})", self.expr.to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn max_expression() {
        let e = MaxExpression {
            expr: Arc::new(ColumnExpression { index: 0 }),
        };
        let accumulator = e.create_accumulator();
        let mut accumulator = accumulator.lock().unwrap();
        let values = [10, 14, 4];
        values
            .iter()
            .for_each(|&v| accumulator.accumulate(ScalarValue::Int32(v)));

        let result = accumulator.final_value();
        assert_eq!(result, ScalarValue::Int32(14));
    }

    #[test]
    fn sum_expression() {
        let e = SumExpression {
            expr: Arc::new(ColumnExpression { index: 0 }),
        };
        let accumulator = e.create_accumulator();
        let mut accumulator = accumulator.lock().unwrap();
        let values = [10, 14, 4];
        values
            .iter()
            .for_each(|&v| accumulator.accumulate(ScalarValue::Int32(v)));

        let result = accumulator.final_value();
        assert_eq!(result, ScalarValue::Int32(28));
    }
}
