use core::panic;
use std::{any::Any, sync::Arc};

use crate::datatypes::ScalarValue;

pub trait Accumulator {
    fn accumulate(&mut self, value: ScalarValue);
    fn final_value(&self) -> ScalarValue;
}

pub struct MaxAccumulator {
    pub acc_value: ScalarValue,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        MaxAccumulator {
            acc_value: ScalarValue::Null,
        }
    }
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: ScalarValue) {
        if self.acc_value != ScalarValue::Null {
            if self.acc_value < value {
                self.acc_value = value
            }
        } else {
            self.acc_value = value;
        }
    }

    fn final_value(&self) -> ScalarValue {
        self.acc_value.clone()
    }
}

pub struct SumAccumulator {
    pub acc_value: ScalarValue,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: ScalarValue) {
        if self.acc_value != ScalarValue::Null {
            match (self.acc_value.clone(), value) {
                (ScalarValue::Int32(acc), ScalarValue::Int32(v)) => {
                    self.acc_value = ScalarValue::Int32(acc + v)
                }
                _ => panic!("Not yet implemented"),
            }
        } else {
            self.acc_value = value;
        }
    }

    fn final_value(&self) -> ScalarValue {
        self.acc_value.clone()
    }
}

impl SumAccumulator {
    pub fn new() -> Self {
        SumAccumulator {
            acc_value: ScalarValue::Null,
        }
    }
}

/// Helper function to clone Any to Arc<dyn Any>
fn clone_arc(value: &dyn Any) -> Arc<dyn Any> {
    if let Some(v) = value.downcast_ref::<i8>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<i16>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<i32>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<i64>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<f32>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<f64>() {
        Arc::new(*v)
    } else if let Some(v) = value.downcast_ref::<String>() {
        Arc::new(v.clone())
    } else {
        panic!("Cloning is not implemented for this data type")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_accumulator_i32() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(ScalarValue::Int32(5));
        acc.accumulate(ScalarValue::Int32(3));
        acc.accumulate(ScalarValue::Int32(7));
        acc.accumulate(ScalarValue::Int32(2));

        let result = acc.final_value();
        assert_eq!(result, ScalarValue::Int32(7));
    }

    #[test]
    fn test_max_accumulator_string() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(ScalarValue::String("apple".to_string()));
        acc.accumulate(ScalarValue::String("banana".to_string()));
        acc.accumulate(ScalarValue::String("cherry".to_string()));

        let result = acc.final_value();
        assert_eq!(result, ScalarValue::String("cherry".to_string()));
    }

    #[test]
    fn test_max_accumulator_empty() {
        let acc = MaxAccumulator::new();
        assert_eq!(acc.final_value(), ScalarValue::Null);
    }

    #[test]
    fn test_max_accumulator_single_value() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(ScalarValue::Int32(42));

        let result = acc.final_value();
        assert_eq!(result, ScalarValue::Int32(42));
    }
}
