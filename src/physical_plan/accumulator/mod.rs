use std::{any::Any, sync::Arc};

pub trait Accumulator {
    fn accumulate(&mut self, value: &dyn Any);
    fn final_value(&self) -> Option<Arc<dyn Any>>;
}

struct MaxAccumulator {
    acc_value: Option<Arc<dyn Any>>,
}

impl MaxAccumulator {
    fn new() -> Self {
        MaxAccumulator { acc_value: None }
    }
    fn compare<T: PartialOrd + 'static>(&self, new_value: &T) -> bool {
        if let Some(current_value) = self.acc_value.as_ref() {
            if let Some(current_value) = current_value.downcast_ref::<T>() {
                return new_value > current_value;
            }
        }
        true
    }
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &dyn Any) {
        let is_max = if self.acc_value.is_none() {
            true
        } else if let Some(v) = value.downcast_ref::<i8>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<i16>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<i32>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<i64>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<f32>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<f64>() {
            self.compare(v)
        } else if let Some(v) = value.downcast_ref::<String>() {
            self.compare(v)
        } else {
            panic!("MAX is not implemented for this data type")
        };

        if is_max {
            self.acc_value = Some(clone_arc(value));
        }
    }

    fn final_value(&self) -> Option<Arc<dyn Any>> {
        self.acc_value.clone()
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
        acc.accumulate(&5i32);
        acc.accumulate(&3i32);
        acc.accumulate(&7i32);
        acc.accumulate(&2i32);
        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<i32>().unwrap();
        assert_eq!(*result, 7);
    }

    #[test]
    fn test_max_accumulator_f64() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&3.13f64);
        acc.accumulate(&2.71f64);
        acc.accumulate(&3.15f64);

        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<f64>().unwrap();
        assert_eq!(*result, 3.15);
    }

    #[test]
    fn test_max_accumulator_string() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&"apple".to_string());
        acc.accumulate(&"banana".to_string());
        acc.accumulate(&"cherry".to_string());

        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<String>().unwrap();
        assert_eq!(result, "cherry");
    }

    #[test]
    fn test_max_accumulator_empty() {
        let acc = MaxAccumulator::new();
        assert!(acc.final_value().is_none());
    }

    #[test]
    fn test_max_accumulator_single_value() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&42i32);

        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<i32>().unwrap();
        assert_eq!(*result, 42);
    }

    #[test]
    fn test_max_accumulator_mixed_types() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&5i32);
        acc.accumulate(&3.13f64);
        acc.accumulate(&"hello".to_string());

        // The accumulator should keep the last max value, which is "hello"
        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<String>().unwrap();
        assert_eq!(result, "hello");
    }

    // TODO: Better error handling. Should be MAX is not supported.
    #[test]
    #[should_panic(expected = "Cloning is not implemented for this data type")]
    fn test_max_accumulator_unsupported_type() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&vec![1, 2, 3]);
    }

    #[test]
    fn test_max_accumulator_all_supported_types() {
        let mut acc = MaxAccumulator::new();

        acc.accumulate(&100i8);
        acc.accumulate(&200i16);
        acc.accumulate(&300i32);
        acc.accumulate(&400i64);
        acc.accumulate(&1.23f32);
        acc.accumulate(&4.56f64);
        acc.accumulate(&"xyz".to_string());

        // The accumulator should keep the last max value, which is "xyz"
        let result = acc.final_value().unwrap();
        let result = result.downcast_ref::<String>().unwrap();
        assert_eq!(result, "xyz");
    }
}
