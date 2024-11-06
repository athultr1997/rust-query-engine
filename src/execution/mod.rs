use std::sync::{Arc, Mutex};

use crate::{
    dataframe::{Dataframe, DataframeImpl},
    datasources::CsvDataSource,
    datatypes::RecordBatch,
    logical_plan::Scan,
    optimizer::{DefaultOptimizer, Optimizer},
    physical_plan::format,
    query_planner::DefaultQueryPlanner,
};

struct ExecutionContext {
    batch_size: usize,
}

impl ExecutionContext {
    fn new() -> Self {
        ExecutionContext { batch_size: 1024 }
    }

    /// Creates a [`Dataframe`] which stores the execution of reading a csv file from the specified path
    fn csv(&self, file_name: String) -> Arc<dyn Dataframe> {
        Arc::new(DataframeImpl {
            plan: Arc::new(Scan {
                path: file_name.clone(),
                data_source: Arc::new(CsvDataSource {
                    filename: file_name,
                    has_headers: true,
                    batch_size: self.batch_size,
                }),
                projection: vec![],
            }),
        })
    }

    /// Executes the [`LogicalPlan`] which the [`Dataframe`] represents.
    fn execute(&self, df: Arc<dyn Dataframe>) -> Arc<Mutex<dyn Iterator<Item = RecordBatch>>> {
        let logical_plan = df.get_logical_plan();
        let optimizer = DefaultOptimizer::new();
        let optimized_logical_plan = optimizer.optimize(&(*logical_plan));

        let query_planner = DefaultQueryPlanner::new();
        let physical_plan = query_planner.create_physical_plan(optimized_logical_plan);
        physical_plan.execute()
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutionContext;
    use crate::logical_plan::{col, lit_string, Eq};
    use std::sync::Arc;

    #[test]
    fn employees_in_co_using_dataframe() {
        let execution_context = ExecutionContext::new();
        let file_name = "testdata/employee.csv";
        let df = execution_context
            .csv(file_name.to_string())
            .filter(Arc::new(Eq {
                left: Arc::new(col("state".to_string())),
                right: Arc::new(lit_string("CO".to_string())),
            }))
            .project(vec![
                Arc::new(col("id".to_string())),
                Arc::new(col("first_name".to_string())),
                Arc::new(col("last_name".to_string())),
            ]);

        let iter = execution_context.execute(df);
        let mut iter = iter.lock().unwrap();
        let mut result = vec![];
        while let Some(v) = iter.next() {
            result.push(v);
        }

        assert_eq!(result.len(), 1);

        let record_batch = result[0].clone();
        let record_batch_str = record_batch.to_csv();

        println!("{}", record_batch_str);
        assert_eq!(record_batch_str, "1,Bob,Ben\n4,Mayne,Vary\n");
    }
}
