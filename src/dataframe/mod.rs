use std::sync::Arc;

use crate::{
    datatypes::Schema,
    logical_plan::{Aggregate, AggregateExpr, LogicalExpr, LogicalPlan, Projection, Selection},
};

/// Helps build Logical Query Plans in a user-friendly way
pub trait Dataframe {
    fn project(&self, expr: Vec<Arc<dyn LogicalExpr>>) -> Arc<dyn Dataframe>;
    fn filter(&self, expr: Arc<dyn LogicalExpr>) -> Arc<dyn Dataframe>;
    fn aggregate(
        &self,
        group_expr: Vec<Arc<dyn LogicalExpr>>,
        aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn Dataframe>;
    fn schema(&self) -> Schema;
    fn get_logical_plan(&self) -> Arc<dyn LogicalPlan>;
}

pub struct DataframeImpl {
    pub plan: Arc<dyn LogicalPlan>,
}

impl Dataframe for DataframeImpl {
    fn project(&self, expr: Vec<Arc<dyn LogicalExpr>>) -> Arc<dyn Dataframe> {
        Arc::new(DataframeImpl {
            plan: Arc::new(Projection {
                input: self.plan.clone(),
                expr,
            }),
        })
    }

    fn filter(&self, expr: Arc<dyn LogicalExpr>) -> Arc<dyn Dataframe> {
        Arc::new(DataframeImpl {
            plan: Arc::new(Selection {
                input: self.plan.clone(),
                expr,
            }),
        })
    }

    fn aggregate(
        &self,
        group_expr: Vec<Arc<dyn LogicalExpr>>,
        aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
    ) -> Arc<dyn Dataframe> {
        Arc::new(DataframeImpl {
            plan: Arc::new(Aggregate {
                input: self.plan.clone(),
                group_expr,
                aggregate_expr,
            }),
        })
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }

    fn get_logical_plan(&self) -> Arc<dyn LogicalPlan> {
        self.plan.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datasources::CsvDataSource,
        logical_plan::{format, Column, Count, Eq, LiteralString, Max, Min, Scan},
    };

    use super::*;

    fn csv() -> Box<dyn Dataframe> {
        let employee_csv = "testdata/employee.csv";
        Box::new(DataframeImpl {
            plan: Arc::new(Scan {
                path: "employee".to_string(),
                data_source: Arc::new(CsvDataSource {
                    filename: employee_csv.to_string(),
                    has_headers: true,
                    batch_size: 1024,
                }),
                projection: vec![],
            }),
        })
    }

    #[test]
    fn build_dataframe() {
        let df = csv()
            .filter(Arc::new(Eq {
                left: Arc::new(Column {
                    name: "state".to_string(),
                }),
                right: Arc::new(LiteralString {
                    str: "CO".to_string(),
                }),
            }))
            .project(vec![
                Arc::new(Column {
                    name: "id".to_string(),
                }),
                Arc::new(Column {
                    name: "first_name".to_string(),
                }),
                Arc::new(Column {
                    name: "last_name".to_string(),
                }),
            ]);
        let actual_logical_plan = format(df.get_logical_plan().as_ref(), None);
        println!("{}", actual_logical_plan);

        let expected_logical_plan = "Projection: #id, #first_name, #last_name\n\tSelection: #state = 'CO'\n\t\tScan: employee; projection=[]\n";

        assert_eq!(expected_logical_plan, actual_logical_plan);
    }

    #[test]
    fn aggregate_query() {
        let df = csv().aggregate(
            vec![Arc::new(Column {
                name: "state".to_string(),
            })],
            vec![
                Arc::new(Min {
                    input: Arc::new(Column {
                        name: "salary".to_string(),
                    }),
                }),
                Arc::new(Max {
                    input: Arc::new(Column {
                        name: "salary".to_string(),
                    }),
                }),
                Arc::new(Count {
                    input: Arc::new(Column {
                        name: "salary".to_string(),
                    }),
                }),
            ],
        );
        let actual_logical_plan = format(df.get_logical_plan().as_ref(), None);
        println!("{}", actual_logical_plan);

        let expected_logical_plan = "Aggregate: group_expr=[#state], aggregate_expr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n\tScan: employee; projection=[]\n".to_string();

        assert_eq!(expected_logical_plan, actual_logical_plan);
    }
}
