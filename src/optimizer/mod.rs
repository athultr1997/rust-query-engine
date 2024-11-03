use core::panic;
use std::{collections::HashSet, sync::Arc};

use crate::logical_plan::{
    Aggregate, AggregateExpr, BinaryExpr, CastExpr, Column, ColumnIndex, Count, Eq, Filter,
    LiteralLong, LiteralString, LogicalExpr, LogicalPlan, Max, Min, Projection, Scan,
};

trait Optimizer {
    fn optimize(&self, plan: &dyn LogicalPlan) -> Arc<dyn LogicalPlan>;
}

trait Rule {
    fn optimize(&self, plan: &dyn LogicalPlan) -> Arc<dyn LogicalPlan>;
}

struct DefaultOptimizer {}

impl Optimizer for DefaultOptimizer {
    fn optimize(&self, plan: &dyn LogicalPlan) -> Arc<dyn LogicalPlan> {
        todo!()
    }
}

struct ProjectionPushDownRule {}

impl Rule for ProjectionPushDownRule {
    fn optimize(&self, plan: &dyn LogicalPlan) -> Arc<dyn LogicalPlan> {
        let mut col_names = vec![];
        self.push_down(plan, &mut col_names)
    }
}

impl ProjectionPushDownRule {
    fn push_down(
        &self,
        plan: &dyn LogicalPlan,
        col_names: &mut Vec<String>,
    ) -> Arc<dyn LogicalPlan> {
        if let Some(projection) = plan.as_any().downcast_ref::<Projection>() {
            extract_columns(&projection.expr, &*projection.input, col_names);
            let input = self.push_down(&*projection.input, col_names);
            Arc::new(Projection {
                expr: projection.expr.clone(),
                input,
            })
        } else if let Some(filter) = plan.as_any().downcast_ref::<Filter>() {
            extract_column(filter.expr.clone(), &*filter.input, col_names);
            let input = self.push_down(&*filter.input, col_names);
            Arc::new(Filter {
                expr: filter.expr.clone(),
                input,
            })
        } else if let Some(aggregate) = plan.as_any().downcast_ref::<Aggregate>() {
            extract_columns(&aggregate.group_expr, &*aggregate.input, col_names);
            let aggregate_log_expr = aggregate
                .aggregate_expr
                .iter()
                .map(|e| e.clone().as_base())
                .collect();
            extract_columns(&aggregate_log_expr, &*aggregate.input, col_names);
            let input = self.push_down(&*aggregate.input, col_names);
            Arc::new(Aggregate {
                group_expr: aggregate.group_expr.clone(),
                aggregate_expr: aggregate.aggregate_expr.clone(),
                input,
            })
        } else if let Some(scan) = plan.as_any().downcast_ref::<Scan>() {
            // This is the crucial part. The [`col_names`] could have duplicate entries from
            // different expressions, hence we are converting it to a [`HashSet`].
            let col_names_set: HashSet<String> = col_names.iter().map(|c| c.to_string()).collect();
            let valid_field_names: HashSet<String> = plan
                .schema()
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();
            let mut push_down: Vec<String> = valid_field_names
                .intersection(&col_names_set)
                .cloned()
                .collect();
            push_down.sort();
            Arc::new(Scan {
                path: scan.path.clone(),
                data_source: scan.data_source.clone(),
                projection: push_down,
            })
        } else {
            panic!("Plan does not support ProjectionPushDownRule: {}", plan)
        }
    }
}

fn extract_columns(
    expr: &Vec<Arc<dyn LogicalExpr>>,
    input: &dyn LogicalPlan,
    accum: &mut Vec<String>,
) {
    let _: Vec<_> = expr
        .iter()
        .map(|e| extract_column(Arc::clone(e), input, accum))
        .collect();
}

fn extract_column(expr: Arc<dyn LogicalExpr>, input: &dyn LogicalPlan, accum: &mut Vec<String>) {
    if let Some(column) = (*expr).as_any().downcast_ref::<Column>() {
        accum.push(column.name.clone());
    } else if let Some(column_index) = (*expr).as_any().downcast_ref::<ColumnIndex>() {
        let col_name = input.schema().fields[column_index.index].name.clone();
        accum.push(col_name);
    } else if let Some(binary_expr) = (*expr).as_any().downcast_ref::<Eq>() {
        extract_column(binary_expr.left(), input, accum);
        extract_column(binary_expr.right(), input, accum);
    } else if let Some(cast_expr) = (*expr).as_any().downcast_ref::<CastExpr>() {
        extract_column(cast_expr.expr.clone(), input, accum);
    } else if let Some(_literal_string) = (*expr).as_any().downcast_ref::<LiteralString>() {
    } else if let Some(_literal_long) = (*expr).as_any().downcast_ref::<LiteralLong>() {
    } else if let Some(min_expr) = (*expr).as_any().downcast_ref::<Min>() {
        extract_column(min_expr.input.clone(), input, accum);
    } else if let Some(max_expr) = (*expr).as_any().downcast_ref::<Max>() {
        extract_column(max_expr.input.clone(), input, accum);
    } else if let Some(count_expr) = (*expr).as_any().downcast_ref::<Count>() {
        extract_column(count_expr.input.clone(), input, accum);
    } else {
        panic!("extract_column does not support the expression: {}", expr);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        dataframe::{Dataframe, DataframeImpl},
        datasources::CsvDataSource,
        logical_plan::{col, format, lit_string, Count, Eq, Max, Min},
    };

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
    fn projection_push_down_basic() {
        let df = csv().project(vec![
            Arc::new(col("id".to_string())),
            Arc::new(col("first_name".to_string())),
            Arc::new(col("last_name".to_string())),
        ]);
        let rule = ProjectionPushDownRule {};
        let optimized_plan = rule.optimize(&*df.get_logical_plan());
        let actual_optimized_plan_str = format(&*optimized_plan, None);

        println!(
            "unoptimized initial plan:\n{}",
            format(&*df.get_logical_plan(), None)
        );
        println!("optimized final plan:\n{}", actual_optimized_plan_str);

        let expected_optimized_plan_str = "Projection: #id, #first_name, #last_name\n\tScan: employee; projection=[first_name, id, last_name]\n";

        assert_eq!(expected_optimized_plan_str, actual_optimized_plan_str);
    }

    #[test]
    fn projection_push_down_selection() {
        let df = csv()
            .filter(Arc::new(Eq {
                left: Arc::new(col("state".to_string())),
                right: Arc::new(lit_string("CO".to_string())),
            }))
            .project(vec![
                Arc::new(col("id".to_string())),
                Arc::new(col("first_name".to_string())),
                Arc::new(col("last_name".to_string())),
            ]);
        let unoptimized_plan_str = format(&*df.get_logical_plan(), None);
        println!("initial unoptimized plan:\n{}", unoptimized_plan_str);

        let rule = ProjectionPushDownRule {};

        let optimized_plan = rule.optimize(&*df.get_logical_plan());
        let actual_optimized_plan_str = format(&*optimized_plan, None);
        println!("final optimized plan:\n{}", actual_optimized_plan_str);

        let expected_optimized_plan_str = "Projection: #id, #first_name, #last_name\n\tFilter: #state = 'CO'\n\t\tScan: employee; projection=[first_name, id, last_name, state]\n";

        assert_eq!(expected_optimized_plan_str, actual_optimized_plan_str);
    }

    #[test]
    fn projection_push_down_with_aggregate_query() {
        let df = csv().aggregate(
            vec![Arc::new(col("state".to_string()))],
            vec![
                Arc::new(Min {
                    input: Arc::new(col("salary".to_string())),
                }),
                Arc::new(Max {
                    input: Arc::new(col("salary".to_string())),
                }),
                Arc::new(Count {
                    input: Arc::new(col("salary".to_string())),
                }),
            ],
        );
        let unoptimized_plan_str = format(&*df.get_logical_plan(), None);
        println!("initial unoptimized_plan:\n{}", unoptimized_plan_str);

        let rule = ProjectionPushDownRule {};
        let actual_optimized_plan = rule.optimize(&*df.get_logical_plan());
        let actual_optimized_plan_str = format(&*actual_optimized_plan, None);
        println!("final optimized plan:\n{}", actual_optimized_plan_str);

        let expected_optimized_plan_str = "Aggregate: group_expr=[#state], aggregate_expr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n\tScan: employee; projection=[salary, state]\n";

        assert_eq!(expected_optimized_plan_str, actual_optimized_plan_str);
    }
}
