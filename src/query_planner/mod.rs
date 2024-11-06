use core::panic;
use std::sync::Arc;

use crate::{
    datatypes::Schema,
    logical_plan::{
        convert_expr_to_type, convert_plan_to_type, Eq, LogicalExpr, LogicalExprType, LogicalPlan,
        LogicalPlanType, Max, Min,
    },
    physical_plan::{
        expression::{
            AggregateExpression, ColumnExpression, EqExpression, Expression, LiteralLongExpression,
            LiteralStringExpression, MaxExpression, SumExpression,
        },
        FilterExec, HashAggregateExec, PhysicalPlan, ProjectionExec, ScanExec,
    },
};

pub trait QueryPlanner {}

pub struct DefaultQueryPlanner {}

impl QueryPlanner for DefaultQueryPlanner {}

impl DefaultQueryPlanner {
    pub fn new() -> Self {
        DefaultQueryPlanner {}
    }

    pub fn create_physical_plan(
        &self,
        logical_plan: Arc<dyn LogicalPlan>,
    ) -> Arc<dyn PhysicalPlan> {
        let logical_plan_type = convert_plan_to_type(logical_plan);
        match logical_plan_type {
            LogicalPlanType::Scan(scan) => Arc::new(ScanExec {
                data_source: scan.data_source.clone(),
                projection: scan.projection.clone(),
            }),
            LogicalPlanType::Filter(filter) => {
                let input = self.create_physical_plan(filter.input.clone());
                let filter_expr = self.create_physical_expr(filter.expr.clone(), &(*filter.input));
                Arc::new(FilterExec {
                    input,
                    expression: filter_expr,
                })
            }
            LogicalPlanType::Projection(projection) => {
                let input = self.create_physical_plan(projection.input.clone());
                let projection_expr: Vec<Arc<dyn Expression>> = projection
                    .expr
                    .iter()
                    .map(|e| self.create_physical_expr(e.clone(), &(*projection.input)))
                    .collect();
                let fields = projection
                    .expr
                    .iter()
                    .map(|e| e.to_field(&(*projection.input)))
                    .collect();
                let projection_schema = Schema { fields };
                Arc::new(ProjectionExec {
                    input,
                    schema: projection_schema,
                    expr: projection_expr,
                })
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input = self.create_physical_plan(aggregate.input.clone());
                let group_exprs: Vec<Arc<dyn Expression>> = aggregate
                    .group_expr
                    .iter()
                    .map(|ge| self.create_physical_expr(ge.clone(), &(*aggregate.input)))
                    .collect();
                let agg_exprs: Vec<Arc<dyn AggregateExpression>> = aggregate
                    .aggregate_expr
                    .iter()
                    .map(|ae| {
                        if let Some(max_expression) = ae.as_any().downcast_ref::<Max>() {
                            Arc::new(MaxExpression {
                                expr: self.create_physical_expr(ae.expr(), &(*aggregate.input)),
                            }) as Arc<dyn AggregateExpression>
                        } else if let Some(sum_expression) = ae.as_any().downcast_ref::<Min>() {
                            Arc::new(SumExpression {
                                expr: self.create_physical_expr(ae.expr(), &(*aggregate.input)),
                            }) as Arc<dyn AggregateExpression>
                        } else {
                            panic!("Aggregate query plan not yet implemented for this aggregation");
                        }
                    })
                    .collect();
                Arc::new(HashAggregateExec {
                    input,
                    schema: aggregate.schema(),
                    group_expr: group_exprs,
                    aggregate_expr: agg_exprs,
                })
            }
            LogicalPlanType::Limit(limit) => todo!(),
        }
    }

    fn create_physical_expr(
        &self,
        expr: Arc<dyn LogicalExpr>,
        input: &dyn LogicalPlan,
    ) -> Arc<dyn Expression> {
        let logical_expr_type = convert_expr_to_type(expr);
        match logical_expr_type {
            LogicalExprType::LiteralString(literal_string) => Arc::new(LiteralStringExpression {
                value: literal_string.str.clone(),
            }),
            LogicalExprType::LiteralLong(literal_long) => Arc::new(LiteralLongExpression {
                value: literal_long.val,
            }),
            LogicalExprType::Column(column) => {
                match input
                    .schema()
                    .fields
                    .iter()
                    .position(|f| f.name == column.name)
                {
                    Some(index) => Arc::new(ColumnExpression { index }),
                    None => panic!("column not found in schema"),
                }
            }
            LogicalExprType::Eq(eq) => {
                let left = self.create_physical_expr(eq.left.clone(), input);
                let right = self.create_physical_expr(eq.right.clone(), input);
                Arc::new(EqExpression { left, right })
            }
            _ => panic!("not yet implemented: {}", logical_expr_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        dataframe::{Dataframe, DataframeImpl},
        datasources::InMemoryDataSource,
        datatypes::{DataType, Field, Schema},
        logical_plan::{col, format, max, AggregateExpr, LogicalExpr, Scan},
        optimizer::{DefaultOptimizer, Optimizer},
        physical_plan::format as physical_plan_format,
        query_planner::DefaultQueryPlanner,
    };

    #[test]
    fn plan_aggregate_qeury() {
        let schema = Schema {
            fields: vec![
                Field {
                    name: "passenger_count".to_string(),
                    data_type: DataType::Int32Type,
                },
                Field {
                    name: "max_fare".to_string(),
                    data_type: DataType::Int32Type,
                },
            ],
        };
        let datasource = InMemoryDataSource {
            schema,
            data: vec![],
        };
        let df = DataframeImpl {
            plan: Arc::new(Scan {
                path: "".to_string(),
                data_source: Arc::new(datasource),
                projection: vec![],
            }),
        };
        let group_expr = vec![Arc::new(col("passenger_count".to_string()))]
            .into_iter()
            .map(|e| Arc::clone(&e) as Arc<dyn LogicalExpr>)
            .collect();
        let aggregate_expr = vec![Arc::new(max(Arc::new(col("max_fare".to_string()))))]
            .into_iter()
            .map(|e| Arc::clone(&e) as Arc<dyn AggregateExpr>)
            .collect();
        let df = df.aggregate(group_expr, aggregate_expr);

        let logical_plan = df.get_logical_plan();
        let logical_plan_str = format(&(*logical_plan), None);
        println!("logical_plan = {}", logical_plan_str);
        assert_eq!(logical_plan_str, "Aggregate: group_expr=[#passenger_count], aggregate_expr=[MAX(#max_fare)]\n\tScan: ; projection=[]\n");

        let optimizer = DefaultOptimizer::new();
        let optimized_logical_plan = optimizer.optimize(&(*logical_plan));
        let optimized_logical_plan_str = format(&(*optimized_logical_plan), None);
        println!("optimized logical_plan = {}", optimized_logical_plan_str);
        assert_eq!(optimized_logical_plan_str, "Aggregate: group_expr=[#passenger_count], aggregate_expr=[MAX(#max_fare)]\n\tScan: ; projection=[max_fare, passenger_count]\n");

        let query_planner = DefaultQueryPlanner::new();
        let physical_plan = query_planner.create_physical_plan(optimized_logical_plan.clone());
        let physical_plan_str = physical_plan_format(&(*physical_plan), None);
        println!("physical_plan = {}", physical_plan_str);
        assert_eq!(physical_plan_str, "HashAggregateExec: group_expr=[#0], aggregate_expr=[MAX(#1)]\n\tScanExec: schema=Schema { fields: [Field { name: \"passenger_count\", data_type: Int32Type }, Field { name: \"max_fare\", data_type: Int32Type }] }, projection=max_fare, passenger_count\n");
    }
}
