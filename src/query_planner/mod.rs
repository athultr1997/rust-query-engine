use std::sync::Arc;

use crate::{
    datatypes::Schema,
    logical_plan::{
        convert_expr_to_type, convert_plan_to_type, LogicalExpr, LogicalExprType, LogicalPlan,
        LogicalPlanType,
    },
    physical_plan::{
        expression::{Expression, LiteralLongExpression},
        PhysicalPlan, ProjectionExec,
    },
};

trait QueryPlanner {}

struct DefaultQueryPlanner {}

impl QueryPlanner for DefaultQueryPlanner {}

impl DefaultQueryPlanner {
    fn create_physical_plan(&self, logical_plan: Arc<dyn LogicalPlan>) -> Arc<dyn PhysicalPlan> {
        let logical_plan_type = convert_plan_to_type(logical_plan);
        match logical_plan_type {
            LogicalPlanType::Scan(scan) => todo!(),
            LogicalPlanType::Selection(selection) => todo!(),
            LogicalPlanType::Projection(projection) => {
                let input = self.create_physical_plan(projection.clone());
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
            LogicalPlanType::Aggregate(aggregate) => todo!(),
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
            LogicalExprType::LiteralString(literal_string) => {
                todo!()
            }
            LogicalExprType::LiteralLong(literal_long) => Arc::new(LiteralLongExpression {
                value: literal_long.val,
            }),
            _ => todo!(),
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
        logical_plan::{col, max, AggregateExpr, LogicalExpr, Scan},
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
                    data_type: DataType::Float32Type,
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
        let plan = df.aggregate(group_expr, aggregate_expr);
        // TODO: complete the test case
    }
}
