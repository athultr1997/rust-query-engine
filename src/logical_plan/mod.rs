use crate::{
    datasources::DataSource,
    datatypes::{DataType, Field, Schema},
};
use core::panic;
use std::{
    any::Any,
    fmt::{Display, Write},
    sync::Arc,
};

pub enum LogicalPlanType {
    Projection(Arc<Projection>),
    Scan(Arc<Scan>),
    Limit(Arc<Limit>),
    Aggregate(Arc<Aggregate>),
    Selection(Arc<Selection>),
}

/// Represents a relation with a known schema. Each logical plan have zero or more logical plan as
/// its inputs. Represents what do and not how to do.
pub trait LogicalPlan: Display + Any {
    /// The schema of the data that will be produced by this plan.
    fn schema(&self) -> Schema;

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>>;

    fn as_any(&self) -> &dyn Any;

    fn into_enum(self: Arc<Self>) -> LogicalPlanType;
}

/// Helps in printing the [`LogicalPlan`] in human readable format. Very helpful for debugging.
pub fn format(plan: &dyn LogicalPlan, indent: Option<usize>) -> String {
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

/// Helper function to convert a [`LogicalPlan`] into its corresponding [`LogicalPlanType`]
pub fn convert_plan_to_type(plan: Arc<dyn LogicalPlan>) -> LogicalPlanType {
    plan.into_enum()
}

pub fn convert_expr_to_type(expr: Arc<dyn LogicalExpr>) -> LogicalExprType {
    expr.into_enum()
}

/// Represents fetching data from a `DataSource`. Only `LogicalPlan` with no other `LogicalPlan` as
/// its input. It is a leaf node in the Query Tree.
pub struct Scan {
    pub path: String,
    pub data_source: Arc<dyn DataSource>,
    pub projection: Vec<String>,
}

impl LogicalPlan for Scan {
    fn schema(&self) -> Schema {
        let schema = self.data_source.schema();
        if self.projection.is_empty() {
            schema
        } else {
            schema.select(self.projection.clone())
        }
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        Vec::new()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_enum(self: Arc<Self>) -> LogicalPlanType {
        LogicalPlanType::Scan(self)
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expr_strings: Vec<String> = self.projection.iter().map(|e| e.to_string()).collect();
        let expr_strings = expr_strings.join(", ");
        write!(f, "Scan: {}; projection=[{}]", self.path, expr_strings)
    }
}

/// Represents a filter over an input
pub struct Selection {
    pub input: Arc<dyn LogicalPlan>,
    pub expr: Arc<dyn LogicalExpr>,
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_enum(self: Arc<Self>) -> LogicalPlanType {
        LogicalPlanType::Selection(self)
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Selection: {}", self.expr)
    }
}

pub struct Limit {
    input: Arc<dyn LogicalPlan>,
    limit: usize,
}

impl LogicalPlan for Limit {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_enum(self: Arc<Self>) -> LogicalPlanType {
        LogicalPlanType::Limit(self)
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit: {}", self.limit)
    }
}

/// Represents a projection of a set of expr from an input.
pub struct Projection {
    pub input: Arc<dyn LogicalPlan>,
    pub expr: Vec<Arc<dyn LogicalExpr>>,
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_enum(self: Arc<Self>) -> LogicalPlanType {
        LogicalPlanType::Projection(self)
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expr_strings: Vec<_> = self.expr.iter().map(|e| e.to_string()).collect();
        let expr_strings = expr_strings.join(", ");
        write!(f, "Projection: {}", expr_strings)
    }
}

pub struct Aggregate {
    pub input: Arc<dyn LogicalPlan>,
    pub group_expr: Vec<Arc<dyn LogicalExpr>>,
    pub aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Schema {
        let mut fields: Vec<Field> = self
            .group_expr
            .iter()
            .map(|g| g.to_field(self.input.as_ref()))
            .collect();
        fields.extend(
            self.aggregate_expr
                .iter()
                .map(|a| AggregateExpr::to_field(a.as_ref(), self.input.as_ref()))
                .collect::<Vec<Field>>(),
        );

        Schema { fields }
    }

    fn children(&self) -> Vec<Arc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_enum(self: Arc<Self>) -> LogicalPlanType {
        LogicalPlanType::Aggregate(self)
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group_expr_str = self
            .group_expr
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let aggregate_expr_str = self
            .aggregate_expr
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "Aggregate: group_expr=[{}], aggregate_expr=[{}]",
            group_expr_str, aggregate_expr_str
        )
    }
}

pub enum LogicalExprType {
    Column(Arc<Column>),
    ColumnIndex(Arc<ColumnIndex>),
    LiteralString(Arc<LiteralString>),
    LiteralLong(Arc<LiteralLong>),
    Max(Arc<Max>),
    Min(Arc<Min>),
    Count(Arc<Count>),
    CastExpr(Arc<CastExpr>),
    Eq(Arc<Eq>),
}

pub fn col(name: String) -> Column {
    Column { name }
}

pub fn max(expr: Arc<dyn LogicalExpr>) -> Max {
    Max {
        input: expr.clone(),
    }
}

pub fn lit_long(val: i64) -> LiteralLong {
    LiteralLong { val }
}

pub fn lit_string(str: String) -> LiteralString {
    LiteralString { str }
}

/// Evaluated against the data at runtime.
/// Expressions can be combined to form deeply nested expression trees.
/// During query planning we need to metadata about the output of an expression.
pub trait LogicalExpr: Display + AsLogicalExpr {
    /// Used in query planning to know about the output of the expression
    fn to_field(&self, input: &dyn LogicalPlan) -> Field;

    fn into_enum(self: Arc<Self>) -> LogicalExprType;

    fn as_any(&self) -> &dyn Any;
}

pub trait AsLogicalExpr {
    fn as_base(self: Arc<Self>) -> Arc<dyn LogicalExpr>;
}

impl<T: LogicalExpr + 'static> AsLogicalExpr for T {
    fn as_base(self: Arc<Self>) -> Arc<dyn LogicalExpr> {
        self
    }
}

pub struct Column {
    pub name: String,
}

impl LogicalExpr for Column {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        input
            .schema()
            .fields
            .iter()
            .find(|f| f.name == self.name)
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "No column named {} in {:?}",
                    self.name,
                    input
                        .schema()
                        .fields
                        .iter()
                        .map(|f| &f.name)
                        .collect::<Vec<_>>()
                )
            })
    }

    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::Column(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.name)
    }
}

pub struct ColumnIndex {
    pub index: usize,
}

impl LogicalExpr for ColumnIndex {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        input.schema().fields[self.index].clone()
    }
    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::ColumnIndex(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.index)
    }
}

pub struct LiteralString {
    pub str: String,
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, _input: &dyn LogicalPlan) -> Field {
        Field {
            name: self.str.clone(),
            data_type: DataType::StringType,
        }
    }

    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::LiteralString(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for LiteralString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.str)
    }
}

pub struct LiteralLong {
    pub val: i64,
}

impl LogicalExpr for LiteralLong {
    fn to_field(&self, _input: &dyn LogicalPlan) -> Field {
        Field {
            name: self.val.to_string(),
            data_type: DataType::Int64Type,
        }
    }

    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::LiteralLong(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for LiteralLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.val)
    }
}

pub trait BinaryExpr: LogicalExpr {
    fn left(&self) -> Arc<dyn LogicalExpr>;
    fn right(&self) -> Arc<dyn LogicalExpr>;
    fn op(&self) -> String;
    fn fmt_impl(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left(), self.op(), self.right())
    }
}

pub trait BooleanBinaryExpr: BinaryExpr {
    fn name(&self) -> String;
    fn to_field(&self, _input: &dyn LogicalPlan) -> Field {
        Field {
            name: self.name(),
            data_type: DataType::BooleanType,
        }
    }
}

pub trait AggregateExpr: LogicalExpr {
    fn name(&self) -> String;
    fn expr(&self) -> &dyn LogicalExpr;
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        Field {
            name: self.name(),
            data_type: self.expr().to_field(input).data_type,
        }
    }
    fn fmt_impl(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.expr())
    }
}

pub struct Max {
    pub input: Arc<dyn LogicalExpr>,
}

impl AggregateExpr for Max {
    fn name(&self) -> String {
        "MAX".to_string()
    }
    fn expr(&self) -> &dyn LogicalExpr {
        self.input.as_ref()
    }
}

impl LogicalExpr for Max {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        AggregateExpr::to_field(self, input)
    }

    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::Max(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Max {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        AggregateExpr::fmt_impl(self, f)
    }
}

pub struct Min {
    pub input: Arc<dyn LogicalExpr>,
}

impl AggregateExpr for Min {
    fn name(&self) -> String {
        "MIN".to_string()
    }
    fn expr(&self) -> &dyn LogicalExpr {
        self.input.as_ref()
    }
}

impl LogicalExpr for Min {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        AggregateExpr::to_field(self, input)
    }
    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::Min(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Min {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        AggregateExpr::fmt_impl(self, f)
    }
}

pub struct Count {
    pub input: Arc<dyn LogicalExpr>,
}

impl AggregateExpr for Count {
    fn name(&self) -> String {
        "COUNT".to_string()
    }
    fn expr(&self) -> &dyn LogicalExpr {
        self.input.as_ref()
    }
}

impl LogicalExpr for Count {
    fn to_field(&self, _input: &dyn LogicalPlan) -> Field {
        Field {
            name: "COUNT".to_string(),
            data_type: DataType::Int32Type,
        }
    }
    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::Count(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COUNT({})", self.expr())
    }
}

pub struct CastExpr {
    pub expr: Arc<dyn LogicalExpr>,
    pub data_type: DataType,
}

impl LogicalExpr for CastExpr {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        Field {
            name: self.expr.to_field(input).name,
            data_type: self.data_type.clone(),
        }
    }

    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::CastExpr(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.data_type)
    }
}

pub struct Eq {
    pub left: Arc<dyn LogicalExpr>,
    pub right: Arc<dyn LogicalExpr>,
}

impl BinaryExpr for Eq {
    fn left(&self) -> Arc<dyn LogicalExpr> {
        self.left.clone()
    }
    fn right(&self) -> Arc<dyn LogicalExpr> {
        self.right.clone()
    }
    fn op(&self) -> String {
        "=".to_string()
    }
}

impl BooleanBinaryExpr for Eq {
    fn name(&self) -> String {
        "eq".to_string()
    }
}

impl LogicalExpr for Eq {
    fn to_field(&self, input: &dyn LogicalPlan) -> Field {
        BooleanBinaryExpr::to_field(self, input)
    }
    fn into_enum(self: Arc<Self>) -> LogicalExprType {
        LogicalExprType::Eq(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Eq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_impl(f)
    }
}

#[cfg(test)]
mod tests {
    use crate::datasources::CsvDataSource;

    use super::*;

    #[test]
    fn build_logical_plan_manually() {
        let csv = CsvDataSource {
            filename: "testdata/test.csv".to_string(),
            has_headers: true,
            batch_size: 10,
        };
        let scan = Scan {
            path: "testdata/test.csv".to_string(),
            data_source: Arc::new(csv),
            projection: vec![],
        };
        let filter_expr = Eq {
            left: Arc::new(Column {
                name: "state".to_string(),
            }),
            right: Arc::new(LiteralString {
                str: "CO".to_string(),
            }),
        };
        let selection = Selection {
            input: Arc::new(scan),
            expr: Arc::new(filter_expr),
        };
        let plan = Projection {
            input: Arc::new(selection),
            expr: vec![
                Arc::new(Column {
                    name: "id".to_string(),
                }),
                Arc::new(Column {
                    name: "first_name".to_string(),
                }),
                Arc::new(Column {
                    name: "last_name".to_string(),
                }),
            ],
        };
        let actual_plan_str = format(&plan, None);
        println!("{}", actual_plan_str);

        let expected_plan_str = "Projection: #id, #first_name, #last_name\n\tSelection: #state = 'CO'\n\t\tScan: testdata/test.csv; projection=[]\n";
        assert_eq!(actual_plan_str, expected_plan_str);
    }

    #[test]
    fn build_aggregate_plan() {
        let csv = CsvDataSource {
            filename: "testdata/test.csv".to_string(),
            has_headers: true,
            batch_size: 10,
        };
        let plan = Aggregate {
            input: Arc::new(Scan {
                path: "testdata/test.csv".to_string(),
                data_source: Arc::new(csv),
                projection: vec![],
            }),
            group_expr: vec![Arc::new(Column {
                name: "state".to_string(),
            })],
            aggregate_expr: vec![Arc::new(Max {
                input: Arc::new(CastExpr {
                    expr: Arc::new(Column {
                        name: "salary".to_string(),
                    }),
                    data_type: DataType::Int32Type,
                }),
            })],
        };

        let actual_plan_str = format(&plan, None);
        println!("{}", actual_plan_str);

        let expected_plan_str = "Aggregate: group_expr=[#state], aggregate_expr=[MAX(CAST(#salary AS Int32))]\n\tScan: testdata/test.csv; projection=[]\n";
        assert_eq!(expected_plan_str, actual_plan_str);
    }
}
