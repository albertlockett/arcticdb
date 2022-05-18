package logicalplan

import (
	"testing"

	"github.com/apache/arrow/go/v8/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestLogicalPlanBuilder(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(nil, "table1").
		Filter(Col("labels.test").Eq(Literal("abc"))).
		Aggregate(
			Sum(Col("value")).Alias("value_sum"),
			Col("stacktrace"),
		).
		Project(Col("stacktrace")).
		Build()

	require.Equal(t, &LogicalPlan{
		Projection: &Projection{
			Exprs: []Expr{
				Column{ColumnName: "stacktrace"},
			},
		},
		Input: &LogicalPlan{
			Aggregation: &Aggregation{
				GroupExprs: []Expr{Column{ColumnName: "stacktrace"}},
				AggExpr: AliasExpr{
					Expr:  AggregationFunction{Func: SumAggFunc, Expr: Column{ColumnName: "value"}},
					Alias: "value_sum",
				},
			},
			Input: &LogicalPlan{
				Filter: &Filter{
					Expr: BinaryExpr{
						Left:  Column{ColumnName: "labels.test"},
						Op:    EqOp,
						Right: LiteralExpr{Value: scalar.MakeScalar("abc")},
					},
				},
				Input: &LogicalPlan{
					TableScan: &TableScan{
						TableProvider: TableProvider(nil),
						TableName:     "table1",
					},
				},
			},
		},
	}, p)
}

func TestLogicalPlanBuilderWithoutProjection(t *testing.T) {
	p, _ := (&Builder{}).
		Scan(nil, "table1").
		Distinct(Col("labels.test")).
		Build()

	require.Equal(t, &LogicalPlan{
		Distinct: &Distinct{
			Columns: []Expr{Column{ColumnName: "labels.test"}},
		},
		Input: &LogicalPlan{
			TableScan: &TableScan{
				TableProvider: TableProvider(nil),
				TableName:     "table1",
			},
		},
	}, p)
}
