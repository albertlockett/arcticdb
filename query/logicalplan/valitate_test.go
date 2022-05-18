package logicalplan

import (
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/stretchr/testify/require"
	"testing"
)

// TODO rename this file it's spelled wrong
// TODO clean up the assertions a bit

func TestFilterBinaryExprLeftSideMustBeColumn(t *testing.T) {
	_, err := (&Builder{}).
		Filter(BinaryExpr{
			Left:  Literal("a"),
			Op:    EqOp,
			Right: Literal("b"),
		}).
		Build()

	require.NotNil(t, err)
}

func TestFilterAndExprEvaluatesEachAndedRule(t *testing.T) {
	_, err := (&Builder{}).
		Scan(&mockTableProvider{dynparquet.NewSampleSchema()}, "table1").
		Filter(And(
			BinaryExpr{
				Left:  Col("example_type"),
				Op:    EqOp,
				Right: Literal("b"),
			},
			BinaryExpr{
				Left:  Literal("a"),
				Op:    EqOp,
				Right: Literal("b"),
			},
		)).
		Build()

	require.NotNil(t, err)
}
