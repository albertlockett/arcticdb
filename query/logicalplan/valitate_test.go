package logicalplan

import (
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

// TODO rename this file it's spelled wrong
// TODO check if this should even be a rule

func TestFilterBinaryExprLeftSideMustBeColumn(t *testing.T) {
	plan := (&Builder{}).
		Filter(BinaryExpr{
			Left:  Literal("a"),
			Op:    EqOp,
			Right: Literal("b"),
		}).
		Build()

	validator := &LogicalPlanValidator{plan}
	err := validator.Validate()
	require.NotNil(t, err)
}

func TestFilterAndExprEvaluatesEachAndedRule(t *testing.T) {
	plan := (&Builder{}).
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

	validator := &LogicalPlanValidator{plan}
	err := validator.Validate()
	log.Printf("%v", err) // TODO remove this
	require.Nil(t, err)
}
