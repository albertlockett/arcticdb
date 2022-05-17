package logicalplan

import (
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

	err := Validate(plan)
	require.NotNil(t, err)
}

func TestFilterAndExprEvaluatesEachAndedRule(t *testing.T) {
	plan := (&Builder{}).
		Filter(And(
			BinaryExpr{
				Left:  Col("a"),
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

	err := Validate(plan)
	log.Printf("%v", err) // TODO remove this
	require.Nil(t, err)
}
