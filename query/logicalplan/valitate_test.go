package logicalplan

import (
	"github.com/stretchr/testify/require"
	"testing"
)

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
