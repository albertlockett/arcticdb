package logicalplan

import (
	"errors"
	"reflect"
)

func Validate(plan *LogicalPlan) error {
	switch {
	case plan.SchemaScan != nil:
		return nil
	case plan.TableScan != nil:
		return nil
	case plan.Filter != nil:
		return ValidateFilter(plan)
	case plan.Distinct != nil:
		return nil
	case plan.Projection != nil:
		return nil
	case plan.Aggregation != nil:
		return nil
	default:
		// TODO return error of unsupported plan
		return nil
	}
}

func ValidateFilter(plan *LogicalPlan) error {
	filter := plan.Filter

	switch expr := filter.Expr.(type) {
	case BinaryExpr:
		return ValidateBinaryExpr(&expr)
	default:
		// TODO does this mean it's unknown - log or do nothing
	}

	return nil
}

func ValidateBinaryExpr(expr *BinaryExpr) error {
	// enforce that that the left side of the binary expression must be a column
	// TODO check if this should even be a rule
	typeFinder := NewTypeFinder((*Column)(nil))
	expr.Left.Accept(&typeFinder)
	if typeFinder.result == nil {
		// TODO it's a validation error
		return errors.New("AHHHHHHHHHH")
	}

	return nil
}

// TODO comments and tests around this thing (if we want it in the code at the end of this)
// TODO this should maybe return a pointer
// It receives a pointer to the type we want to find
func NewTypeFinder(val interface{}) findExpressionForTypeVisitor {
	return findExpressionForTypeVisitor{exprType: reflect.TypeOf(val).Elem()}
}

type findExpressionForTypeVisitor struct {
	exprType reflect.Type
	result   Expr
}

func (v *findExpressionForTypeVisitor) PreVisit(expr Expr) bool {
	return true
}

func (v *findExpressionForTypeVisitor) PostVisit(expr Expr) bool {
	found := v.exprType == reflect.TypeOf(expr)
	if found {
		v.result = expr
	}
	return !found
}
