package logicalplan

import (
	"log"
	"reflect"
	"strings"
)

// ExprValidationError is the error for an invalid expression that was found during validation.
type ExprValidationError struct {
	message  string
	expr     Expr
	children []*ExprValidationError
}

// ExprValidationError.Error implements the error interface.
func (err *ExprValidationError) Error() string {
	return err.message
}

type LogicalPlanValidator struct {
	plan *LogicalPlan
}

// Validate validates the logical plan.
func (v *LogicalPlanValidator) Validate() error {
	// TODO here we could check that only one field is set on the plan

	switch {
	case v.plan.SchemaScan != nil:
		return nil
	case v.plan.TableScan != nil:
		return nil
	case v.plan.Filter != nil:
		return v.ValidateFilter()
	case v.plan.Distinct != nil:
		return nil
	case v.plan.Projection != nil:
		return nil
	case v.plan.Aggregation != nil:
		return nil
	default:
		// TODO return error of unsupported plan
		return nil
	}
}

// ValidateFilter validates the logical plan's filter step.
func (v *LogicalPlanValidator) ValidateFilter() error {
	return v.ValidateFilterExpr(v.plan.Filter.Expr)
}

// ValidateFilterExpr validates filter's expression.
func (v *LogicalPlanValidator) ValidateFilterExpr(e Expr) *ExprValidationError {
	switch expr := e.(type) {

	case BinaryExpr:
		return v.ValidateFilterBinaryExpr(&expr)
	default:
		// TODO does this mean it's unknown - log or do nothing
	}

	return nil
}

// ValidateFilterBinaryExpr validates the filter's binary expression.
func (v *LogicalPlanValidator) ValidateFilterBinaryExpr(expr *BinaryExpr) *ExprValidationError {
	if expr.Op == AndOp {
		return v.ValidateFilterAndBinaryExpr(expr)
	}

	typeFinder := NewTypeFinder((*Column)(nil))
	expr.Left.Accept(&typeFinder)
	if typeFinder.result != nil {
		columnExpr := typeFinder.result.(Column)
		schema := v.plan.InputSchema()
		column, found := schema.ColumnByName(columnExpr.ColumnName)
		if found {
			// TODO do something with column
			log.Printf("%v", column)

		} else {
			// TODO - should we add a rule that the column must exist in the schema?
		}
	} else {
		// TODO - should we add a rule that the left side must be a column?
	}

	return nil
}

//ValidateFilterAndBinaryExpr validates the filter's binary expression if the
// expression is and-ing two other binary expressions together.
func (v *LogicalPlanValidator) ValidateFilterAndBinaryExpr(expr *BinaryExpr) *ExprValidationError {
	leftErr := v.ValidateFilterExpr(expr.Left)
	rightErr := v.ValidateFilterExpr(expr.Right)

	if leftErr != nil || rightErr != nil {
		message := make([]string, 0, 3)
		message = append(message, "invalid children:")

		validationErr := ExprValidationError{
			expr:     expr,
			children: make([]*ExprValidationError, 0),
		}

		if leftErr != nil {
			lve := leftErr
			message = append(message, "left")
			validationErr.children = append(validationErr.children, lve)
		}

		if rightErr != nil {
			lve := rightErr
			message = append(message, "right")
			validationErr.children = append(validationErr.children, lve)
		}

		validationErr.message = strings.Join(message, " ")
		return &validationErr
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
