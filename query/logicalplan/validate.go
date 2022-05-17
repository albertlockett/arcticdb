package logicalplan

import (
	"fmt"
	"github.com/apache/arrow/go/v8/arrow/scalar"
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
func (e *ExprValidationError) Error() string {
	message := make([]string, 0)
	message = append(message, e.message)
	message = append(message, ": ")
	message = append(message, fmt.Sprintf("%s", e.expr))
	for _, child := range e.children {
		message = append(message, "\ninvalid expression: ")
		message = append(message, child.Error())
	}

	return strings.Join(message, "")
}

type PlanValidationError struct {
	message  string
	plan     *LogicalPlan
	children []*ExprValidationError
}

// PlanValidationError.Error implements the error interface.
func (e *PlanValidationError) Error() string {
	message := make([]string, 0)
	message = append(message, e.message)
	message = append(message, ": ")
	message = append(message, fmt.Sprintf("%s", e.plan))

	for _, child := range e.children {
		message = append(message, "\ninvalid expression: ")
		message = append(message, child.Error())
		message = append(message, "\n---")
	}

	return strings.Join(message, "")
}

type Validator struct {
	plan *LogicalPlan
}

func NewValidator(plan *LogicalPlan) *Validator {
	return &Validator{plan}
}

// Validate validates the logical plan.
func (v *Validator) Validate() error {
	// TODO here we could check that only one field is set on the plan

	switch {
	case v.plan.SchemaScan != nil:
		return nil
	case v.plan.TableScan != nil:
		return nil
	case v.plan.Filter != nil:
		err := v.ValidateFilter()
		return err
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
func (v *Validator) ValidateFilter() error {
	// TODO something weird happening w/ casting here
	if err := v.ValidateFilterExpr(v.plan.Filter.Expr); err != nil {
		return &PlanValidationError{
			message:  "invalid filter",
			plan:     v.plan,
			children: []*ExprValidationError{err},
		}
	}
	return nil
}

// ValidateFilterExpr validates filter's expression.
func (v *Validator) ValidateFilterExpr(e Expr) *ExprValidationError {
	switch expr := e.(type) {

	case BinaryExpr:
		err := v.ValidateFilterBinaryExpr(&expr)
		return err
	default:
		// TODO does this mean it's unknown - log or do nothing
	}

	return nil
}

// ValidateFilterBinaryExpr validates the filter's binary expression.
func (v *Validator) ValidateFilterBinaryExpr(expr *BinaryExpr) *ExprValidationError {
	if expr.Op == AndOp {
		return v.ValidateFilterAndBinaryExpr(expr)
	}

	// try to find the column expression on the left side of the binary expression
	leftColumnFinder := newTypeFinder((*Column)(nil))
	expr.Left.Accept(&leftColumnFinder)
	if leftColumnFinder.result == nil {
		// TODO - should we add a rule that the left side must be a column?
	} else {
		columnExpr := leftColumnFinder.result.(Column)
		schema := v.plan.InputSchema()
		column, found := schema.ColumnByName(columnExpr.ColumnName)

		if !found {
			// TODO - should we add a rule that the column must exist in the schema?
		} else {
			rightLiteralFinder := newTypeFinder((*LiteralExpr)(nil))
			expr.Right.Accept(&rightLiteralFinder)
			if rightLiteralFinder.result == nil {
				// TODO - should we add a rule that the right must be a literal if left is a column

			} else {
				// ensure that the column type is compatible with the literal being compared to it
				t := column.StorageLayout.Type()
				literalExpr := rightLiteralFinder.result.(LiteralExpr)
				lt := t.LogicalType()

				switch {
				case lt.UTF8 != nil:
					switch literalExpr.Value.(type) {
					case *scalar.Int64:
						return &ExprValidationError{
							message: "incompatible types: string column cannot be compared with numeric literal",
							expr:    expr,
						}
					}
				case lt.Integer != nil:
					switch literalExpr.Value.(type) {
					case *scalar.String:
						return &ExprValidationError{
							message: "incompatible types: numeric column cannot be compared with string literal",
							expr:    expr,
						}
					}
				}
			}
		}
	}

	return nil
}

//ValidateFilterAndBinaryExpr validates the filter's binary expression if the
// expression is and-ing two other binary expressions together.
func (v *Validator) ValidateFilterAndBinaryExpr(expr *BinaryExpr) *ExprValidationError {
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

// NewTypeFinder returns an instance of the findExpressionForTypeVisitor which
// that can be used to find the type. It expects to receive a pointer to the
// type it is to find
func newTypeFinder(val interface{}) findExpressionForTypeVisitor {
	return findExpressionForTypeVisitor{exprType: reflect.TypeOf(val).Elem()}
}

// findExpressionForTypeVisitor is an instance of Visitor that will try to find
// an expression of the given type while visiting the expressions.
type findExpressionForTypeVisitor struct {
	exprType reflect.Type
	// if an expression of the type is found, it will be set on this field after
	// visiting. Otherwise this field will be null
	result Expr
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
