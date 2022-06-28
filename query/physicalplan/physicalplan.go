package physicalplan

import (
	"context"
	"errors"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"sync"

	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query/logicalplan"
)

type PhysicalPlan interface {
	Callback(r arrow.Record) error
	SetNextCallback(next func(r arrow.Record) error)
}

type ScanPhysicalPlan interface {
	Execute(ctx context.Context, pool memory.Allocator) error
}

type PrePlanVisitorFunc func(plan *logicalplan.LogicalPlan) bool

func (f PrePlanVisitorFunc) PreVisit(plan *logicalplan.LogicalPlan) bool {
	return f(plan)
}

func (f PrePlanVisitorFunc) PostVisit(plan *logicalplan.LogicalPlan) bool {
	return false
}

type PostPlanVisitorFunc func(plan *logicalplan.LogicalPlan) bool

func (f PostPlanVisitorFunc) PreVisit(plan *logicalplan.LogicalPlan) bool {
	return true
}

func (f PostPlanVisitorFunc) PostVisit(plan *logicalplan.LogicalPlan) bool {
	return f(plan)
}

type OutputPlan struct {
	callback func(r arrow.Record) error
	scan     ScanPhysicalPlan
}

func (e *OutputPlan) Callback(r arrow.Record) error {
	return e.callback(r)
}

func (e *OutputPlan) SetNextCallback(next func(r arrow.Record) error) {
	e.callback = next
}

func (e *OutputPlan) Execute(ctx context.Context, pool memory.Allocator, callback func(r arrow.Record) error) error {
	e.callback = callback
	return e.scan.Execute(ctx, pool)
}

type IteratorProvider struct {
	scan *TableScan
}

func (p *IteratorProvider) Iterator() func(record arrow.Record) error {
	return p.scan.nextBuilder().Callback
}

type TableScan struct {
	options     *logicalplan.TableScan
	nextBuilder func() PhysicalPlan
	finisher    func() error
}

func (s *TableScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}
	err := table.Iterator(
		ctx,
		pool,
		s.options.Projection,
		s.options.Filter,
		s.options.Distinct,
		&IteratorProvider{scan: s},
	)
	if err != nil {
		return err
	}

	return s.finisher()
}

type SchemaScan struct {
	options     *logicalplan.SchemaScan
	nextBuilder func() PhysicalPlan
	finisher    func() error
}

func (s *SchemaScan) Execute(ctx context.Context, pool memory.Allocator) error {
	table := s.options.TableProvider.GetTable(s.options.TableName)
	if table == nil {
		return errors.New("table not found")
	}
	err := table.SchemaIterator(
		ctx,
		pool,
		s.options.Projection,
		s.options.Filter,
		s.options.Distinct,
		s.nextBuilder().Callback,
	)
	if err != nil {
		return err
	}

	return s.finisher()
}

func Build(pool memory.Allocator, s *dynparquet.Schema, plan *logicalplan.LogicalPlan) (*OutputPlan, error) {
	outputPlan := &OutputPlan{}
	//var err error

	finisher, nextBuilder := planBuilder(pool, s, plan, outputPlan)
	plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
		switch {
		case plan.SchemaScan != nil:
			outputPlan.scan = &SchemaScan{
				options:     plan.SchemaScan,
				nextBuilder: nextBuilder,
				finisher:    finisher.Finish,
			}
			return false
		case plan.TableScan != nil:
			outputPlan.scan = &TableScan{
				options:     plan.TableScan,
				nextBuilder: nextBuilder,
				finisher:    finisher.Finish,
			}
			return false
		case plan.Aggregation != nil:
			break
		case plan.Distinct != nil:
			break
		case plan.Filter != nil:
			break
		case plan.Projection != nil:
			break
		default:
			panic("Unsupported plan")
		}

		return true
	}))
	return outputPlan, nil
}

// planBuilder TODO a better name & some comments?
func planBuilder(
	pool memory.Allocator,
	s *dynparquet.Schema,
	plan *logicalplan.LogicalPlan,
	outputPlan *OutputPlan,
) (*Finisher, func() PhysicalPlan) {
	finisher := Finisher{
		pool: pool,
	}

	// instances of distinct are not shared across threads so that multiple instances do not have to syncrhonize which
	// distinct values they have seen. we'll only create one instance of the phy plan distinct per logical plan
	disticts := make(map[*logicalplan.Distinct]*Distinction)
	distintsLock := sync.Mutex{}

	return &finisher, func() PhysicalPlan {
		var (
			err  error
			prev PhysicalPlan = outputPlan
		)
		plan.Accept(PrePlanVisitorFunc(func(plan *logicalplan.LogicalPlan) bool {
			var phyPlan PhysicalPlan
			switch {
			case plan.SchemaScan != nil:
				return false
			case plan.TableScan != nil:
				return false
			case plan.Projection != nil:
				phyPlan, err = Project(pool, plan.Projection.Exprs)
			case plan.Distinct != nil:
				distintsLock.Lock()
				defer distintsLock.Unlock()
				// if the distinct instance for the logical plan has not been insantiated, do it now
				if _, ok := disticts[plan.Distinct]; !ok {
					matchers := make([]logicalplan.ColumnMatcher, 0, len(plan.Distinct.Columns))
					for _, col := range plan.Distinct.Columns {
						matchers = append(matchers, col.Matcher())
					}
					disticts[plan.Distinct] = Distinct(pool, matchers)
				}
				// use same distinct in all threads
				phyPlan = disticts[plan.Distinct]
			case plan.Filter != nil:
				phyPlan, err = Filter(pool, plan.Filter.Expr)

			case plan.Aggregation != nil:
				var agg *HashAggregate
				agg, err = Aggregate(pool, s, plan.Aggregation)
				phyPlan = agg
				if agg != nil {
					finisher.AddHashAgg(agg)
				}
			default:
				panic("Unsupported plan")
			}
			if err != nil {
				return false
			}

			phyPlan.SetNextCallback(prev.Callback)
			prev = phyPlan
			return true
		}))
		return prev
	}
}
