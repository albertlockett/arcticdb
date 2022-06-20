package physicalplan

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"sync"
)

type Finisher struct {
	pool         memory.Allocator
	mutex        sync.Mutex
	aggregations []*HashAggregate
}

// TODO somehow need to extend this to work for multiple callbacks

func (f *Finisher) Finish() error {
	// TODO is this a stupid way to check this?
	if f.aggregations != nil && len(f.aggregations) > 0 {
		callbackOriginal := f.aggregations[0].nextCallback
		records := make([]arrow.Record, 0)

		appendToRecords := func(record arrow.Record) error {
			records = append(records, record)
			return nil
		}

		// TODO maybe we should make this a diff method
		for _, agg := range f.aggregations {
			agg.SetNextCallback(appendToRecords)
			agg.Finish()
		}

		// TODO here we're gonna be combining hash aggregation results

		combined := f.combineRecords(records)
		callbackOriginal(combined)
	}
	return nil
}

func (f *Finisher) combineRecords(records []arrow.Record) arrow.Record {
	resultTree := make(map[interface{}]interface{})
	for _, record := range records {
		for i := 0; i < record.Column(0).Len(); i++ {
			currTree := resultTree
			for j, _ := range record.Schema().Fields() {
				col := record.Column(j)
				bin, ok := col.(*array.Binary)

				var key interface{}
				if ok {
					key = bin.ValueString(i)
				}

				num, isNum := col.(*array.Int64)
				if isNum {
					key = num.Value(i)
				}

				if j < len(record.Schema().Fields())-1 {
					// here we're extending the tree
					if _, ok := currTree[key]; !ok {
						currTree[key] = make(map[interface{}]interface{})
					}
					currTree = currTree[key].(map[interface{}]interface{})
				} else {
					// TODO we need to handle the other types of aggregations
					if isNum {
						if _, ok := currTree["_val"]; !ok {
							currTree["_val"] = int64(0)
						}
						// TODO need to actually check if we're summing here
						currVal := currTree["_val"].(int64)
						plusVal := key.(int64)
						currTree["_val"] = currVal + plusVal
					}
				}
			}
		}
	}

	resultBuilders := make([]array.Builder, 0)
	for j := range records[0].Schema().Fields() {
		col := records[0].Column(j)
		resultBuilders = append(resultBuilders, array.NewBuilder(f.pool, col.DataType()))
	}

	tuple := make([]interface{}, 0)
	numColumns := len(records[0].Schema().Fields())
	numRows := 0
	appendToRecord := func(tuple []interface{}) {
		numRows++
		for i, val := range tuple {
			if bin, ok := resultBuilders[i].(*array.BinaryBuilder); ok {
				bin.AppendString(val.(string))
			}
			if num, ok := resultBuilders[i].(*array.Int64Builder); ok {
				num.Append(val.(int64))
			}
		}
	}
	combineCombine(tuple, numColumns, appendToRecord, resultTree)

	cols := make([]arrow.Array, 0)
	for _, builder := range resultBuilders {
		cols = append(cols, builder.NewArray())
	}
	result := array.NewRecord(records[0].Schema(), cols, int64(numRows))
	return result
	//return records[0]
}

// TODO very messy
func combineCombine(
	tupleStack []interface{},
	numColumns int, // TODO not strictly needed
	callback func([]interface{}),
	resultTree map[interface{}]interface{},
) {
	for key := range resultTree {
		tupleStack = append(tupleStack, key) // push
		if len(tupleStack) == numColumns {
			// TODO this is weird ...
			tupleStack = tupleStack[0 : len(tupleStack)-1]
			tupleStack = append(tupleStack, resultTree["_val"])
			callback(tupleStack)
		} else {
			nextTree := resultTree[key].(map[interface{}]interface{})
			// TODO could panic if invalid tree passed?
			combineCombine(tupleStack, numColumns, callback, nextTree)
		}
		tupleStack = tupleStack[0 : len(tupleStack)-1] // pop
	}
}

func (f *Finisher) AddHashAgg(agg *HashAggregate) {
	f.mutex.Lock()
	if f.aggregations == nil {
		f.aggregations = make([]*HashAggregate, 0)
	}
	f.aggregations = append(f.aggregations, agg)
	f.mutex.Unlock()
}
