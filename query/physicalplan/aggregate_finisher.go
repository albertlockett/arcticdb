package physicalplan

import (
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

type HashAggregateFinisher struct {
	pool         memory.Allocator
	aggregations []*HashAggregate
}

func (f *HashAggregateFinisher) Finish() error {
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

func (f *HashAggregateFinisher) combineRecords(records []arrow.Record) arrow.Record {
	// this method is building a
	resultBuilders := make([]array.Builder, 0)
	for j := range records[0].Schema().Fields() {
		col := records[0].Column(j)
		resultBuilders = append(resultBuilders, array.NewBuilder(f.pool, col.DataType()))
	}

	mergeTree := f.buildMergeTree(records)
	numRows := 0
	f.traverseAndAggregate(mergeTree, make([]interface{}, 0), func(tuple []interface{}, array2 arrow.Array) {
		numRows++
		for i, val := range tuple {
			appendArrayVal(resultBuilders[i], val)
		}

		// aggregate the results ...
		// we're assuming that are the aggregates here have basically the same aggregation function
		aggArray, _ := f.aggregations[0].aggregationFunction.Aggregate(f.pool, []arrow.Array{array2})
		aggResultBuilder := resultBuilders[len(resultBuilders)-1]
		appendArrayVal(aggResultBuilder, getArrayVal(aggArray, 0))
	})

	cols := make([]arrow.Array, 0)
	for _, builder := range resultBuilders {
		cols = append(cols, builder.NewArray())
	}
	result := array.NewRecord(records[0].Schema(), cols, int64(numRows))
	return result
}

func (f *HashAggregateFinisher) buildMergeTree(records []arrow.Record) map[interface{}]interface{} {
	mergeTree := make(map[interface{}]interface{})
	for _, record := range records {
		for i := int64(0); i < record.NumRows(); i++ {
			currTree := mergeTree
			for colIndex, _ := range record.Schema().Fields() {
				col := record.Column(colIndex)
				key := getArrayVal(col, int(i)) // TODO null check

				if colIndex < len(record.Schema().Fields())-2 {
					// here we're extending the tree
					if _, ok := currTree[key]; !ok {
						currTree[key] = make(map[interface{}]interface{})
					}
					currTree = currTree[key].(map[interface{}]interface{})
				} else {
					// here we're adding the leaf
					aggCol := record.Column(colIndex + 1)
					if _, ok := currTree[key]; !ok {
						currTree[key] = array.NewBuilder(f.pool, aggCol.DataType())
					}
					arrayList := currTree[key].(array.Builder)
					appendArrayVal(arrayList, getArrayVal(aggCol, int(i))) // TODO null check?
					break
				}
			}
		}
	}
	return mergeTree
}

func (f *HashAggregateFinisher) traverseAndAggregate(
	mergeTree map[interface{}]interface{},
	tupleStack []interface{},
	callback func([]interface{}, arrow.Array),
) {
	for key := range mergeTree {
		tupleStack = append(tupleStack, key) // push
		nextTree, ok := mergeTree[key].(map[interface{}]interface{})
		if ok {
			f.traverseAndAggregate(nextTree, tupleStack, callback)
		} else {
			arrayBuilder := mergeTree[key].(array.Builder) // TODO check here and panic?
			callback(tupleStack, arrayBuilder.NewArray())
		}
		tupleStack = tupleStack[0 : len(tupleStack)-1] // pop

	}
}

func getArrayVal(col arrow.Array, i int) interface{} {
	bin, ok := col.(*array.Binary)
	if ok {
		return bin.ValueString(int(i))
	}

	num, isNum := col.(*array.Int64)
	if isNum {
		return num.Value(int(i))
	}
	return nil
}

func appendArrayVal(arrayBuilder array.Builder, val interface{}) {
	if bin, ok := arrayBuilder.(*array.BinaryBuilder); ok {
		bin.AppendString(val.(string))
	}
	if num, ok := arrayBuilder.(*array.Int64Builder); ok {
		num.Append(val.(int64))
	}
}
