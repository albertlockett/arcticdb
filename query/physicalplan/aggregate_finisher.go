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

// Finish combines the aggregation results from multiple parallel execution
// threads into a single record and then continues finishing by doing the
// callback. It assumes that each aggregation in it is finishing has the same
// aggregation function and returns results with the same schema.
func (f *HashAggregateFinisher) Finish() error {
	callbackOriginal := f.aggregations[0].nextCallback
	records := make([]arrow.Record, 0)

	// for each aggregation, call the finisher and add the aggregated record to the list of records
	for _, agg := range f.aggregations {
		agg.SetNextCallback(func(record arrow.Record) error {
			records = append(records, record)
			return nil
		})
		err := agg.Finish()
		if err != nil {
			return err
		}
	}

	combined := f.combineRecords(records)
	err := callbackOriginal(combined)
	if err != nil {
		return err
	}
	return nil
}

// combineRecords combines the results from multiple aggregations into the single record which it retuns. It assumes
// that all the records have the same schema where the order of columns can differ but the column being aggregated is
// the last column.
//
func (f *HashAggregateFinisher) combineRecords(records []arrow.Record) arrow.Record {
	// create a list of builders for each column in the data set (based on the schema of the first record, as all should
	// have the same schema)
	resultBuilders := make([]array.Builder, 0)
	for j := range records[0].Schema().Fields() {
		col := records[0].Column(j)
		resultBuilders = append(resultBuilders, array.NewBuilder(f.pool, col.DataType()))
	}

	mergeTree := f.buildMergeTree(records)

	// traverse the merge tree and for each path (which is a tuple in our result set), add the values into each of the
	// result builders
	numRows := 0
	f.traverseAndAggregate(mergeTree, make([]interface{}, 0), func(pathTuple []interface{}, array2 arrow.Array) {
		numRows++
		for i, val := range pathTuple {
			appendArrayVal(resultBuilders[i], val)
		}

		// we're assuming that are the aggregates here have the same aggregation function
		aggFunc := f.aggregations[0].aggregationFunction

		// aggregate the results from each parallel exeuction
		aggArray, _ := aggFunc.Aggregate(f.pool, []arrow.Array{array2})
		aggResultBuilder := resultBuilders[len(resultBuilders)-1]
		appendArrayVal(aggResultBuilder, getArrayVal(aggArray, 0))
	})

	// combine our result builders into the list of columns
	cols := make([]arrow.Array, 0)
	for _, builder := range resultBuilders {
		cols = append(cols, builder.NewArray())
	}

	// create and return the final result
	result := array.NewRecord(records[0].Schema(), cols, int64(numRows))
	return result
}

// buildMergeTree creates a tree where each level of the tree is a column in the result set, and the leaf nodes are
// arrays values to be aggregated. For example the records:
//
// record1:
// col1    ["a", "b"]
// col2    ["c", "d"]
// sum(c3) [ 1,   2 ]
//
// record2:
// col1    ["a", "b"]
// col2    ["c", "f"]
// sum(c3) [ 1,   3 ]
//
//   root
//  /    \
// "a"   "b"__
//  |     |   \
// "c"   "d"  "f"
//  |     |    |
// [1,1] [2]  [3].
//
func (f *HashAggregateFinisher) buildMergeTree(records []arrow.Record) map[interface{}]interface{} {
	mergeTree := make(map[interface{}]interface{})
	// for each record ...
	for _, record := range records {
		// for each row ...
		for i := int64(0); i < record.NumRows(); i++ {
			currTree := mergeTree
			for colIndex := range record.Schema().Fields() {
				// must ensure that the same field is added at the same level of the tree, but the records columns can be in
				// different orders, so use records[0] as a reference for the order
				fieldName := records[0].Schema().Fields()[colIndex].Name

				col := columnForName(fieldName, record)
				key := getArrayVal(col, int(i))

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
					appendArrayVal(arrayList, getArrayVal(aggCol, int(i)))
					break
				}
			}
		}
	}
	return mergeTree
}

// columnForName returns the column from the record for the field with the passed name.
func columnForName(name string, record arrow.Record) arrow.Array {
	for columnIndex, field := range record.Schema().Fields() {
		if field.Name == name {
			return record.Column(columnIndex)
		}
	}
	return nil
}

// traverses the merge tree DFS and when it reaches a leaf node, calls the callback.
func (f *HashAggregateFinisher) traverseAndAggregate(
	mergeTree map[interface{}]interface{},
	pathStack []interface{},
	callback func([]interface{}, arrow.Array),
) {
	for key := range mergeTree {
		pathStack = append(pathStack, key) // push path element ot stack
		nextTree, ok := mergeTree[key].(map[interface{}]interface{})
		if ok {
			f.traverseAndAggregate(nextTree, pathStack, callback)
		} else {
			arrayBuilder := mergeTree[key].(array.Builder)
			callback(pathStack, arrayBuilder.NewArray())
		}
		pathStack = pathStack[0 : len(pathStack)-1] // pop
	}
}

// getArrayVal is a helper method of getting the value at some column out of the arrow array.
func getArrayVal(col arrow.Array, i int) interface{} {
	bin, ok := col.(*array.Binary)
	if ok {
		return bin.ValueString(i)
	}

	num, isNum := col.(*array.Int64)
	if isNum {
		return num.Value(i)
	}
	return nil
}

// appendArrayVal is a helper function for appending the value into the arrow array.
func appendArrayVal(arrayBuilder array.Builder, val interface{}) {
	if bin, ok := arrayBuilder.(*array.BinaryBuilder); ok {
		bin.AppendString(val.(string))
	}
	if num, ok := arrayBuilder.(*array.Int64Builder); ok {
		num.Append(val.(int64))
	}
}
