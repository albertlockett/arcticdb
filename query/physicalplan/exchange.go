package physicalplan

import (
	"sync"

	"github.com/apache/arrow/go/v8/arrow"

	"github.com/polarsignals/arcticdb/query/logicalplan"
)

type ExchangeOperator struct {
	ch           chan arrow.Record
	wg           sync.WaitGroup
	nextCallback func(r arrow.Record) error
}

func (o *ExchangeOperator) SetNextCallback(callback func(r arrow.Record) error) {
	o.nextCallback = callback
}

func (o *ExchangeOperator) Callback(r arrow.Record) error {
	r.Retain()
	o.ch <- r
	return nil
}

func (o *ExchangeOperator) Finish() error {
	close(o.ch)
	o.wg.Wait()
	return nil
}

func Exchange(options *logicalplan.Exchange) (*ExchangeOperator, error) {
	// TODO validate options

	operator := ExchangeOperator{
		ch: make(chan arrow.Record, options.BackPressure),
		wg: sync.WaitGroup{},
	}

	// setup listeners
	for i := 0; i < options.Parallelism; i++ {
		go func() {
			operator.wg.Add(1)
			for r := range operator.ch {
				err := operator.nextCallback(r)
				if err != nil {
				}
				r.Release()
				// TODO handle the error
			}
			operator.wg.Done()
		}()
	}
	return &operator, nil
}
