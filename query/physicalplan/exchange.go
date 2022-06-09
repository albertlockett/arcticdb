package physicalplan

import (
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"

	"github.com/polarsignals/arcticdb/query/logicalplan"
)

type ExchangeOperator struct {
	ch           chan arrow.Record
	wg           sync.WaitGroup
	nextCallback func(r arrow.Record) error
	workerError  error
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
	if o.workerError != nil {
		return o.workerError
	}
	return nil
}

func Exchange(options *logicalplan.Exchange) (*ExchangeOperator, error) {
	if options.BackPressure <= 0 {
		return nil, fmt.Errorf("invalid exchange backpressure value: %d", options.BackPressure)
	}
	if options.Parallelism <= 0 {
		return nil, fmt.Errorf("invalid exchange parallelism value: %d", options.Parallelism)
	}

	operator := ExchangeOperator{
		ch: make(chan arrow.Record, options.BackPressure),
		wg: sync.WaitGroup{},
	}

	// setup workers
	for i := 0; i < options.Parallelism; i++ {
		go func() {
			operator.wg.Add(1)
			for r := range operator.ch {
				if operator.workerError != nil {
					break
				}
				err := operator.nextCallback(r)
				r.Release()
				if err != nil {
					operator.workerError = err
					break
				}
			}
			operator.wg.Done()
		}()
	}
	return &operator, nil
}
