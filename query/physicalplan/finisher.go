package physicalplan

import "sync"

type Finisher struct {
	mutex        sync.Mutex
	aggregations []*HashAggregate
}

func (f *Finisher) Finish() error {
	if f.aggregations != nil {
		for _, agg := range f.aggregations {
			agg.Finish()
		}
	}
	return nil
}

func (f *Finisher) AddHashAgg(agg *HashAggregate) {
	f.mutex.Lock()
	if f.aggregations == nil {
		f.aggregations = make([]*HashAggregate, 0)
	}
	f.aggregations = append(f.aggregations, agg)
	f.mutex.Unlock()
}
