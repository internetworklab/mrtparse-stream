package lister

import "context"

// The user of Lister should treat the data as opaque.
// The lister shouldn't bound to specific querying logic either, you
// can write custom 'Querier'-like struct/interface for such purpose.
type Lister interface {
	List(ctx context.Context) (any, error)
	ListAsStream(ctx context.Context) (<-chan any, error)
}

type SliceListerWrapper struct {
	data []any
}

func SliceLister(s []any) Lister {
	return &SliceListerWrapper{data: s}
}

func (w *SliceListerWrapper) List(_ context.Context) (any, error) {
	return w.data, nil
}

func (w *SliceListerWrapper) ListAsStream(ctx context.Context) (<-chan any, error) {
	ch := make(chan any)
	go func() {
		defer close(ch)
		for _, item := range w.data {
			select {
			case ch <- item:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
