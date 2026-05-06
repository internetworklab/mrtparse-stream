package lister

import "context"

// The user of Lister should treat the data as opaque.
// The lister shouldn't bound to specific querying logic either, you
// can write custom 'Querier'-like struct/interface for such purpose.
type Lister interface {
	List(ctx context.Context) (any, error)
	ListAsStream(ctx context.Context) (<-chan any, error)
}
