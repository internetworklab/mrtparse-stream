package lister

import (
	"context"
	"net"
)

// MRTEntriesQuerier defines the set of queries available for MRT (BGP routing) entries.
type MRTEntriesQuerier interface {

	// QueryMRTEntriesByOriginASN returns entries whose AS path originates
	// from the given origin ASN (i.e. the last AS in the AS path).
	QueryMRTEntriesByOriginASN(ctx context.Context, originASN uint32, provider string) (Lister, error)

	// QueryMRTEntriesByASSegments returns entries whose AS path contains
	// all of the given AS numbers (in any order).
	QueryMRTEntriesByASSegments(ctx context.Context, asSegments []uint32, provider string) (Lister, error)

	// QueryMRTEntriesByNeighborASN returns entries whose AS path starts
	// with the given neighbor ASN (i.e. the first AS in the AS path).
	QueryMRTEntriesByNeighborASN(ctx context.Context, neighborASN uint32, provider string) (Lister, error)

	// QueryMRTEntriesByIP returns entries whose prefix contains the given
	// IP address (i.e. prefix >> targetIP).
	QueryMRTEntriesByIP(ctx context.Context, targetIP net.IP, provider string) (Lister, error)

	// QueryMRTEntriesByCIDR returns entries whose prefix encompasses or
	// matches the given CIDR (i.e. prefix >>= targetCIDR).
	QueryMRTEntriesByCIDR(ctx context.Context, targetCIDR net.IPNet, provider string) (Lister, error)
}
