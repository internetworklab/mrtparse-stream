package handler

import (
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/internetworklab/mrtparse-stream/pkg/lister"
)

const (
	QueryKeyOriginASN   = "originAsn"
	QueryKeyASSegments  = "asSegments"
	QueryKeyNeighborASN = "neighborAsn"
	QueryKeyIP          = "ip"
	QueryKeyCIDR        = "cidr"
)

type MRTEntriesQueryHandler struct {
	Querier lister.MRTEntriesQuerier
}

func (h *MRTEntriesQueryHandler) getProvider(r *http.Request) string {
	return r.PathValue("provider")
}

func (h *MRTEntriesQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	var lister_ lister.Lister
	var err error
	provider := h.getProvider(r)
	if provider == "" {
		writeErr(w, http.StatusBadRequest, "missing provider")
		return
	}

	if originASN := q.Get(QueryKeyOriginASN); originASN != "" {
		asn, parseErr := strconv.ParseUint(originASN, 10, 32)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyOriginASN, parseErr))
			return
		}
		lister_, err = h.Querier.QueryMRTEntriesByOriginASN(ctx, uint32(asn), provider)
	} else if asSegments := q.Get(QueryKeyASSegments); asSegments != "" {
		segments, parseErr := parseUint32List(asSegments)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyASSegments, parseErr))
			return
		}
		lister_, err = h.Querier.QueryMRTEntriesByASSegments(ctx, segments, provider)
	} else if neighborASN := q.Get(QueryKeyNeighborASN); neighborASN != "" {
		asn, parseErr := strconv.ParseUint(neighborASN, 10, 32)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyNeighborASN, parseErr))
			return
		}
		lister_, err = h.Querier.QueryMRTEntriesByNeighborASN(ctx, uint32(asn), provider)
	} else if ip := q.Get(QueryKeyIP); ip != "" {
		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %q is not a valid IP address", QueryKeyIP, ip))
			return
		}
		lister_, err = h.Querier.QueryMRTEntriesByIP(ctx, parsedIP, provider)
	} else if cidr := q.Get(QueryKeyCIDR); cidr != "" {
		_, parsedCIDR, parseErr := net.ParseCIDR(cidr)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyCIDR, parseErr))
			return
		}
		lister_, err = h.Querier.QueryMRTEntriesByCIDR(ctx, *parsedCIDR, provider)
	} else {
		writeErr(w, http.StatusBadRequest, "missing query parameter: one of originAsn, asSegments, neighborAsn, ip, or cidr is required")
		return
	}

	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	NewListerStreamingHandler(lister_).ServeHTTP(w, r)
}
