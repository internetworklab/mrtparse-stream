package handler

import (
	"fmt"
	"net"
	"net/http"
	"strconv"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkglister "github.com/internetworklab/mrtparse-stream/pkg/lister"
)

const (
	QueryKeyOriginASN   = "originAsn"
	QueryKeyASSegments  = "asSegments"
	QueryKeyNeighborASN = "neighborAsn"
	QueryKeyIP          = "ip"
	QueryKeyCIDR        = "cidr"
)

type MRTEntriesQueryHandler struct {
	Querier pkgdb.MRTEntriesReader
}

func (h *MRTEntriesQueryHandler) getProvider(r *http.Request) string {
	return r.PathValue("provider")
}

func (h *MRTEntriesQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	var lister pkglister.Lister
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
		lister = pkglister.ChannelLister(h.Querier.GetMRTEntriesByOriginAS(ctx, uint32(asn), provider))
	} else if asSegments := q.Get(QueryKeyASSegments); asSegments != "" {
		segments, parseErr := parseUint32List(asSegments)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyASSegments, parseErr))
			return
		}
		lister = pkglister.ChannelLister(h.Querier.GetMRTEntriesByASSegments(ctx, segments, provider))
	} else if neighborASN := q.Get(QueryKeyNeighborASN); neighborASN != "" {
		asn, parseErr := strconv.ParseUint(neighborASN, 10, 32)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyNeighborASN, parseErr))
			return
		}
		lister = pkglister.ChannelLister(h.Querier.GetMRTEntriesByNeighborAS(ctx, uint32(asn), provider))
	} else if ip := q.Get(QueryKeyIP); ip != "" {
		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %q is not a valid IP address", QueryKeyIP, ip))
			return
		}
		lister = pkglister.ChannelLister(h.Querier.GetMRTEntriesByIP(ctx, parsedIP, provider))
	} else if cidr := q.Get(QueryKeyCIDR); cidr != "" {
		_, parsedCIDR, parseErr := net.ParseCIDR(cidr)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid %s: %v", QueryKeyCIDR, parseErr))
			return
		}
		lister = pkglister.ChannelLister(h.Querier.GetMRTEntriesByCIDR(ctx, *parsedCIDR, provider))
	} else {
		writeErr(w, http.StatusBadRequest, "missing query parameter: one of originAsn, asSegments, neighborAsn, ip, or cidr is required")
		return
	}

	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	NewListerStreamingHandler(lister).ServeHTTP(w, r)
}
