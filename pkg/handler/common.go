package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	QueryKeyOriginASN       = "originAsn"
	QueryKeyASSegments      = "asSegments"
	QueryKeyNeighborASN     = "neighborAsn"
	QueryKeyIP              = "ip"
	QueryKeyCIDR            = "cidr"
	QueryKeyCursorID        = "cursor_id"
	QueryKeyCounterMaxVal   = "cnt_max_val"
	QueryKeyCounterTickIntv = "cnt_tick_intv"
	QueryKeyCursorLifespan  = "cursor_lifespan"
	QueryKeyPageSize        = "page_size"
)

const (
	HeaderKeyCursorId = "x-cloudping-cursor-id"
)

type ErrResp struct {
	Err string `json:"error"`
}

type DataResp struct {
	Data any `json:"data"`
}

type ResumableResponseStreamEvent struct {
	Data any `json:"data"`

	// you can use this to resume the stream that was suspended due to page size limit!
	CursorID string `json:"cursor_id,omitempty"`
}

func parseUint32List(s string) ([]uint32, error) {
	parts := strings.Split(s, ",")
	result := make([]uint32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid ASN %q: %w", part, err)
		}
		result = append(result, uint32(v))
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no valid ASN values provided")
	}
	return result, nil
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrResp{Err: msg})
}
