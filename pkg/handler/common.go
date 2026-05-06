package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type ErrResp struct {
	Err string `json:"error"`
}

type DataResp struct {
	Data any `json:"data"`
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
