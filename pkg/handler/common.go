package handler

type ErrResp struct {
	Err string `json:"error"`
}

type DataResp struct {
	Data any `json:"data"`
}
