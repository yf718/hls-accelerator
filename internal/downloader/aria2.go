package downloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hls-accelerator/internal/config"
	"net/http"
	"strconv"
	"time"
)

type Aria2Client struct {
	RPCUrl string
	Secret string
	Client *http.Client
}

func NewClient() *Aria2Client {
	return &Aria2Client{
		RPCUrl: config.GlobalConfig.Aria2RPCUrl,
		Secret: config.GlobalConfig.Aria2Secret,
		Client: &http.Client{Timeout: 10 * time.Second},
	}
}

type JsonRpcRequest struct {
	JsonRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	ID      string        `json:"id"`
	Params  []interface{} `json:"params"`
}

type JsonRpcResponse struct {
	ID     string        `json:"id"`
	Result interface{}   `json:"result"`
	Error  *JsonRpcError `json:"error,omitempty"`
}

type JsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcMethodCall struct {
	methodName string
	params     []interface{}
	fallback   func() error
}

type AddURIRequest struct {
	URI      string
	Dir      string
	Filename string
	Headers  map[string]string
}

type StatusFile struct {
	Path string `json:"path"`
}

type StatusDetail struct {
	Gid             string       `json:"gid"`
	Status          string       `json:"status"`
	Dir             string       `json:"dir"`
	CompletedLength string       `json:"completedLength"`
	TotalLength     string       `json:"totalLength"`
	ErrorMessage    string       `json:"errorMessage"`
	Files           []StatusFile `json:"files"`
}

func (c *Aria2Client) Call(method string, params ...interface{}) (interface{}, error) {
	// If secret is set, it must be the first parameter as "token:secret"
	finalParams := make([]interface{}, 0)
	if c.Secret != "" {
		finalParams = append(finalParams, "token:"+c.Secret)
	}
	finalParams = append(finalParams, params...)

	reqBody := JsonRpcRequest{
		JsonRPC: "2.0",
		Method:  method,
		ID:      "hls-accel",
		Params:  finalParams,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	resp, err := c.Client.Post(c.RPCUrl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rpcResp JsonRpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

func (c *Aria2Client) AddUri(uri string, dir string, filename string, headers map[string]string, gid string) (string, error) {
	opts := map[string]interface{}{
		"dir": dir,
		"out": filename,
	}

	if gid != "" {
		opts["gid"] = gid
	}

	headerList := []string{}
	for k, v := range headers {
		headerList = append(headerList, fmt.Sprintf("%s: %s", k, v))
	}
	if len(headerList) > 0 {
		opts["header"] = headerList
	}

	// aria2.addUri expects [uris] as first arg (after secret)
	res, err := c.Call("aria2.addUri", []string{uri}, opts)
	if err != nil {
		return "", err
	}

	gidStr, ok := res.(string)
	if !ok {
		// It might be possible aria2 returns GID even on error in some clients? No.
		return "", fmt.Errorf("invalid response type for gid")
	}
	return gidStr, nil
}

func (s StatusDetail) FirstFilePath() string {
	if len(s.Files) == 0 {
		return ""
	}
	return s.Files[0].Path
}

func (s StatusDetail) CompletedLengthInt64() int64 {
	if s.CompletedLength == "" {
		return 0
	}
	value, _ := strconv.ParseInt(s.CompletedLength, 10, 64)
	return value
}

type Aria2Status struct {
	Gid string `json:"gid"`
	Dir string `json:"dir"`
}

func (c *Aria2Client) TellActive() ([]Aria2Status, error) {
	// aria2.tellActive(keys)
	// We ask for "gid" and "dir"
	res, err := c.Call("aria2.tellActive", []string{"gid", "dir"})
	if err != nil {
		return nil, err
	}

	// Result is a list of maps
	list, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	statuses := []Aria2Status{}
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if ok {
			s := Aria2Status{}
			if v, k := m["gid"].(string); k {
				s.Gid = v
			}
			if v, k := m["dir"].(string); k {
				s.Dir = v
			}
			statuses = append(statuses, s)
		}
	}
	return statuses, nil
}

func (c *Aria2Client) Remove(gid string) error {
	_, err := c.Call("aria2.remove", gid)
	return err
}

func (c *Aria2Client) ForceRemove(gid string) error {
	_, err := c.Call("aria2.forceRemove", gid)
	return err
}

func (c *Aria2Client) multiCallRaw(calls []rpcMethodCall) (interface{}, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	methods := make([]interface{}, 0, len(calls))
	for _, call := range calls {
		methods = append(methods, map[string]interface{}{
			"methodName": call.methodName,
			"params":     call.params,
		})
	}
	return c.Call("system.multicall", methods)
}

func normalizeGIDs(gids []string) []string {
	if len(gids) == 0 {
		return nil
	}
	out := make([]string, 0, len(gids))
	seen := make(map[string]struct{}, len(gids))
	for _, gid := range gids {
		if gid == "" {
			continue
		}
		if _, ok := seen[gid]; ok {
			continue
		}
		seen[gid] = struct{}{}
		out = append(out, gid)
	}
	return out
}

// innerRPCParams builds per-method params for nested RPC calls (e.g. system.multicall).
func (c *Aria2Client) innerRPCParams(args ...interface{}) []interface{} {
	if c.Secret == "" {
		return args
	}
	out := make([]interface{}, 0, len(args)+1)
	out = append(out, "token:"+c.Secret)
	out = append(out, args...)
	return out
}

func (c *Aria2Client) invokeMultiCall(calls []rpcMethodCall) []int {
	if len(calls) == 0 {
		return nil
	}

	result, err := c.multiCallRaw(calls)
	if err != nil {
		return allIndexes(len(calls))
	}

	failed, ok := collectFailedMultiCallIndexes(result, len(calls))
	if !ok {
		return allIndexes(len(calls))
	}
	return failed
}

func collectFailedMultiCallIndexes(result interface{}, expected int) ([]int, bool) {
	list, ok := result.([]interface{})
	if !ok || len(list) != expected {
		return nil, false
	}

	failed := make([]int, 0)
	for i, item := range list {
		if isMultiCallItemError(item) {
			failed = append(failed, i)
		}
	}
	return failed, true
}

func isMultiCallItemError(item interface{}) bool {
	switch v := item.(type) {
	case map[string]interface{}:
		return hasRPCErrorFields(v)
	case []interface{}:
		for _, nested := range v {
			if nestedMap, ok := nested.(map[string]interface{}); ok && hasRPCErrorFields(nestedMap) {
				return true
			}
		}
	}
	return false
}

func hasRPCErrorFields(m map[string]interface{}) bool {
	_, hasCode := m["code"]
	_, hasMessage := m["message"]
	return hasCode || hasMessage
}

func allIndexes(n int) []int {
	indexes := make([]int, n)
	for i := range indexes {
		indexes[i] = i
	}
	return indexes
}

// ForceRemoveMany removes many downloads from aria2 queues using batched system.multicall
// (one HTTP request per batch instead of one per GID).
func (c *Aria2Client) ForceRemoveMany(gids []string) {
	if c == nil {
		return
	}
	gids = normalizeGIDs(gids)
	if len(gids) == 0 {
		return
	}

	const batch = 64
	for i := 0; i < len(gids); i += batch {
		j := i + batch
		if j > len(gids) {
			j = len(gids)
		}
		chunk := gids[i:j]

		calls := make([]rpcMethodCall, 0, len(chunk))
		for _, gid := range chunk {
			gid := gid
			calls = append(calls, rpcMethodCall{
				methodName: "aria2.forceRemove",
				params:     c.innerRPCParams(gid),
				fallback: func() error {
					return c.ForceRemove(gid)
				},
			})
		}

		for _, idx := range c.invokeMultiCall(calls) {
			_ = calls[idx].fallback()
		}
	}
}

// CleanupTaskDownloads runs forceRemove and removeDownloadResult for each GID using
// batched system.multicall. Sequential per-GID RPC was a major bottleneck when deleting
// HLS tasks with thousands of segments.
func (c *Aria2Client) CleanupTaskDownloads(gids []string) {
	if c == nil {
		return
	}
	gids = normalizeGIDs(gids)
	if len(gids) == 0 {
		return
	}

	const batch = 32 // 32 gids * 2 calls = 64 sub-calls per HTTP request
	for i := 0; i < len(gids); i += batch {
		j := i + batch
		if j > len(gids) {
			j = len(gids)
		}
		chunk := gids[i:j]

		calls := make([]rpcMethodCall, 0, len(chunk)*2)
		for _, gid := range chunk {
			gid := gid
			calls = append(calls, rpcMethodCall{
				methodName: "aria2.forceRemove",
				params:     c.innerRPCParams(gid),
				fallback: func() error {
					return c.ForceRemove(gid)
				},
			})
			calls = append(calls, rpcMethodCall{
				methodName: "aria2.removeDownloadResult",
				params:     c.innerRPCParams(gid),
				fallback: func() error {
					return c.RemoveDownloadResult(gid)
				},
			})
		}

		for _, idx := range c.invokeMultiCall(calls) {
			_ = calls[idx].fallback()
		}
	}
}

func (c *Aria2Client) TellWaiting(offset, num int) ([]Aria2Status, error) {
	// aria2.tellWaiting(offset, num, keys)
	res, err := c.Call("aria2.tellWaiting", offset, num, []string{"gid", "dir"})
	if err != nil {
		return nil, err
	}

	// Result is a list of maps
	list, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	statuses := []Aria2Status{}
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if ok {
			s := Aria2Status{}
			if v, k := m["gid"].(string); k {
				s.Gid = v
			}
			if v, k := m["dir"].(string); k {
				s.Dir = v
			}
			statuses = append(statuses, s)
		}
	}
	return statuses, nil
}

func (c *Aria2Client) TellStopped(offset, num int) ([]Aria2Status, error) {
	res, err := c.Call("aria2.tellStopped", offset, num, []string{"gid", "dir"})
	if err != nil {
		return nil, err
	}

	list, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	statuses := []Aria2Status{}
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if ok {
			s := Aria2Status{}
			if v, k := m["gid"].(string); k {
				s.Gid = v
			}
			if v, k := m["dir"].(string); k {
				s.Dir = v
			}
			statuses = append(statuses, s)
		}
	}
	return statuses, nil
}

func (c *Aria2Client) taskQueueGIDs(dir string) ([]string, error) {
	if c == nil || dir == "" {
		return nil, nil
	}

	statuses, err := c.TellActive()
	if err != nil {
		return nil, err
	}

	gids := make([]string, 0, len(statuses))
	for _, status := range statuses {
		if status.Dir == dir {
			gids = append(gids, status.Gid)
		}
	}

	const pageSize = 1000
	for offset := 0; ; offset += pageSize {
		waiting, err := c.TellWaiting(offset, pageSize)
		if err != nil {
			return nil, err
		}
		for _, status := range waiting {
			if status.Dir == dir {
				gids = append(gids, status.Gid)
			}
		}
		if len(waiting) < pageSize {
			break
		}
	}

	return normalizeGIDs(gids), nil
}

func (c *Aria2Client) taskResultGIDs(dir string) ([]string, error) {
	if c == nil || dir == "" {
		return nil, nil
	}

	gids := make([]string, 0)
	const pageSize = 1000
	for offset := 0; ; offset += pageSize {
		stopped, err := c.TellStopped(offset, pageSize)
		if err != nil {
			return nil, err
		}
		for _, status := range stopped {
			if status.Dir == dir {
				gids = append(gids, status.Gid)
			}
		}
		if len(stopped) < pageSize {
			break
		}
	}

	return normalizeGIDs(gids), nil
}

// ForceRemoveTaskDownloads removes active/waiting downloads that belong to the
// specified task directory. Completed results are intentionally excluded because
// they are not running anymore and can be purged in bulk when deleting.
func (c *Aria2Client) ForceRemoveTaskDownloads(dir string) (int, error) {
	gids, err := c.taskQueueGIDs(dir)
	if err != nil {
		return 0, err
	}
	c.ForceRemoveMany(gids)
	return len(gids), nil
}

func (c *Aria2Client) CleanupTaskByDir(dir string) (int, error) {
	if c == nil || dir == "" {
		return 0, nil
	}
	queueGIDs, err := c.taskQueueGIDs(dir)
	if err != nil {
		return 0, err
	}
	resultGIDs, err := c.taskResultGIDs(dir)
	if err != nil {
		return 0, err
	}
	gids := normalizeGIDs(append(queueGIDs, resultGIDs...))
	c.CleanupTaskDownloads(gids)
	return len(gids), nil
}

func (c *Aria2Client) PurgeDownloadResult() error {
	if c == nil {
		return nil
	}
	_, err := c.Call("aria2.purgeDownloadResult")
	return err
}

// RemoveDownloadResult removes a completed/error/removed download from the memory
func (c *Aria2Client) RemoveDownloadResult(gid string) error {
	_, err := c.Call("aria2.removeDownloadResult", gid)
	// Ignore errors - the GID might not exist in results
	return err
}

func (c *Aria2Client) BatchPause(gids []string) error {
	return c.batchSimpleCall("aria2.pause", gids, c.Call)
}

func (c *Aria2Client) BatchUnpause(gids []string) error {
	return c.batchSimpleCall("aria2.unpause", gids, c.Call)
}

func (c *Aria2Client) batchSimpleCall(method string, gids []string, fallback func(string, ...interface{}) (interface{}, error)) error {
	if c == nil {
		return nil
	}
	gids = normalizeGIDs(gids)
	if len(gids) == 0 {
		return nil
	}
	const batchSize = 64
	for i := 0; i < len(gids); i += batchSize {
		j := i + batchSize
		if j > len(gids) {
			j = len(gids)
		}
		chunk := gids[i:j]
		calls := make([]rpcMethodCall, 0, len(chunk))
		for _, gid := range chunk {
			localGID := gid
			calls = append(calls, rpcMethodCall{
				methodName: method,
				params:     c.innerRPCParams(localGID),
				fallback: func() error {
					_, err := fallback(method, localGID)
					return err
				},
			})
		}
		for _, idx := range c.invokeMultiCall(calls) {
			_ = calls[idx].fallback()
		}
	}
	return nil
}

func (c *Aria2Client) BatchAddURIs(requests []AddURIRequest) ([]string, error) {
	if c == nil || len(requests) == 0 {
		return nil, nil
	}

	calls := make([]rpcMethodCall, 0, len(requests))
	for _, request := range requests {
		opts := map[string]interface{}{
			"dir": request.Dir,
			"out": request.Filename,
		}
		if len(request.Headers) > 0 {
			headers := make([]string, 0, len(request.Headers))
			for key, value := range request.Headers {
				headers = append(headers, fmt.Sprintf("%s: %s", key, value))
			}
			opts["header"] = headers
		}

		req := request
		calls = append(calls, rpcMethodCall{
			methodName: "aria2.addUri",
			params:     c.innerRPCParams([]string{req.URI}, opts),
			fallback:   nil,
		})
	}

	result, err := c.multiCallRaw(calls)
	if err != nil {
		return c.batchAddFallback(requests)
	}

	list, ok := result.([]interface{})
	if !ok || len(list) != len(requests) {
		return c.batchAddFallback(requests)
	}

	gids := make([]string, len(requests))
	for idx, item := range list {
		values, ok := item.([]interface{})
		if !ok || len(values) == 0 {
			return c.batchAddFallback(requests)
		}
		gid, ok := values[0].(string)
		if !ok || gid == "" {
			return c.batchAddFallback(requests)
		}
		gids[idx] = gid
	}
	return gids, nil
}

func (c *Aria2Client) batchAddFallback(requests []AddURIRequest) ([]string, error) {
	gids := make([]string, 0, len(requests))
	for _, request := range requests {
		gid, err := c.AddUri(request.URI, request.Dir, request.Filename, request.Headers, "")
		if err != nil {
			return gids, err
		}
		gids = append(gids, gid)
	}
	return gids, nil
}

func (c *Aria2Client) BatchTellStatus(gids []string) (map[string]StatusDetail, error) {
	if c == nil {
		return map[string]StatusDetail{}, nil
	}
	gids = normalizeGIDs(gids)
	if len(gids) == 0 {
		return map[string]StatusDetail{}, nil
	}

	out := make(map[string]StatusDetail, len(gids))
	const batchSize = 100
	for i := 0; i < len(gids); i += batchSize {
		j := i + batchSize
		if j > len(gids) {
			j = len(gids)
		}
		chunk := gids[i:j]
		calls := make([]rpcMethodCall, 0, len(chunk))
		for _, gid := range chunk {
			calls = append(calls, rpcMethodCall{
				methodName: "aria2.tellStatus",
				params: c.innerRPCParams(gid, []string{
					"gid", "status", "dir", "completedLength", "totalLength", "files", "errorMessage",
				}),
			})
		}
		result, err := c.multiCallRaw(calls)
		if err != nil {
			return nil, err
		}
		list, ok := result.([]interface{})
		if !ok || len(list) != len(chunk) {
			return nil, fmt.Errorf("invalid multicall tellStatus response")
		}
		for idx, raw := range list {
			values, ok := raw.([]interface{})
			if !ok || len(values) == 0 {
				continue
			}
			payload, err := json.Marshal(values[0])
			if err != nil {
				return nil, err
			}
			var detail StatusDetail
			if err := json.Unmarshal(payload, &detail); err != nil {
				return nil, err
			}
			if detail.Gid == "" {
				detail.Gid = chunk[idx]
			}
			out[detail.Gid] = detail
		}
	}
	return out, nil
}

func (c *Aria2Client) QueueStatusesByDir(dir string) ([]StatusDetail, error) {
	if c == nil || dir == "" {
		return nil, nil
	}

	gids, err := c.taskQueueGIDs(dir)
	if err != nil {
		return nil, err
	}
	if len(gids) == 0 {
		return nil, nil
	}

	statusMap, err := c.BatchTellStatus(gids)
	if err != nil {
		return nil, err
	}

	out := make([]StatusDetail, 0, len(gids))
	for _, gid := range gids {
		if detail, ok := statusMap[gid]; ok {
			out = append(out, detail)
		}
	}
	return out, nil
}
