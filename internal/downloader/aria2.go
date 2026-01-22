package downloader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hls-accelerator/internal/config"
	"net/http"
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

// RemoveDownloadResult removes a completed/error/removed download from the memory
func (c *Aria2Client) RemoveDownloadResult(gid string) error {
	_, err := c.Call("aria2.removeDownloadResult", gid)
	// Ignore errors - the GID might not exist in results
	return err
}
