package downloader

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNormalizeGIDs(t *testing.T) {
	got := normalizeGIDs([]string{"gid-1", "", "gid-1", "gid-2", "gid-2"})
	want := []string{"gid-1", "gid-2"}

	if len(got) != len(want) {
		t.Fatalf("normalizeGIDs length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalizeGIDs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCollectFailedMultiCallIndexes(t *testing.T) {
	result := []interface{}{
		[]interface{}{"ok"},
		[]interface{}{map[string]interface{}{"code": 1, "message": "boom"}},
		map[string]interface{}{"code": 2, "message": "bad"},
	}

	failed, ok := collectFailedMultiCallIndexes(result, 3)
	if !ok {
		t.Fatal("collectFailedMultiCallIndexes returned !ok")
	}

	want := []int{1, 2}
	if len(failed) != len(want) {
		t.Fatalf("failed length = %d, want %d", len(failed), len(want))
	}
	for i := range want {
		if failed[i] != want[i] {
			t.Fatalf("failed[%d] = %d, want %d", i, failed[i], want[i])
		}
	}
}

func TestCollectFailedMultiCallIndexesRejectsUnexpectedShape(t *testing.T) {
	if _, ok := collectFailedMultiCallIndexes("bad", 1); ok {
		t.Fatal("expected unexpected type to be rejected")
	}
	if _, ok := collectFailedMultiCallIndexes([]interface{}{}, 1); ok {
		t.Fatal("expected wrong length to be rejected")
	}
}

func TestTaskQueueGIDsFiltersByDirAcrossPages(t *testing.T) {
	var tellWaitingOffsets []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req JsonRpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		resp := JsonRpcResponse{ID: req.ID}
		switch req.Method {
		case "aria2.tellActive":
			resp.Result = []map[string]string{
				{"gid": "active-match", "dir": "cache/task-a"},
				{"gid": "active-other", "dir": "cache/task-b"},
			}
		case "aria2.tellWaiting":
			offset := int(req.Params[0].(float64))
			tellWaitingOffsets = append(tellWaitingOffsets, offset)
			switch offset {
			case 0:
				page := make([]map[string]string, 0, 1000)
				page = append(page, map[string]string{"gid": "waiting-match-1", "dir": "cache/task-a"})
				for i := 1; i < 1000; i++ {
					page = append(page, map[string]string{"gid": "waiting-other", "dir": "cache/task-b"})
				}
				resp.Result = page
			case 1000:
				resp.Result = []map[string]string{
					{"gid": "waiting-match-2", "dir": "cache/task-a"},
				}
			default:
				t.Fatalf("unexpected waiting offset %d", offset)
			}
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client := &Aria2Client{
		RPCUrl: srv.URL,
		Client: &http.Client{Timeout: time.Second},
	}

	got, err := client.taskQueueGIDs("cache/task-a")
	if err != nil {
		t.Fatalf("taskQueueGIDs error: %v", err)
	}

	want := []string{"active-match", "waiting-match-1", "waiting-match-2"}
	if len(got) != len(want) {
		t.Fatalf("taskQueueGIDs length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("taskQueueGIDs[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	if len(tellWaitingOffsets) != 2 || tellWaitingOffsets[0] != 0 || tellWaitingOffsets[1] != 1000 {
		t.Fatalf("tellWaiting offsets = %v, want [0 1000]", tellWaitingOffsets)
	}
}

func TestForceRemoveTaskDownloadsAndPurge(t *testing.T) {
	var (
		multicallCount int
		purgeCount     int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req JsonRpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		resp := JsonRpcResponse{ID: req.ID}
		switch req.Method {
		case "aria2.tellActive":
			resp.Result = []map[string]string{
				{"gid": "gid-1", "dir": "cache/task-a"},
			}
		case "aria2.tellWaiting":
			resp.Result = []map[string]string{}
		case "system.multicall":
			multicallCount++
			resp.Result = []interface{}{
				[]interface{}{"OK"},
			}
		case "aria2.purgeDownloadResult":
			purgeCount++
			resp.Result = "OK"
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client := &Aria2Client{
		RPCUrl: srv.URL,
		Client: &http.Client{Timeout: time.Second},
	}

	removed, err := client.ForceRemoveTaskDownloads("cache/task-a")
	if err != nil {
		t.Fatalf("ForceRemoveTaskDownloads error: %v", err)
	}
	if removed != 1 {
		t.Fatalf("ForceRemoveTaskDownloads removed = %d, want 1", removed)
	}
	if multicallCount != 1 {
		t.Fatalf("system.multicall count = %d, want 1", multicallCount)
	}

	if err := client.PurgeDownloadResult(); err != nil {
		t.Fatalf("PurgeDownloadResult error: %v", err)
	}
	if purgeCount != 1 {
		t.Fatalf("purge count = %d, want 1", purgeCount)
	}
}
