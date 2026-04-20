package downloader

import "testing"

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
