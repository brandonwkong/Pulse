package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// setupTestWAL points the package-level wal/metrics at a throwaway temp
// file so tests never touch the real metrics.log, and returns a cleanup
// func to remove it.
func setupTestWAL(t *testing.T) func() {
	t.Helper()

	tmp, err := os.CreateTemp("", "pulse_test_*.log")
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	metrics = make(map[string][]Metric)
	wal = tmp
	mu.Unlock()

	return func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}
}

func postMetric(t *testing.T, m Metric) {
	t.Helper()

	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	ingestHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d %s", rec.Code, rec.Body.String())
	}
}

func TestQueryFiltersByMetricTagsAndRange(t *testing.T) {
	cleanup := setupTestWAL(t)
	defer cleanup()

	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 100, Value: 1})
	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 200, Value: 2})
	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 300, Value: 3})
	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "b"}, Timestamp: 150, Value: 9})
	postMetric(t, Metric{Metric: "mem", Tags: map[string]string{"host": "a"}, Timestamp: 150, Value: 42})

	req := httptest.NewRequest(http.MethodGet, "/query?metric=cpu&host=a&from=150&to=250", nil)
	rec := httptest.NewRecorder()
	queryHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("query failed: %d %s", rec.Code, rec.Body.String())
	}

	var result map[string][]Metric
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected exactly one matching series, got %d: %v", len(result), result)
	}

	points, ok := result["cpu{host=a}"]
	if !ok {
		t.Fatalf("expected series cpu{host=a} in result, got %v", result)
	}
	if len(points) != 1 || points[0].Timestamp != 200 {
		t.Fatalf("expected single point at ts=200, got %+v", points)
	}
}

func TestQueryRequiresMetric(t *testing.T) {
	cleanup := setupTestWAL(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rec := httptest.NewRecorder()
	queryHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing metric, got %d", rec.Code)
	}
}

func TestQueryUnboundedRangeReturnsAllPointsForSeries(t *testing.T) {
	cleanup := setupTestWAL(t)
	defer cleanup()

	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 100, Value: 1})
	postMetric(t, Metric{Metric: "cpu", Tags: map[string]string{"host": "a"}, Timestamp: 200, Value: 2})

	req := httptest.NewRequest(http.MethodGet, "/query?metric=cpu&host=a", nil)
	rec := httptest.NewRecorder()
	queryHandler(rec, req)

	var result map[string][]Metric
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if len(result["cpu{host=a}"]) != 2 {
		t.Fatalf("expected both points with no from/to bounds, got %+v", result)
	}
}
