package main

import (
	"bufio"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Metric struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
}

// seriesKey canonicalizes a metric name + tag set into one string so that
// {host=a,region=b} and {region=b,host=a} map to the same series regardless
// of the order tags arrived in the JSON body.
func seriesKey(m Metric) string {
	if len(m.Tags) == 0 {
		return m.Metric
	}

	keys := make([]string, 0, len(m.Tags))
	for k := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(m.Metric)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m.Tags[k])
	}
	b.WriteByte('}')
	return b.String()
}

var metrics = make(map[string][]Metric)
var mu sync.RWMutex
var wal *os.File

func healthHandler(w http.ResponseWriter, r *http.Request) {
	// Tell the client we're returning JSON
	w.Header().Set("Content-Type", "application/json")

	// Create our response
	response := map[string]string{
		"status": "okkk",
	}

	// Convert the Go map to JSON and send it through w
	json.NewEncoder(w).Encode(response)
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	var metric Metric

	if err := json.NewDecoder(r.Body).Decode(&metric); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(metric)
	if err != nil {
		http.Error(w, "failed to encode metric", http.StatusInternalServerError)
		return
	}

	// Protect the entire write operation
	mu.Lock()
	defer mu.Unlock()

	// 1. Append to WAL
	_, err = wal.Write(append(data, '\n'))
	if err != nil {
		http.Error(w, "failed to write WAL", http.StatusInternalServerError)
		return
	}

	// 2. Sync to persistent storage this means guarantee it gets written to metrics.log and not lost if computer dies while it stores in RAM before going to metrics.log previously
	if err := wal.Sync(); err != nil {
		http.Error(w, "failed to sync WAL", http.StatusInternalServerError)
		return
	}

	// 3. Update RAM
	key := seriesKey(metric)
	metrics[key] = append(
		metrics[key],
		metric,
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "accepted",
	})
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow GET requests
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Tell the client we're returning JSON
	w.Header().Set("Content-Type", "application/json")
	mu.RLock()
	defer mu.RUnlock()

	// Convert the metrics slice to JSON and send it back
	err := json.NewEncoder(w).Encode(metrics)
	if err != nil {
		http.Error(w, "failed to encode metrics", http.StatusInternalServerError)
		return
	}
}

// queryHandler answers GET /query?metric=X&from=&to=&<tag>=<value>.
// from/to are inclusive Unix timestamps; any query param other than
// metric/from/to is treated as an exact-match tag filter. Unspecified
// tags on a series are ignored, so "metric=cpu&host=a" matches every
// cpu series tagged host=a regardless of what other tags it carries.
func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()

	metricName := q.Get("metric")
	if metricName == "" {
		http.Error(w, "metric is required", http.StatusBadRequest)
		return
	}

	from := int64(math.MinInt64)
	if v := q.Get("from"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			http.Error(w, "invalid from", http.StatusBadRequest)
			return
		}
		from = parsed
	}

	to := int64(math.MaxInt64)
	if v := q.Get("to"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			http.Error(w, "invalid to", http.StatusBadRequest)
			return
		}
		to = parsed
	}

	tagFilters := make(map[string]string)
	for k, v := range q {
		if k == "metric" || k == "from" || k == "to" {
			continue
		}
		tagFilters[k] = v[0]
	}

	mu.RLock()
	defer mu.RUnlock()

	result := make(map[string][]Metric)
	for key, points := range metrics {
		if len(points) == 0 || points[0].Metric != metricName {
			continue
		}
		if !matchesTags(points[0].Tags, tagFilters) {
			continue
		}

		// Points within a series are appended in arrival order, which we
		// rely on being timestamp order (see M1's open item: nothing
		// currently enforces that). Binary search assumes it holds.
		lo := sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp >= from
		})
		hi := sort.Search(len(points), func(i int) bool {
			return points[i].Timestamp > to
		})

		if lo < hi {
			result[key] = points[lo:hi]
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, "failed to encode query result", http.StatusInternalServerError)
		return
	}
}

func matchesTags(tags, filters map[string]string) bool {
	for k, v := range filters {
		if tags[k] != v {
			return false
		}
	}
	return true
}

func replayWAL() error {
	file, err := os.Open("metrics.log")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var metric Metric

		err := json.Unmarshal(scanner.Bytes(), &metric)
		if err != nil {
			return err
		}

		key := seriesKey(metric)
		metrics[key] = append(
			metrics[key],
			metric,
		)
	}

	return scanner.Err()
}

func main() {
	// 1. Recover existing data into memory
	if err := replayWAL(); err != nil {
		log.Fatal(err)
	}

	// 2. Open WAL for future writes
	var err error
	wal, err = os.OpenFile(
		"metrics.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)

	if err != nil {
		log.Fatal(err)
	}

	defer wal.Close()

	// 3. Register HTTP routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/ingest", ingestHandler)
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/query", queryHandler)

	// 4. Start server
	log.Println("Pulse running on :8080")

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
