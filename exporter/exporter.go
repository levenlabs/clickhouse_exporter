package exporter

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/mailru/go-clickhouse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	namespace = "clickhouse" // For Prometheus metrics.
)

// Exporter collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	db             *sql.DB
	scrapeFailures prometheus.Counter
}

// NewExporter returns an initialized Exporter. The user/password can either be in the
// uri or sent and they'll be automatically added to the uri.
func NewExporter(uri url.URL, insecure bool, user, password string) (*Exporter, error) {
	if uri.User.Username() == "" && user != "" {
		if password != "" {
			uri.User = url.UserPassword(user, password)
		} else {
			uri.User = url.User(user)
		}
	}

	if insecure {
		clickhouse.RegisterTLSConfig("insecure", &tls.Config{InsecureSkipVerify: insecure})
		q := uri.Query()
		q.Set("tls_config", "insecure")
		uri.RawQuery = q.Encode()
	}

	db, err := sql.Open("clickhouse", uri.String())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &Exporter{
		db: db,
		scrapeFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrape_failures_total",
			Help:      "Number of errors while scraping clickhouse.",
		}),
	}, nil
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	metrics, err := e.parseKeyValueDescQuery("SELECT metric, value, description FROM system.metrics")
	if err != nil {
		return fmt.Errorf("Error querying system.metrics: %v", err)
	}

	for _, m := range metrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricName(m.key),
			Help:      m.desc,
		}, []string{}).WithLabelValues()
		newMetric.Set(float64(m.value))
		newMetric.Collect(ch)
	}

	asyncMetrics, err := e.parseAsyncQuery("SELECT metric, value FROM system.asynchronous_metrics")
	if err != nil {
		return fmt.Errorf("Error querying system.asynchronous_metrics: %v", err)
	}

	for _, am := range asyncMetrics {
		newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricName(am.key),
			Help:      "Number of " + am.key + " async processed",
		}, []string{}).WithLabelValues()
		newMetric.Set(float64(am.value))
		newMetric.Collect(ch)
	}

	events, err := e.parseKeyValueDescQuery("SELECT event, value, description FROM system.events")
	if err != nil {
		return fmt.Errorf("Error querying system.events: %v", err)
	}

	for _, ev := range events {
		newMetric, err := prometheus.NewConstMetric(
			prometheus.NewDesc(namespace+"_"+metricName(ev.key)+"_total", ev.desc, []string{}, nil),
			prometheus.CounterValue, float64(ev.value))
		if err != nil {
			return fmt.Errorf("Error calling NewConstMetric: %v", err)
		}
		ch <- newMetric
	}

	parts, err := e.parsePartsQuery(`SELECT database, table, active, sum(bytes) AS bytes, count() as parts, sum(rows) AS rows, max(max_date) as max_date, max(max_time) as max_time
		FROM system.parts GROUP BY database, table, active`)
	if err != nil {
		return fmt.Errorf("Error querying system.parts: %v", err)
	}

	for _, part := range parts {
		if part.active == 1 {
			newBytesMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "table_parts_bytes",
				Help:      "Table size in bytes",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newBytesMetric.Set(float64(part.bytes))
			newBytesMetric.Collect(ch)

			newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "table_parts_count",
				Help:      "Number of active parts of the table",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newCountMetric.Set(float64(part.parts))
			newCountMetric.Collect(ch)

			newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "table_parts_rows",
				Help:      "Number of rows in the table",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newRowsMetric.Set(float64(part.rows))
			newRowsMetric.Collect(ch)

			// use either max_date or max_time
			t := part.maxDate
			if part.maxTime.After(t) {
				t = part.maxTime
			}
			if !t.IsZero() {
				newTimeMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Namespace: namespace,
					Name:      "table_parts_max_time",
					Help:      "Maximum value of the date/time key in the table as a unix timestamp",
				}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
				newTimeMetric.Set(float64(t.Unix()))
				newTimeMetric.Collect(ch)
			}
		} else {
			newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "table_inactive_parts_count",
				Help:      "Number of inactive parts of the table",
			}, []string{"database", "table"}).WithLabelValues(part.database, part.table)
			newCountMetric.Set(float64(part.parts))
			newCountMetric.Collect(ch)
		}
	}

	replicas, err := e.parseReplicasQuery(`SELECT database, table, replica_name, queue_size, absolute_delay,
	 total_replicas, active_replicas, future_parts, parts_to_check, inserts_in_queue, merges_in_queue
	 FROM system.replicas`)
	if err != nil {
		return fmt.Errorf("Error querying system.replicas: %v", err)
	}

	for _, repl := range replicas {
		newSizeMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_queue_size",
			Help:      "Number of queue entries to execute",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newSizeMetric.Set(float64(repl.size))
		newSizeMetric.Collect(ch)

		newDelayMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_absolute_delay",
			Help:      "Number of seconds that the replica is behind the current time",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newDelayMetric.Set(float64(repl.delay))
		newDelayMetric.Collect(ch)

		newTotalMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_total_replicas",
			Help:      "Total number of replicas",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newTotalMetric.Set(float64(repl.total))
		newTotalMetric.Collect(ch)

		newActiveMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_active_replicas",
			Help:      "Number of active replicas",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newActiveMetric.Set(float64(repl.active))
		newActiveMetric.Collect(ch)

		newFuturePartsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_future_parts",
			Help:      "Number of data parts that will appear as the result of INSERTs or merges that haven't been done yet",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newFuturePartsMetric.Set(float64(repl.futureParts))
		newFuturePartsMetric.Collect(ch)

		newPartsToCheckMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_parts_to_check",
			Help:      "Number of data parts in the queue for verification",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newPartsToCheckMetric.Set(float64(repl.partsToCheck))
		newPartsToCheckMetric.Collect(ch)

		newInsertsInQueueMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_inserts_in_queue",
			Help:      "Number of insert blocks of data that need to be made",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newInsertsInQueueMetric.Set(float64(repl.insertsInQueue))
		newInsertsInQueueMetric.Collect(ch)

		newMergesInQueueMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "replica_merges_in_queue",
			Help:      "Number of merges waiting to be made",
		}, []string{"database", "table", "name"}).WithLabelValues(repl.database, repl.table, repl.name)
		newMergesInQueueMetric.Set(float64(repl.mergesInQueue))
		newMergesInQueueMetric.Collect(ch)

	}

	mutations, err := e.parseMutationsResponse(`SELECT database, table, count() as mutations, sum(parts_to_do) as parts_to_do
		FROM system.mutations WHERE is_done = 0
		GROUP BY database, table`)
	if err != nil {
		return fmt.Errorf("Error querying system.mutations: %v", err)
	}

	for _, mut := range mutations {
		newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_mutations_count",
			Help:      "Number of mutations pending for the table",
		}, []string{"database", "table"}).WithLabelValues(mut.database, mut.table)
		newCountMetric.Set(float64(mut.mutations))
		newCountMetric.Collect(ch)

		newPartsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "table_mutations_parts",
			Help:      "Number of pending mutation parts to do of the table",
		}, []string{"database", "table"}).WithLabelValues(mut.database, mut.table)
		newPartsMetric.Set(float64(mut.partsToDo))
		newPartsMetric.Collect(ch)
	}

	processes, err := e.parseProcessesResponse(`SELECT count() as processes, sum(memory_usage) as memory_usage,
		sum(peak_memory_usage) as peak_memory_usage, sum(elapsed) as elapsed, sum(read_rows) as read_rows
		FROM system.processes
		WHERE query NOT LIKE '%FROM system.%'`)
	if err != nil {
		return fmt.Errorf("Error querying system.processes: %v", err)
	}

	for _, proc := range processes {
		newCountMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processes_count",
			Help:      "Number of ongoing processes",
		}, []string{}).WithLabelValues()
		newCountMetric.Set(float64(proc.processes))
		newCountMetric.Collect(ch)

		newMemoryMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processes_memory_usage_bytes",
			Help:      "Total memory usage of all ongoing processes",
		}, []string{}).WithLabelValues()
		newMemoryMetric.Set(float64(proc.memoryUsage))
		newMemoryMetric.Collect(ch)

		newPeakMemoryMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processes_peak_memory_usage_bytes",
			Help:      "Total memory usage of all ongoing processes",
		}, []string{}).WithLabelValues()
		newPeakMemoryMetric.Set(float64(proc.peakMemoryUsage))
		newPeakMemoryMetric.Collect(ch)

		newElapsedMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processes_elapsed_seconds",
			Help:      "Total elapsed time of all ongoing processes",
		}, []string{}).WithLabelValues()
		newElapsedMetric.Set(proc.elapsed)
		newElapsedMetric.Collect(ch)

		newRowsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "processes_read_rows",
			Help:      "Total amount of rows read so far by all ongoing processes",
		}, []string{}).WithLabelValues()
		newRowsMetric.Set(float64(proc.readRows))
		newRowsMetric.Collect(ch)
	}

	return nil
}

type keyValResult struct {
	key   string
	value int64
	desc  string
}

func (e *Exporter) parseKeyValueDescQuery(query string) ([]keyValResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []keyValResult
	for rows.Next() {
		var l keyValResult
		if err := rows.Scan(&l.key, &l.value, &l.desc); err != nil {
			return nil, err
		}
		results = append(results, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type asyncResult struct {
	key   string
	value float64
}

func (e *Exporter) parseAsyncQuery(query string) ([]asyncResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []asyncResult
	for rows.Next() {
		var l asyncResult
		if err := rows.Scan(&l.key, &l.value); err != nil {
			return nil, err
		}
		results = append(results, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type partsResult struct {
	database string
	table    string
	active   int
	bytes    int64
	parts    int64
	rows     int64
	maxDate  time.Time
	maxTime  time.Time
}

func (e *Exporter) parsePartsQuery(query string) ([]partsResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []partsResult
	for rows.Next() {
		var p partsResult
		if err := rows.Scan(&p.database, &p.table, &p.active, &p.bytes, &p.parts, &p.rows, &p.maxDate, &p.maxTime); err != nil {
			return nil, err
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type replicaResult struct {
	database       string
	table          string
	name           string
	size           int64
	delay          int64
	total          int64
	active         int64
	futureParts    int64
	partsToCheck   int64
	insertsInQueue int64
	mergesInQueue  int64
}

func (e *Exporter) parseReplicasQuery(query string) ([]replicaResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []replicaResult
	for rows.Next() {
		var r replicaResult
		if err := rows.Scan(&r.database, &r.table, &r.name, &r.size, &r.delay, &r.total, &r.active,
			&r.futureParts, &r.partsToCheck, &r.insertsInQueue, &r.mergesInQueue); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type mutationsResult struct {
	database  string
	table     string
	mutations int64
	partsToDo int64
}

func (e *Exporter) parseMutationsResponse(query string) ([]mutationsResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []mutationsResult
	for rows.Next() {
		var r mutationsResult
		if err := rows.Scan(&r.database, &r.table, &r.mutations, &r.partsToDo); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

type processesResult struct {
	processes       int64
	memoryUsage     int64
	peakMemoryUsage int64
	elapsed         float64
	readRows        int64
}

func (e *Exporter) parseProcessesResponse(query string) ([]processesResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		return nil, err
	}
	var results []processesResult
	for rows.Next() {
		var r processesResult
		if err := rows.Scan(&r.processes, &r.memoryUsage, &r.peakMemoryUsage, &r.elapsed, &r.readRows); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if err := e.collect(ch); err != nil {
		log.Errorf("Error scraping clickhouse: %s", err)
		e.scrapeFailures.Inc()
		e.scrapeFailures.Collect(ch)
	}
}

func metricName(in string) string {
	out := toSnake(in)
	return strings.Replace(out, ".", "_", -1)
}

// toSnake convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}

// check interface
var _ prometheus.Collector = (*Exporter)(nil)
