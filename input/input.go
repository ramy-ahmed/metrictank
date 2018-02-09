// Package in provides interfaces, concrete implementations, and utilities
// to ingest data into metrictank
package input

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Handler interface {
	Process(metric *schema.MetricData, partition int32)
}

// TODO: clever way to document all metrics for all different inputs

// Default is a base handler for a metrics packet, aimed to be embedded by concrete implementations
type DefaultHandler struct {
	metricsReceived *stats.Counter32
	MetricInvalid   *stats.Counter32 // metric metric_invalid is a count of times a metric did not validate
	MsgsAge         *stats.Meter32   // in ms
	pressureIdx     *stats.Counter32
	pressureTank    *stats.Counter32

	metrics     mdata.Metrics
	metricIndex idx.MetricIndex

	buffer sync.Map
}

type IntervalLookupRecord struct {
	sync.Mutex

	Interval   int
	DataPoints []*schema.MetricData
	Last       time.Time
	pos        int
}

func NewDefaultHandler(metrics mdata.Metrics, metricIndex idx.MetricIndex, input string) *DefaultHandler {
	return &DefaultHandler{
		metricsReceived: stats.NewCounter32(fmt.Sprintf("input.%s.metrics_received", input)),
		MetricInvalid:   stats.NewCounter32(fmt.Sprintf("input.%s.metric_invalid", input)),
		MsgsAge:         stats.NewMeter32(fmt.Sprintf("input.%s.message_age", input), false),
		pressureIdx:     stats.NewCounter32(fmt.Sprintf("input.%s.pressure.idx", input)),
		pressureTank:    stats.NewCounter32(fmt.Sprintf("input.%s.pressure.tank", input)),

		metrics:     metrics,
		metricIndex: metricIndex,
	}
}

// process makes sure the data is stored and the metadata is in the index
// concurrency-safe.
func (in *DefaultHandler) Process(metric *schema.MetricData, partition int32) {
	if metric == nil {
		return
	}

	if metric.Interval <= 0 {
		// we need to buffer enough of this metric to be able to work out its interval.
		// once that is done, we can update the Id and call in.Process again
		in.SetInterval(metric, partition)
		return
	}
	in.metricsReceived.Inc()
	err := metric.Validate()
	if err != nil {
		in.MetricInvalid.Inc()
		log.Debug("in: Invalid metric %s %v", err, metric)
		return
	}
	if metric.Time == 0 {
		in.MetricInvalid.Inc()
		log.Warn("in: invalid metric. metric.Time is 0. %s", metric.Id)
		return
	}

	pre := time.Now()
	archive := in.metricIndex.AddOrUpdate(metric, partition)
	in.pressureIdx.Add(int(time.Since(pre).Nanoseconds()))

	pre = time.Now()
	m := in.metrics.GetOrCreate(metric.Id, metric.Name, archive.SchemaId, archive.AggId)
	m.Add(uint32(metric.Time), metric.Value)
	in.pressureTank.Add(int(time.Since(pre).Nanoseconds()))
}

func (in *DefaultHandler) SetInterval(metric *schema.MetricData, partition int32) {
	b, ok := in.buffer.Load(metric.Id)
	if !ok {
		dp := make([]*schema.MetricData, 1, 3)
		dp[0] = metric
		in.buffer.Store(metric.Id, &IntervalLookupRecord{
			DataPoints: dp,
		})
		return
	}
	ilr := b.(*IntervalLookupRecord)
	ilr.Lock()
	// check for duplicate TS
	if ilr.DataPoints[ilr.pos].Time == metric.Time {
		//drop the metric as it is a duplicate.
		ilr.Unlock()
		return
	}

	// add this datapoint to our circular buffer
	if len(ilr.DataPoints) < 3 {
		ilr.DataPoints = append(ilr.DataPoints, metric)
		ilr.pos++
	} else {
		if ilr.pos == 2 {
			ilr.pos = 0
		} else {
			ilr.pos++
		}
		ilr.DataPoints[ilr.pos] = metric
	}

	// if the interval is already known and was updated in the last 24hours, use it.
	if ilr.Interval != 0 && time.Since(ilr.Last) > 84600 {
		metric.Interval = ilr.Interval
		metric.SetId()
		in.Process(metric, partition)
		ilr.Unlock()
		return
	}

	if len(ilr.DataPoints) < 3 {
		// we dont have 3 points yet.
		ilr.Unlock()
		return
	}
	log.Debug("input: calculating interval of %s", metric.Id)

	delta1 := ilr.DataPoints[1].Time - ilr.DataPoints[0].Time
	// make sure the points are not out of order
	if delta1 < 0 {
		delta1 = -1 * delta1
	}
	// make sure the points are not out of order
	delta2 := ilr.DataPoints[2].Time - ilr.DataPoints[1].Time
	if delta2 < 0 {
		delta2 = -1 * delta2
	}
	interval := 0

	// To protect against dropped metrics and out of order metrics, use the smallest delta as the interval.
	// Because we have 3 datapoints, it doesnt matter what order they are in.  The smallest delta is always
	// going to be correct. (unless their are out of order and dropped metrics)
	if delta1 <= delta2 {
		interval = int(delta1)
	} else {
		interval = int(delta2)
	}

	// TODO: align the interval to the likely value. eg. if the value is 27, make it 30. if it is 68, use 60. etc...

	if ilr.Last.IsZero() {
		// sort the datapoints then update their interval and process them properly.
		sort.Sort(SortedMetricData(ilr.DataPoints))
		ilr.pos = 2 // as the metrics are sorted, the oldest point is at the end of the slice
		for _, md := range ilr.DataPoints {
			md.Interval = interval
			md.SetId()
			in.Process(md, partition)
		}
	} else {
		metric.Interval = interval
		metric.SetId()
		in.Process(metric, partition)
	}
	ilr.Interval = interval
	ilr.Last = time.Now()
	ilr.Unlock()
}

type SortedMetricData []*schema.MetricData

func (a SortedMetricData) Len() int           { return len(a) }
func (a SortedMetricData) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedMetricData) Less(i, j int) bool { return a[i].Time < a[j].Time }
