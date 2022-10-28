package promhttp

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// PromTrimGatherer trim these head/tail buckets which have zero cumulative counts.
var PromTrimGatherer = &trimBucketGatherer{
	defaultGatherer: prometheus.DefaultGatherer,
}

type trimBucketGatherer struct {
	defaultGatherer prometheus.Gatherer
}

// Gather implements Gatherer.
func (r *trimBucketGatherer) Gather() ([]*dto.MetricFamily, error) {
	metrics, err := r.defaultGatherer.Gather()
	for _, metric := range metrics {
		if !strings.HasPrefix(*metric.Name, "soften") {
			continue
		}
		if *metric.Type != dto.MetricType_HISTOGRAM {
			continue
		}
		for _, vecMetric := range metric.Metric {
			histogram := vecMetric.Histogram
			if histogram == nil {
				continue
			}
			// trim bucket
			start, end := r.calcTrimPosition(histogram.Bucket)
			if end-start < len(histogram.Bucket) { // trim as partial buckets
				histogram.Bucket = histogram.Bucket[start:end]
			}
		}
	}
	return metrics, err
}

// calcTrimPosition calculates the start index and end index of trim bucket
func (r *trimBucketGatherer) calcTrimPosition(buckets []*dto.Bucket) (start, end int) {
	length := len(buckets)
	if length <= 0 {
		return
	}
	// calc start
	if *buckets[start].CumulativeCount == 0 {
		for start < length {
			if start == length-1 {
				break
			}
			if *buckets[start+1].CumulativeCount > 0 {
				break
			}
			start++
		}
	}
	// all cumulative counts of all buckets are zero
	if start == length-1 {
		start, end = 0, 0
		return
	}
	// calc end
	maxCumulativeCount := *buckets[length-1].CumulativeCount
	prev := length - 2
	for prev >= 0 && *buckets[prev].CumulativeCount == maxCumulativeCount {
		prev--
	}
	end = prev + 2
	return
}
