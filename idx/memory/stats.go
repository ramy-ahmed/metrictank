package memory

import "github.com/grafana/metrictank/stats"

var (
	// metric recovered_errors.idx.memory.corrupt-index is how many times
	// a corruption has been detected in one of the internal index structures
	// each time this happens, an error is logged with more details.
	corruptIndex = stats.NewCounter32("recovered_errors.idx.memory.corrupt-index")

	// metric recovered_errors.idx.memory.invalid-id is how many times
	// an invalid metric id is encountered.
	// each time this happens, an error is logged with more details.
	invalidId = stats.NewCounter32("recovered_errors.idx.memory.invalid-id")

	// metric recovered_errors.idx.memory.invalid-tag is how many times
	// an invalid tag for a metric is encountered.
	// each time this happens, an error is logged with more details.
	invalidTag = stats.NewCounter32("recovered_errors.idx.memory.invalid-tag")
)
