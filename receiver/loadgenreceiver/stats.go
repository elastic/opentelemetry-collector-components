package loadgenreceiver

type TelemetryStats struct {
	Requests       int
	FailedRequests int

	LogRecords       int
	MetricDataPoints int
	Spans            int

	FailedLogRecords       int
	FailedMetricDataPoints int
	FailedSpans            int
}

func (s TelemetryStats) Add(other TelemetryStats) TelemetryStats {
	s.Requests += other.Requests
	s.FailedRequests += other.FailedRequests
	s.LogRecords += other.LogRecords
	s.MetricDataPoints += other.MetricDataPoints
	s.Spans += other.Spans
	s.FailedLogRecords += other.FailedLogRecords
	s.FailedMetricDataPoints += other.FailedMetricDataPoints
	s.FailedSpans += other.FailedSpans
	return s
}
