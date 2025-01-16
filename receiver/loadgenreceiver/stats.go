package loadgenreceiver

type TelemetryStats struct {
	Requests         int
	LogRecords       int
	MetricDataPoints int
	Spans            int
}

func (s TelemetryStats) Add(other TelemetryStats) TelemetryStats {
	s.Requests += other.Requests
	s.LogRecords += other.LogRecords
	s.MetricDataPoints += other.MetricDataPoints
	s.Spans += other.Spans
	return s
}
