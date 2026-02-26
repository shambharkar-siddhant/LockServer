package obs

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AcquireTotal *prometheus.CounterVec // result=success|fail|busy
	RenewTotal   *prometheus.CounterVec // result=success|fail|busy
	ReleaseTotal *prometheus.CounterVec // result=success|fail|busy

	OpLatencyMS *prometheus.HistogramVec // op=acquire|renew|release

	DBBusyTotal *prometheus.CounterVec // op=acquire|renew|release
	LocksHeld     prometheus.Gauge
	ExpiredTotal  prometheus.Counter
}

func NewMetrics() *Metrics {
	m := &Metrics{
		AcquireTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lock_acquire_total",
				Help: "Total acquire attempts by result",
			},
			[]string{"result"},
		),
		RenewTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lock_renew_total",
				Help: "Total renew attempts by result",
			},
			[]string{"result"},
		),
		ReleaseTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lock_release_total",
				Help: "Total release attempts by result",
			},
			[]string{"result"},
		),
		OpLatencyMS: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "lock_op_latency_ms",
				Help:    "Latency of lock operations (ms)",
				Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1ms .. ~2048ms
			},
			[]string{"op"},
		),
		DBBusyTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "lock_db_busy_total",
				Help: "Total sqlite busy/locked errors",
			},
			[]string{"op"},
		),
		LocksHeld: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "locks_held",
			Help: "Number of currently held (unexpired) locks",
		}),
		ExpiredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lock_expired_total",
			Help: "Total number of leases that expired and were detected by the monitor",
		}),
	}

	prometheus.MustRegister(
		m.AcquireTotal,
		m.RenewTotal,
		m.ReleaseTotal,
		m.OpLatencyMS,
		m.DBBusyTotal,
		m.LocksHeld,
		m.ExpiredTotal,
	)

	return m
}