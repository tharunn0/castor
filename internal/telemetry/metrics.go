package telemetry

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry        *prometheus.Registry
	Handler         http.Handler
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
}

var defaultMetrics *Metrics

func NewMetrics(serviceName string, env ...string) *Metrics {
	targetEnv := GetEnv()
	if len(env) > 0 && strings.TrimSpace(env[0]) != "" {
		targetEnv = NormalizeEnv(env[0])
	}

	reg := prometheus.NewRegistry()

	if targetEnv == EnvOff {
		return &Metrics{
			Registry: reg,
			Handler:  http.NotFoundHandler(),
			RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "noop_requests_total",
			}, []string{"method", "status"}),
			RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Name: "noop_request_duration_seconds",
			}, []string{"method", "status"}),
		}
	}

	namespace := strings.ReplaceAll(serviceName, "-", "_")
	if namespace == "" {
		namespace = "castor"
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: namespace,
		}),
	)

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "Total requests processed",
		},
		[]string{"method", "status"},
	)

	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "request_duration_seconds",
			Help:      "Duration of requests in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "status"},
	)

	reg.MustRegister(requestsTotal, requestDuration)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	return &Metrics{
		Registry:        reg,
		Handler:         handler,
		RequestsTotal:   requestsTotal,
		RequestDuration: requestDuration,
	}
}

func InitMetrics(serviceName string, env ...string) *Metrics {
	defaultMetrics = NewMetrics(serviceName, env...)
	return defaultMetrics
}

func DefaultMetrics() *Metrics {
	return defaultMetrics
}

func MetricsHandler() http.Handler {
	if defaultMetrics != nil && defaultMetrics.Handler != nil {
		return defaultMetrics.Handler
	}
	return promhttp.Handler()
}
