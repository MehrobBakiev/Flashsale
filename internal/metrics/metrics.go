// Package metrics — метрики Prometheus, общие для API и воркера.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flashsale_http_requests_total",
		Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "flashsale_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"route"})

	Reservations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flashsale_reservations_total",
		Help: "Reservation attempts by result.",
	}, []string{"result"})

	ReserveDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "flashsale_reserve_duration_seconds",
		Help:    "Latency of the reservation business operation.",
		Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .025, .05, .1},
	})

	OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flashsale_outbox_published_total",
		Help: "Events published from outbox to Kafka.",
	})

	OutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "flashsale_outbox_backlog",
		Help: "Events in outbox not yet published.",
	})

	OrdersExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "flashsale_orders_expired_total",
		Help: "Reservations expired by the background expirer.",
	})

	ConsumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "flashsale_consumer_messages_total",
		Help: "Kafka messages handled by consumer group and result.",
	}, []string{"group", "result"})
)
