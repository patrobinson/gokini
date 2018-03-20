package gokini

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// MonitoringConfiguration allows you to configure how record processing metrics are exposed
type MonitoringConfiguration struct {
	MonitoringService string // Type of monitoring to expose. Supported types are "prometheus"
	Prometheus        Prometheus
	service           monitoringService
}

type monitoringService interface {
	init() error
	incrRecordsProcessed(string, int)
	incrBytesProcessed(string, int64)
	millisBehindLatest(string, int64)
	leaseGained(string)
	leaseLost(string)
	leaseRenewed(string)
	recordGetRecordsTime(string, float64)
	recordProcessRecordsTime(string, float64)
}

func (m *MonitoringConfiguration) init(streamName string, workerID string) error {
	if m.MonitoringService == "" {
		m.service = &noopMonitoringService{}
		return nil
	}

	switch m.MonitoringService {
	case "prometheus":
		m.Prometheus.KinesisStream = streamName
		m.Prometheus.WorkerID = workerID
		m.service = &m.Prometheus
	default:
		return fmt.Errorf("Invalid monitoring service type %s", m.MonitoringService)
	}
	return m.service.init()
}

// Prometheus configures an HTTP endpoint for Prometheus to scrape
type Prometheus struct {
	ListenAddress      string
	Namespace          string
	KinesisStream      string
	WorkerID           string
	processedRecords   *prometheus.CounterVec
	processedBytes     *prometheus.CounterVec
	behindLatestMillis *prometheus.GaugeVec
	leasesHeld         *prometheus.GaugeVec
	leaseRenewals      *prometheus.CounterVec
	getRecordsTime     *prometheus.HistogramVec
	processRecordsTime *prometheus.HistogramVec
}

const defaultNamespace = "gokini"

func (p *Prometheus) init() error {
	if p.Namespace == "" {
		p.Namespace = defaultNamespace
	}
	p.processedBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_processed_bytes`,
		Help: "Number of bytes processed",
	}, []string{"kinesisStream", "shard"})
	p.processedRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_processed_records`,
		Help: "Number of records processed",
	}, []string{"kinesisStream", "shard"})
	p.behindLatestMillis = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: p.Namespace + `_behind_latest_millis`,
		Help: "The amount of milliseconds processing is behind",
	}, []string{"kinesisStream", "shard"})
	p.leasesHeld = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: p.Namespace + `_leases_held`,
		Help: "The number of leases held by the worker",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.leaseRenewals = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_lease_renewals`,
		Help: "The number of successful lease renewals",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.getRecordsTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: p.Namespace + `_get_records_duration_milliseconds`,
		Help: "The time taken to fetch records and process them",
	}, []string{"kinesisStream", "shard"})
	p.processRecordsTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: p.Namespace + `_process_records_duration_milliseconds`,
		Help: "The time taken to process records",
	}, []string{"kinesisStream", "shard"})

	metrics := []prometheus.Collector{
		p.processedBytes,
		p.processedRecords,
		p.behindLatestMillis,
		p.leasesHeld,
		p.leaseRenewals,
		p.getRecordsTime,
		p.processRecordsTime,
	}
	for _, metric := range metrics {
		err := prometheus.Register(metric)
		if err != nil {
			return err
		}
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Debugf("Starting Prometheus listener on %s", p.ListenAddress)
		err := http.ListenAndServe(p.ListenAddress, nil)
		if err != nil {
			log.Errorln("Error starting Prometheus metrics endpoint", err)
		}
	}()
	return nil
}

func (p *Prometheus) incrRecordsProcessed(shard string, count int) {
	p.processedRecords.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *Prometheus) incrBytesProcessed(shard string, count int64) {
	p.processedBytes.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *Prometheus) millisBehindLatest(shard string, millSeconds int64) {
	p.behindLatestMillis.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Set(float64(millSeconds))
}

func (p *Prometheus) leaseGained(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *Prometheus) leaseLost(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Dec()
}

func (p *Prometheus) leaseRenewed(shard string) {
	p.leaseRenewals.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *Prometheus) recordGetRecordsTime(shard string, time float64) {
	p.getRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}

func (p *Prometheus) recordProcessRecordsTime(shard string, time float64) {
	p.processRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}

type noopMonitoringService struct{}

func (n *noopMonitoringService) init() error {
	return nil
}

func (n *noopMonitoringService) incrRecordsProcessed(shard string, count int)        {}
func (n *noopMonitoringService) incrBytesProcessed(shard string, count int64)        {}
func (n *noopMonitoringService) millisBehindLatest(shard string, millSeconds int64)  {}
func (n *noopMonitoringService) leaseGained(shard string)                            {}
func (n *noopMonitoringService) leaseLost(shard string)                              {}
func (n *noopMonitoringService) leaseRenewed(shard string)                           {}
func (n *noopMonitoringService) recordGetRecordsTime(shard string, time float64)     {}
func (n *noopMonitoringService) recordProcessRecordsTime(shard string, time float64) {}
