package gokini

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// MonitoringConfiguration allows you to configure how record processing metrics are exposed
type MonitoringConfiguration struct {
	MonitoringService string // Type of monitoring to expose. Supported types are "prometheus"
	Prometheus        prometheusMonitoringService
	CloudWatch        cloudWatchMonitoringService
	service           monitoringService
}

type monitoringService interface {
	init() error
	incrRecordsProcessed(string, int)
	incrBytesProcessed(string, int64)
	millisBehindLatest(string, float64)
	leaseGained(string)
	leaseLost(string)
	leaseRenewed(string)
	recordGetRecordsTime(string, float64)
	recordProcessRecordsTime(string, float64)
}

func (m *MonitoringConfiguration) init(streamName string, workerID string, sess *session.Session) error {
	if m.MonitoringService == "" {
		m.service = &noopMonitoringService{}
		return nil
	}

	switch m.MonitoringService {
	case "prometheus":
		m.Prometheus.KinesisStream = streamName
		m.Prometheus.WorkerID = workerID
		m.service = &m.Prometheus
	case "cloudwatch":
		m.CloudWatch.Session = sess
		m.CloudWatch.KinesisStream = streamName
		m.CloudWatch.WorkerID = workerID
		m.service = &m.CloudWatch
	default:
		return fmt.Errorf("Invalid monitoring service type %s", m.MonitoringService)
	}
	return m.service.init()
}

type prometheusMonitoringService struct {
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

func (p *prometheusMonitoringService) init() error {
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

func (p *prometheusMonitoringService) incrRecordsProcessed(shard string, count int) {
	p.processedRecords.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *prometheusMonitoringService) incrBytesProcessed(shard string, count int64) {
	p.processedBytes.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *prometheusMonitoringService) millisBehindLatest(shard string, millSeconds float64) {
	p.behindLatestMillis.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Set(millSeconds)
}

func (p *prometheusMonitoringService) leaseGained(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *prometheusMonitoringService) leaseLost(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Dec()
}

func (p *prometheusMonitoringService) leaseRenewed(shard string) {
	p.leaseRenewals.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *prometheusMonitoringService) recordGetRecordsTime(shard string, time float64) {
	p.getRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}

func (p *prometheusMonitoringService) recordProcessRecordsTime(shard string, time float64) {
	p.processRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}

type noopMonitoringService struct{}

func (n *noopMonitoringService) init() error {
	return nil
}

func (n *noopMonitoringService) incrRecordsProcessed(shard string, count int)         {}
func (n *noopMonitoringService) incrBytesProcessed(shard string, count int64)         {}
func (n *noopMonitoringService) millisBehindLatest(shard string, millSeconds float64) {}
func (n *noopMonitoringService) leaseGained(shard string)                             {}
func (n *noopMonitoringService) leaseLost(shard string)                               {}
func (n *noopMonitoringService) leaseRenewed(shard string)                            {}
func (n *noopMonitoringService) recordGetRecordsTime(shard string, time float64)      {}
func (n *noopMonitoringService) recordProcessRecordsTime(shard string, time float64)  {}

type cloudWatchMonitoringService struct {
	Namespace     string
	KinesisStream string
	WorkerID      string
	// What granularity we should send metrics to CW at. Note setting this to 1 will cost quite a bit of money
	// At the time of writing (March 2018) about US$200 per month
	ResolutionSec int
	Session       *session.Session
	svc           cloudwatchiface.CloudWatchAPI
	shardMetrics  map[string]*cloudWatchMetrics
}

type cloudWatchMetrics struct {
	processedRecords   int64
	processedBytes     int64
	behindLatestMillis []float64
	leasesHeld         int64
	leaseRenewals      int64
	getRecordsTime     []float64
	processRecordsTime []float64
	sync.Mutex
}

func (cw *cloudWatchMonitoringService) init() error {
	if cw.ResolutionSec == 0 {
		cw.ResolutionSec = 60
	}

	cw.svc = cloudwatch.New(cw.Session)
	cw.shardMetrics = make(map[string]*cloudWatchMetrics)
	return nil
}

func (cw *cloudWatchMonitoringService) flushDaemon() {
	previousFlushTime := time.Now()
	resolutionDuration := time.Duration(cw.ResolutionSec) * time.Second
	for {
		time.Sleep(resolutionDuration - time.Now().Sub(previousFlushTime))
		err := cw.flush()
		if err != nil {
			log.Errorln("Error sending metrics to CloudWatch", err)
		}
		previousFlushTime = time.Now()
	}
}

func (cw *cloudWatchMonitoringService) flush() error {
	for shard, metric := range cw.shardMetrics {
		metric.Lock()
		defaultDimensions := []*cloudwatch.Dimension{
			&cloudwatch.Dimension{
				Name:  aws.String("shard"),
				Value: &shard,
			},
			&cloudwatch.Dimension{
				Name:  aws.String("KinesisStreamName"),
				Value: &cw.KinesisStream,
			},
		}
		leaseDimensions := make([]*cloudwatch.Dimension, len(defaultDimensions))
		copy(defaultDimensions, leaseDimensions)
		leaseDimensions = append(leaseDimensions, &cloudwatch.Dimension{
			Name:  aws.String("WorkerID"),
			Value: &cw.WorkerID,
		})
		metricTimestamp := time.Now()
		_, err := cw.svc.PutMetricData(&cloudwatch.PutMetricDataInput{
			Namespace: aws.String(cw.Namespace),
			MetricData: []*cloudwatch.MetricDatum{
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("RecordsProcessed"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.processedRecords)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("DataBytesProcessed"),
					Unit:       aws.String("Byte"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.processedBytes)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("MillisBehindLatest"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.behindLatestMillis))),
						Sum:         sumFloat64(metric.behindLatestMillis),
						Maximum:     maxFloat64(metric.behindLatestMillis),
						Minimum:     minFloat64(metric.behindLatestMillis),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("KinesisDataFetcher.getRecords.Time"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.getRecordsTime))),
						Sum:         sumFloat64(metric.getRecordsTime),
						Maximum:     maxFloat64(metric.getRecordsTime),
						Minimum:     minFloat64(metric.getRecordsTime),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("RecordProcessor.processRecords.Time"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.processRecordsTime))),
						Sum:         sumFloat64(metric.processRecordsTime),
						Maximum:     maxFloat64(metric.processRecordsTime),
						Minimum:     minFloat64(metric.processRecordsTime),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: leaseDimensions,
					MetricName: aws.String("RenewLease.Success"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.leaseRenewals)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: leaseDimensions,
					MetricName: aws.String("CurrentLeases"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.leasesHeld)),
				},
			},
		})
		if err == nil {
			metric.processedRecords = 0
			metric.processedBytes = 0
			metric.behindLatestMillis = []float64{}
			metric.leaseRenewals = 0
			metric.getRecordsTime = []float64{}
			metric.processRecordsTime = []float64{}
		}
		metric.Unlock()
		return err
	}
	return nil
}

func (cw *cloudWatchMonitoringService) incrRecordsProcessed(shard string, count int) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processedRecords += int64(count)
}

func (cw *cloudWatchMonitoringService) incrBytesProcessed(shard string, count int64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processedBytes += count
}

func (cw *cloudWatchMonitoringService) millisBehindLatest(shard string, millSeconds float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].behindLatestMillis = append(cw.shardMetrics[shard].behindLatestMillis, millSeconds)
}

func (cw *cloudWatchMonitoringService) leaseGained(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leasesHeld++
}

func (cw *cloudWatchMonitoringService) leaseLost(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leasesHeld--
}

func (cw *cloudWatchMonitoringService) leaseRenewed(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leaseRenewals++
}

func (cw *cloudWatchMonitoringService) recordGetRecordsTime(shard string, time float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].getRecordsTime = append(cw.shardMetrics[shard].getRecordsTime, time)
}
func (cw *cloudWatchMonitoringService) recordProcessRecordsTime(shard string, time float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processRecordsTime = append(cw.shardMetrics[shard].processRecordsTime, time)
}

func sumFloat64(slice []float64) *float64 {
	sum := float64(0)
	for _, num := range slice {
		sum += num
	}
	return &sum
}

func maxFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	max := slice[0]
	for _, num := range slice {
		if num > max {
			max = num
		}
	}
	return &max
}

func minFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	min := slice[0]
	for _, num := range slice {
		if num < min {
			min = num
		}
	}
	return &min
}
