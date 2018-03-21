package gokini

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type mockCloudWatch struct {
	cloudwatchiface.CloudWatchAPI
	metricData []*cloudwatch.MetricDatum
}

func (m *mockCloudWatch) PutMetricData(input *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	m.metricData = append(m.metricData, input.MetricData...)
	return &cloudwatch.PutMetricDataOutput{}, nil
}

func TestCloudWatchMonitoring(t *testing.T) {
	mockCW := &mockCloudWatch{}
	cwService := &cloudWatchMonitoringService{
		Namespace:     "testCloudWatchMonitoring",
		KinesisStream: "cloudwatch_monitoring",
		WorkerID:      "abc123",
		ResolutionSec: 1,
		svc:           mockCW,
		shardMetrics:  map[string]*cloudWatchMetrics{},
	}
	cwService.incrRecordsProcessed("00001", 10)
	err := cwService.flush()
	if err != nil {
		t.Errorf("Received error sending data to cloudwatch %s", err)
	}
	if len(mockCW.metricData) < 1 {
		t.Fatal("Expected at least one metric to be sent to cloudwatch")
	}

	if *mockCW.metricData[0].Value != float64(10) {
		t.Errorf("Expected metric value to be 10.0, got %f", *mockCW.metricData[0].Value)
	}
}
