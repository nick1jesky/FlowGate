package models

import "time"

type TelemetryPoint struct {
	DeviceID  string    `json:"device_id" binding:"required"`
	Metric    string    `json:"metric" binding:"required"`
	Value     float64   `json:"value" binding:"required"`
	Timestamp time.Time `json:"timestamp" binding:"required"`
}

type IngestRequest []TelemetryPoint

type AggregatedPoint struct {
	DeviceID string    `json:"device_id"`
	Minute   time.Time `json:"minute"`
	AvgValue float64   `json:"avg_value"`
}
