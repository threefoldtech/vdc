package main

import (
	"time"
)

// variables which will be set during the build time.
var (
	// GitCommit tell the latest git commit image is built from
	GitCommit string
	// DriverVersion which will be driver version
	DriverVersion string
)

// Config holds the parameters list which can be configured.
type Config struct {
	Vtype           string // driver type [rbd|cephfs|liveness|controller]
	Endpoint        string // CSI endpoint
	DriverName      string // name of the driver
	DriverNamespace string // namespace in which driver is deployed
	NodeID          string // node id
	PluginPath      string // location of cephcsi plugin
	DomainLabels    string // list of domain labels to read from the node

	// metrics related flags
	MetricsPath     string        // path of prometheus endpoint where metrics will be available
	HistogramOption string        // Histogram option for grpc metrics, should be comma separated value, ex:= "0.5,2,6" where start=0.5 factor=2, count=6
	MetricsIP       string        // TCP port for liveness/ metrics requests
	MetricsPort     int           // TCP port for liveness/grpc metrics requests
	PollTime        time.Duration // time interval in seconds between each poll
	PoolTimeout     time.Duration // probe timeout in seconds

	IsControllerServer bool // if set to true start provisoner server
	IsNodeServer       bool // if set to true start node server
	Version            bool // cephcsi version
}
