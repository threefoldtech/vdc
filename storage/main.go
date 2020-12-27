package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/threefoldtech/vdc/storage/controller"

	"github.com/threefoldtech/vdc/storage/controller/persistentvolume"

	"k8s.io/apimachinery/pkg/util/validation"
	klog "k8s.io/klog/v2"
)

const (
	zerofsType     = "zerofs"
	livenessType   = "liveness"
	controllerType = "controller"

	zerofsDefaultName   = "zerofs.csi.threefold.tech"
	livenessDefaultName = "liveness.csi.threefold.tech"

	pollTime     = 60 // seconds
	probeTimeout = 3  // seconds

	// use default namespace if namespace is not set
	defaultNS = "default"
)

var (
	conf Config
)

func init() {
	// common flags
	flag.StringVar(&conf.Vtype, "type", "zerofs", "driver type [zerofs|liveness|controller]")
	flag.StringVar(&conf.Endpoint, "endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	flag.StringVar(&conf.DriverName, "drivername", "", "name of the driver")
	flag.StringVar(&conf.DriverNamespace, "drivernamespace", defaultNS, "namespace in which driver is deployed")
	flag.StringVar(&conf.NodeID, "nodeid", "", "node id")
	flag.BoolVar(&conf.IsControllerServer, "controllerserver", false, "start cephcsi controller server")
	flag.BoolVar(&conf.IsNodeServer, "nodeserver", false, "start cephcsi node server")
	flag.StringVar(&conf.DomainLabels, "domainlabels", "", "list of kubernetes node labels, that determines the topology"+
		" domain the node belongs to, separated by ','")

	// liveness/grpc metrics related flags
	flag.IntVar(&conf.MetricsPort, "metricsport", 8080, "TCP port for liveness/grpc metrics requests")
	flag.StringVar(&conf.MetricsPath, "metricspath", "/metrics", "path of prometheus endpoint where metrics will be available")
	flag.DurationVar(&conf.PollTime, "polltime", time.Second*pollTime, "time interval in seconds between each poll")
	flag.DurationVar(&conf.PoolTimeout, "timeout", time.Second*probeTimeout, "probe timeout in seconds")

	flag.BoolVar(&conf.Version, "version", false, "Print zerofscsi version information")

	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Exitf("failed to set logtostderr flag: %v", err)
	}
	flag.Parse()
}

func getDriverName() string {
	// was explicitly passed a driver name
	if conf.DriverName != "" {
		return conf.DriverName
	}
	// select driver name based on volume type
	switch conf.Vtype {
	case zerofsType:
		return zerofsDefaultName
	case livenessType:
		return livenessDefaultName
	default:
		return ""
	}
}

// ValidateURL validates the url.
func ValidateURL(c *Config) error {
	_, err := url.Parse(c.MetricsPath)
	return err
}

// ValidateDriverName validates the driver name.
func ValidateDriverName(driverName string) error {
	if driverName == "" {
		return errors.New("driver name is empty")
	}

	const reqDriverNameLen = 63
	if len(driverName) > reqDriverNameLen {
		return errors.New("driver name length should be less than 63 chars")
	}
	var err error
	for _, msg := range validation.IsDNS1123Subdomain(strings.ToLower(driverName)) {
		if err == nil {
			err = errors.New(msg)
			continue
		}
		err = fmt.Errorf("%s: %w", msg, err)
	}
	return err
}

func main() {
	if conf.Version {
		fmt.Println("Zerocsi Version:", "TODO")
		fmt.Println("Git Commit:", "TODO")
		fmt.Println("Go Version:", runtime.Version())
		fmt.Println("Compiler:", runtime.Compiler)
		fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}
	klog.Infof("Driver version: %s and Git version: %s", "TODO", "TODO")

	if conf.Vtype == "" {
		logAndExit("driver type not specified")
	}

	dname := getDriverName()
	err := ValidateDriverName(dname)
	if err != nil {
		logAndExit(err.Error())
	}

	if conf.Vtype == livenessType {
		// validate metrics endpoint
		conf.MetricsIP = os.Getenv("POD_IP")

		if conf.MetricsIP == "" {
			klog.Warning("missing POD_IP env var defaulting to 0.0.0.0")
			conf.MetricsIP = "0.0.0.0"
		}
		err = ValidateURL(&conf)
		if err != nil {
			logAndExit(err.Error())
		}
	}

	klog.Infof("Starting driver type: %v with name: %v", conf.Vtype, dname)
	switch conf.Vtype {

	case zerofsType:
		driver := NewDriver()
		driver.Run(&conf)

	case livenessType:
		//TODO: liveness.Run(&conf)

	case controllerType:
		cfg := controller.Config{
			DriverName: dname,
			Namespace:  conf.DriverNamespace,
		}
		// initialize all controllers before starting.
		initControllers()
		err = controller.Start(cfg)
		if err != nil {
			logAndExit(err.Error())
		}
	}

	os.Exit(0)
}

// initControllers will initialize all the controllers.
func initControllers() {
	// Add list of controllers here.
	persistentvolume.Init()
}

func logAndExit(msg string) {
	klog.Errorln(msg)
	os.Exit(1)
}
