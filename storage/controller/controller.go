package controller

import (
	clientConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	klog "k8s.io/klog/v2"
)

// ContollerManager is the interface that will wrap Add function.
// The New controllers which gets added, as to implement Add function to get
// started by the manager.
type ContollerManager interface {
	Add(manager.Manager, Config) error
}

// Config holds the drivername and namespace name.
type Config struct {
	DriverName string
	Namespace  string
}

// ControllerList holds the list of managers need to be started.
var ControllerList []ContollerManager

// addToManager calls the registered managers Add method.
func addToManager(mgr manager.Manager, config Config) error {
	for _, c := range ControllerList {
		err := c.Add(mgr, config)
		if err != nil {
			return err
		}
	}
	return nil
}

// Start will start all the registered managers.
func Start(config Config) error {
	electionID := config.DriverName + "-" + config.Namespace
	opts := manager.Options{
		LeaderElection: true,
		// disable metrics
		MetricsBindAddress:      "0",
		LeaderElectionNamespace: config.Namespace,
		LeaderElectionID:        electionID,
	}
	mgr, err := manager.New(clientConfig.GetConfigOrDie(), opts)
	if err != nil {
		klog.Errorf("failed to create manager %s", err)
		return err
	}
	err = addToManager(mgr, config)
	if err != nil {
		klog.Errorf("failed to add manager %s", err)
		return err
	}
	err = mgr.Start(signals.SetupSignalHandler())
	if err != nil {
		klog.Errorf("failed to start manager %s", err)
	}
	return err
}
