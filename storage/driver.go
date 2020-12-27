package main

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
)

const (
	// VolumeOperationAlreadyExistsFmt string format to return for concurrent operation
	VolumeOperationAlreadyExistsFmt = "an operation with the given Volume ID %s already exists"

	// SnapshotOperationAlreadyExistsFmt string format to return for concurrent operation
	SnapshotOperationAlreadyExistsFmt = "an operation with the given Snapshot ID %s already exists"
)

// Driver contains the default identity,node and controller struct.
type Driver struct {
	is *IdentityServer
	ns *NodeServer
	cs *ControllerServer
}

// NewDriver returns new ceph driver.
func NewDriver() *Driver {
	return &Driver{}
}
func k8sGetNodeLabels(nodeName string) (map[string]string, error) {
	client := NewK8sClient()
	node, err := client.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get node %q information: %w", nodeName, err)
	}

	return node.GetLabels(), nil
}

const (
	keySeparator   rune   = '/'
	labelSeparator string = ","
)

// GetTopologyFromDomainLabels returns the CSI topology map, determined from
// the domain labels and their values from the CO system
// Expects domainLabels in arg to be in the format "[prefix/]<name>,[prefix/]<name>,...",.
func GetTopologyFromDomainLabels(domainLabels, nodeName, driverName string) (map[string]string, error) {
	if domainLabels == "" {
		return nil, nil
	}

	// size checks on domain label prefix
	topologyPrefix := strings.ToLower("topology." + driverName)
	const lenLimit = 63
	if len(topologyPrefix) > lenLimit {
		return nil, fmt.Errorf("computed topology label prefix %q for node exceeds length limits", topologyPrefix)
	}
	// driverName is validated, and we are adding a lowercase "topology." to it, so no validation for conformance

	// Convert passed in labels to a map, and check for uniqueness
	labelsToRead := strings.SplitN(domainLabels, labelSeparator, -1)
	klog.Infof("passed in node labels for processing: %+v", labelsToRead)

	labelsIn := make(map[string]bool)
	labelCount := 0
	for _, label := range labelsToRead {
		// as we read the labels from k8s, and check for missing labels,
		// no label conformance checks here
		if _, ok := labelsIn[label]; ok {
			return nil, fmt.Errorf("duplicate label %q found in domain labels", label)
		}

		labelsIn[label] = true
		labelCount++
	}

	nodeLabels, err := k8sGetNodeLabels(nodeName)
	if err != nil {
		return nil, err
	}

	// Determine values for requested labels from node labels
	domainMap := make(map[string]string)
	found := 0
	for key, value := range nodeLabels {
		if _, ok := labelsIn[key]; !ok {
			continue
		}
		// label found split name component and store value
		nameIdx := strings.IndexRune(key, keySeparator)
		domain := key[nameIdx+1:]
		domainMap[domain] = value
		labelsIn[key] = false
		found++
	}

	// Ensure all labels are found
	if found != labelCount {
		missingLabels := []string{}
		for key, missing := range labelsIn {
			if missing {
				missingLabels = append(missingLabels, key)
			}
		}
		return nil, fmt.Errorf("missing domain labels %v on node %q", missingLabels, nodeName)
	}

	klog.Infof("list of domains processed: %+v", domainMap)

	topology := make(map[string]string)
	for domain, value := range domainMap {
		topology[topologyPrefix+"/"+domain] = value
		// TODO: when implementing domain takeover/giveback, enable a domain value that can remain pinned to the node
		// topology["topology."+driverName+"/"+domain+"-pinned"] = value
	}

	return topology, nil
}

// Run start a non-blocking grpc controller,node and identityserver for
// ceph CSI driver which can serve multiple parallel requests.
func (fs *Driver) Run(conf *Config) {
	var err error
	var topology map[string]string

	// Configuration
	if err = loadAvailableMounters(conf); err != nil {
		klog.Fatalf("zerofs: failed to load zerofs mounters: %v", err)
	}

	// Create gRPC servers

	fs.is = NewIdentityServer(conf.DriverName, DriverVersion)

	if conf.IsNodeServer {
		topology, err = GetTopologyFromDomainLabels(conf.DomainLabels, conf.NodeID, conf.DriverName)
		if err != nil {
			klog.Fatal(err.Error())
		}
		fs.ns = NewNodeServer(conf.NodeID, conf.Vtype, topology)
	}

	if conf.IsControllerServer {
		fs.cs = NewControllerServer()
	}
	if !conf.IsControllerServer && !conf.IsNodeServer {
		topology, err = GetTopologyFromDomainLabels(conf.DomainLabels, conf.NodeID, conf.DriverName)
		if err != nil {
			klog.Fatal(err.Error())
		}
		fs.ns = NewNodeServer(conf.NodeID, conf.Vtype, topology)
		fs.cs = NewControllerServer()
	}

	server := NewNonBlockingGRPCServer()
	server.Start(conf.Endpoint, conf.HistogramOption, fs.is, fs.cs, fs.ns, false)
	server.Wait()
}
