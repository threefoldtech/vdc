package main

import (
	"os"

	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	klog "k8s.io/klog/v2"
)

// NewK8sClient create kubernetes client.
func NewK8sClient() *k8s.Clientset {
	var cfg *rest.Config
	var err error
	cPath := os.Getenv("KUBERNETES_CONFIG_PATH")
	if cPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", cPath)
		if err != nil {
			klog.Fatalf("Failed to get cluster config with error: %v\n", err)
		}
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Failed to get cluster config with error: %v\n", err)
		}
	}
	client, err := k8s.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Failed to create client with error: %v\n", err)
	}
	return client
}
