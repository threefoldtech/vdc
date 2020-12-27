/*
Copyright 2017 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/container-storage-interface/spec/lib/go/csi"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	klog "k8s.io/klog/v2"
)

// NonBlockingGRPCServer defines Non blocking GRPC server interfaces.
type NonBlockingGRPCServer interface {
	// Start services at the endpoint
	Start(endpoint, hstOptions string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer, metrics bool)
	// Waits for the service to stop
	Wait()
	// Stops the service gracefully
	Stop()
	// Stops the service forcefully
	ForceStop()
}

// NewNonBlockingGRPCServer return non-blocking GRPC.
func NewNonBlockingGRPCServer() NonBlockingGRPCServer {
	return &nonBlockingGRPCServer{}
}

// NonBlocking server.
type nonBlockingGRPCServer struct {
	wg     sync.WaitGroup
	server *grpc.Server
}

// Start start service on endpoint.
func (s *nonBlockingGRPCServer) Start(endpoint, hstOptions string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer, metrics bool) {
	s.wg.Add(1)
	go s.serve(endpoint, hstOptions, ids, cs, ns, metrics)
}

// Wait blocks until the WaitGroup counter.
func (s *nonBlockingGRPCServer) Wait() {
	s.wg.Wait()
}

// GracefulStop stops the gRPC server gracefully.
func (s *nonBlockingGRPCServer) Stop() {
	s.server.GracefulStop()
}

// Stop stops the gRPC server.
func (s *nonBlockingGRPCServer) ForceStop() {
	s.server.Stop()
}

func parseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(strings.ToLower(ep), "unix://") || strings.HasPrefix(strings.ToLower(ep), "tcp://") {
		s := strings.SplitN(ep, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("invalid endpoint: %v", ep)
}

func getReqID(req interface{}) string {
	// if req is nil empty string will be returned
	reqID := ""
	switch r := req.(type) {
	case *csi.CreateVolumeRequest:
		reqID = r.Name

	case *csi.DeleteVolumeRequest:
		reqID = r.VolumeId

	case *csi.CreateSnapshotRequest:
		reqID = r.Name
	case *csi.DeleteSnapshotRequest:
		reqID = r.SnapshotId

	case *csi.ControllerExpandVolumeRequest:
		reqID = r.VolumeId

	case *csi.NodeStageVolumeRequest:
		reqID = r.VolumeId
	case *csi.NodeUnstageVolumeRequest:
		reqID = r.VolumeId

	case *csi.NodePublishVolumeRequest:
		reqID = r.VolumeId
	case *csi.NodeUnpublishVolumeRequest:
		reqID = r.VolumeId

	case *csi.NodeExpandVolumeRequest:
		reqID = r.VolumeId
	}
	return reqID
}

var id uint64

func contextIDInjector(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	atomic.AddUint64(&id, 1)
	ctx = context.WithValue(ctx, CtxKey, id)
	reqID := getReqID(req)
	if reqID != "" {
		ctx = context.WithValue(ctx, ReqID, reqID)
	}
	return handler(ctx, req)
}

func panicHandler(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("panic occurred: %v", r)
			debug.PrintStack()
			err = status.Errorf(codes.Internal, "panic %v", r)
		}
	}()
	return handler(ctx, req)
}
func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.Infof(AddContextToLogMessage(ctx, "GRPC call: %s"), info.FullMethod)
	//klog.Infof(AddContextToLogMessage(ctx, "GRPC request: %s"), protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "GRPC error: %v"), err)
	} else {
		klog.Infof(AddContextToLogMessage(ctx, "GRPC response: %s"), protosanitizer.StripSecrets(resp))
	}
	return resp, err
}
func (s *nonBlockingGRPCServer) serve(endpoint, hstOptions string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer, metrics bool) {
	proto, addr, err := parseEndpoint(endpoint)
	if err != nil {
		klog.Fatal(err.Error())
	}

	if proto == "unix" {
		addr = "/" + addr
		if e := os.Remove(addr); e != nil && !os.IsNotExist(e) {
			klog.Fatalf("Failed to remove %s, error: %s", addr, e.Error())
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		klog.Fatalf("Failed to listen: %v", err)
	}

	middleWare := []grpc.UnaryServerInterceptor{contextIDInjector, logGRPC, panicHandler}
	if metrics {
		middleWare = append(middleWare, grpc_prometheus.UnaryServerInterceptor)
	}
	opts := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(middleWare...),
	}

	server := grpc.NewServer(opts...)
	s.server = server

	if ids != nil {
		csi.RegisterIdentityServer(server, ids)
	}
	if cs != nil {
		csi.RegisterControllerServer(server, cs)
	}
	if ns != nil {
		csi.RegisterNodeServer(server, ns)
	}
	klog.Infof("Listening for connections on address: %#v", listener.Addr())
	/* if metrics {
		ho := strings.Split(hstOptions, ",")
		const expectedHo = 3
		if len(ho) != expectedHo {
			klog.Fatalf("invalid histogram options provided: %v", hstOptions)
		}
		start, e := strconv.ParseFloat(ho[0], 32)
		if e != nil {
			klog.Fatalf("failed to parse histogram start value: %v", e)
		}
		factor, e := strconv.ParseFloat(ho[1], 32)
		if err != nil {
			klog.Fatalf("failed to parse histogram factor value: %v", e)
		}
		count, e := strconv.Atoi(ho[2])
		if err != nil {
			klog.Fatalf("failed to parse histogram count value: %v", e)
		}
		buckets := prometheus.ExponentialBuckets(start, factor, count)
		bktOptios := grpc_prometheus.WithHistogramBuckets(buckets)
		grpc_prometheus.EnableHandlingTimeHistogram(bktOptios)
		grpc_prometheus.Register(server)
	} */
	err = server.Serve(listener)
	if err != nil {
		klog.Fatalf("Failed to server: %v", err)
	}
}
