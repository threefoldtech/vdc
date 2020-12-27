package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/mount"

	"golang.org/x/sys/unix"
)

// NodeServer struct of zorofs  CSI driver with supported methods of CSI
// node server spec.
type NodeServer struct {
	NodeID string
	Type   string

	// topology constraints that this nodeserver will advertise
	topology map[string]string
	// A map storing all volumes with ongoing operations so that additional operations
	// for that same volume (as defined by VolumeID) return an Aborted error
	VolumeLocks *VolumeLocks
}

// NewNodeServer initializes a node server
func NewNodeServer(nodeID, t string, topology map[string]string) *NodeServer {
	return &NodeServer{
		NodeID:   nodeID,
		Type:     t,
		topology: topology,
	}
}

// NodeExpandVolume returns unimplemented response.
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodeGetInfo returns node ID.
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {

	csiTopology := &csi.Topology{
		Segments: ns.topology,
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             ns.NodeID,
		AccessibleTopology: csiTopology,
	}, nil
}

// FsInfo linux returns (available bytes, byte capacity, byte usage, total inodes, inodes free, inode usage, error)
// for the filesystem that path resides upon.
func FsInfo(path string) (int64, int64, int64, int64, int64, int64, error) {
	statfs := &unix.Statfs_t{}
	err := unix.Statfs(path, statfs)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	// Available is blocks available * fragment size
	available := int64(statfs.Bavail) * int64(statfs.Bsize)

	// Capacity is total block count * fragment size
	capacity := int64(statfs.Blocks) * int64(statfs.Bsize)

	// Usage is block being used * fragment size (aka block size).
	usage := (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize)

	inodes := int64(statfs.Files)
	inodesFree := int64(statfs.Ffree)
	inodesUsed := inodes - inodesFree

	return available, capacity, usage, inodes, inodesFree, inodesUsed, nil
}

// NodeGetVolumeStats returns volume stats.
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	var err error
	targetPath := req.GetVolumePath()
	if targetPath == "" {
		err = fmt.Errorf("targetpath %v is empty", targetPath)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	isMnt, err := IsMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.InvalidArgument, "targetpath %s does not exist", targetPath)
		}
		return nil, err
	}
	if !isMnt {
		return nil, status.Errorf(codes.InvalidArgument, "targetpath %s is not mounted", targetPath)
	}

	available, capacity, used, inodes, inodesFree, inodesUsed, err := FsInfo(targetPath)
	if err != nil {
		klog.Errorf("Failed to get filesystem indo: %v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get FsInfo due to error %v", err))
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: available,
				Total:     capacity,
				Used:      used,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: inodesFree,
				Total:     inodes,
				Used:      inodesUsed,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// MountOptionContains checks the opt is present in mountOptions.
func MountOptionContains(mountOptions []string, opt string) bool {
	for _, mnt := range mountOptions {
		if mnt == opt {
			return true
		}
	}
	return false

}

// checkDirExists checks directory  exists or not.
func checkDirExists(p string) bool {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return false
	}
	return true
}

// ValidateNodeStageVolumeRequest validates the node stage request.
func ValidateNodeStageVolumeRequest(req *csi.NodeStageVolumeRequest) error {
	if req.GetVolumeCapability() == nil {
		return status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume ID missing in request")
	}

	if req.GetStagingTargetPath() == "" {
		return status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return status.Error(codes.InvalidArgument, "stage secrets cannot be nil or empty")
	}

	// validate stagingpath exists
	ok := checkDirExists(req.GetStagingTargetPath())
	if !ok {
		return status.Errorf(codes.InvalidArgument, "staging path %s does not exist on node", req.GetStagingTargetPath())
	}
	return nil
}

// ValidateNodeUnstageVolumeRequest validates the node unstage request.
func ValidateNodeUnstageVolumeRequest(req *csi.NodeUnstageVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume ID missing in request")
	}

	if req.GetStagingTargetPath() == "" {
		return status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	return nil
}

// NodeStageVolume mounts the volume to a staging path on the node.
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	var (
		volOptions *volumeOptions
	)
	if err := ValidateNodeStageVolumeRequest(req); err != nil {
		return nil, err
	}

	// Configuration

	stagingTargetPath := req.GetStagingTargetPath()
	volID := req.GetVolumeId()

	if acquired := ns.VolumeLocks.TryAcquire(req.GetVolumeId()); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, req.GetVolumeId())
	}
	defer ns.VolumeLocks.Release(req.GetVolumeId())

	// Check if the volume is already mounted

	isMnt, err := IsMountPoint(stagingTargetPath)

	if err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "stat failed: %v"), err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if isMnt {
		klog.Infof(AddContextToLogMessage(ctx, "zerofs: volume %s is already mounted to %s, skipping"), volID, stagingTargetPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// It's not, mount now
	if err = ns.mount(ctx, volOptions, req); err != nil {
		return nil, err
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully mounted volume %s to %s"), volID, stagingTargetPath)

	return &csi.NodeStageVolumeResponse{}, nil
}

// IsMountPoint checks if the given path is mountpoint or not.
func IsMountPoint(p string) (bool, error) {
	dummyMount := mount.New("")
	notMnt, err := dummyMount.IsLikelyNotMountPoint(p)
	if err != nil {
		return false, status.Error(codes.Internal, err.Error())
	}

	return !notMnt, nil
}

// Mount mounts the source to target path.
func Mount(source, target, fstype string, options []string) error {
	dummyMount := mount.New("")
	return dummyMount.Mount(source, target, fstype, options)
}

func (*NodeServer) mount(ctx context.Context, volOptions *volumeOptions, req *csi.NodeStageVolumeRequest) error {
	stagingTargetPath := req.GetStagingTargetPath()
	volID := req.GetVolumeId()

	m, err := newMounter(volOptions)
	if err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "failed to create mounter for volume %s: %v"), volID, err)
		return status.Error(codes.Internal, err.Error())
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: mounting volume %s with %s"), volID, m.name())

	readOnly := "ro"
	fuseMountOptions := strings.Split(volOptions.FuseMountOptions, ",")

	if req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
		req.VolumeCapability.AccessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
		if !MountOptionContains(strings.Split(volOptions.FuseMountOptions, ","), readOnly) {
			volOptions.FuseMountOptions = MountOptionsAdd(volOptions.FuseMountOptions, readOnly)
			fuseMountOptions = append(fuseMountOptions, readOnly)
		}
	}

	if err = m.mount(ctx, stagingTargetPath, volOptions); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "failed to mount volume %s: %v Check dmesg logs if required."), volID, err)
		return status.Error(codes.Internal, err.Error())
	}
	if !MountOptionContains(fuseMountOptions, readOnly) {
		// #nosec - allow anyone to write inside the stagingtarget path
		err = os.Chmod(stagingTargetPath, 0777)
		if err != nil {
			klog.Errorf(AddContextToLogMessage(ctx, "failed to change stagingtarget path %s permission for volume %s: %v"), stagingTargetPath, volID, err)
			uErr := unmountVolume(ctx, stagingTargetPath)
			if uErr != nil {
				klog.Errorf(AddContextToLogMessage(ctx, "failed to umount stagingtarget path %s for volume %s: %v"), stagingTargetPath, volID, uErr)
			}
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

// ValidateNodePublishVolumeRequest validates the node publish request.
func ValidateNodePublishVolumeRequest(req *csi.NodePublishVolumeRequest) error {
	if req.GetVolumeCapability() == nil {
		return status.Error(codes.InvalidArgument, "volume capability missing in request")
	}

	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume ID missing in request")
	}

	if req.GetTargetPath() == "" {
		return status.Error(codes.InvalidArgument, "target path missing in request")
	}

	if req.GetStagingTargetPath() == "" {
		return status.Error(codes.InvalidArgument, "staging target path missing in request")
	}

	return nil
}

// ValidateNodeUnpublishVolumeRequest validates the node unpublish request.
func ValidateNodeUnpublishVolumeRequest(req *csi.NodeUnpublishVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume ID missing in request")
	}

	if req.GetTargetPath() == "" {
		return status.Error(codes.InvalidArgument, "target path missing in request")
	}

	return nil
}

// ConstructMountOptions returns only unique mount options in slice.
func ConstructMountOptions(mountOptions []string, volCap *csi.VolumeCapability) []string {
	if m := volCap.GetMount(); m != nil {
		hasOption := func(options []string, opt string) bool {
			for _, o := range options {
				if o == opt {
					return true
				}
			}
			return false
		}
		for _, f := range m.MountFlags {
			if !hasOption(mountOptions, f) {
				mountOptions = append(mountOptions, f)
			}
		}
	}
	return mountOptions
}

// CreateMountPoint creates the directory with given path.
func CreateMountPoint(mountPath string) error {
	return os.MkdirAll(mountPath, 0750)
}

func contains(s []string, key string) bool {
	for _, v := range s {
		if v == key {
			return true
		}
	}

	return false
}

// MountOptionsAdd adds the `add` mount options to the `options` and returns a
// new string. In case `add` is already present in the `options`, `add` is not
// added again.
func MountOptionsAdd(options string, add ...string) string {
	opts := strings.Split(options, ",")
	newOpts := []string{}
	// clean original options from empty strings
	for _, opt := range opts {
		if opt != "" {
			newOpts = append(newOpts, opt)
		}
	}

	for _, opt := range add {
		if opt != "" && !contains(newOpts, opt) {
			newOpts = append(newOpts, opt)
		}
	}

	return strings.Join(newOpts, ",")
}

func bindMount(ctx context.Context, from, to string, readOnly bool, mntOptions []string) error {
	mntOptionSli := strings.Join(mntOptions, ",")
	if _, _, err := ExecCommand(ctx, "mount", "-o", mntOptionSli, from, to); err != nil {
		return fmt.Errorf("failed to bind-mount %s to %s: %w", from, to, err)
	}

	if readOnly {
		mntOptionSli = MountOptionsAdd(mntOptionSli, "remount")
		if _, _, err := ExecCommand(ctx, "mount", "-o", mntOptionSli, to); err != nil {
			return fmt.Errorf("failed read-only remount of %s: %w", to, err)
		}
	}

	return nil
}

// NodePublishVolume mounts the volume mounted to the staging path to the target
// path.
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	mountOptions := []string{"bind", "_netdev"}
	if err := ValidateNodePublishVolumeRequest(req); err != nil {
		return nil, err
	}

	targetPath := req.GetTargetPath()
	volID := req.GetVolumeId()

	if acquired := ns.VolumeLocks.TryAcquire(volID); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, volID)
	}
	defer ns.VolumeLocks.Release(volID)

	if err := CreateMountPoint(targetPath); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "failed to create mount point at %s: %v"), targetPath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	mountOptions = ConstructMountOptions(mountOptions, req.GetVolumeCapability())

	// Check if the volume is already mounted

	isMnt, err := IsMountPoint(targetPath)

	if err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "stat failed: %v"), err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	if isMnt {
		klog.Infof(AddContextToLogMessage(ctx, "cephfs: volume %s is already bind-mounted to %s"), volID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// It's not, mount now

	if err = bindMount(ctx, req.GetStagingTargetPath(), req.GetTargetPath(), req.GetReadonly(), mountOptions); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "failed to bind-mount volume %s: %v"), volID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully bind-mounted volume %s to %s"), volID, targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path.
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	var err error
	if err = ValidateNodeUnpublishVolumeRequest(req); err != nil {
		return nil, err
	}

	volID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	if acquired := ns.VolumeLocks.TryAcquire(volID); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, volID)
	}
	defer ns.VolumeLocks.Release(volID)

	// Unmount the bind-mount
	if err = unmountVolume(ctx, targetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = os.Remove(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully unbinded volume %s from %s"), req.GetVolumeId(), targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeUnstageVolume unstages the volume from the staging path.
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	var err error
	if err = ValidateNodeUnstageVolumeRequest(req); err != nil {
		return nil, err
	}

	volID := req.GetVolumeId()
	if acquired := ns.VolumeLocks.TryAcquire(volID); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, volID)
	}
	defer ns.VolumeLocks.Release(volID)

	stagingTargetPath := req.GetStagingTargetPath()
	// Unmount the volume
	if err = unmountVolume(ctx, stagingTargetPath); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully unmounted volume %s from %s"), req.GetVolumeId(), stagingTargetPath)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodeGetCapabilities returns the supported capabilities of the node server.
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}
