package main

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/cloud-provider/volume/helpers"

	klog "k8s.io/klog/v2"
)

// ControllerServer struct of CEPH CSI driver with supported methods of CSI
// controller server spec.
type ControllerServer struct {
	Capabilities []*csi.ControllerServiceCapability

	// A map storing all volumes with ongoing operations so that additional operations
	// for that same volume (as defined by VolumeID/volume name) return an Aborted error
	VolumeLocks *VolumeLocks

	// A map storing all snapshots with ongoing operations so that additional operations
	// for that same snapshot (as defined by SnapshotID/snapshot name) return an Aborted error
	SnapshotLocks *VolumeLocks

	// A map storing all volumes/snapshots with ongoing operations.
	OperationLocks *OperationLock
}

// ControllerGetVolume fetch volume information.
func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")

}

// ControllerPublishVolume publish volume on node.
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerUnpublishVolume unpublish on node.
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListVolumes lists volumes.
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// CreateSnapshot creates snapshot.
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// DeleteSnapshot deletes snapshot.
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// GetCapacity get volume capacity.
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListSnapshots lists snapshots.
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// newControllerServiceCapability returns controller capabilities.
func newControllerServiceCapability(ctrlCap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
	return &csi.ControllerServiceCapability{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{
				Type: ctrlCap,
			},
		},
	}
}

// ControllerGetCapabilities implements the default GRPC callout.
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.Capabilities,
	}, nil
}

// NewControllerServer initializes a controller server.
func NewControllerServer() *ControllerServer {
	cl := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	var csc []*csi.ControllerServiceCapability
	for _, c := range cl {
		klog.Info("Enabling controller service capability: %v", c.String())
		csc = append(csc, newControllerServiceCapability(c))
	}
	return &ControllerServer{
		Capabilities:   csc,
		VolumeLocks:    NewVolumeLocks(),
		OperationLocks: NewOperationLock(),
	}
}

// ValidateControllerServiceRequest validates the controller
// plugin capabilities.
func (cs *ControllerServer) ValidateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, capability := range cs.Capabilities {
		if c == capability.GetRpc().GetType() {
			return nil
		}
	}
	return status.Error(codes.InvalidArgument, fmt.Sprintf("%s", c)) //nolint
}

// CheckReadOnlyManyIsSupported checks the request is to create ReadOnlyMany
// volume is from source as empty ReadOnlyMany is not supported.
func CheckReadOnlyManyIsSupported(req *csi.CreateVolumeRequest) error {
	for _, capability := range req.GetVolumeCapabilities() {
		if m := capability.GetAccessMode().Mode; m == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY || m == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY {
			if req.GetVolumeContentSource() == nil {
				return status.Error(codes.InvalidArgument, "readOnly accessMode is supported only with content source")
			}
		}
	}
	return nil
}

// Controller service request validation.
func (cs *ControllerServer) validateCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	if err := cs.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		return fmt.Errorf("invalid CreateVolumeRequest: %w", err)
	}

	if req.GetName() == "" {
		return status.Error(codes.InvalidArgument, "volume Name cannot be empty")
	}

	reqCaps := req.GetVolumeCapabilities()
	if reqCaps == nil {
		return status.Error(codes.InvalidArgument, "volume Capabilities cannot be empty")
	}

	for _, capability := range reqCaps {
		if capability.GetBlock() != nil {
			return status.Error(codes.Unimplemented, "block volume not supported")
		}
	}

	// Allow readonly access mode for volume with content source
	err := CheckReadOnlyManyIsSupported(req)
	if err != nil {
		return err
	}

	return nil
}

func (cs *ControllerServer) validateDeleteVolumeRequest() error {
	if err := cs.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		return fmt.Errorf("invalid DeleteVolumeRequest: %w", err)
	}

	return nil
}

// Controller expand volume request validation.
func (cs *ControllerServer) validateExpandVolumeRequest(req *csi.ControllerExpandVolumeRequest) error {
	if err := cs.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return fmt.Errorf("invalid ExpandVolumeRequest: %w", err)
	}

	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return status.Error(codes.InvalidArgument, "CapacityRange cannot be empty")
	}

	return nil
}

// RoundOffBytes converts roundoff the size
// 1.1Mib will be round off to 2Mib same for GiB
// size less than 1MiB will be round off to 1MiB.
func RoundOffBytes(bytes int64) int64 {
	var num int64
	floatBytes := float64(bytes)
	// round off the value if its in decimal
	if floatBytes < helpers.GiB {
		num = int64(math.Ceil(floatBytes / helpers.MiB))
		num *= helpers.MiB
	} else {
		num = int64(math.Ceil(floatBytes / helpers.GiB))
		num *= helpers.GiB
	}
	return num
}

// CreateVolume creates a reservation and the volume in backend, if it is not already present.
// nolint:gocognit:gocyclo // TODO: reduce complexity
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := cs.validateCreateVolumeRequest(req); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "CreateVolumeRequest validation failed: %v"), err)
		return nil, err
	}

	// Configuration
	secret := req.GetSecrets()
	requestName := req.GetName()

	// Existence and conflict checks
	if acquired := cs.VolumeLocks.TryAcquire(requestName); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), requestName)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, requestName)
	}
	defer cs.VolumeLocks.Release(requestName)

	volOptions, err := NewVolumeOptions(ctx, requestName, req)
	if err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "validation and extraction of volume options failed: %v"), err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer volOptions.Destroy()

	if req.GetCapacityRange() != nil {
		volOptions.Size = RoundOffBytes(req.GetCapacityRange().GetRequiredBytes())
	}

	// TODO return error message if requested vol size is too bigg

	// Reservation
	vID, err := reserveVol(ctx, volOptions, secret)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	defer func() {
		if err != nil {
			errDefer := undoVolReservation(ctx, volOptions, *vID, secret)
			if errDefer != nil {
				klog.Warningf(AddContextToLogMessage(ctx, "failed undoing reservation of volume: %s (%s)"),
					requestName, errDefer)

			}
		}
	}()

	// Create a volume
	err = createVolume(ctx, volOptions, vID.FsSubvolName, volOptions.Size)
	if err != nil {
		return nil, err
	}
	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully created backing volume named %s for request name %s"), vID.FsSubvolName, requestName)
	volumeContext := req.GetParameters()
	volumeContext["subvolumeName"] = vID.FsSubvolName
	volume := &csi.Volume{
		VolumeId:      vID.VolumeID,
		CapacityBytes: volOptions.Size,
		ContentSource: req.GetVolumeContentSource(),
		VolumeContext: volumeContext,
	}
	if volOptions.Topology != nil {
		volume.AccessibleTopology =
			[]*csi.Topology{
				{
					Segments: volOptions.Topology,
				},
			}
	}
	return &csi.CreateVolumeResponse{Volume: volume}, nil
}

// DeleteVolume deletes the volume in backend and its reservation.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if err := cs.validateDeleteVolumeRequest(); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "DeleteVolumeRequest validation failed: %v"), err)
		return nil, err
	}

	volID := req.GetVolumeId()
	secrets := req.GetSecrets()

	// lock out parallel delete operations
	if acquired := cs.VolumeLocks.TryAcquire(string(volID)); !acquired {
		klog.Errorf(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, string(volID))
	}
	defer cs.VolumeLocks.Release(string(volID))

	// lock out volumeID for clone and expand operation
	if err := cs.OperationLocks.GetDeleteLock(req.GetVolumeId()); err != nil {
		klog.Error(AddContextToLogMessage(ctx, err.Error()))
		return nil, status.Error(codes.Aborted, err.Error())
	}
	defer cs.OperationLocks.ReleaseDeleteLock(req.GetVolumeId())

	// Find the volume using the provided VolumeID
	volOptions, vID, err := newVolumeOptionsFromVolID(ctx, string(volID), nil, secrets)
	if err != nil {
		// if error is ErrKeyNotFound, then a previous attempt at deletion was complete
		// or partially complete (subvolume is garbage collected already), hence
		// return success as deletion is complete
		if errors.Is(err, ErrKeyNotFound) {
			return &csi.DeleteVolumeResponse{}, nil
		}

		klog.Errorf(AddContextToLogMessage(ctx, "Error returned from newVolumeOptionsFromVolID: %v"), err)

		// All errors other than ErrVolumeNotFound should return an error back to the caller
		if !errors.Is(err, ErrVolumeNotFound) {
			return nil, status.Error(codes.Internal, err.Error())
		}

		return &csi.DeleteVolumeResponse{}, nil
	}
	defer volOptions.Destroy()

	// lock out parallel delete and create requests against the same volume name as we
	// cleanup the subvolume and associated omaps for the same
	if acquired := cs.VolumeLocks.TryAcquire(volOptions.RequestName); !acquired {
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, volOptions.RequestName)
	}
	defer cs.VolumeLocks.Release(volOptions.RequestName)

	if err = volOptions.purgeVolume(ctx, vID.FsSubvolName, false); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "failed to delete volume %s: %v"), volID, err)

		if !errors.Is(err, ErrVolumeNotFound) {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if err := undoVolReservation(ctx, volOptions, *vID, secrets); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof(AddContextToLogMessage(ctx, "zerofs: successfully deleted volume %s"), volID)

	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	// zerofsfs doesn't support Block volumes
	for _, capability := range req.VolumeCapabilities {
		if capability.GetBlock() != nil {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

// ControllerExpandVolume expands CephFS Volumes on demand based on resizer request.
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if err := cs.validateExpandVolumeRequest(req); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, "ControllerExpandVolumeRequest validation failed: %v"), err)
		return nil, err
	}

	volID := req.GetVolumeId()
	_ = req.GetSecrets()

	// lock out parallel delete operations
	if acquired := cs.VolumeLocks.TryAcquire(volID); !acquired {
		klog.Error(AddContextToLogMessage(ctx, VolumeOperationAlreadyExistsFmt), volID)
		return nil, status.Errorf(codes.Aborted, VolumeOperationAlreadyExistsFmt, volID)
	}
	defer cs.VolumeLocks.Release(volID)

	// lock out volumeID for clone and delete operation
	if err := cs.OperationLocks.GetExpandLock(volID); err != nil {
		klog.Errorf(AddContextToLogMessage(ctx, err.Error()))
		return nil, status.Error(codes.Aborted, err.Error())
	}
	defer cs.OperationLocks.ReleaseExpandLock(volID)

	RoundOffSize := RoundOffBytes(req.GetCapacityRange().GetRequiredBytes())
	klog.Info("Volume expansion requested at controller")

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         RoundOffSize,
		NodeExpansionRequired: false,
	}, nil
}
