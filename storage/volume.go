package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	klog "k8s.io/klog/v2"
)

type volumeOptions struct {
	Topology    map[string]string
	RequestName string
	NamePrefix  string
	Size        int64
	// ReservedID represents the ID reserved for a subvolume
	ReservedID       string
	Mounter          string `json:"mounter"`
	ProvisionVolume  bool   `json:"provisionVolume"`
	FuseMountOptions string `json:"fuseMountOptions"`
	SubvolumeGroup   string
	Features         []string
}

// newVolumeOptions generates a new instance of volumeOptions from the provided
// CSI request parameters.
func NewVolumeOptions(ctx context.Context, requestName string, req *csi.CreateVolumeRequest) (*volumeOptions, error) {
	var (
		opts volumeOptions
	)

	_ = req.GetParameters()

	//TODO: actual implementation

	opts.RequestName = requestName

	opts.ProvisionVolume = true

	return &opts, nil
}

// Destroy cleans up the volume object and releases any connections
func (vo *volumeOptions) Destroy() {

}

// volumeIdentifier structure contains an association between the CSI VolumeID to its subvolume
// name on the backing zeroFS instance.
type volumeIdentifier struct {
	FsSubvolName string
	VolumeID     string
}

// This maximum comes from the CSI spec on max bytes allowed in the various CSI ID fields.
const maxVolIDLen = 128

const (
	uuidSize = 36
)

/*
CSIIdentifier contains the elements that form a CSI ID to be returned by the CSI plugin.

The CSIIdentifier structure carries the following fields,
- EncodingVersion: Carries the version number of the encoding scheme used to encode the CSI ID,
  and is preserved for any future proofing w.r.t changes in the encoding scheme, and to retain
  ability to parse backward compatible encodings.
- ObjectUUID: Is the on-disk uuid of the object (image/snapshot) name, for the CSI volume that
  corresponds to this CSI ID.
*/
type CSIIdentifier struct {
	EncodingVersion uint16
	ObjectUUID      string
}

/*
ComposeCSIID composes a CSI ID from passed in parameters.
Version 1 of the encoding scheme is as follows,
	[csi_id_version=1:4byte] + [-:1byte]
	[ObjectUUID:36bytes]
*/
func (ci CSIIdentifier) ComposeCSIID() (string, error) {
	buf16 := make([]byte, 2)

	if len(ci.ObjectUUID) != uuidSize {
		return "", errors.New("CSI ID invalid object uuid")
	}

	binary.BigEndian.PutUint16(buf16, ci.EncodingVersion)
	versionEncodedHex := hex.EncodeToString(buf16)

	return strings.Join([]string{versionEncodedHex, ci.ObjectUUID}, "-"), nil
}

/*
DecomposeCSIID composes a CSIIdentifier from passed in string.
*/
func (ci *CSIIdentifier) DecomposeCSIID(composedCSIID string) (err error) {
	bytesToProcess := uint16(len(composedCSIID))

	buf16, err := hex.DecodeString(composedCSIID[0:4])
	if err != nil {
		return err
	}
	ci.EncodingVersion = binary.BigEndian.Uint16(buf16)
	// 4 for version encoding and 1 for '-' separator
	bytesToProcess -= 5

	nextFieldStartIdx := 6

	// has to be an exact match
	if bytesToProcess != uuidSize {
		return errors.New("failed to decode CSI identifier, string size mismatch")
	}
	ci.ObjectUUID = composedCSIID[nextFieldStartIdx : nextFieldStartIdx+uuidSize]

	return err
}

// newVolumeOptionsFromVolID generates a new instance of volumeOptions and volumeIdentifier
// from the provided CSI VolumeID.
func newVolumeOptionsFromVolID(ctx context.Context, volID string, volOpt, secrets map[string]string) (*volumeOptions, *volumeIdentifier, error) {
	var (
		vi         CSIIdentifier
		volOptions volumeOptions
		vid        volumeIdentifier
	)

	// Decode the VolID first, to detect older volumes or pre-provisioned volumes
	// before other errors
	err := vi.DecomposeCSIID(volID)
	if err != nil {
		err = fmt.Errorf("error decoding volume ID (%s): %w", volID, err)
		return nil, nil, ErrInvalidVolID
	}
	vid.VolumeID = volID

	// in case of an error, volOptions is not returned, release any
	// resources that may have been allocated
	defer func() {
		if err != nil {
			volOptions.Destroy()
		}
	}()

	if volOpt != nil {

		if err = extractOptionalOption(&volOptions.FuseMountOptions, "fuseMountOptions", volOpt); err != nil {
			return nil, nil, err
		}

		if err = extractMounter(&volOptions.Mounter, volOpt); err != nil {
			return nil, nil, err
		}
	}

	volOptions.ProvisionVolume = true

	return &volOptions, &vid, err
}

// reserveVol is a helper routine to request a UUID reservation for the CSI VolumeName and,
// to generate the volume identifier for the reserved UUID.
func reserveVol(ctx context.Context, volOptions *volumeOptions, secret map[string]string) (*volumeIdentifier, error) {
	var (
		vid volumeIdentifier
	)

	// TODO: generate the volume ID to return to the CO system
	klog.Infof("reserveVol: TODO: generate the volume ID to return to the CO system")

	return &vid, nil
}

// undoVolReservation is a helper routine to undo a name reservation for a CSI VolumeName.
func undoVolReservation(ctx context.Context, volOptions *volumeOptions, vid volumeIdentifier, secret map[string]string) error {

	//TODO: implement
	return nil
}

func (vo *volumeOptions) purgeVolume(ctx context.Context, volID string, force bool) error {

	klog.Infof("PurgeVolume requested of volume %v --TO IMPLEMENT", volID)

	return nil
}

func createVolume(ctx context.Context, volOptions *volumeOptions, volID string, bytesQuota int64) error {

	klog.Infof("createVolume requested of volume %v --TO IMPLEMENT", volID)

	return nil
}

func validateNonEmptyField(field, fieldName string) error {
	if field == "" {
		return fmt.Errorf("parameter '%s' cannot be empty", fieldName)
	}

	return nil
}

func extractOptionalOption(dest *string, optionLabel string, options map[string]string) error {
	opt, ok := options[optionLabel]
	if !ok {
		// Option not found, no error as it is optional
		return nil
	}

	if err := validateNonEmptyField(opt, optionLabel); err != nil {
		return err
	}

	*dest = opt
	return nil
}
func extractOption(dest *string, optionLabel string, options map[string]string) error {
	opt, ok := options[optionLabel]
	if !ok {
		return fmt.Errorf("missing required field %s", optionLabel)
	}

	if err := validateNonEmptyField(opt, optionLabel); err != nil {
		return err
	}

	*dest = opt
	return nil
}

func extractMounter(dest *string, options map[string]string) error {
	if err := extractOptionalOption(dest, "mounter", options); err != nil {
		return err
	}

	if *dest != "" {
		if err := validateMounter(*dest); err != nil {
			return err
		}
	}

	return nil
}

func validateMounter(m string) error {
	switch m {
	case volumeMounterFuse:
	default:
		return fmt.Errorf("unknown mounter '%s'. Valid options are 'fuse' and 'kernel'", m)
	}

	return nil
}
