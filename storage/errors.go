package main

import "errors"

var (
	// ErrKeyNotFound is returned when requested key is not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrObjectExists is returned when named volume already exists
	ErrObjectExists = errors.New("object exists")
	// ErrObjectNotFound is returned when named volume is not found
	ErrObjectNotFound = errors.New("object not found")
	// ErrVolumeNotFound is returned when a subvolume is not found in zeroFS.
	ErrVolumeNotFound = errors.New("volume not found")
	// ErrInvalidVolID is returned when a CSI passed VolumeID is not conformant to any known volume ID
	// formats.
	ErrInvalidVolID = errors.New("invalid VolumeID")
)
