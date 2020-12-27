package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	klog "k8s.io/klog/v2"
)

const (
	volumeMounterFuse = "fuse"
)

var (
	availableMounters []string

	// maps a mountpoint to PID of its FUSE daemon
	fusePidMap    = make(map[string]int)
	fusePidMapMtx sync.Mutex

	fusePidRx = regexp.MustCompile(`(?m)^ceph-fuse\[(.+)\]: starting fuse$`)
)

// Load available zerofs mounters installed on system into availableMounters
// Called from driver.go's Run().
func loadAvailableMounters(conf *Config) error {
	fuseMounterProbe := exec.Command("zerofs-fuse", "--version")

	err := fuseMounterProbe.Run()
	if err != nil {
		klog.Errorf("failed to run zerofs-fuse %v", err)
	} else {
		klog.Infof("loaded mounter: %s", volumeMounterFuse)
		availableMounters = append(availableMounters, volumeMounterFuse)
	}

	if len(availableMounters) == 0 {
		return errors.New("no zerofs mounters found on system")
	}

	return nil
}

type volumeMounter interface {
	mount(ctx context.Context, mountPoint string, volOptions *volumeOptions) error
	name() string
}

func newMounter(volOptions *volumeOptions) (volumeMounter, error) {
	// Get the mounter from the configuration

	wantMounter := volOptions.Mounter

	// Verify that it's available

	var chosenMounter string

	for _, availMounter := range availableMounters {
		if availMounter == wantMounter {
			chosenMounter = wantMounter
			break
		}
	}

	if chosenMounter == "" {
		return nil, fmt.Errorf("unknown mounter '%s'", chosenMounter)
	}

	// Create the mounter

	switch chosenMounter {
	case volumeMounterFuse:
		return &fuseMounter{}, nil
	}

	return nil, fmt.Errorf("unknown mounter '%s'", chosenMounter)
}

type fuseMounter struct{}

func mountFuse(ctx context.Context, mountPoint string, volOptions *volumeOptions) error {
	args := []string{
		mountPoint,
	}

	fmo := "nonempty"
	if volOptions.FuseMountOptions != "" {
		fmo += "," + strings.TrimSpace(volOptions.FuseMountOptions)
	}
	args = append(args, "-o", fmo)

	_, stderr, err := ExecCommand(ctx, "zerofs-fuse", args[:]...)
	if err != nil {
		return err
	}

	// Parse the output:
	// We need "starting fuse" meaning the mount is ok
	// and PID of the ceph-fuse daemon for unmount

	match := fusePidRx.FindSubmatch([]byte(stderr))
	// validMatchLength is set to 2 as match is expected
	// to have 2 items, starting fuse and PID of the fuse daemon
	const validMatchLength = 2
	if len(match) != validMatchLength {
		return fmt.Errorf("zerofs-fuse failed: %s", stderr)
	}

	pid, err := strconv.Atoi(string(match[1]))
	if err != nil {
		return fmt.Errorf("failed to parse FUSE daemon PID: %w", err)
	}

	fusePidMapMtx.Lock()
	fusePidMap[mountPoint] = pid
	fusePidMapMtx.Unlock()

	return nil
}

func (m *fuseMounter) mount(ctx context.Context, mountPoint string, volOptions *volumeOptions) error {
	if err := CreateMountPoint(mountPoint); err != nil {
		return err
	}

	return mountFuse(ctx, mountPoint, volOptions)
}

func (m *fuseMounter) name() string { return "Zerofs  FUSE driver" }

// ExecCommand executes passed in program with args and returns separate stdout
// and stderr streams. In case ctx is not set to context.TODO(), the command
// will be logged after it was executed.
func ExecCommand(ctx context.Context, program string, args ...string) (string, string, error) {
	var (
		cmd           = exec.Command(program, args...) // #nosec:G204, commands executing not vulnerable.
		sanitizedArgs = StripSecretInArgs(args)
		stdoutBuf     bytes.Buffer
		stderrBuf     bytes.Buffer
	)

	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()

	if err != nil {
		err = fmt.Errorf("an error (%w) occurred while running %s args: %v", err, program, sanitizedArgs)
		if ctx != context.TODO() {
			klog.Infof(AddContextToLogMessage(ctx, "%s"), err)
		}
		return stdout, stderr, err
	}

	if ctx != context.TODO() {
		klog.Infof(AddContextToLogMessage(ctx, "command succeeded: %s %v"), program, sanitizedArgs)
	}

	return stdout, stderr, nil
}
func unmountVolume(ctx context.Context, mountPoint string) error {
	if _, _, err := ExecCommand(ctx, "umount", mountPoint); err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("exit status 32: umount: %s: not mounted", mountPoint)) ||
			strings.Contains(err.Error(), "No such file or directory") {
			return nil
		}
		return err
	}

	fusePidMapMtx.Lock()
	pid, ok := fusePidMap[mountPoint]
	if ok {
		delete(fusePidMap, mountPoint)
	}
	fusePidMapMtx.Unlock()

	if ok {
		p, err := os.FindProcess(pid)
		if err != nil {
			klog.Warningf(AddContextToLogMessage(ctx, "failed to find process %d: %v"), pid, err)
		} else {
			if _, err = p.Wait(); err != nil {
				klog.Warningf(AddContextToLogMessage(ctx, "%d is not a child process: %v"), pid, err)
			}
		}
	}

	return nil
}
