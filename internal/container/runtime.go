package container

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// RunParent spawns a re-executed child process inside isolated Linux namespaces.
func RunParent(rootfsPath string, command []string) error {
	args := append([]string{"child", rootfsPath}, command...)
	cmd := exec.Command("/proc/self/exe", args...)

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Isolate Hostname (UTS), Process IDs (PID), and Mounts (NS)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:   syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS,
		Unshareflags: syscall.CLONE_NEWNS,
	}

	return cmd.Run()
}

// RunChild executes inside the newly created namespaces, locks down the filesystem,
// mounts /proc, and executes the target command as PID 1.
func RunChild(rootfsPath string, command []string) error {
	if err := syscall.Sethostname([]byte("raft-container")); err != nil {
		return fmt.Errorf("sethostname failed: %w", err)
	}

	if err := syscall.Mount("", "/", "", syscall.MS_PRIVATE|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("make root private failed: %w", err)
	}

	absRoot, err := filepath.Abs(rootfsPath)
	if err != nil {
		return err
	}
	if err := syscall.Chroot(absRoot); err != nil {
		return fmt.Errorf("chroot failed: %w", err)
	}
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("chdir / failed: %w", err)
	}

	if err := syscall.Mount("proc", "/proc", "proc", 0, ""); err != nil {
		return fmt.Errorf("mount proc failed: %w", err)
	}
	defer syscall.Unmount("/proc", 0)

	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
