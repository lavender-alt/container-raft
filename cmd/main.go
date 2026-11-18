package main

import (
	"container-raft/internal/container"
	"container-raft/internal/raft"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./engine <run|cluster> [args]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		if len(os.Args) < 3 {
			fmt.Println("Specify a command to run inside the container (e.g., /bin/sh)")
			os.Exit(1)
		}
		if err := container.RunParent("./rootfs", os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Container error: %d\n", err)
			os.Exit(1)
		}
	case "child":
		if len(os.Args) < 4 {
			os.Exit(1)
		}
		if err := container.RunChild(os.Args[2], os.Args[3:]); err != nil {
			fmt.Fprintf(os.Stderr, "Child error: %v\n", err)
			os.Exit(1)
		}
	case "cluster":
		runClusterDemo()
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

func runClusterDemo() {
	addrs := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"}
	nodes := make([]*raft.Node, 3)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		var peers []string
		for j, a := range addrs {
			if i != j {
				peers = append(peers, a)
			}
		}
		nodeID := id
		nodes[i] = raft.NewNode(nodeID, addrs[i], peers, func(cmd string) {
			fmt.Printf("[%s] COMMITTED -> spawning isolated container for: %q\n,n", nodeID, cmd)
			if strings.HasPrefix(cmd, "/bin/") {
				_ = container.RunParent("./rootfs", []string{"/bin/sh", "-c", strings.TrimPrefix(cmd, "/bin/sh -c ")})
			}
		})
		if err := nodes[i].Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start %s: %d\n", id, err)
			os.Exit(1)
		}
	}

	time.Sleep(1 * time.Second)

	for _, n := range nodes {
		if n.Propose("/bin/sh -c echo 'Running inside '$(hostname) ' as PID '$$") {
			break
		}
	}

	time.Sleep(1 * time.Second)
}
