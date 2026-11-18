# container-raft

A lightweight Linux container runtime and distributed Raft consensus engine built from scratch in Go.

## Architecture
- **Container Runtime (`internal/container`):** Uses Linux kernel namespaces (`CLONE_NEWUTS`, `CLONE_NEWPID`, `CLONE_NEWNS`) and `chroot` over an Alpine Linux root filesystem to isolate hostname, process trees, and mount points.
- **Raft Consensus Engine (`internal/raft`):** Implements leader election (`RequestVote`), randomized election timeouts, heartbeats, and log replication (`AppendEntries`) over `net/rpc`.
- **Cluster Integration (`cmd/main.go`):** Boots a 3-node Raft cluster where committed log entries trigger isolated container execution across nodes.

## Usage
```bash
go build -o engine cmd/main.go
sudo ./engine cluster
