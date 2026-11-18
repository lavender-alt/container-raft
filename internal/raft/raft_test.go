package raft

import (
	"sync"
	"testing"
	"time"
)

func TestQuorumCommitAndLeaderFailover(t *testing.T) {
	addrs := map[string]string{
		"node-1": "127.0.0.1:19001",
		"node-2": "127.0.0.1:19002",
		"node-3": "127.0.0.1:19003",
	}

	var mu sync.Mutex
	applied := map[string][]string{
		"node-1": {},
		"node-2": {},
		"node-3": {},
	}

	nodes := make(map[string]*Node)
	for id, addr := range addrs {
		var peers []string
		for peerID, peerAddr := range addrs {
			if peerID != id {
				peers = append(peers, peerAddr)
			}
		}
		n := NewNode(id, addr, peers, func(nodeID, cmd string) {
			mu.Lock()
			applied[nodeID] = append(applied[nodeID], cmd)
			mu.Unlock()
		})
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start %s: %v", id, err)
		}
		nodes[id] = n
	}
	defer func() {
		for _, n := range nodes {
			n.Kill()
		}
	}()

	// 1. Wait for initial leader election
	leader := waitForLeader(t, nodes, 3*time.Second)
	t.Logf("Initial leader elected: %s", leader.id)

	// 2. Propose command #1 and verify quorum commit across all 3 nodes
	if idx, ok := leader.Propose("deploy-container-alpha"); !ok || idx != 1 {
		t.Fatalf("expected initial proposal to commit at index 1, got idx=%d ok=%v", idx, ok)
	}
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	for id, cmds := range applied {
		if len(cmds) != 1 || cmds[0] != "deploy-container-alpha" {
			t.Fatalf("node %s failed to apply initial command: %v", id, cmds)
		}
	}
	mu.Unlock()

	// 3. CHAOS EVENT: Kill the active leader
	firstLeaderID := leader.id
	leader.Kill()

	// 4. Verify surviving nodes elect a new leader
	newLeader := waitForLeader(t, nodes, 3*time.Second)
	if newLeader.id == firstLeaderID {
		t.Fatalf("expected a new leader after killing %s", firstLeaderID)
	}
	t.Logf("Failover complete! New leader elected: %s", newLeader.id)

	// 5. Propose command #2 with 2/3 nodes alive (quorum = 2)
	if idx, ok := newLeader.Propose("deploy-container-beta"); !ok || idx != 2 {
		t.Fatalf("expected failover proposal to commit at index 2, got idx=%d ok=%v", idx, ok)
	}
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	for id, cmds := range applied {
		if id == firstLeaderID {
			continue
		}
		if len(cmds) != 2 || cmds[1] != "deploy-container-beta" {
			t.Fatalf("surviving node %s did not apply failover command: %v", id, cmds)
		}
	}
	mu.Unlock()

	// 6. CHAOS EVENT 2: Kill a second node (only 1/3 alive -> quorum lost)
	newLeader.Kill()
	time.Sleep(400 * time.Millisecond)

	for id, n := range nodes {
		if id == firstLeaderID || id == newLeader.id {
			continue
		}
		if _, ok := n.Propose("should-fail-no-quorum"); ok {
			t.Fatalf("node %s committed without majority quorum!", id)
		}
	}
	t.Logf("Verified safety invariant: cluster halts commits when majority quorum is lost.")
}

func waitForLeader(t *testing.T, nodes map[string]*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader election")
	return nil
}