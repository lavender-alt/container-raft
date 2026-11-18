package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type LogEntry struct {
	Term    int
	Command string
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type Node struct {
	mu          sync.Mutex
	id          string
	addr        string
	peers       []string
	role        Role
	currentTerm int
	votedFor    string
	log         []LogEntry
	commitIndex int
	lastApplied int

	electionReset time.Time
	listener      net.Listener
	server        *rpc.Server
	dead          bool
	onCommit      func(nodeID string, command string)
}

func NewNode(id, addr string, peers []string, onCommit func(string, string)) *Node {
	return &Node{
		id:            id,
		addr:          addr,
		peers:         peers,
		role:          Follower,
		currentTerm:   0,
		log:           []LogEntry{{Term: 0, Command: ""}}, // 1-indexed dummy entry
		commitIndex:   0,
		lastApplied:   0,
		electionReset: time.Now(),
		onCommit:      onCommit,
	}
}

func (n *Node) Start() error {
	n.server = rpc.NewServer()
	if err := n.server.RegisterName("Node", n); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n.mu.Lock()
			isDead := n.dead
			n.mu.Unlock()
			if isDead {
				conn.Close()
				return
			}
			go n.server.ServeConn(conn)
		}
	}()

	go n.electionLoop()
	return nil
}

// Kill simulates an abrupt node crash by closing its RPC listener and stopping loops.
func (n *Node) Kill() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dead = true
	n.role = Follower
	if n.listener != nil {
		n.listener.Close()
	}
	fmt.Printf("[%s] CRASHED (simulated node failure)\n", n.id)
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.dead && n.role == Leader
}

func (n *Node) GetState() (Role, int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm, n.commitIndex
}

func (n *Node) electionLoop() {
	for {
		timeout := time.Duration(300+rand.Intn(250)) * time.Millisecond
		time.Sleep(50 * time.Millisecond)

		n.mu.Lock()
		if n.dead {
			n.mu.Unlock()
			return
		}
		if n.role != Leader && time.Since(n.electionReset) >= timeout {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionReset = time.Now()
	term := n.currentTerm
	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term
	fmt.Printf("[%s] Starting election for term %d\n", n.id, term)

	votes := 1
	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1

	for _, peer := range n.peers {
		go func(peerAddr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if callRPC(peerAddr, "Node.RequestVote", &args, &reply) {
				n.mu.Lock()
				defer n.mu.Unlock()
				if n.dead || n.role != Candidate || n.currentTerm != term {
					return
				}
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
					return
				}
				if reply.VoteGranted {
					votes++
					if votes >= majority && n.role == Candidate {
						n.role = Leader
						fmt.Printf("[%s] Elected LEADER for term %d!\n", n.id, n.currentTerm)
						go n.heartbeatLoop(n.currentTerm)
					}
				}
			}
		}(peer)
	}
}

func (n *Node) heartbeatLoop(term int) {
	for {
		n.mu.Lock()
		if n.dead || n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		n.replicateToPeers(term)
		time.Sleep(100 * time.Millisecond)
	}
}

// Propose appends a command on the leader and blocks until a strict majority confirms replication.
func (n *Node) Propose(command string) (int, bool) {
	n.mu.Lock()
	if n.dead || n.role != Leader {
		n.mu.Unlock()
		return -1, false
	}

	term := n.currentTerm
	entry := LogEntry{Term: term, Command: command}
	n.log = append(n.log, entry)
	targetIndex := len(n.log) - 1
	totalNodes := len(n.peers) + 1
	majority := totalNodes/2 + 1
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	var wg sync.WaitGroup
	var ackMu sync.Mutex
	acks := 1 // Leader counts as 1 replica

	for _, peer := range peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			n.mu.Lock()
			if n.dead || n.role != Leader || n.currentTerm != term {
				n.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      append([]LogEntry(nil), n.log[1:]...),
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()

			var reply AppendEntriesReply
			if callRPC(peerAddr, "Node.AppendEntries", &args, &reply) {
				n.mu.Lock()
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
					n.mu.Unlock()
					return
				}
				n.mu.Unlock()

				if reply.Success {
					ackMu.Lock()
					acks++
					ackMu.Unlock()
				}
			}
		}(peer)
	}

	// Wait with timeout so crashed followers don't stall quorum
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(350 * time.Millisecond):
	}

	ackMu.Lock()
	quorumReached := acks >= majority
	ackMu.Unlock()

	if !quorumReached {
		return -1, false
	}

	n.mu.Lock()
	if n.dead || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return -1, false
	}
	if targetIndex > n.commitIndex {
		n.commitIndex = targetIndex
	}
	n.applyCommittedLocked()
	n.mu.Unlock()

	// Broadcast updated LeaderCommit immediately so followers apply right away
	n.replicateToPeers(term)
	return targetIndex, true
}

func (n *Node) replicateToPeers(term int) {
	n.mu.Lock()
	if n.dead || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      append([]LogEntry(nil), n.log[1:]...),
		LeaderCommit: n.commitIndex,
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peerAddr string) {
			var reply AppendEntriesReply
			if callRPC(peerAddr, "Node.AppendEntries", &args, &reply) {
				n.mu.Lock()
				defer n.mu.Unlock()
				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
				}
			}
		}(peer)
	}
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dead {
		return fmt.Errorf("node %s is dead", n.id)
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}

	reply.Term = n.currentTerm
	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term

	logUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) && logUpToDate {
		n.votedFor = args.CandidateID
		n.electionReset = time.Now()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.dead {
		return fmt.Errorf("node %s is dead", n.id)
	}

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}
	n.role = Follower
	n.electionReset = time.Now()
	reply.Term = n.currentTerm
	reply.Success = true

	// Synchronize full log state from authoritative leader
	n.log = append([]LogEntry{{Term: 0, Command: ""}}, args.Entries...)

	if args.LeaderCommit > n.commitIndex {
		lastIdx := len(n.log) - 1
		if args.LeaderCommit < lastIdx {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastIdx
		}
		n.applyCommittedLocked()
	}
	return nil
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		cmd := n.log[n.lastApplied].Command
		if n.onCommit != nil {
			go n.onCommit(n.id, cmd)
		}
	}
}

func callRPC(addr, method string, args interface{}, reply interface{}) bool {
	conn, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		done <- client.Call(method, args, reply)
	}()

	select {
	case err := <-done:
		return err == nil
	case <-time.After(200 * time.Millisecond):
		return false
	}
}