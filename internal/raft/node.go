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
	resetTimer  chan struct{}
	onCommit    func(cmd string)
}

func NewNode(id, addr string, peers []string, onCommit func(string)) *Node {
	return &Node{
		id:         id,
		addr:       addr,
		peers:      peers,
		role:       Follower,
		log:        []LogEntry{{Term: 0, Command: ""}},
		resetTimer: make(chan struct{}, 1),
		onCommit:   onCommit,
	}
}

func (n *Node) Start() error {
	server := rpc.NewServer()
	if err := server.Register(n); err != nil {
		return err
	}
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	go server.Accept(l)
	go n.runElectionLoop()
	return nil
}

func (n *Node) runElectionLoop() {
	for {
		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		select {
		case <-n.resetTimer:
			continue
		case <-time.After(timeout):
			n.mu.Lock()
			if n.role != Leader {
				n.startElection()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term
	votes := 1

	fmt.Printf("[%s] Starting election for term %d\n", n.id, term)

	for _, peer := range n.peers {
		go func(peerAddr string) {
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			if err := client.Call("Node.RequestVote", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.role = Follower
				n.votedFor = ""
				return
			}
			if n.role == Candidate && n.currentTerm == term && reply.VoteGranted {
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.role = Leader
					fmt.Printf("[%s] Elected LEADER for term %d!\n", n.id, n.currentTerm)
					go n.sendHeartbeats()
				}
			}
		}(peer)
	}
}

func (n *Node) sendHeartbeats() {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		commitIdx := n.commitIndex
		entries := append([]LogEntry(nil), n.log[1:]...)
		n.mu.Unlock()

		for _, peer := range n.peers {
			go func(peerAddr string) {
				client, err := rpc.Dial("tcp", peerAddr)
				if err != nil {
					return
				}
				defer client.Close()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     n.id,
					Entries:      entries,
					LeaderCommit: commitIdx,
				}
				var reply AppendEntriesReply
				if err := client.Call("Node.AppendEntries", args, &reply); err == nil && reply.Term > term {
					n.mu.Lock()
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
					n.mu.Unlock()
				}
			}(peer)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) Propose(command string) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: command})
	n.commitIndex = len(n.log) - 1
	n.mu.Unlock()

	if n.onCommit != nil {
		n.onCommit(command)
	}
	return true
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}
	reply.Term = n.currentTerm

	lastIdx := len(n.log) - 1
	lastTerm := n.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		select {
		case n.resetTimer <- struct{}{}:
		default:
		}
	}
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	n.currentTerm = args.Term
	n.role = Follower
	select {
	case n.resetTimer <- struct{}{}:
	default:
	}

	n.log = append([]LogEntry{{Term: 0}}, args.Entries...)
	if args.LeaderCommit > n.commitIndex {
		for i := n.commitIndex + 1; i <= args.LeaderCommit && i < len(n.log); i++ {
			if n.onCommit != nil {
				n.onCommit(n.log[i].Command)
			}
		}
		n.commitIndex = args.LeaderCommit
	}

	reply.Term = n.currentTerm
	reply.Success = true
	return nil
}