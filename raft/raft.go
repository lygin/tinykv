// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// current value of election interval
	currentElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, confState, _ := c.Storage.InitialState()
	lastIndex, _ := c.Storage.LastIndex()
	term, _ := c.Storage.Term(lastIndex)
	raft := &Raft{
		id:               c.ID,
		Term:             max(hardState.GetTerm(), term),
		Vote:             hardState.GetVote(),
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	raft.currentElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	peers := c.peers
	if peers == nil {
		peers = confState.Nodes
	}
	for _, peer := range peers {
		if peer == c.ID {
			raft.Prs[peer] = &Progress{
				Match: lastIndex,
				Next:  lastIndex + 1,
			}
		} else {
			raft.Prs[peer] = &Progress{
				Match: 0,
				Next:  lastIndex + 1,
			}
		}
	}
	return raft
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, _ := r.RaftLog.storage.Snapshot()
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	nextIndex := r.Prs[to].Next
	if nextIndex < r.RaftLog.FirstIndex() {
		r.sendSnapshot(to)
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0, lastIndex-nextIndex+1)
	for i := nextIndex; i <= lastIndex; i++ {
		entries = append(entries, &r.RaftLog.entries[r.RaftLog.idxEntries(i)])
	}
	logTerm, _ := r.RaftLog.Term(nextIndex - 1)
	//fmt.Println(r.id, to, r.Term, nextIndex, len(entries))
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   nextIndex - 1,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	//fmt.Println(r.id, to, r.Term)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.currentElectionTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.currentElectionTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.currentElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.currentElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term = r.Term + 1
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	})
	lastIndex := r.RaftLog.LastIndex()
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.Prs[peer].Next = lastIndex
		} else {
			r.Prs[peer].Next = lastIndex + 1
			r.Prs[peer].Match = lastIndex
		}
	}
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

func (r *Raft) bcastAppend() {
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.becomeFollower(m.Term, None)
		} else {
			r.becomeFollower(m.Term, m.From)
		}
	}
	switch r.State {
	case StateFollower:
		r.stepStateFollower(m)
	case StateCandidate:
		r.stepStateCandidate(m)
	case StateLeader:
		r.stepStateLeader(m)
	}
	return nil
}

func (r *Raft) stepStateFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		m.From = r.id
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}
}

func (r *Raft) stepStateCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepStateLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgPropose:
		r.appendEntry(m)
	}
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	for peer, _ := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//fmt.Println(r.id, m.From, r.Term, m.Term, r.Vote, m.LogTerm, m.Index, r.RaftLog.LastIndex(), r.RaftLog.)
	if m.Term < r.Term || r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
	} else {
		lastTerm := r.RaftLog.LastTerm()
		lastIndex := r.RaftLog.LastIndex()
		if lastTerm < m.LogTerm || lastTerm == m.LogTerm && lastIndex <= m.Index {
			r.Vote = m.From
			r.sendRequestVoteResponse(m.From, false)
		} else {
			r.sendRequestVoteResponse(m.From, true)
		}
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	cnt := 0
	for _, val := range r.votes {
		if val {
			cnt++
		}
	}
	if cnt > len(r.Prs)/2 {
		r.becomeLeader()
	} else if len(r.votes) - cnt > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.Prs[m.From].Next = m.Index + 1
	if m.Reject {
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = m.Index
	len := len(r.Prs)
	commitIndex := make(uint64Slice, 0, len)
	for _, pr := range r.Prs {
		commitIndex = append(commitIndex, pr.Match)
	}
	sort.Sort(commitIndex)
	term, err := r.RaftLog.Term(commitIndex[(len-1)/2])
	if commitIndex[(len-1)/2] > r.RaftLog.committed && (term == r.Term || err == ErrCompacted) {
		r.RaftLog.committed = commitIndex[(len-1)/2]
		r.bcastAppend()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None)
		return
	}
	r.Lead = m.From
	if r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	}
	term, err := r.RaftLog.Term(m.Index)
	entriesIndex := r.RaftLog.idxEntries(m.Index)

	if err == ErrUnavailable {
		r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
		return
	}
	if err != ErrCompacted && term != m.LogTerm {
		var idx uint64
		if entriesIndex < 0 {
			idx = r.RaftLog.headEntry.Index
		} else {
			i := sort.Search(entriesIndex+1, func(i int) bool {
				return r.RaftLog.entries[i].Term == term
			})
			idx = r.RaftLog.entries[i].Index - 1
		}
		r.sendAppendResponse(m.From, true, idx)
		return
	}
	entries := make([]pb.Entry, 0, entriesIndex+len(m.Entries)+1)
	entries = append(entries, r.RaftLog.entries[0:entriesIndex+1]...)
	flag := true
	i := -entriesIndex - 1
	if i < 0 {
		i = 0
	}
	for ; i < len(m.Entries); i++ {
		entries = append(entries, *m.Entries[i])
		iTerm, err := r.RaftLog.Term(m.Entries[i].Index)
		if err == ErrUnavailable || iTerm != m.Entries[i].Term {
			r.RaftLog.stabled = min(r.RaftLog.stabled, m.Entries[i].Index - 1)
			flag = false
		}
	}
	if flag {
		i = entriesIndex+1+len(m.Entries)
		if i < 0 {
			i = 0
		}
		entries = append(entries, r.RaftLog.entries[i:]...)
	}
	r.RaftLog.entries = entries
	r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(m.Entries)))
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true, None)
	}
	if r.State != StateFollower {
		r.becomeFollower(m.Term, m.From)
	} else {
		r.Lead = m.From
		r.electionElapsed = 0
	}
	r.sendHeartbeatResponse(m.From, false, r.RaftLog.LastIndex())
}

func (r *Raft) appendEntry(m pb.Message) {
	index := r.RaftLog.LastIndex()
	for i := 0; i < len(m.Entries); i++ {
		entry := *m.Entries[i]
		index++
		entry.Term = r.Term
		entry.Index = index
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

func (r *Raft) getHardState() pb.HardState {
	return pb.HardState{
		Term:                 r.Term,
		Vote:                 r.Vote,
		Commit:               r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
