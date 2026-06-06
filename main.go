package main

import (
	"fmt"
	"sync"
)

// DelayedOperation represents a delayed operation in the purgatory
type DelayedOperation struct {
	completed bool
	onComplete func()
	lock      sync.Mutex
}

func NewDelayedOperation(onComplete func()) *DelayedOperation {
	return &DelayedOperation{
		onComplete: onComplete,
	}
}

func (o *DelayedOperation) ForceComplete() bool {
	o.lock.Lock()
	if o.completed {
		o.lock.Unlock()
		return false
	}
	o.completed = true
	o.lock.Unlock()
	if o.onComplete != nil {
		o.onComplete()
	}
	return true
}

// MemberMetadata holds the state of a consumer group member
type MemberMetadata struct {
	ID                 string
	GroupID            string
	Assignment         []byte
	SupportedProtocols []string
	PendingHeartbeat   *DelayedOperation
}

func NewMemberMetadata(id, groupID string, protocols []string) *MemberMetadata {
	return &MemberMetadata{
		ID:                 id,
		GroupID:            groupID,
		SupportedProtocols: protocols,
	}
}

func (m *MemberMetadata) Clear() {
	m.Assignment = nil
	m.SupportedProtocols = nil
	if m.PendingHeartbeat != nil {
		m.PendingHeartbeat.ForceComplete()
		m.PendingHeartbeat = nil
	}
}

// GroupState represents the state of a consumer group
type GroupState string

const (
	Empty              GroupState = "Empty"
	PreparingRebalance GroupState = "PreparingRebalance"
	Stable             GroupState = "Stable"
	Dead               GroupState = "Dead"
)

// GroupMetadata holds the state of a consumer group
type GroupMetadata struct {
	ID          string
	State       GroupState
	Members     map[string]*MemberMetadata
	PendingJoin *DelayedOperation
	lock        sync.Mutex
}

func NewGroupMetadata(id string) *GroupMetadata {
	return &GroupMetadata{
		ID:      id,
		State:   Empty,
		Members: make(map[string]*MemberMetadata),
	}
}

func (g *GroupMetadata) TransitionTo(state GroupState) {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.State = state
	if state == Dead {
		for _, member := range g.Members {
			member.Clear()
		}
		g.Members = make(map[string]*MemberMetadata)
		if g.PendingJoin != nil {
			g.PendingJoin.ForceComplete()
			g.PendingJoin = nil
		}
	} else if state == Empty {
		if g.PendingJoin != nil {
			g.PendingJoin.ForceComplete()
			g.PendingJoin = nil
		}
	}
}

func (g *GroupMetadata) Remove(memberID string) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if member, exists := g.Members[memberID]; exists {
		member.Clear()
		delete(g.Members, memberID)
	}
}

// Purgatory manages delayed operations
type Purgatory struct {
	operations map[string][]*DelayedOperation
	lock       sync.Mutex
}

func NewPurgatory() *Purgatory {
	return &Purgatory{
		operations: make(map[string][]*DelayedOperation),
	}
}

func (p *Purgatory) Watch(key string, op *DelayedOperation) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.operations[key] = append(p.operations[key], op)
}

func (p *Purgatory) Purge() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for key, ops := range p.operations {
		var active []*DelayedOperation
		for _, op := range ops {
			op.lock.Lock()
			completed := op.completed
			op.lock.Unlock()
			if !completed {
				active = append(active, op)
			}
		}
		if len(active) == 0 {
			delete(p.operations, key)
		} else {
			p.operations[key] = active
		}
	}
}

func (p *Purgatory) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	size := 0
	for _, ops := range p.operations {
		size += len(ops)
	}
	return size
}

// GroupCoordinator coordinates group rebalances and heartbeats
type GroupCoordinator struct {
	Groups             map[string]*GroupMetadata
	JoinPurgatory      *Purgatory
	HeartbeatPurgatory *Purgatory
	lock               sync.Mutex
}

func NewGroupCoordinator() *GroupCoordinator {
	return &GroupCoordinator{
		Groups:             make(map[string]*GroupMetadata),
		JoinPurgatory:      NewPurgatory(),
		HeartbeatPurgatory: NewPurgatory(),
	}
}

func (c *GroupCoordinator) GetGroup(groupID string) *GroupMetadata {
	c.lock.Lock()
	defer c.lock.Unlock()
	if group, exists := c.Groups[groupID]; exists {
		return group
	}
	group := NewGroupMetadata(groupID)
	c.Groups[groupID] = group
	return group
}

func (c *GroupCoordinator) PrepareRebalance(group *GroupMetadata) {
	group.lock.Lock()
	if group.PendingJoin != nil {
		group.PendingJoin.ForceComplete()
	}
	
	delayedJoin := NewDelayedOperation(func() {
		group.lock.Lock()
		if group.PendingJoin != nil && group.PendingJoin.completed {
			group.PendingJoin = nil
		}
		group.lock.Unlock()
		c.OnRebalanceComplete(group)
	})
	group.PendingJoin = delayedJoin
	group.State = PreparingRebalance
	group.lock.Unlock()

	c.JoinPurgatory.Watch(group.ID, delayedJoin)
}

func (c *GroupCoordinator) OnRebalanceComplete(group *GroupMetadata) {
	// Rebalance completion logic
}

func (c *GroupCoordinator) CompleteAndScheduleNextHeartbeatExpiration(group *GroupMetadata, member *MemberMetadata) {
	group.lock.Lock()
	defer group.lock.Unlock()

	if member.PendingHeartbeat != nil {
		member.PendingHeartbeat.ForceComplete()
	}

	delayedHeartbeat := NewDelayedOperation(func() {
		group.lock.Lock()
		if member.PendingHeartbeat != nil && member.PendingHeartbeat.completed {
			member.PendingHeartbeat = nil
		}
		group.lock.Unlock()
		c.OnHeartbeatExpiration(group, member)
	})
	member.PendingHeartbeat = delayedHeartbeat

	c.HeartbeatPurgatory.Watch(member.ID, delayedHeartbeat)
}

func (c *GroupCoordinator) OnHeartbeatExpiration(group *GroupMetadata, member *MemberMetadata) {
	// Heartbeat expiration logic
}

func main() {
	fmt.Println("Running GroupCoordinator Memory Leak Simulation...")

	coordinator := NewGroupCoordinator()
	group := coordinator.GetGroup("test-group")

	// Simulate rebalance storm
	fmt.Println("Simulating rebalance storm with rapid joins...")
	for i := 1; i <= 100; i++ {
		memberID := fmt.Sprintf("member-%d", i)
		member := NewMemberMetadata(memberID, group.ID, []string{"range"})
		
		group.lock.Lock()
		group.Members[memberID] = member
		group.lock.Unlock()

		coordinator.PrepareRebalance(group)
		coordinator.CompleteAndScheduleNextHeartbeatExpiration(group, member)
	}

	// Transition group to Dead (simulating coordinator partition unloading)
	fmt.Println("Transitioning group to Dead...")
	group.TransitionTo(Dead)

	// Purge completed operations from purgatories
	coordinator.JoinPurgatory.Purge()
	coordinator.HeartbeatPurgatory.Purge()

	// Assertions
	joinSize := coordinator.JoinPurgatory.Size()
	heartbeatSize := coordinator.HeartbeatPurgatory.Size()
	memberCount := len(group.Members)

	fmt.Printf("Active Members: %d (Expected: 0)\n", memberCount)
	fmt.Printf("Join Purgatory Size: %d (Expected: 0)\n", joinSize)
	fmt.Printf("Heartbeat Purgatory Size: %d (Expected: 0)\n", heartbeatSize)

	if memberCount == 0 && joinSize == 0 && heartbeatSize == 0 {
		fmt.Println("SUCCESS: Memory leak resolved! All references cleared and purgatories purged.")
	} else {
		fmt.Println("FAILURE: Memory leak detected!")
	}
}