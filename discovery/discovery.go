package discovery

import (
	"context"
)

type Node struct {
	Id      string
	Address string
	Port    uint
}

type ServiceDiscovery struct {
	Discovery Discovery
	// Name      string
}

type ClusterEvent int

const (
	SkippingDiscovery ClusterEvent = iota
	NodeJoined
	NodeLeft
	NodeDisconnected
	NodeUnhealthy
)

type Discovery interface {
	Start(ctx context.Context) error
	JoinCluster(ctx context.Context) error
	LeaveCluster(ctx context.Context) error
	HealthCheck(ctx context.Context) (int, error)
	Nodes(ctx context.Context) ([]Node, error)
}

// Represents some component that needs to know when a server joins our leaves the cluster
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}
