package cassandra

import (
	"errors"
)

type Cluster struct {
	ContactPoints   []string
	Port            int
	ProtocolVersion int
}

type clusterOption func(*Cluster)

func ContactPoints(contactPoints []string) clusterOption {
	return func(cluster *Cluster) {
		cluster.ContactPoints = contactPoints
	}
}

func Port(port int) clusterOption {
	return func(cluster *Cluster) {
		cluster.Port = port
	}
}

func ProtocolVersion(pv int) clusterOption {
	return func(cluster *Cluster) {
		cluster.ProtocolVersion = pv
	}
}

var DefaultOptions = []clusterOption{
	Port(9042),
	ContactPoints([]string{"127.0.0.1"}),
	ProtocolVersion(4)}

var (
	NoHostAvailableError = errors.New("no host available")
)

func NewCluster(options ...clusterOption) (*Cluster, error) {
	cluster := Cluster{}

	allOptions := append(DefaultOptions, options...)
	for _, option := range allOptions {
		option(&cluster)
	}
	return &cluster, nil
}
