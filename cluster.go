package cassandra

import (
	"errors"
)

type Cluster struct {
	ContactPoints   []string
	Port            int
	ProtocolVersion int
}

type ClusterOption func(*Cluster)

func ContactPoints(contactPoints []string) ClusterOption {
	return func(cluster *Cluster) {
		cluster.ContactPoints = contactPoints
	}
}

func Port(port int) ClusterOption {
	return func(cluster *Cluster) {
		cluster.Port = port
	}
}

func ProtocolVersion(pv int) ClusterOption {
	return func(cluster *Cluster) {
		cluster.ProtocolVersion = pv
	}
}

var DefaultOptions = []ClusterOption{
	Port(9042),
	ContactPoints([]string{"127.0.0.1"}),
	ProtocolVersion(4)}

const MAX_SUPPORTED_VERSION int = 4

var (
	NoHostAvailableError        = errors.New("no host available")
	ConfigurationExceptionError = errors.New("configuration error")
	AlreadyExistsError          = errors.New("schema item already exists")
	OperationTimedOutError      = errors.New("operation timed out")
	ReadTimeoutError            = errors.New("timeout on read")
	ReadFailureError            = errors.New("failure on read")
	WriteTimeoutError           = errors.New("timeout on write")
	WriteFailureErorr           = errors.New("failure on write")
)

func NewCluster(options ...ClusterOption) (*Cluster, error) {
	cluster := Cluster{}

	allOptions := append(DefaultOptions, options...)
	for _, option := range allOptions {
		option(&cluster)
	}
	return &cluster, nil
}

func (cluster *Cluster) Connect() (*Session, error) {
	return nil, nil
}

func (cluster *Cluster) Shutdown() error {
	return nil
}
