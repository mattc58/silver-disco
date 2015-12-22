// +build unit

package cassandra

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetPort(t *testing.T) {
	p := 9042
	cluster, _ := NewCluster(Port(p))
	assert.Equal(t, p, cluster.Port)
}

func TestDefaultOptions(t *testing.T) {
	cluster, _ := NewCluster()
	assert.Equal(t, 9042, cluster.Port)
	assert.Equal(t, "127.0.0.1", cluster.ContactPoints[0])
}

func TestOverrideDefaultOptions(t *testing.T) {
	cluster, _ := NewCluster()
	assert.Equal(t, 4, cluster.ProtocolVersion)
	cluster, _ = NewCluster(ProtocolVersion(88))
	assert.Equal(t, 88, cluster.ProtocolVersion)
}
