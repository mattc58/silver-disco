// +build integration

package cassandra_test

import (
	"testing"

	"github.com/mattc58/silver-disco"
	"github.com/stretchr/testify/assert"
)

func TestConnectToBadHosts(t *testing.T) {
	cluster, err := cassandra.NewCluster(
		cassandra.ContactPoints([]string{"127.1.2.9", "127.1.2.10"}),
		cassandra.ProtocolVersion(4))
	assert.NotNil(t, cluster)
	assert.Equal(t, cassandra.NoHostAvailableError, err)
}
