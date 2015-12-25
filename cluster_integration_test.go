// +build integration

package cassandra_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/mattc58/silver-disco"
	"github.com/stretchr/testify/assert"
)

var (
	ENV_CASSANDRA_VERSION = os.Getenv("CASSANDRA_VERSION")
	ENV_PROTOCOL_VERSION  = os.Getenv("PROTOCOL_VERSION")
	PROTOCOL_VERSION      int
)

func TestMain(m *testing.M) {
	var err error
	if PROTOCOL_VERSION, err = strconv.Atoi(ENV_PROTOCOL_VERSION); err != nil {
		PROTOCOL_VERSION = 4
	}
	os.Exit(m.Run())
}

func getConnectionOrFail(t *testing.T, options ...cassandra.ClusterOption) (*cassandra.Cluster, error) {
	cluster, err := cassandra.NewCluster(options...)
	if !assert.NotNil(t, cluster) {
		t.FailNow()
	}
	return cluster, err
}

func getSessionOrFail(t *testing.T, cluster *cassandra.Cluster) (*cassandra.Session, error) {
	session, err := cluster.Connect()
	if !assert.NotNil(t, session) {
		t.FailNow()
	}
	return session, err
}

func executeUntilPass(t *testing.T, session *cassandra.Session, query string) (cassandra.QueryResults, error) {
	var result cassandra.QueryResults
	var err error
	for i := 0; i < 100; i++ {
		result, err = session.Execute(query)
		if err == nil {
			return result, err
		} else if err == cassandra.OperationTimedOutError || err == cassandra.ReadTimeoutError {
			t.Logf("Received error %d %s on query %s", i, err, query)
		} else {
			t.FailNow()
		}
	}
	t.Fatalf("failing on query %s after 100 attempts", query)
	return nil, nil
}

func executeWithLongWaitRetry(t *testing.T, session *cassandra.Session, query string) (cassandra.QueryResults, error) {
	var result cassandra.QueryResults
	var err error
	for i := 0; i < 10; i++ {
		result, err = session.Execute(query, cassandra.SessionTimeout(30))
		if err == nil {
			return result, err
		} else if err == cassandra.OperationTimedOutError || err == cassandra.ReadTimeoutError {
			t.Logf("Received error %d %s on query %s", i, err, query)
		} else {
			t.FailNow()
		}
	}
	t.Fatalf("failing on query %s after 10 attempts", query)
	return nil, nil
}

func TestRaiseErrorOnControlConnectionTimeout(t *testing.T) {
	// uses CCM to pause and resume a node
	t.Error("Not implemented")
}

func TestBasic(t *testing.T) {
	cluster, _ := getConnectionOrFail(t, cassandra.ProtocolVersion(PROTOCOL_VERSION))
	session, _ := getSessionOrFail(t, cluster)
	defer cluster.Shutdown()

	result, _ := executeUntilPass(t, session, `CREATE KEYSPACE 
		clustertests WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`)
	assert.Equal(t, false, result[0])

	result, _ = executeWithLongWaitRetry(t, session, `CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )`)
	assert.Equal(t, false, result[0])

	result, _ = session.Execute("INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')")
	assert.Equal(t, false, result[0])

	result, _ = session.Execute("SELECT * FORM clustertests.cf0")
	assert.Equal(t, []string{"a", "b", "c"}, result[0])

	_, _ = executeWithLongWaitRetry(t, session, "DROP KEYSPACE clustertests")
}

func TestProtocolNegotiation(t *testing.T) {
	cluster, _ := cassandra.NewCluster()
	if !assert.NotNil(t, cluster) {
		t.FailNow()
	}
	assert.Equal(t, cluster.ProtocolVersion, cassandra.MAX_SUPPORTED_VERSION)
	session, _ := cluster.Connect()
	if !assert.NotNil(t, session) {
		t.FailNow()
	}
	defer cluster.Shutdown()

	updatedProtocolVersion := session.ProtocolVersion
	updatedClusterVersion := cluster.ProtocolVersion

	t.Logf("Cassandra version %s", ENV_CASSANDRA_VERSION)
	if ENV_CASSANDRA_VERSION >= "2.2" {
		assert.Equal(t, updatedProtocolVersion, 4)
		assert.Equal(t, updatedClusterVersion, 4)
	} else if ENV_CASSANDRA_VERSION >= "2.1" {
		assert.Equal(t, updatedProtocolVersion, 3)
		assert.Equal(t, updatedClusterVersion, 3)
	} else if ENV_CASSANDRA_VERSION >= "2.0" {
		assert.Equal(t, updatedProtocolVersion, 2)
		assert.Equal(t, updatedClusterVersion, 2)
	} else {
		assert.Equal(t, updatedProtocolVersion, 1)
		assert.Equal(t, updatedClusterVersion, 1)
	}
}

func TestConnectOnKeyspace(t *testing.T) {
	t.Error("Not implemented")
}

func TestSetKeyspaceTwice(t *testing.T) {
	t.Error("Not implemented")
}

func TestDefaultConnections(t *testing.T) {
	t.Error("Not implemented")
}

func TestConnectToAlreadyShutdownCluster(t *testing.T) {
	t.Error("Not implemented")
}

func TestAuthProviderIsCallable(t *testing.T) {
	t.Error("Not implemented")
}

func TestV2AuthProvider(t *testing.T) {
	t.Error("Not implemented")
}

func TestConvictionPolicyFactoryIsCallable(t *testing.T) {
	t.Error("Not implemented")
}

func TestConnectToBadHosts(t *testing.T) {
	cluster, err := cassandra.NewCluster(
		cassandra.ContactPoints([]string{"127.1.2.9", "127.1.2.10"}),
		cassandra.ProtocolVersion(4))
	assert.NotNil(t, cluster)
	assert.Equal(t, cassandra.NoHostAvailableError, err)
}

func TestClusterSettings(t *testing.T) {
	t.Error("Not implemented")
}

func TestRefreshSchema(t *testing.T) {
	t.Error("Not implemented")
}

func TestRefreshSchemaKeyspace(t *testing.T) {
	t.Error("Not implemented")
}

func TestRefreshSchemaTable(t *testing.T) {
	t.Error("Not implemented")
}

func TestRefreshSchemaType(t *testing.T) {
	t.Error("Not implemented")
}

func TestRefreshSchemaNoWait(t *testing.T) {
	t.Error("Not implemented")
}

func TestTrace(t *testing.T) {
	t.Error("Not implemented")
}

func TestTraceTimeout(t *testing.T) {
	t.Error("Not implemented")
}
func TestStringCoverage(t *testing.T) {
	t.Error("Not implemented")
}
func TestIdleHeartbeat(t *testing.T) {
	t.Error("Not implemented")
}
func TestIdleHeartbeatDisabled(t *testing.T) {
	t.Error("Not implemented")
}

func TestPoolManagement(t *testing.T) {
	t.Error("Not implemented")
}
