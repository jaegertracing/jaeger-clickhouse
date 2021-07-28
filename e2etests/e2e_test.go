package e2etests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go" // import driver
	"github.com/ecodia/golang-awaitility/awaitility"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	clickHouseImage = "yandex/clickhouse-server:21"
	jaegerImage     = "jaegertracing/all-in-one:1.24.0"

	networkName     = "chi-jaeger-test"
	clickhousePort  = "9000/tcp"
	jaegerQueryPort = "16686/tcp"
	jaegerAdminPort = "14269/tcp"
)

func TestE2E(t *testing.T) {
	if os.Getenv("E2E_TEST") == "" {
		t.Skip("Set E2E_TEST=true to run the test")
	}

	ctx := context.Background()
	workingDir, err := os.Getwd()
	require.NoError(t, err)

	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{Name: networkName},
	})
	require.NoError(t, err)
	defer network.Remove(ctx)

	chReq := testcontainers.ContainerRequest{
		Image:        clickHouseImage,
		ExposedPorts: []string{clickhousePort},
		WaitingFor:   &clickhouseWaitStrategy{test: t, pollInterval: time.Millisecond * 200, startupTimeout: time.Minute},
		Networks:     []string{networkName},
		Hostname:     "chi",
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer chContainer.Terminate(ctx)

	jaegerReq := testcontainers.ContainerRequest{
		Image:        jaegerImage,
		ExposedPorts: []string{jaegerQueryPort, jaegerAdminPort},
		WaitingFor:   wait.ForHTTP("/").WithPort(jaegerAdminPort).WithStartupTimeout(time.Second * 10),
		Env: map[string]string{
			"SPAN_STORAGE_TYPE": "grpc-plugin",
		},
		Cmd: []string{
			"--grpc-storage-plugin.binary=/project-dir/jaeger-clickhouse-linux-amd64",
			"--grpc-storage-plugin.configuration-file=/project-dir/e2etests/config.yaml",
			"--grpc-storage-plugin.log-level=debug",
		},
		BindMounts: map[string]string{
			workingDir + "/..": "/project-dir",
		},
		Networks: []string{networkName},
	}
	jaegerContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: jaegerReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer func() {
		logs, errLogs := jaegerContainer.Logs(ctx)
		require.NoError(t, errLogs)
		all, errLogs := ioutil.ReadAll(logs)
		require.NoError(t, errLogs)
		fmt.Printf("Jaeger logs:\n---->\n%s<----\n\n", string(all))
		jaegerContainer.Terminate(ctx)
	}()

	chContainer.MappedPort(ctx, clickhousePort)
	jaegerQueryPort, err := jaegerContainer.MappedPort(ctx, jaegerQueryPort)
	require.NoError(t, err)

	err = awaitility.Await(100*time.Millisecond, time.Second*3, func() bool {
		// Jaeger traces itself so this request generates some spans
		response, errHTTP := http.Get(fmt.Sprintf("http://localhost:%d/api/services", jaegerQueryPort.Int()))
		require.NoError(t, errHTTP)
		body, errHTTP := ioutil.ReadAll(response.Body)
		require.NoError(t, errHTTP)
		var r result
		errHTTP = json.Unmarshal(body, &r)
		require.NoError(t, errHTTP)
		return len(r.Data) == 1 && r.Data[0] == "jaeger-query"
	})
	assert.NoError(t, err)
}

type result struct {
	Data []string `json:"data"`
}

type clickhouseWaitStrategy struct {
	test           *testing.T
	pollInterval   time.Duration
	startupTimeout time.Duration
}

var _ wait.Strategy = (*clickhouseWaitStrategy)(nil)

func (c *clickhouseWaitStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) error {
	ctx, cancelContext := context.WithTimeout(ctx, c.startupTimeout)
	defer cancelContext()

	port, err := target.MappedPort(ctx, clickhousePort)
	require.NoError(c.test, err)
	db, err := sql.Open("clickhouse", fmt.Sprintf("tcp://localhost:%d?database=default", port.Int()))
	require.NoError(c.test, err)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.pollInterval):
			if err := db.Ping(); err != nil {
				continue
			}
			return nil
		}
	}
}
