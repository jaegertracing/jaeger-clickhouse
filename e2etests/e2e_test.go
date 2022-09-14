package e2etests

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ecodia/golang-awaitility/awaitility"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	clickHouseImage = "clickhouse/clickhouse-server:22"
	jaegerImage     = "jaegertracing/all-in-one:1.32.0"

	networkName     = "chi-jaeger-test"
	clickhousePort  = "9000/tcp"
	jaegerQueryPort = "16686/tcp"
	jaegerAdminPort = "14269/tcp"
)

type testCase struct {
	configs []string
	chiconf *string
}

func TestE2E(t *testing.T) {
	if os.Getenv("E2E_TEST") == "" {
		t.Skip("Set E2E_TEST=true to run the test")
	}

	// Minimal additional configuration (config.d) to enable cluster mode
	chireplconf := "clickhouse-replicated.xml"

	tests := map[string]testCase{
		"local-single": {
			configs: []string{"config-local-single.yaml"},
			chiconf: nil,
		},
		"local-multi": {
			configs: []string{"config-local-multi1.yaml", "config-local-multi2.yaml"},
			chiconf: nil,
		},
		"replication-single": {
			configs: []string{"config-replication-single.yaml"},
			chiconf: &chireplconf,
		},
		"replication-multi": {
			configs: []string{"config-replication-multi1.yaml", "config-replication-multi2.yaml"},
			chiconf: &chireplconf,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testE2E(t, test)
		})
	}
}

func testE2E(t *testing.T, test testCase) {
	ctx := context.Background()
	workingDir, err := os.Getwd()
	require.NoError(t, err)

	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{Name: networkName},
	})
	require.NoError(t, err)
	defer network.Remove(ctx)

	var bindMounts map[string]string
	if test.chiconf != nil {
		bindMounts = map[string]string{
			fmt.Sprintf("%s/%s", workingDir, *test.chiconf): "/etc/clickhouse-server/config.d/testconf.xml",
		}
	} else {
		bindMounts = map[string]string{}
	}
	chReq := testcontainers.ContainerRequest{
		Image:        clickHouseImage,
		ExposedPorts: []string{clickhousePort},
		WaitingFor:   &clickhouseWaitStrategy{test: t, pollInterval: time.Millisecond * 200, startupTimeout: time.Minute},
		Networks:     []string{networkName},
		Hostname:     "chi",
		BindMounts:   bindMounts,
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: chReq,
		Started:          true,
	})
	require.NoError(t, err)
	defer chContainer.Terminate(ctx)

	jaegerContainers := make([]testcontainers.Container, 0)
	for _, pluginConfig := range test.configs {
		jaegerReq := testcontainers.ContainerRequest{
			Image:        jaegerImage,
			ExposedPorts: []string{jaegerQueryPort, jaegerAdminPort},
			WaitingFor:   wait.ForHTTP("/").WithPort(jaegerAdminPort).WithStartupTimeout(time.Second * 10),
			Env: map[string]string{
				"SPAN_STORAGE_TYPE": "grpc-plugin",
			},
			Cmd: []string{
				"--grpc-storage-plugin.binary=/project-dir/jaeger-clickhouse-linux-amd64",
				fmt.Sprintf("--grpc-storage-plugin.configuration-file=/project-dir/e2etests/%s", pluginConfig),
				"--grpc-storage-plugin.log-level=debug",
			},
			BindMounts: map[string]string{
				workingDir + "/..": "/project-dir",
			},
			Networks: []string{networkName},
		}
		// Call Start() manually here so that if it fails then we can still access the logs.
		jaegerContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: jaegerReq,
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
		err = jaegerContainer.Start(ctx)
		require.NoError(t, err)

		jaegerContainers = append(jaegerContainers, jaegerContainer)
	}

	for _, jaegerContainer := range jaegerContainers {
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

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{
			fmt.Sprintf("localhost:%d", port.Int()),
		},
		Auth: clickhouse.Auth{
			Database: "default",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
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
