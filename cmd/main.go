package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/drycc-addons/valkey-sentinel-proxy/proxy"
	"github.com/valkey-io/valkey-go"
)

var (
	listen                  = flag.String("listen", ":9999", "listen address")
	master                  = flag.String("master", "", "name of the master valkey node")
	maxProcs                = flag.Int("max-procs", 1, "sets the maximum number of CPUs that can be executing")
	sentinelAddr            = flag.String("sentinel-addr", ":26379", "comma-separated Sentinel addresses")
	sentinelUser            = flag.String("sentinel-user", "", "username to use when connecting to Sentinel servers")
	sentinelPass            = flag.String("sentinel-pass", "", "password to use when connecting to Sentinel servers")
	nodeUser                = flag.String("node-user", "", "username to use when validating Valkey data nodes")
	nodePass                = flag.String("node-pass", "", "password to use when validating Valkey data nodes")
	topologyRefreshInterval = flag.Duration("topology-refresh-interval", 5*time.Second, "interval for reconciling the Sentinel topology")
)

func parseSentinelAddrs(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("at least one Sentinel address is required")
	}
	parts := strings.Split(raw, ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			return nil, fmt.Errorf("Sentinel address list contains an empty item")
		}
		addresses = append(addresses, addr)
	}
	return addresses, nil
}

func proxyConfiguration() (valkey.ClientOption, []proxy.ServerOption, error) {
	addresses, err := parseSentinelAddrs(*sentinelAddr)
	if err != nil {
		return valkey.ClientOption{}, nil, err
	}
	if *topologyRefreshInterval <= 0 {
		return valkey.ClientOption{}, nil, fmt.Errorf("topology refresh interval must be positive")
	}
	clientOption := valkey.ClientOption{
		InitAddress: addresses,
		Username:    *sentinelUser,
		Password:    *sentinelPass,
	}
	serverOptions := []proxy.ServerOption{
		proxy.WithNodeCredentials(*nodeUser, *nodePass),
		proxy.WithTopologyRefreshInterval(*topologyRefreshInterval),
	}
	return clientOption, serverOptions, nil
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*maxProcs)
	listenAddr, err := net.ResolveTCPAddr("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to resolve local address: %s", err)
	}
	clientOption, serverOptions, err := proxyConfiguration()
	if err != nil {
		log.Fatalf("invalid proxy configuration: %s", err)
	}
	server, err := proxy.NewValkeyProxyServer(listenAddr, clientOption, *master, serverOptions...)
	if err != nil {
		log.Fatalf("failed to create proxy server: %s", err)
	}
	defer server.Close()
	server.Serve()
}
