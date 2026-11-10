package proxy

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/valkey-io/valkey-go"
)

type sentinelControl interface {
	MasterAddr(context.Context, string) (string, error)
	Close()
}

type ipResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type valkeySentinelControl struct {
	addr     string
	option   valkey.ClientOption
	resolver ipResolver

	mu     sync.Mutex
	client valkey.Client
	closed bool
}

func newValkeySentinelControl(addr string, option valkey.ClientOption) sentinelControl {
	return &valkeySentinelControl{addr: addr, option: option, resolver: net.DefaultResolver}
}

func directValkeyClientOption(addr string, option valkey.ClientOption) valkey.ClientOption {
	option.InitAddress = []string{addr}
	option.Sentinel = valkey.SentinelOption{}
	option.ForceSingleClient = true
	option.AlwaysRESP2 = true
	option.DisableCache = true
	option.ClientSetInfo = valkey.DisableClientSetInfo
	option.Dialer.Timeout = topologyOperationTimeout
	return option
}

func (c *valkeySentinelControl) getClient() (valkey.Client, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errServerClosed
	}
	if c.client != nil {
		client := c.client
		c.mu.Unlock()
		return client, nil
	}
	c.mu.Unlock()

	client, err := valkey.NewClient(directValkeyClientOption(c.addr, c.option))
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		client.Close()
		return nil, errServerClosed
	}
	if c.client != nil {
		existing := c.client
		c.mu.Unlock()
		client.Close()
		return existing, nil
	}
	c.client = client
	c.mu.Unlock()
	return client, nil
}

func (c *valkeySentinelControl) MasterAddr(ctx context.Context, masterName string) (string, error) {
	client, err := c.getClient()
	if err != nil {
		return "", fmt.Errorf("connect to Sentinel %s: %w", c.addr, err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, topologyOperationTimeout)
	defer cancel()
	resp := client.Do(queryCtx, client.B().Arbitrary("SENTINEL", "GET-MASTER-ADDR-BY-NAME").Args(masterName).Build())
	parts, err := resp.AsStrSlice()
	if err != nil {
		return "", fmt.Errorf("query Sentinel %s: %w", c.addr, err)
	}
	addr, err := parseMasterAddressContext(queryCtx, c.resolver, parts)
	if err != nil {
		return "", fmt.Errorf("query Sentinel %s: %w", c.addr, err)
	}
	return addr, nil
}

func (c *valkeySentinelControl) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client != nil {
		client.Close()
	}
}

func parseMasterAddress(parts []string) (string, error) {
	return parseMasterAddressContext(context.Background(), net.DefaultResolver, parts)
}

func parseMasterAddressContext(ctx context.Context, resolver ipResolver, parts []string) (string, error) {
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid master address response: %v", parts)
	}
	host := strings.TrimSpace(parts[0])
	port := strings.TrimSpace(parts[1])
	if host == "" || port == "" {
		return "", fmt.Errorf("invalid master address response: %v", parts)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("invalid master port %q", port)
	}
	if ip := net.ParseIP(host); ip != nil {
		return (&net.TCPAddr{IP: ip, Port: portNumber}).String(), nil
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", fmt.Errorf("resolve master host %q: %w", host, err)
	}
	if len(addresses) == 0 || addresses[0].IP == nil {
		return "", fmt.Errorf("resolve master host %q: no addresses", host)
	}
	return (&net.TCPAddr{IP: addresses[0].IP, Port: portNumber, Zone: addresses[0].Zone}).String(), nil
}

var _ sentinelControl = (*valkeySentinelControl)(nil)
