package proxy

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

func TestParseMasterAddress(t *testing.T) {
	tests := []struct {
		name     string
		parts    []string
		want     string
		loopback bool
		wantErr  bool
	}{
		{name: "IPv4", parts: []string{"127.0.0.1", "6379"}, want: "127.0.0.1:6379"},
		{name: "hostname", parts: []string{"localhost", "6380"}, loopback: true},
		{name: "IPv6", parts: []string{"2001:db8::1", "6379"}, want: "[2001:db8::1]:6379"},
		{name: "empty response", parts: nil, wantErr: true},
		{name: "missing port", parts: []string{"127.0.0.1"}, wantErr: true},
		{name: "extra field", parts: []string{"127.0.0.1", "6379", "extra"}, wantErr: true},
		{name: "empty host", parts: []string{"", "6379"}, wantErr: true},
		{name: "empty port", parts: []string{"127.0.0.1", ""}, wantErr: true},
		{name: "invalid port", parts: []string{"127.0.0.1", "not-a-port"}, wantErr: true},
		{name: "port out of range", parts: []string{"127.0.0.1", "70000"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMasterAddress(tc.parts)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseMasterAddress(%v) = %q, nil, want error", tc.parts, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMasterAddress(%v) error = %v", tc.parts, err)
			}
			if tc.loopback {
				addr, err := net.ResolveTCPAddr("tcp", got)
				if err != nil || !addr.IP.IsLoopback() || addr.Port != 6380 {
					t.Fatalf("parseMasterAddress(%v) = %q, want numeric loopback address on port 6380", tc.parts, got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("parseMasterAddress(%v) = %q, want %q", tc.parts, got, tc.want)
			}
		})
	}
}

func TestValkeySentinelControlUnreachableDoesNotPanic(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	control := newValkeySentinelControl(addr, valkey.ClientOption{})
	defer control.Close()
	if _, err := control.MasterAddr(context.Background(), "mymaster"); err == nil {
		t.Fatal("MasterAddr() error = nil, want connection error")
	}
}

type blockingIPResolver struct{}

func (blockingIPResolver) LookupIPAddr(ctx context.Context, _ string) ([]net.IPAddr, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestValkeySentinelControlDNSRespectsContext(t *testing.T) {
	server := startRESPTestServer(t, func(command []string) string {
		return "*2\r\n$9\r\nlocalhost\r\n$4\r\n6379\r\n"
	})
	control := newValkeySentinelControl(server.addr(), valkey.ClientOption{}).(*valkeySentinelControl)
	defer control.Close()
	control.resolver = blockingIPResolver{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := control.MasterAddr(ctx, "mymaster"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MasterAddr() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestValkeySentinelControlMasterAddr(t *testing.T) {
	server := startRESPTestServer(t, func(command []string) string {
		if reflect.DeepEqual(command, []string{"SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"}) {
			return "*2\r\n$9\r\n127.0.0.1\r\n$4\r\n6379\r\n"
		}
		return "-ERR unexpected command\r\n"
	})
	control := newValkeySentinelControl(server.addr(), valkey.ClientOption{})
	defer control.Close()

	addr, err := control.MasterAddr(context.Background(), "mymaster")
	if err != nil {
		t.Fatalf("MasterAddr() error = %v", err)
	}
	if addr != testPrimaryA {
		t.Fatalf("MasterAddr() = %q, want %q", addr, testPrimaryA)
	}
	if got := <-server.commands; !reflect.DeepEqual(got, []string{"SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"}) {
		t.Fatalf("command = %v, want SENTINEL GET-MASTER-ADDR-BY-NAME", got)
	}
}
