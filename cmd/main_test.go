package main

import (
	"net"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	originalListen := *listen
	originalMaster := *master
	originalMaxProcs := *maxProcs
	originalSentinelAddr := *sentinelAddr
	originalSentinelUser := *sentinelUser
	originalSentinelPass := *sentinelPass
	originalNodeUser := *nodeUser
	originalNodePass := *nodePass
	originalRefreshInterval := *topologyRefreshInterval

	code := m.Run()

	*listen = originalListen
	*master = originalMaster
	*maxProcs = originalMaxProcs
	*sentinelAddr = originalSentinelAddr
	*sentinelUser = originalSentinelUser
	*sentinelPass = originalSentinelPass
	*nodeUser = originalNodeUser
	*nodePass = originalNodePass
	*topologyRefreshInterval = originalRefreshInterval
	os.Exit(code)
}

func TestFlagDefaults(t *testing.T) {
	tests := []struct {
		name     string
		actual   interface{}
		expected interface{}
	}{
		{"listen default", *listen, ":9999"},
		{"master default", *master, ""},
		{"maxProcs default", *maxProcs, 1},
		{"sentinelAddr default", *sentinelAddr, ":26379"},
		{"sentinelUser default", *sentinelUser, ""},
		{"sentinelPass default", *sentinelPass, ""},
		{"nodeUser default", *nodeUser, ""},
		{"nodePass default", *nodePass, ""},
		{"topology refresh default", *topologyRefreshInterval, 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.actual != tt.expected {
				t.Errorf("actual = %v, want %v", tt.actual, tt.expected)
			}
		})
	}
}

func TestParseSentinelAddrs(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{name: "single", raw: "127.0.0.1:26379", want: []string{"127.0.0.1:26379"}},
		{name: "multiple with whitespace", raw: " s1:26379, s2:26380 , s3:26381 ", want: []string{"s1:26379", "s2:26380", "s3:26381"}},
		{name: "duplicates retain order", raw: "s2:26379,s1:26379,s2:26379", want: []string{"s2:26379", "s1:26379", "s2:26379"}},
		{name: "empty", raw: "", wantErr: true},
		{name: "blank", raw: "  ", wantErr: true},
		{name: "empty middle item", raw: "s1:26379,,s2:26379", wantErr: true},
		{name: "empty first item", raw: ",s1:26379", wantErr: true},
		{name: "empty last item", raw: "s1:26379,", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSentinelAddrs(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseSentinelAddrs(%q) = %v, nil, want error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSentinelAddrs(%q) error = %v", tc.raw, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseSentinelAddrs(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestClientOptionConstruction(t *testing.T) {
	*sentinelAddr = "s1:26379, s2:26379"
	*sentinelUser = "sentinel-user"
	*sentinelPass = "sentinel-pass"
	*nodeUser = "node-user"
	*nodePass = "node-pass"
	*topologyRefreshInterval = 2 * time.Second

	option, serverOptions, err := proxyConfiguration()
	if err != nil {
		t.Fatalf("proxyConfiguration() error = %v", err)
	}
	if want := []string{"s1:26379", "s2:26379"}; !reflect.DeepEqual(option.InitAddress, want) {
		t.Fatalf("InitAddress = %v, want %v", option.InitAddress, want)
	}
	if option.Username != "sentinel-user" || option.Password != "sentinel-pass" {
		t.Fatal("Sentinel credentials were not preserved")
	}
	if option.Sentinel.MasterSet != "" {
		t.Fatalf("Sentinel.MasterSet = %q, want empty for direct Sentinel controls", option.Sentinel.MasterSet)
	}
	if len(serverOptions) != 2 {
		t.Fatalf("ServerOption count = %d, want node credentials and refresh interval", len(serverOptions))
	}
}

func TestClientOptionConstructionRejectsInvalidConfiguration(t *testing.T) {
	*sentinelAddr = "s1:26379,,s2:26379"
	if _, _, err := proxyConfiguration(); err == nil {
		t.Fatal("proxyConfiguration() error = nil for empty Sentinel item")
	}
}

func TestResolveTCPAddr(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", ":9999")
	if err != nil {
		t.Fatalf("ResolveTCPAddr() error = %v", err)
	}
	if addr.Port != 9999 {
		t.Fatalf("port = %d, want 9999", addr.Port)
	}
}

func TestResolveTCPAddrInvalid(t *testing.T) {
	for _, addr := range []string{"invalid:address:format", ":99999999", "256.256.256.256:8080"} {
		t.Run(addr, func(t *testing.T) {
			if _, err := net.ResolveTCPAddr("tcp", addr); err == nil {
				t.Fatalf("ResolveTCPAddr(%q) error = nil", addr)
			}
		})
	}
}

func TestFlagParsing(t *testing.T) {
	*listen = ":8888"
	*master = "testmaster"
	*maxProcs = 2
	if *listen != ":8888" || *master != "testmaster" || *maxProcs != 2 {
		t.Fatal("flag values were not applied")
	}
}

func BenchmarkClientOptionCreation(b *testing.B) {
	*sentinelAddr = "s1:26379,s2:26379"
	*sentinelUser = "sentinel-user"
	*sentinelPass = "sentinel-pass"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = proxyConfiguration()
	}
}
