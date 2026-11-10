package proxy

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

func roleOnlyHandler(response string) respCommandHandler {
	return func(command []string) string {
		if len(command) == 1 && command[0] == "ROLE" {
			return response
		}
		return "-ERR unexpected command\r\n"
	}
}

func TestPrimaryValidatorMaster(t *testing.T) {
	server := startRESPTestServer(t, roleOnlyHandler(respRole("master")))
	validator := newPrimaryValidator(valkey.ClientOption{})
	role, err := validator.Validate(context.Background(), server.addr())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if role != "master" {
		t.Fatalf("Validate() role = %q, want master", role)
	}
}

func TestPrimaryValidatorNotMaster(t *testing.T) {
	for _, expected := range []string{"slave", "sentinel"} {
		t.Run(expected, func(t *testing.T) {
			server := startRESPTestServer(t, roleOnlyHandler(respRole(expected)))
			validator := newPrimaryValidator(valkey.ClientOption{})
			role, err := validator.Validate(context.Background(), server.addr())
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if role != expected {
				t.Fatalf("Validate() role = %q, want %q", role, expected)
			}
		})
	}
}

func TestPrimaryValidatorAuth(t *testing.T) {
	t.Run("credentials are sent", func(t *testing.T) {
		server := startRESPTestServer(t, func(command []string) string {
			if len(command) == 3 && command[0] == "AUTH" {
				return "+OK\r\n"
			}
			if len(command) == 1 && command[0] == "ROLE" {
				return respRole("master")
			}
			return "-ERR unexpected command\r\n"
		})
		validator := newPrimaryValidator(valkey.ClientOption{Username: "node-user", Password: "node-pass"})
		role, err := validator.Validate(context.Background(), server.addr())
		if err != nil || role != "master" {
			t.Fatalf("Validate() = %q, %v, want master, nil", role, err)
		}
		if got := <-server.commands; !reflect.DeepEqual(got, []string{"AUTH", "node-user", "node-pass"}) {
			t.Fatalf("first command = %v, want AUTH with node credentials", got)
		}
		if got := <-server.commands; !reflect.DeepEqual(got, []string{"ROLE"}) {
			t.Fatalf("second command = %v, want ROLE", got)
		}
	})

	tests := []struct {
		name        string
		username    string
		password    string
		authReply   string
		roleReply   string
		wantMessage string
	}{
		{name: "NOAUTH", authReply: "+OK\r\n", roleReply: "-NOAUTH Authentication required.\r\n", wantMessage: "NOAUTH"},
		{name: "WRONGPASS", username: "node-user", password: "do-not-log-this", authReply: "-WRONGPASS invalid username-password pair\r\n", roleReply: respRole("master"), wantMessage: "WRONGPASS"},
		{name: "NOPERM", username: "node-user", password: "do-not-log-this", authReply: "+OK\r\n", roleReply: "-NOPERM this user has no permissions to run the role command\r\n", wantMessage: "NOPERM"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := startRESPTestServer(t, func(command []string) string {
				switch command[0] {
				case "AUTH":
					return tc.authReply
				case "ROLE":
					return tc.roleReply
				default:
					return "-ERR unexpected command\r\n"
				}
			})
			validator := newPrimaryValidator(valkey.ClientOption{Username: tc.username, Password: tc.password})
			role, err := validator.Validate(context.Background(), server.addr())
			if err == nil {
				t.Fatalf("Validate() = %q, nil, want authentication/permission error", role)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Validate() error = %q, want %s", err, tc.wantMessage)
			}
			if tc.password != "" && strings.Contains(err.Error(), tc.password) {
				t.Fatalf("Validate() error leaked password: %q", err)
			}
		})
	}
}

func TestPrimaryValidatorMalformedRole(t *testing.T) {
	for name, response := range map[string]string{
		"empty array":        "*0\r\n",
		"non-string first":   "*1\r\n*0\r\n",
		"not an array reply": "+master\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			server := startRESPTestServer(t, roleOnlyHandler(response))
			validator := newPrimaryValidator(valkey.ClientOption{})
			if role, err := validator.Validate(context.Background(), server.addr()); err == nil {
				t.Fatalf("Validate() = %q, nil, want malformed ROLE error", role)
			}
		})
	}
}

func TestPrimaryValidatorUnreachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	validator := newPrimaryValidator(valkey.ClientOption{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if role, err := validator.Validate(ctx, addr); err == nil {
		t.Fatalf("Validate() = %q, nil, want connection error", role)
	}
}
