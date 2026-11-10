package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type respCommandHandler func(command []string) string

type respTestServer struct {
	listener net.Listener
	commands chan []string
	done     chan struct{}
	mu       sync.Mutex
	conn     net.Conn
}

func startRESPTestServer(t *testing.T, handler respCommandHandler) *respTestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	s := &respTestServer{
		listener: listener,
		commands: make(chan []string, 16),
		done:     make(chan struct{}),
	}
	go s.serve(handler)
	t.Cleanup(s.close)
	return s
}

func (s *respTestServer) addr() string {
	return s.listener.Addr().String()
}

func (s *respTestServer) serve(handler respCommandHandler) {
	defer close(s.done)
	conn, err := s.listener.Accept()
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		s.commands <- command
		if _, err := io.WriteString(conn, handler(command)); err != nil {
			return
		}
	}
}

func (s *respTestServer) close() {
	_ = s.listener.Close()
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	<-s.done
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' || !strings.HasSuffix(line, "\r\n") {
		return nil, fmt.Errorf("invalid RESP array header %q", line)
	}
	count, err := strconv.Atoi(line[1 : len(line)-2])
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid RESP array length %q", line)
	}
	command := make([]string, count)
	for i := range command {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' || !strings.HasSuffix(line, "\r\n") {
			return nil, fmt.Errorf("invalid RESP bulk header %q", line)
		}
		length, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid RESP bulk length %q", line)
		}
		value := make([]byte, length+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		if string(value[length:]) != "\r\n" {
			return nil, fmt.Errorf("invalid RESP bulk terminator %q", value[length:])
		}
		command[i] = string(value[:length])
	}
	return command, nil
}

func respRole(role string) string {
	switch role {
	case "master":
		return "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n"
	case "slave":
		return "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nconnected\r\n:0\r\n"
	case "sentinel":
		return "*2\r\n$8\r\nsentinel\r\n*0\r\n"
	default:
		panic("unsupported test role " + role)
	}
}
