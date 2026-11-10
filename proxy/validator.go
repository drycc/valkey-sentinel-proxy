package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

const topologyOperationTimeout = 5 * time.Second

type roleValidator interface {
	Validate(ctx context.Context, addr string) (role string, err error)
}

type primaryValidator struct {
	option valkey.ClientOption
}

func newPrimaryValidator(option valkey.ClientOption) *primaryValidator {
	return &primaryValidator{option: option}
}

func (v *primaryValidator) Validate(ctx context.Context, addr string) (string, error) {
	option := v.option
	option.InitAddress = []string{addr}
	option.Sentinel = valkey.SentinelOption{}
	option.ForceSingleClient = true
	option.AlwaysRESP2 = true
	option.DisableCache = true
	option.ClientSetInfo = valkey.DisableClientSetInfo
	option.Dialer.Timeout = topologyOperationTimeout

	client, err := valkey.NewClient(option)
	if err != nil {
		return "", fmt.Errorf("connect to candidate %s: %w", addr, err)
	}
	defer client.Close()

	queryCtx, cancel := context.WithTimeout(ctx, topologyOperationTimeout)
	defer cancel()
	resp := client.Do(queryCtx, client.B().Role().Build())
	parts, err := resp.ToArray()
	if err != nil {
		return "", fmt.Errorf("read ROLE from candidate %s: %w", addr, err)
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("read ROLE from candidate %s: empty response", addr)
	}
	role, err := parts[0].ToString()
	if err != nil {
		return "", fmt.Errorf("read ROLE from candidate %s: invalid role: %w", addr, err)
	}
	return role, nil
}

var _ roleValidator = (*primaryValidator)(nil)
