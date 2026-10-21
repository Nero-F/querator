package discovery_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/querator/discovery"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestDiscovery(t *testing.T) {
	m, handler := setupNode(t, nil)
	m, _ = setupNode(t, m)
	m, _ = setupNode(t, m)

	ctx := context.Background()

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members(ctx)) &&
			0 == len(handler.leaves)
	}, 3*time.Second, 250*time.Millisecond)

	require.NoError(t, m[2].LeaveCluster(ctx))

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			2 == len(m[0].Members(ctx)) &&
			1 == len(handler.leaves)
	}, 5*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("NODE-%d", m[2].Conf.BindPort), <-handler.leaves)
}

func setupNode(t *testing.T, nodes []*discovery.MemberlistDiscovery) ([]*discovery.MemberlistDiscovery, *handler) {
	id := len(nodes)
	addr := "127.0.0.1"
	ports := dynaport.Get(1)
	ctx := context.WithValue(context.Background(), "env", "TEST")

	cfg := discovery.MemberlistConfig{
		BindAddress: addr,
		BindPort:    uint16(ports[0]),
		Name:        fmt.Sprintf("%d", id),
	}
	h := &handler{}
	if len(nodes) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		ipeer := fmt.Sprintf("%s:%d", nodes[0].Conf.BindAddress, nodes[0].Conf.BindPort)
		cfg.InitialPeers = []string{ipeer}
	}
	m, err := discovery.New(ctx, h, cfg)
	require.NoError(t, err)
	nodes = append(nodes, m)
	return nodes, h
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{"id": id, "addr": addr}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
