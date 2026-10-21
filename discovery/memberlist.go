package discovery

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
)

type slogger struct {
	log *slog.Logger
}

// TODO: fix log later
func (s *slogger) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	lvl := slog.LevelInfo

	switch {
	case strings.HasPrefix(msg, "[DEBUG]"):
		// lvl = slog.LevelDebug
		msg = strings.TrimPrefix(msg, "[DEBUG]")
	case strings.HasPrefix(msg, "[WARN]"):
		// lvl = slog.LevelWarn
		msg = strings.TrimPrefix(msg, "[WARN]")
	case strings.HasPrefix(msg, "[ERR]"):
		// lvl = slog.LevelError
		msg = strings.TrimPrefix(msg, "[ERR]")
	case strings.HasPrefix(msg, "[ERR]"):
		msg = strings.TrimPrefix(msg, "[INFO]")
	}

	s.log.Log(context.Background(), lvl, msg)
	return len(p), nil
}

type MemberlistConfig struct {
	InitialPeers []string
	BindAddress  string
	BindPort     uint16
	Log          *slog.Logger
}

type MemberlistDiscovery struct {
	Conf    MemberlistConfig
	Handler Handler
	m       *memberlist.Memberlist
	events  chan memberlist.NodeEvent
	log     *slog.Logger
}

func New(ctx context.Context, handler Handler, conf MemberlistConfig) (*MemberlistDiscovery, error) {
	m := NewMemberlistDiscovery(conf)
	m.Handler = handler

	if err := m.Start(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func NewMemberlistDiscovery(conf MemberlistConfig) *MemberlistDiscovery {
	if conf.Log == nil {
		conf.Log = slog.Default()
	}

	name := fmt.Sprintf("[%s:%d]", conf.BindAddress, conf.BindPort)
	return &MemberlistDiscovery{
		Conf: conf,
		log:  conf.Log.With("code.namespace", "MemberlistDiscovery", "node.name", name),
	}
}

// TODO: error handling
func (md *MemberlistDiscovery) Start(ctx context.Context) error {
	var mconf *memberlist.Config

	if ctx.Value("env") == "TEST" {
		mconf = memberlist.DefaultLocalConfig()
	} else {
		mconf = memberlist.DefaultWANConfig()
	}
	bport := int(md.Conf.BindPort)

	mconf.BindAddr = md.Conf.BindAddress
	mconf.BindPort = bport
	mconf.AdvertiseAddr = md.Conf.BindAddress
	mconf.AdvertisePort = bport
	mconf.Name = fmt.Sprintf("NODE-%d", bport)

	mconf.Logger = log.New(&slogger{log: md.Conf.Log}, "", 0)

	md.events = make(chan memberlist.NodeEvent, 16)
	mconf.Events = &memberlist.ChannelEventDelegate{Ch: md.events}

	m, err := memberlist.Create(mconf)

	if err != nil {
		return err
	}

	md.m = m

	md.log.Info("Service Discovery Started", "endpoint", m.LocalNode().Addr, "port", m.LocalNode().Port)

	go md.eventHandler(ctx)

	if err := md.JoinCluster(ctx); err != nil {
		return err
	}
	return nil
}

func (md *MemberlistDiscovery) eventHandler(ctx context.Context) {
	if md.Handler == nil {
		md.log.Debug("Leaving eventHandler as Handler not specified")
		return
	}
	for {
		select {
		case e := <-md.events:
			md.log.Debug("Event Received", "event", e.Event)
			switch e.Event {
			case memberlist.NodeJoin:
				// check if local so we don't call handler
				if e.Node.Name != md.m.LocalNode().Name {
					md.log.Info("Joining", "event", e.Event)
					md.handleJoin(e.Node)
				}
			case memberlist.NodeLeave:
				md.log.Info("Leaving", "event", e.Event)
				if e.Node.Name != md.m.LocalNode().Name {
					md.handleLeave(e.Node)
				}
			case memberlist.NodeUpdate:
				md.log.Info("NODE Update", "event", e.Event)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (md *MemberlistDiscovery) handleJoin(node *memberlist.Node) {
	if md.Handler == nil {
		return
	}
	if err := md.Handler.Join(node.Name, node.Address()); err != nil {
		md.log.Error("Failed to Join cluster", "node.adress", node.Address())
	}
}

func (md *MemberlistDiscovery) handleLeave(node *memberlist.Node) {
	if md.Handler == nil {
		return
	}
	if err := md.Handler.Leave(node.Name); err != nil {
		md.log.Error("Failed to Leave cluster", "ff", err)
	}
}

func canJoin(ipeers []string) bool {
	if len(ipeers) == 0 || (len(ipeers) == 1 && ipeers[0] == "") {
		return false
	}
	return true
}

func (md *MemberlistDiscovery) JoinCluster(ctx context.Context) error {
	ipeers := md.Conf.InitialPeers
	if !canJoin(ipeers) {
		md.log.Info("No initial-peers skipping cluster discovery")
		return nil
	}

	md.log.Info("initial-peers ", "addrs", ipeers)
	md.log.Info("Trying to join the cluster...")

	joined, err := md.m.Join(ipeers)

	if err != nil {
		return err
	}
	md.log.Info("Successfuly joined cluster", "joined", joined)

	return nil
}

func (md *MemberlistDiscovery) LeaveCluster(ctx context.Context) error {
	md.log.Info("Leaving cluster...")

	if err := md.m.Leave(3 * time.Second); err != nil {
		md.log.Error("Memberlist Leave failed")
		return err
	}

	if err := md.m.Shutdown(); err != nil {
		md.log.Error("Memberlist shutdown failed")
		return err
	}
	return nil
}

func (md *MemberlistDiscovery) HealthCheck(ctx context.Context) (int, error) {
	return md.m.GetHealthScore(), nil
}

func (md *MemberlistDiscovery) Members(ctx context.Context) []*memberlist.Node {
	return md.m.Members()
}

func (md *MemberlistDiscovery) Nodes(ctx context.Context) ([]Node, error) {
	_nodes := md.m.Members()
	var nodes []Node

	for i := 0; i < len(_nodes); i++ {
		node := Node{
			Id:      _nodes[i].Name,
			Address: string(_nodes[i].Addr),
			Port:    uint(_nodes[i].Port),
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (md *MemberlistDiscovery) LocalNode(ctx context.Context) *memberlist.Node {
	return md.m.LocalNode()
}
