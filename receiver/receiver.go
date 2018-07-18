package receiver

import (
	"net/url"
	"context"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"sync"
)

// Constants for instrumentation.
const namespace = "monibench"

var (
	receiverTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "monitor_data_receives_total",
			Help:      "The total number of monitor data received.",
		},
	)
)

type receiverMetric struct {
	receiverTotal prometheus.Counter
}
// Appendable returns an Appender.
type Appendable interface {
	Appender() (storage.Appender, error)
}
// ManagerOptions bundles options for the Manager.
type ManagerOptions struct {
	ExternalURL *url.URL
	Context     context.Context
	Appendable  Appendable
	Logger      log.Logger
	Registerer  prometheus.Registerer
}

// The Manager manages recording and alerting rules.
type Manager struct {
	opts   *ManagerOptions
	mtx    sync.RWMutex
	block  chan struct{}
	logger log.Logger
}

// NewManager returns an implementation of Manager, ready to be started
// by calling the Run method.
func NewManager(o *ManagerOptions) *Manager {
	m := &Manager{
		opts:   o,
		block:  make(chan struct{}),
		logger: o.Logger,
	}
	if o.Registerer != nil {
		o.Registerer.MustRegister(receiverTotal)
	}
	return m
}

func (m *Manager) Run() {
	close(m.block)
}



