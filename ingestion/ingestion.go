package ingestion

import (
	"container/list"
	"context"
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	nlist "github.com/toolkits/container/list"
	"net"
	"net/rpc"
	"sync"
	"time"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	DefaultIngestionQueueMaxSize = 102400                //10.24w
	DefaultSendTaskSleepInterval = time.Millisecond * 50 //默认睡眠间隔为50ms
	EndPoint                     = "endpoint"
	Metric                       = "__name__"
)

type conn_list struct {
	sync.RWMutex
	list *list.List
}

type sample struct {
	labels labels.Labels
	value  float64
	ts     int64
	ref    *string
}

func (l *conn_list) insert(c net.Conn) *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.list.PushBack(c)
}
func (l *conn_list) remove(e *list.Element) net.Conn {
	l.Lock()
	defer l.Unlock()
	return l.list.Remove(e).(net.Conn)
}

var (
	Close_chan, Close_done_chan chan int
	connects                    conn_list
	IngestionForFalconQueue     *nlist.SafeListLimited
)

func init() {
	Close_chan = make(chan int, 1)
	Close_done_chan = make(chan int, 1)
	connects = conn_list{list: list.New()}
	IngestionForFalconQueue = nlist.NewSafeListLimited(DefaultIngestionQueueMaxSize)

}

// IngesterOptions bundles options for the Ingester.
type IngestionOptions struct {
	Context context.Context
	Storage *tsdb.DB
	Logger  log.Logger
	mtx     sync.RWMutex
}

// The Ingester ingestion data
type Ingestion struct {
	opts   *IngestionOptions
	mtx    sync.RWMutex
	block  chan struct{}
	logger log.Logger
}

func NewIngestion(o *IngestionOptions) *Ingestion {
	return &Ingestion{
		opts:   o,
		block:  make(chan struct{}),
		logger: o.Logger,
	}
}


func (this *Ingestion) Start() {
	level.Info(this.logger).Log("msg","Starting Ingestion For Falcon")
	go this.startRpc()
}

func (this *Ingestion) Stop() {
	level.Info(this.logger).Log("msg","Stopping Ingestion For falcon ")
}

func (this *Ingestion) startRpc() {

	addr := "0.0.0.0:8889"
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		level.Error(this.logger).Log("rpc","rpc.Start error, net.ResolveTCPAddr failed",err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		level.Error(this.logger).Log("rpc","Start error, listen ",addr,"failed",err)
	} else {
		level.Info(this.logger).Log("rpc","Start ok, listening on",addr)
	}

	rpc.Register(new(IngestionForFalcon))

	go func() {
		var tempDelay time.Duration // how long to sleep on accept failure
		for {
			conn, err := listener.Accept()
			if err != nil {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			tempDelay = 0
			go func() {
				e := connects.insert(conn)
				defer connects.remove(e)
				rpc.ServeConn(conn)
			}()
		}
	}()

	go this.ingestionLoop()

	select {

	case <-Close_chan:
		listener.Close()
		Close_done_chan <- 1
		connects.Lock()
		for e := connects.list.Front(); e != nil; e = e.Next() {
			e.Value.(net.Conn).Close()
		}
		connects.Unlock()
		return
	}
}

func (this *Ingestion) ingestionLoop() {

}

func falconItem2Labels(item *cmodel.GraphItem) labels.Labels {
	l := make([]labels.Label, 0, len(item.Tags)+2)
	for k, v := range item.Tags {
		l = append(l, labels.Label{Name: k, Value: v})
	}
	l = append(l, labels.Label{Name: EndPoint, Value: item.Endpoint})
	l = append(l, labels.Label{Name: Metric, Value: item.Metric})
	return labels.New(l...)
}

type IngestionForFalcon int

func (this *IngestionForFalcon) Ping(req cmodel.NullRpcRequest, resp *cmodel.SimpleRpcResponse) error {
	return nil
}
func (this *IngestionForFalcon) Send(items []*cmodel.GraphItem, resp *cmodel.SimpleRpcResponse) error {
	Push2IngestionForFalconQueue(items)
	return nil
}

// put the original data to kafka sender queues
func Push2IngestionForFalconQueue(items []*cmodel.GraphItem) {
	for _, item := range items {
		isSuccess := IngestionForFalconQueue.PushFront(item)
		if !isSuccess {
		}
	}
}
