package receiver

import (
	"time"
	"sync"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"

	"context"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"math"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/client_golang/prometheus"
	"errors"
	"fmt"
	"github.com/prometheus/common/model"
	nlist "github.com/toolkits/container/list"
)

const (
	maxAheadTime            = 10 * time.Minute
	limit                   = 10000
)

var (
	targetScrapeSampleLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "umonibench_target_scrapes_exceeded_sample_limit_total",
			Help: "Total number of scrapes that hit the sample limit and were rejected.",
		},
	)
	targetScrapeSampleDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "umonibench_target_scrapes_sample_duplicate_timestamp_total",
			Help: "Total number of samples rejected due to duplicate timestamps but different values",
		},
	)
	targetScrapeSampleOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "umonibench_target_scrapes_sample_out_of_order_total",
			Help: "Total number of samples rejected due to not being out of the expected order",
		},
	)
	targetScrapeSampleOutOfBounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "umonibench_target_scrapes_sample_out_of_bounds_total",
			Help: "Total number of samples rejected due to timestamp falling outside of the time bounds",
		},
	)

	IngestionForFalconQueue *nlist.SafeListLimited
)

func init() {
	prometheus.MustRegister(targetScrapeSampleLimit)
	prometheus.MustRegister(targetScrapeSampleDuplicate)
	prometheus.MustRegister(targetScrapeSampleOutOfOrder)
	prometheus.MustRegister(targetScrapeSampleOutOfBounds)
}


type labelsMutator func(labels.Labels) labels.Labels

type Sample struct {
	Labels labels.Labels
	Value  float64
	Ts     int64
	Ref    *uint64
}

func (this *Sample) ToString() string {
	return this.Labels.String()
}

func NewReceiverLoop(r <-chan []Sample, app storage.Storage, log log.Logger) *ReceiverLoop {

	level.Info(log).Log("NewReceiverLoop", "Instance Create Begin")
	var (
		myReceiver = &channelReceiver{recevChannel: r, timeout: maxAheadTime}
	)

	cache := newReceiverCache()
	l := newReceiverLoop(context.Background(), myReceiver, log,
		func(l labels.Labels) labels.Labels { return mutateSampleLabels(l) },
		func(l labels.Labels) labels.Labels { return mutateReportSampleLabels(l) },
		func() storage.Appender {
			app, err := app.Appender()
			if err != nil {
				panic(err)
			}
			return appender(app, limit)
		},
		cache)

	level.Info(log).Log("NewReceiverLoop", "Instance Create Done")
	return l
}

func mutateSampleLabels(lset labels.Labels) labels.Labels {
	lb := labels.NewBuilder(lset)
	res := lb.Labels()
	return res
}

func mutateReportSampleLabels(lset labels.Labels) labels.Labels {

	lb := labels.NewBuilder(lset)

	for _, l := range lset {

		lv := lset.Get(l.Name)
		if lv != "" {
			lb.Set(model.ExportedLabelPrefix+l.Name, lv)
		}

		lb.Set(model.ExportedLabelPrefix+l.Name, lv)

		lb.Set(l.Name, l.Value)
	}

	return lb.Labels()

}

// appender returns an appender for ingested samples from the target.
func appender(app storage.Appender, limit int) storage.Appender {
	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}

	// The limit is applied after metrics are potentially dropped via relabeling.
	if limit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    limit,
		}
	}
	return app
}

var errSampleLimit = errors.New("sample limit exceeded")

// limitAppender limits the number of total appended samples in a batch.
type limitAppender struct {
	storage.Appender

	limit int
	i     int
}

func (app *limitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return 0, errSampleLimit
		}
	}
	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *limitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return errSampleLimit
		}
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}

type timeLimitAppender struct {
	storage.Appender

	maxTime int64
}

func (app *timeLimitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if t > app.maxTime {
		return 0, storage.ErrOutOfBounds
	}

	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *timeLimitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if t > app.maxTime {
		return storage.ErrOutOfBounds
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}


type sampleEntry struct {
	lastIter uint64 // Last scrape iteration the entry was observed at.
}

// ReceiverCache tracks mappings of exposed metric strings to label sets and
// storage references. Additionally, it tracks staleness of series between
// scrapes.
type ReceiverCache struct {
	iter uint64 // Current scrape iteration.

	// Parsed string to an entry with information about the actual label set
	// and its storage reference.
	series map[string]*cacheEntry

	// Cache of dropped metric strings and their iteration. The iteration must
	// be a pointer so we can update it without setting a new entry with an unsafe
	// string in addDropped().
	droppedSeries map[string]*uint64

	// seriesCur and seriesPrev store the labels of series that were seen
	// in the current and previous scrape.
	// We hold two maps and swap them out to save allocations.
	seriesCur  map[uint64]labels.Labels
	seriesPrev map[uint64]labels.Labels

	metaMtx  sync.Mutex
	metadata map[string]*sampleEntry

}



func newReceiverCache() *ReceiverCache {
	return &ReceiverCache{
		series:        map[string]*cacheEntry{},
		droppedSeries: map[string]*uint64{},
		seriesCur:     map[uint64]labels.Labels{},
		seriesPrev:    map[uint64]labels.Labels{},
	}
}

func (c *ReceiverCache) iterDone() {
	// All caches may grow over time through series churn
	// or multiple string representations of the same metric. Clean up entries
	// that haven't appeared in the last scrape.
	for s, e := range c.series {
		if c.iter-e.lastIter > 2 {
			delete(c.series, s)
		}
	}
	for s, iter := range c.droppedSeries {
		if c.iter - *iter > 2 {
			delete(c.droppedSeries, s)
		}
	}

	c.metaMtx.Lock()
	for m, e := range c.metadata {
		// Keep metadata around for 10 scrapes after its metric disappeared.
		if c.iter-e.lastIter > 10 {
			delete(c.metadata, m)
		}
	}
	c.metaMtx.Unlock()

	// Swap current and previous series.
	c.seriesPrev, c.seriesCur = c.seriesCur, c.seriesPrev

	// We have to delete every single key in the map.
	for k := range c.seriesCur {
		delete(c.seriesCur, k)
	}

	c.iter++
}

func (c *ReceiverCache) get(met string) (*cacheEntry, bool) {
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}
	e.lastIter = c.iter
	return e, true
}

func (c *ReceiverCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
	if ref == 0 {
		return
	}
	c.series[met] = &cacheEntry{ref: ref, lastIter: c.iter, lset: lset, hash: hash}
}

func (c *ReceiverCache) addDropped(met string) {
	iter := c.iter
	c.droppedSeries[met] = &iter
}

func (c *ReceiverCache) getDropped(met string) bool {
	iterp, ok := c.droppedSeries[met]
	if ok {
		*iterp = c.iter
	}
	return ok
}

func (c *ReceiverCache) trackStaleness(hash uint64, lset labels.Labels) {
	c.seriesCur[hash] = lset
}

func (c *ReceiverCache) forEachStale(f func(labels.Labels) bool) {
	for h, lset := range c.seriesPrev {
		if _, ok := c.seriesCur[h]; !ok {
			if !f(lset) {
				break
			}
		}
	}
}

type cacheEntry struct {
	ref      uint64
	lastIter uint64
	hash     uint64
	lset     labels.Labels
}

// metaEntry holds meta information about a metric.
type metaEntry struct {
	lastIter uint64 // Last scrape iteration the entry was observed at.
	typ      textparse.MetricType
	help     string
}

// targetScraper implements the scraper interface for a target.
type channelReceiver struct {
	recevChannel <-chan []Sample
	buffer       []Sample
	timeout      time.Duration
	offsetDur    time.Duration
	lastStart    time.Time
	lastDuration time.Duration
	lastError    error
}

func (this *channelReceiver) receive(ctx context.Context) error {

	var ok bool
	timer := time.NewTicker(this.timeout)
	select {
	case this.buffer, ok = <-this.recevChannel:
		if !ok {
			fmt.Println("channel closed")
			return errors.New("channel closed")
		}
	case <-timer.C:
		fmt.Println("receive data timeout")
		break
	}
	return nil
}

func (this *channelReceiver) report(start time.Time, dur time.Duration, err error) {
	this.lastStart = start
	this.lastDuration = dur
	this.lastError = err
}

func (this *channelReceiver) offset(interval time.Duration) time.Duration {
	this.offsetDur = interval
	return this.offsetDur
}

type ReceiverLoop struct {
	log                 log.Logger
	receiver            *channelReceiver
	cache               *ReceiverCache
	lastScrapeSize      int
	appender            func() storage.Appender
	sampleMutator       labelsMutator
	reportSampleMutator labelsMutator
	ctx                 context.Context
	scrapeCtx           context.Context
	cancel              func()
	stopped             chan struct{}
}

func newReceiverLoop(ctx context.Context,
	rc *channelReceiver,
	l log.Logger,
	sampleMutator labelsMutator,
	reportSampleMutator labelsMutator,
	appender func() storage.Appender,
	cache *ReceiverCache,
) *ReceiverLoop {
	if l == nil {
		l = log.NewNopLogger()
	}

	if cache == nil {
		cache = newReceiverCache()
	}

	sl := &ReceiverLoop{
		receiver:            rc,
		cache:               cache,
		appender:            appender,
		sampleMutator:       sampleMutator,
		reportSampleMutator: reportSampleMutator,
		stopped:             make(chan struct{}),
		log:                 l,
		ctx:                 ctx,
	}
	sl.scrapeCtx, sl.cancel = context.WithCancel(ctx)
	return sl
}

func (sl *ReceiverLoop) Start() {
	level.Info(sl.log).Log("method", "Start")

	go sl.run(100, 1000, nil)
}

func (sl *ReceiverLoop) run(interval, timeout time.Duration, errc chan<- error) {

	var last time.Time
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

mainLoop:
	for {

		level.Info(sl.log).Log("msg", "do Receiver Channel begin")
		sl.receiver.buffer = []Sample{}
		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		default:
		}

		var (
			start             = time.Now()
			scrapeCtx, cancel = context.WithTimeout(sl.ctx, timeout)
		)

		scrapeErr := sl.receiver.receive(scrapeCtx)
		cancel()
		if scrapeErr == nil {
			// NOTE: There were issues with misbehaving clients in the past
			// that occasionally returned empty results. We don't want those
			// to falsely reset our buffer size.
			if len(sl.receiver.buffer) > 0 {
				sl.lastScrapeSize = len(sl.receiver.buffer)
			}
		} else {
			level.Debug(sl.log).Log("msg", "Scrape failed", "err", scrapeErr.Error())

			if errc != nil {
				errc <- scrapeErr
			}
		}

		level.Info(sl.log).Log("ReceiverScrapeLen", len(sl.receiver.buffer))

		// A failed scrape is the same as an empty scrape,
		// we still call sl.append to trigger stale markers.
		total, added, appErr := sl.append(sl.receiver.buffer)

		if scrapeErr == nil {
			scrapeErr = appErr
		}

		sl.report(start, time.Since(start), total, added, scrapeErr)
		last = start
		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		case <-ticker.C:
		}
	}

	close(sl.stopped)
	sl.endOfRunStaleness(last, ticker, interval)
}

func (sl *ReceiverLoop) endOfRunStaleness(last time.Time, ticker *time.Ticker, interval time.Duration) {
	// Scraping has stopped. We want to write stale markers but
	// the target may be recreated, so we wait just over 2 scrape intervals
	// before creating them.
	// If the context is cancelled, we presume the server is shutting down
	// and will restart where is was. We do not attempt to write stale markers
	// in this case.

	if last.IsZero() {
		// There never was a scrape, so there will be no stale markers.
		return
	}

	// Wait for when the next scrape would have been, record its timestamp.
	var staleTime time.Time
	select {
	case <-sl.ctx.Done():
		return
	case <-ticker.C:
		staleTime = time.Now()
	}

	// Wait for when the next scrape would have been, if the target was recreated
	// samples should have been ingested by now.
	select {
	case <-sl.ctx.Done():
		return
	case <-ticker.C:
	}

	// Wait for an extra 10% of the interval, just to be safe.
	select {
	case <-sl.ctx.Done():
		return
	case <-time.After(interval / 10):
	}

	// Call sl.append again with an empty scrape to trigger stale markers.
	// If the target has since been recreated and scraped, the
	// stale markers will be out of order and ignored.
	if _, _, err := sl.append([]Sample{}); err != nil {
		level.Error(sl.log).Log("msg", "stale append failed", "err", err)
	}
	if err := sl.reportStale(staleTime); err != nil {
		level.Error(sl.log).Log("msg", "stale report failed", "err", err)
	}
}

// Stop the scraping. May still write data and stale markers after it has
// returned. Cancel the context to stop all writes.
func (sl *ReceiverLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

func (sl *ReceiverLoop) append(sample []Sample) (total, added int, err error) {
	var (
		app            = sl.appender()
		numOutOfOrder  = 0
		numDuplicates  = 0
		numOutOfBounds = 0
	)
	var sampleLimitErr error

	for _, s := range sample {
		t := s.Ts
		met, v := s.ToString(), s.Value
		if sl.cache.getDropped(met) {
			continue
		}
		ce, ok := sl.cache.get(met)

		if ok {
			switch err = app.AddFast(ce.lset, ce.ref, t, v); err {
			case storage.ErrNotFound:
				ok = false
			case storage.ErrOutOfOrderSample:
				numOutOfOrder++
				level.Debug(sl.log).Log("msg", "Out of order sample", "series", string(met))
				targetScrapeSampleOutOfOrder.Inc()
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				numDuplicates++
				level.Debug(sl.log).Log("msg", "Duplicate sample for timestamp", "series", string(met))
				targetScrapeSampleDuplicate.Inc()
				continue
			case storage.ErrOutOfBounds:
				numOutOfBounds++
				level.Debug(sl.log).Log("msg", "Out of bounds metric", "series", string(met))
				targetScrapeSampleOutOfBounds.Inc()
				continue
			case errSampleLimit:
				// Keep on parsing output if we hit the limit, so we report the correct
				// total number of samples scraped.
				sampleLimitErr = err
				added++
				continue
			default:
				break
			}
		}
		if !ok {

			var lset labels.Labels

			hash := s.Labels.Hash()

			// Hash label set as it is seen local to the target. Then add target labels
			// and relabeling and store the final label set.
			lset = sl.sampleMutator(s.Labels)

			// The label set may be set to nil to indicate dropping.
			if lset == nil {
				sl.cache.addDropped(s.ToString())
				continue
			}

			var ref uint64
			ref, err = app.Add(lset, t, v)
			// TODO(fabxc): also add a dropped-cache?
			switch err {
			case nil:
			case storage.ErrOutOfOrderSample:
				err = nil
				numOutOfOrder++
				level.Debug(sl.log).Log("msg", "Out of order sample", "series", string(met))
				targetScrapeSampleOutOfOrder.Inc()
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				err = nil
				numDuplicates++
				level.Debug(sl.log).Log("msg", "Duplicate sample for timestamp", "series", string(met))
				targetScrapeSampleDuplicate.Inc()
				continue
			case storage.ErrOutOfBounds:
				err = nil
				numOutOfBounds++
				level.Debug(sl.log).Log("msg", "Out of bounds metric", "series", string(met))
				targetScrapeSampleOutOfBounds.Inc()
				continue
			case errSampleLimit:
				sampleLimitErr = err
				added++
				continue
			default:
				level.Debug(sl.log).Log("msg", "unexpected error", "series", string(met), "err", err)
				break
			}

			sl.cache.addRef(met, ref, lset, hash)
		}
		added++
	}

	if sampleLimitErr != nil {
		if err == nil {
			err = sampleLimitErr
		}
		// We only want to increment this once per scrape, so this is Inc'd outside the loop.
		targetScrapeSampleLimit.Inc()
	}
	if numOutOfOrder > 0 {
		level.Warn(sl.log).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", numOutOfOrder)
	}
	if numDuplicates > 0 {
		level.Warn(sl.log).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", numDuplicates)
	}
	if numOutOfBounds > 0 {
		level.Warn(sl.log).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", numOutOfBounds)
	}

	if err == nil {
		sl.cache.forEachStale(func(lset labels.Labels) bool {
			// Series no longer exposed, mark it stale.
			_, err = app.Add(lset, math.MaxInt64, math.Float64frombits(value.StaleNaN))
			//_, err = app.Add(lset, defTime, math.Float64frombits(value.StaleNaN))
			switch err {
			case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
				// Do not count these in logging, as this is expected if a target
				// goes away and comes back again with a new scrape loop.
				err = nil
			}
			return err == nil
		})
	}

	if err != nil {
		app.Rollback()
		return total, added, err
	}
	if err := app.Commit(); err != nil {
		return total, added, err
	}

	sl.cache.iterDone()

	return total, added, nil
}

// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
// with scraped metrics in the cache.
const (
	scrapeHealthMetricName       = "up" + "\xff"
	scrapeDurationMetricName     = "receiver_duration_seconds" + "\xff"
	scrapeSamplesMetricName      = "receiver_samples_scraped" + "\xff"
	samplesPostRelabelMetricName = "receiver_samples_post_metric_relabeling" + "\xff"
)

func (sl *ReceiverLoop) report(start time.Time, duration time.Duration, scraped, appended int, err error) error {
	sl.receiver.report(start, duration, err)

	ts := timestamp.FromTime(start)

	var health float64
	if err == nil {
		health = 1
	}
	app := sl.appender()

	if err := sl.addReportSample(app, scrapeHealthMetricName, ts, health); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeDurationMetricName, ts, duration.Seconds()); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeSamplesMetricName, ts, float64(scraped)); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, samplesPostRelabelMetricName, ts, float64(appended)); err != nil {
		app.Rollback()
		return err
	}
	return app.Commit()
}

func (sl *ReceiverLoop) reportStale(start time.Time) error {
	ts := timestamp.FromTime(start)
	app := sl.appender()

	stale := math.Float64frombits(value.StaleNaN)

	if err := sl.addReportSample(app, scrapeHealthMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeDurationMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeSamplesMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, samplesPostRelabelMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	return app.Commit()
}

func (sl *ReceiverLoop) addReportSample(app storage.Appender, s string, t int64, v float64) error {
	ce, ok := sl.cache.get(s)
	if ok {
		err := app.AddFast(ce.lset, ce.ref, t, v)
		switch err {
		case nil:
			return nil
		case storage.ErrNotFound:
			// Try an Add.
		case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
			// Do not log here, as this is expected if a target goes away and comes back
			// again with a new scrape loop.
			return nil
		default:
			return err
		}
	}
	lset := labels.Labels{
		// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
		// with scraped metrics in the cache.
		// We have to drop it when building the actual metric.
		labels.Label{Name: labels.MetricName, Value: s[:len(s)-1]},
	}

	hash := lset.Hash()
	lset = sl.reportSampleMutator(lset)

	ref, err := app.Add(lset, t, v)
	switch err {
	case nil:
		sl.cache.addRef(s, ref, lset, hash)
		return nil
	case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
		return nil
	default:
		return err
	}
}
