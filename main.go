package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	requestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "shield",
			Name:      "requests_total",
			Help:      "Total number of requests received by prometheus shield",
		}, []string{"method", "path"},
	)
	hitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "shield",
			Name:      "requests_cache_hit_total",
			Help:      "Total number of requests served from cache",
		}, []string{"method", "path"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prometheus",
			Subsystem: "shield",
			Name:      "request_duration_seconds",
			Help:      "Duration of request made handled by prometheus shield",
			Buckets:   prometheus.ExponentialBuckets(0.01, 5, 3),
		},
		[]string{"method", "path"},
	)
	errorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "shield",
			Name:      "errors_total",
			Help:      "Total number of error encountered in prometheus shield",
		},
		[]string{"area"},
	)
)

// Cache Item //
type cacheItem struct {
	content []byte
	expires time.Time
}

func newCacheItem(content []byte, ttl time.Duration) *cacheItem {
	return &cacheItem{
		content: content,
		expires: time.Now().Add(ttl),
	}
}

func (i *cacheItem) expired() bool {
	return i.expires.Before(time.Now())
}

// Cache //
type cache struct {
	sync.RWMutex
	cache map[string]*cacheItem
}

func (c *cache) cacheKey(req *http.Request) string {
	return req.Method + req.URL.String()
}
func (c *cache) get(req *http.Request) *cacheItem {
	key := c.cacheKey(req)
	c.RLock()
	item, ok := c.cache[key]
	c.RUnlock()
	if !ok {
		return nil
	}
	if item.expired() {
		return nil
	}
	return item
}

func (c *cache) store(req *http.Request, resp *http.Response, ttl time.Duration) error {
	respB, err := httputil.DumpResponse(resp, true)
	if err != nil {
		return err
	}
	key := c.cacheKey(req)
	c.Lock()
	c.cache[key] = newCacheItem(respB, ttl)
	c.Unlock()
	return nil
}

type proxy struct {
	http.RoundTripper
	*httputil.ReverseProxy
	cache cache
	ttl   time.Duration
}

func (p *proxy) RoundTrip(req *http.Request) (*http.Response, error) {
	ci := p.cache.get(req)
	if ci != nil {
		log.Debug("	- cache hit")
		hitCounter.WithLabelValues(req.Method, req.URL.Path).Inc() // Path is bounded by switch in serveHTTP
		return http.ReadResponse(bufio.NewReader(bytes.NewBuffer(ci.content)), req)
	}
	log.Debug("	- cache miss")

	resp, err := p.RoundTripper.RoundTrip(req)
	if err != nil {
		errorCounter.WithLabelValues("RoundTrip").Inc()
		return nil, err
	}

	if err := p.cache.store(req, resp, p.ttl); err != nil {
		errorCounter.WithLabelValues("CacheStore").Inc()
		log.Warn(err)
	}
	return resp, nil
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, reqIn *http.Request) {
	log.WithFields(log.Fields{"method": reqIn.Method}).Info(reqIn.URL.String())
	boundedPath := reqIn.URL.Path
	start := time.Now()

	now := time.Now()
	nowHour := now.Truncate(1 * time.Hour)
	oneHourAgo := now.Add(-1 * time.Hour)

	defer func() {
		requestCounter.WithLabelValues(reqIn.Method, boundedPath).Inc()
		requestDuration.WithLabelValues(reqIn.Method, boundedPath).Observe(float64(time.Since(start)))
	}()
	switch reqIn.URL.Path {
	case "/api/v1/series":
		req := &http.Request{}
		*req = *reqIn
		req.URL.RawQuery = url.Values{
			"start": []string{strconv.FormatInt(nowHour.Add(-1*time.Hour).Unix(), 10)},
			"end":   []string{strconv.FormatInt(nowHour.Unix(), 10)},
			"match": req.URL.Query()["match"],
		}.Encode()
		p.ReverseProxy.ServeHTTP(w, req)

	case "/api/v1/label/__name__/values":
		req := &http.Request{}
		*req = *reqIn
		req.URL.RawQuery = ""
		p.ReverseProxy.ServeHTTP(w, req)

	case "/api/v1/query":
		req := &http.Request{}
		*req = *reqIn
		req.URL.RawQuery = url.Values{
			"query": req.URL.Query()["query"],
		}.Encode()
		p.ReverseProxy.ServeHTTP(w, req)

	case "/api/v1/query_range":
		req := &http.Request{}
		*req = *reqIn // Copy request

		params := req.URL.Query()

		start, err := timeStr(params.Get("start"))
		if err != nil {
			errorCounter.WithLabelValues("ParseRequest").Inc()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if start.Before(oneHourAgo) {
			log.Debugf("  start(%s) is earlier than oneHourAgo(%s)", start, oneHourAgo)
			start = oneHourAgo
		}
		if start.After(now) {
			log.Debugf("  start(%s) is after now(%s)", start, now)
			start = now
		}

		end, err := timeStr(params.Get("end"))
		if err != nil {
			errorCounter.WithLabelValues("ParseRequest").Inc()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if end.Before(oneHourAgo) {
			log.Debugf("  end(%s) is earlier than oneHourAgo(%s)", end, oneHourAgo)
			end = oneHourAgo
		}
		if end.After(now) {
			log.Debugf("  end(%s) is after now(%s)", end, now)
			end = now
		}

		step, err := strconv.Atoi(params.Get("step"))
		if err != nil {
			errorCounter.WithLabelValues("ParseRequest").Inc()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if step < 15 {
			step = 15
		}

		req.URL = &url.URL{
			Scheme: req.URL.Scheme,
			Path:   req.URL.Path,
			RawQuery: url.Values{
				"query": params["query"],
				"start": []string{strconv.FormatInt(start.Truncate(p.ttl).Unix(), 10)},
				"end":   []string{strconv.FormatInt(end.Truncate(p.ttl).Unix(), 10)},
				"step":  []string{strconv.Itoa(step)},
			}.Encode(),
		}
		p.ReverseProxy.ServeHTTP(w, req)

	default:
		boundedPath = "" // Make sure path is bounded
		errorCounter.WithLabelValues("NotAllowed").Inc()
		http.Error(w, "Not allowed", http.StatusForbidden)
	}
}

func newProxy(promURL *url.URL, ttl time.Duration) *proxy {
	p := &proxy{
		ReverseProxy: httputil.NewSingleHostReverseProxy(promURL),
		RoundTripper: http.DefaultTransport,
		cache:        cache{cache: map[string]*cacheItem{}},
		ttl:          ttl,
	}
	p.ReverseProxy.Transport = p // hrmm...
	return p
}

func main() {
	var (
		promAddr   = flag.String("-u", "http://localhost:9090", "URL of prometheus server")
		listenAddr = flag.String("-l", "0.0.0.0:9191", "Address to listen on")
		ttln       = flag.Int("-t", 60, "TTL")
	)

	flag.Parse()
	promURL, err := url.Parse(*promAddr)
	if err != nil {
		log.Fatal(err)
	}
	ttl := time.Duration(*ttln) * time.Second
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", newProxy(promURL, ttl))
	log.Fatal(http.ListenAndServe(*listenAddr, nil))
}

func timeStr(ts string) (t time.Time, err error) {
	start, err := strconv.Atoi(ts)
	if err != nil {
		return t, fmt.Errorf("Couldn't parse %s: %s", ts, err)
	}
	return time.Unix(int64(start), 0), nil
}
