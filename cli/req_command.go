// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"math"
	"os/signal"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/choria-io/fisk"
	"github.com/nats-io/nats.go"
	iu "github.com/nats-io/natscli/internal/util"
)

type reqCmd struct {
	subject        string
	body           string
	bodyIsSet      bool
	req            bool
	replyTo        string
	raw            bool
	hdrs           []string
	cnt            int
	replyCount     int
	replyTimeout   time.Duration
	forceStdin     bool
	translate      string
	sendOn         string
	quiet          bool
	templates      bool
	sleep          time.Duration
	templateScript string
	workerCount    int
	errCount       atomic.Int64
	successCount   atomic.Int64
	toPrintStats   bool
	mx             sync.Mutex
	durations      []time.Duration
	errorHeader    map[string]string
	showProgress   bool
}

type reqItem struct {
	nc   *nats.Conn
	pub  *iu.Publisher
	body string
}

func configureReqCommand(app commandHost) {
	c := &reqCmd{errorHeader: make(map[string]string)}

	requestHelp := `Body and Header values of the messages may use Go templates to
create unique messages.

   nats request test --count 10 "Message {{Count}} @ {{Time}}"

Multiple messages with random strings between 10 and 100 long:

   nats request test --count 10 "Message {{Count}}: {{ Random 10 100 }}"

Multiple messages from STDIN with 20 concurrent workers:

   nats request test --send-on newline --force-stdin --workers 20 < FILE

Available template functions are:

   Count                the message number
   TimeStamp            RFC3339 format current time
   Unix                 seconds since 1970 in UTC
   UnixNano             nano seconds since 1970 in UTC
   Time                 the current time
   ID                   an unique ID
   UUID                 a random UUID
   Random(min, max)     random string at least min long, at most max
   RandomInt(min, max)  random integer at least min long, at most max
`

	req := app.Command("request", "Generic request-reply request utility").Alias("req").Action(c.requestAction)
	req.HelpLong(requestHelp)
	req.Arg("subject", "Subject to subscribe to").Required().StringVar(&c.subject)
	req.Arg("body", "Message body").IsSetByUser(&c.bodyIsSet).StringVar(&c.body)
	req.Flag("wait", "Wait for a reply from a service").Short('w').Default("true").Hidden().BoolVar(&c.req)
	req.Flag("raw", "Show just the output received").Short('r').UnNegatableBoolVar(&c.raw)
	req.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	req.Flag("count", "Publish multiple messages").Default("1").IntVar(&c.cnt)
	req.Flag("replies", "Wait for multiple replies from services. 0 waits until timeout").Default("1").IntVar(&c.replyCount)
	req.Flag("reply-timeout", "Maximum time between replies when waiting for more than one").Default("300ms").DurationVar(&c.replyTimeout)
	req.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.translate)
	req.Flag("force-stdin", "Force reading from stdin").UnNegatableBoolVar(&c.forceStdin)
	req.Flag("send-on", "When to send data from stdin: 'eof' (default) or 'newline'").Default("eof").EnumVar(&c.sendOn, "newline", "eof")
	req.Flag("templates", "Enables template functions in the body and subject (does not affect headers)").Default("true").BoolVar(&c.templates)
	req.Flag("init-template", "Template expression to be used in templates (intended to use SetVar)").StringVar(&c.templateScript)
	req.Flag("workers", "Worker count").Default("1").IntVar(&c.workerCount)
	req.Flag("print-stats", "Print statistics after all messages sent").Default("false").BoolVar(&c.toPrintStats)
	req.Flag("error-by-header", "Count errors if header values present in response").StringMapVar(&c.errorHeader)
	req.Flag("show-progress", "Show progress explicitly (when read from STDIN").BoolVar(&c.showProgress)
}

func init() {
	registerCommand("req", 11, configureReqCommand)
}

func (c *reqCmd) doReq(ctx context.Context, nc *nats.Conn, pub *iu.Publisher, body string) {
	logOutput := !c.raw && pub.Tracker == nil

	for i := 1; i <= c.cnt; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		bdy, subj, vars, bodyErr, subjErr := pub.ParseTemplates(body, c.subject, i)
		if logOutput {
			log.Printf("Sending request on %q: %q\n", subj, bdy)
		}

		if bodyErr != nil {
			log.Printf("Could not parse body template: %s", bodyErr)
		}
		if subjErr != nil {
			log.Printf("Could not parse subject template: %s", subjErr)
		}
		if logOutput {
			log.Printf("Sending request on %q\n", subj)
		}

		msg, err := pub.PrepareMsg(subj, c.replyTo, []byte(bdy), c.hdrs, i, vars)
		if err != nil {
			return
		}

		msg.Reply = nc.NewRespInbox()

		s, err := nc.SubscribeSync(msg.Reply)
		if err != nil {
			return
		}

		err = nc.PublishMsg(msg)
		if err != nil {
			return
		}

		if pub.Tracker != nil {
			pub.Tracker.Increment(1)
		}

		// loop through the reply count.
		start := time.Now()

		// Honor the overall timeout for the first response.  No
		// responders will circuit break.
		timeout := opts().Timeout

		// loop until reply count is met, or if zero, until we
		// timeout receiving messages.
		rc := 0
		var rttAg time.Duration
		for {
			m, err := s.NextMsg(timeout)
			if err != nil {
				c.incErrCount()
				if err == nats.ErrTimeout {
					if c.cnt == 1 {
						return
					}
					// continue to publish additional messages.
					break
				}
				if err == nats.ErrNoResponders {
					log.Printf("No responders are available")
				}
				return
			}
			if c.errorIfHeader() && len(m.Header) > 0 && c.ifErrorByHeader(m.Header) {
				c.incErrCount()
			} else {
				c.incSuccessCount()
			}

			rtt := time.Since(start)
			c.mx.Lock()
			c.durations = append(c.durations, rtt)
			c.mx.Unlock()

			switch {
			case c.raw:
				outPutMSGBody(m.Data, c.translate, m.Subject, "")
			case logOutput:
				log.Printf("Received with rtt %v", rtt)

				if len(m.Header) > 0 {
					for h, vals := range m.Header {
						for _, val := range vals {
							log.Printf("%s: %s", h, val)
						}
					}
					log.Println()
				}

				outPutMSGBody(m.Data, c.translate, m.Subject, "")
			}

			rc++
			if c.replyCount > 0 && rc == c.replyCount {
				break
			}

			if c.replyCount == 0 {
				// if we are waiting for the general timeout then
				// calculate remaining
				timeout = opts().Timeout - time.Since(start)
			} else {
				// Otherwise, use the average response deltas
				rttAg += rtt
				timeout = rttAg/time.Duration(rc) + c.replyTimeout
			}
		}

		// Unsubscribe for the unbound case, NOOP is already auto unsubscribed.
		s.Unsubscribe()

		// If applicable, account for the wait duration in a publish sleep.
		if c.cnt > 1 && c.sleep > 0 {
			st := c.sleep - time.Since(start)
			if st > 0 {
				time.Sleep(st)
			}
		}
	}
	return
}

func (c *reqCmd) requestAction(_ *fisk.ParseContext) error {
	reqQueue := make(chan reqItem)
	c.durations = make([]time.Duration, 0, c.cnt*c.workerCount)

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer cancel()

	wg := sync.WaitGroup{}
	go c.runPool(ctx, &wg, reqQueue)

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	if c.cnt < 1 {
		c.cnt = math.MaxInt16
	}

	pub, err := iu.NewPublisher(iu.PublisherConfig{
		BodyIsSet:      c.bodyIsSet,
		ForceStdin:     c.forceStdin,
		Count:          c.cnt,
		Raw:            c.raw,
		Templates:      c.templates,
		TemplateScript: c.templateScript,
		Opts:           opts(),
		ShowProgress:   c.showProgress,
	})
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() {
		pub.StopProgress()
		if c.toPrintStats {
			c.printStats(time.Since(start), c.durations)
		}
	}()

	if c.sendOn == "newline" {
		pub.SetSendOnNewLine()
	}

	if pub.UseStdin && !c.quiet {
		log.Println("Reading payload from STDIN")
	}

	eof := c.bodyIsSet
	err = pub.Run(ctx, func() error {
		defer close(reqQueue)
		for {
			body := c.body
			if pub.UseStdin {
				var newEof bool
				var err error
				body, newEof, err = pub.ReadStdin()
				if err != nil {
					return err
				}
				if newEof {
					eof = true
				}
				if body == "" && eof {
					return nil
				}
			}

			reqQueue <- reqItem{nc: nc, pub: pub, body: body}
			if pub.IsSendOnEOF() || eof {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	})
	wg.Wait()

	return err
}

func (c *reqCmd) runPool(ctx context.Context, wg *sync.WaitGroup, queue chan reqItem) {
	for range c.workerCount {
		wg.Go(func() {
			for item := range queue {
				c.doReq(ctx, item.nc, item.pub, item.body)
			}
		})
	}
}

// Make time durations a bit prettier.
func (c *reqCmd) fmtDur(t time.Duration) time.Duration {
	// e.g 234us, 4.567ms, 1.234567s
	return t.Truncate(time.Microsecond)
}

func (c *reqCmd) printStats(duration time.Duration, durations []time.Duration) {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var highestTrackableValue int64
	if len(durations) > 0 {
		highestTrackableValue = int64(durations[len(durations)-1])
	}
	h := hdrhistogram.New(1, highestTrackableValue, 5)
	for _, d := range durations {
		h.RecordValue(int64(d))
	}

	log.Printf("\n=====  Statistics =====\n"+
		"    time taken: %s (%.2f RPS)\n"+
		"    requests succeeded: %d/%d (%.2f%%)\n"+
		"    request errors: %d/%d (%.2f%%)\n"+
		"=====  RTT Percentiles: =====\n"+
		"    50:       %v\n"+
		"    75:       %v\n"+
		"    90:       %v\n"+
		"    99:       %v\n"+
		"    100:      %v\n",
		c.fmtDur(duration), c.rps(duration),
		c.successCount.Load(), c.allReqCount(), c.successPercent(),
		c.errCount.Load(), c.allReqCount(), c.errPercent(),
		c.fmtDur(time.Duration(h.ValueAtQuantile(50))),
		c.fmtDur(time.Duration(h.ValueAtQuantile(75))),
		c.fmtDur(time.Duration(h.ValueAtQuantile(90))),
		c.fmtDur(time.Duration(h.ValueAtQuantile(99))),
		c.fmtDur(time.Duration(h.ValueAtQuantile(100))),
	)
}

func (c *reqCmd) incErrCount() {
	c.errCount.Add(1)
}

func (c *reqCmd) incSuccessCount() {
	c.successCount.Add(1)
}

func (c *reqCmd) allReqCount() int64 {
	return c.successCount.Load() + c.errCount.Load()
}

func (c *reqCmd) successPercent() float64 {
	return 100 * float64(c.successCount.Load()) / float64(c.allReqCount())
}

func (c *reqCmd) errPercent() float64 {
	return 100 * float64(c.errCount.Load()) / float64(c.allReqCount())
}

func (c *reqCmd) rps(duration time.Duration) float64 {
	if duration == 0 {
		// impossible(?) but just to be sure and avoid division by zero
		return 0
	}
	return float64(c.allReqCount()) / duration.Seconds()
}

func (c *reqCmd) errorIfHeader() bool {
	return len(c.errorHeader) > 0
}

func (c *reqCmd) ifErrorByHeader(header nats.Header) bool {
	for h, v := range header {
		for k, vv := range c.errorHeader {
			if h == k {
				if slices.Contains(v, vv) {
					return true
				}
			}
		}
	}
	return false
}
