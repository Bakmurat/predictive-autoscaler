package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"
)

// What a reconcile may wait for the two services it calls before it decides (R3-01): the forecasting service and the
// metrics backend. Both are called inside Reconcile, and one worker reconciles every autoscaler in turn
// (SetupWithManager), so whatever one request waits is added to the decision of every autoscaler queued behind it. A
// forecasting service that accepted the connection and never answered used to hold each reconcile for the HTTP client's
// 120 s, and a hung metrics backend for 10 s, per autoscaler, every round.
//
// The wait is accounted cumulatively, per service (waitAllowance): every request draws its deadline from an allowance,
// and the time it really waited is charged back, whatever came of it: an answer, an error status, a refusal or a
// timeout. A slow success costs what a timeout costs.
//
//	one request waits at most                       waitPolicy.Request   5 s (its context deadline: connect, headers, body)
//	the fleet allowance holds at most               waitPolicy.Request   5 s
//	  and refills completely in                     waitPolicy.Fleet    30 s (the shortest decision cadence)
//	each autoscaler's own allowance holds at most   waitPolicy.Request   5 s
//	  and refills completely in                     waitPolicy.Key      60 s
//	a request is sent only with at least            waitPolicy.Minimum   250 ms of both allowances left
//
// A request's deadline is the smaller of the two allowances. So all requests to one service together wait, in any
// window of W seconds, at most 5 s + W/6 (10 s in a 30 s round that starts with a full allowance, 5 s per 30 s
// sustained), plus at most one request's overrun past its deadline, which is charged too and repaid before the next
// request is sent. That holds for any number of autoscalers and any mix of answers. The autoscaler's own allowance is
// for fairness: one whose requests hang can take the whole fleet allowance once, then only what its own allowance has
// regained (half in 30 s), which leaves the rest to the others. Recovery needs no special state: answers that come
// quickly cost little, both allowances refill with time, and the next request gets a full deadline again.
//
// The figures are sized, not guaranteed, for about 20 autoscalers on one operator: 20 answers of 250 ms each are one
// round's refill. With more autoscalers, or slower answers, some requests of a round are not sent; that is the price
// of the bound. Nothing here limits the other waits of a reconcile (the Kubernetes API: the startup lists, 3 s each;
// the coexistence check, 10 s; the identity guard, 5 s; the scale write, 10 s), which are bounded per reconcile only.
//
// When a request is not sent, or runs out of its deadline (it is then cancelled), the reconcile continues as after any
// failed request. Forecast: a v1 cached forecast under predictionStaleMax may stand in, otherwise the decision is
// reactive. Metrics: the rate is unavailable, never zero, and the decision holds. Nothing runs outside the reconcile,
// so there is still one writer per autoscaler and one decision per interval, and a cancelled request leaves nothing
// behind that a later decision could use.
//
// Three things stay distinct, here and in the ledger:
//
//  1. The forecasting service issued an answer. It journals an issuance when it has computed one, whether or not the
//     operator is still listening. The operator cannot know this for a request it cancelled: a cancelled request may
//     have left a committed issuance on the service. That is not a fault, and it is never evidence of delivery or use.
//  2. The operator received an answer before its deadline, complete and verified. v1: the attempt's outcome is
//     decoded_response and the lookup's resolution fresh_response. v2: the ledger's v2.request is "answered" and
//     v2.issuance_id is set. Whether it is eligible is the verdict (v2.service_eligible, v2.verdict).
//  3. The operator used it in a decision: forecast_status "used" (v2.accepted true).
//
// A request that ran out of its deadline is outcome "timeout" (v1) or v2.request "timeout"; one that was not sent is
// "not_sent". Neither says anything about 1.
type waitPolicy struct {
	Request time.Duration // the longest one request waits, and what each allowance holds when full
	Fleet   time.Duration // the fleet allowance refills completely in this time
	Key     time.Duration // an autoscaler's own allowance refills completely in this time
	Minimum time.Duration // with less than this left, no request is sent
}

// Variables so that tests can shorten them; production never changes them.
var (
	forecastWaitPolicy = waitPolicy{Request: 5 * time.Second, Fleet: time.Duration(minReconcileSeconds) * time.Second,
		Key: time.Minute, Minimum: 250 * time.Millisecond}
	metricsWaitPolicy = forecastWaitPolicy
)

// waitBucket is one allowance: what is left, as of at. Negative after an overrun: it is repaid before more is granted.
type waitBucket struct {
	left time.Duration
	at   time.Time
}

func (b *waitBucket) refill(now time.Time, full, period time.Duration) {
	if b.at.IsZero() {
		b.left, b.at = full, now
		return
	}
	if d := now.Sub(b.at); d > 0 {
		// d/period of a full allowance, computed in float64: d * full overflows int64 after a long idle time.
		b.left += time.Duration(float64(full) * (float64(d) / float64(period)))
		if b.left > full || d >= 2*period {
			b.left = full
		}
		b.at = now
	}
}

// waitAllowance accounts the waits for one service. Only the single reconcile worker uses it (no lock).
type waitAllowance struct {
	policy *waitPolicy
	now    func() time.Time // nil: time.Now (the wall clock: a wait is real time, not r.Clock)
	fleet  waitBucket
	keys   map[string]*waitBucket
	// waited is everything charged so far (tests assert the bound on it).
	waited time.Duration
}

func (a *waitAllowance) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

func (a *waitAllowance) bucket(key string, now time.Time) *waitBucket {
	if a.keys == nil {
		a.keys = map[string]*waitBucket{}
	}
	b := a.keys[key]
	if b == nil {
		if len(a.keys) >= 1024 { // deleted autoscalers: an allowance that is full again is the same as none
			for k, old := range a.keys {
				if old.refill(now, a.policy.Request, a.policy.Key); old.left >= a.policy.Request {
					delete(a.keys, k)
				}
			}
		}
		b = &waitBucket{}
		a.keys[key] = b
	}
	b.refill(now, a.policy.Request, a.policy.Key)
	return b
}

// grant is the deadline one request of the autoscaler key may have now: the smaller of the fleet's and its own
// allowance. ok is false when that is below the minimum: the request is then not sent.
func (a *waitAllowance) grant(key string) (time.Duration, bool) {
	now := a.clock()
	a.fleet.refill(now, a.policy.Request, a.policy.Fleet)
	d := a.bucket(key, now).left
	if a.fleet.left < d {
		d = a.fleet.left
	}
	return d, d >= a.policy.Minimum
}

// charge books what a request really waited against both allowances, whatever its result.
func (a *waitAllowance) charge(key string, waited time.Duration) {
	if waited < 0 {
		waited = 0
	}
	now := a.clock()
	a.fleet.refill(now, a.policy.Request, a.policy.Fleet)
	a.fleet.left -= waited
	a.bucket(key, now).left -= waited
	a.waited += waited
}

// errNotAsked marks a request that was not sent because the wait allowance did not cover the minimum deadline.
var errNotAsked = stderrors.New("not asked: the wait allowance is used up")

// bounded gives one request of the autoscaler key to a service its deadline. The caller passes the request's error to
// done when it returns (also nil): done releases the context, charges the time waited, and names the deadline in the
// error of a request that ran out of it. Without enough allowance no context is returned and the error wraps
// errNotAsked.
func (a *waitAllowance) bounded(ctx context.Context, key, service string) (context.Context, func(error) error, error) {
	budget, ok := a.grant(key)
	if !ok {
		return nil, nil, fmt.Errorf("%s was %w (%s left, at least %s needed); the decision does not wait for it",
			service, errNotAsked, budget.Round(time.Millisecond), a.policy.Minimum)
	}
	start := a.clock()
	bctx, cancel := context.WithTimeout(ctx, budget)
	return bctx, func(err error) error {
		cancel()
		a.charge(key, a.clock().Sub(start))
		if !timeoutError(err) {
			return err // answered (also with an error status) or failed at once; the time it took is charged all the same
		}
		return fmt.Errorf("%s did not answer within %s; the decision does not wait longer: %w", service,
			budget.Round(time.Millisecond), err)
	}, nil
}

// forecastContext bounds one request to the forecasting service (see the top of this file).
func (r *PredictiveAutoscalerReconciler) forecastContext(ctx context.Context, key string) (context.Context, func(error) error, error) {
	if r.forecastWait.policy == nil {
		r.forecastWait.policy = &forecastWaitPolicy
	}
	return r.forecastWait.bounded(ctx, key, "the forecasting service")
}

// measureRPM is queryCurrentRPM under the metrics wait allowance. Whatever goes wrong (not asked, no answer in time,
// an unusable answer) is an error: the rate is then unavailable and the decision holds. It is never zero traffic.
func (r *PredictiveAutoscalerReconciler) measureRPM(ctx context.Context, key, query string) (float64, error) {
	if r.metricsWait.policy == nil {
		r.metricsWait.policy = &metricsWaitPolicy
	}
	mctx, done, err := r.metricsWait.bounded(ctx, key, "the metrics backend")
	if err != nil {
		return 0, fmt.Errorf("%w: %v", errMetricsUnavailable, err)
	}
	rpm, err := r.queryCurrentRPM(mctx, query)
	if err = done(err); err != nil {
		return 0, err
	}
	return rpm, nil
}
