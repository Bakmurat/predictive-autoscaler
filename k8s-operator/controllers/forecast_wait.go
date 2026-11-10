package controllers

import (
	"context"
	"fmt"
	"time"
)

// How long a decision waits for the forecasting service (R3-01). The forecast is requested inside Reconcile, before the
// measurement and the write, and one worker reconciles every autoscaler in turn (SetupWithManager), so whatever one
// request waits is added to the reactive decision of every autoscaler queued behind it. A service that accepts the
// connection and never answers, or whose packets are dropped, used to hold each reconcile for the HTTP client's 120 s.
//
// Every request to the forecasting service (v1 /predict and v2 /v2/predict) now runs under a wait budget, enforced as
// the request's context deadline (connect, response headers and body together); when it runs out the request is
// cancelled and the reconcile continues exactly as after any transport failure: a recent cached v1 forecast may stand
// in (predictionStaleMax), otherwise the decision is reactive. Nothing else changes: no request runs outside the
// reconcile, so there is still one writer per autoscaler and one decision per interval, and a cancelled request leaves
// nothing behind that a later decision could use.
//
// The budget is derived from the decision cadence and the number of autoscalers one operator is sized for:
//
//	shortest decision cadence               T = minReconcileSeconds                    = 30 s
//	share of a round the service may cost   T / 3                                      = 10 s
//	autoscalers per operator                N = forecastSupportedAutoscalers           = 20
//	full budget (at most one per minute)    forecastWaitBudget                         =  5 s
//	probe budget (every other request)      forecastProbeBudget = (T/3 - 5 s) / N      = 250 ms
//
// A request gets the full budget unless one ran out of it less than forecastRearmInterval ago; until then every request,
// of every autoscaler, gets the probe budget. So a service that does not answer delays a round of N decisions by at
// most forecastWaitBudget + N x forecastProbeBudget = 10 s in the minute it stops answering and N x forecastProbeBudget
// = 5 s per round afterwards (plus one full budget per forecastRearmInterval), and a single decision by at most
// forecastWaitBudget. With more than N autoscalers each adds one probe budget per round. A service that is up but
// needs longer than the probe budget is asked with the full budget again within forecastRearmInterval; an answer that
// needs longer than the full budget is treated like an outage. A refused connection fails at once and costs nothing.
// The rule is by time only, and for all autoscalers together: an answer for one autoscaler does not restore the full
// budget, so a service that hangs on some requests and answers others cannot make every round pay the full budget.
const forecastSupportedAutoscalers = 20

// Variables so that tests can shorten them; production never changes them.
var (
	forecastWaitBudget    = 5 * time.Second
	forecastProbeBudget   = 250 * time.Millisecond
	forecastRearmInterval = time.Minute
)

// forecastContext bounds one request to the forecasting service by the current wait budget. The caller passes the
// request's error to answered when it returns (also nil): answered releases the context, notes a request that ran out
// of the full budget, and names the budget in the error it returns. Only the single reconcile worker calls it.
func (r *PredictiveAutoscalerReconciler) forecastContext(ctx context.Context) (context.Context, func(error) error) {
	budget, full := forecastWaitBudget, true
	if now := time.Now(); now.Before(r.forecastFullBudgetAt) { // the wall clock: the wait is real time, not r.Clock
		budget, full = forecastProbeBudget, false
	}
	fctx, cancel := context.WithTimeout(ctx, budget)
	return fctx, func(err error) error {
		cancel()
		if !timeoutError(err) {
			return err // answered (a forecast, a refusal, an error status) or failed at once: nothing was waited out
		}
		if full {
			r.forecastFullBudgetAt = time.Now().Add(forecastRearmInterval)
		}
		return fmt.Errorf("the forecasting service did not answer within %s; the decision does not wait longer: %w", budget, err)
	}
}
