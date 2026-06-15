package rate

import (
	"math"
	"sync"
	"time"
)

// InfDuration is returned by Delay/DelayFrom when the reservation is not OK.
const InfDuration = time.Duration(math.MaxInt64)

// Reservation is a held reservation produced by Reserve/ReserveN. Its usage
// methods mirror golang.org/x/time/rate.Reservation. Unlike upstream, Cancel
// runs a Redis round trip, so a Cancel failure surfaces via the limiter's error
// handler (the credit is then simply not returned — a fail-safe, stricter
// outcome). The added Err field reports the error from the originating Reserve.
type Reservation struct {
	ok        bool
	timeToAct time.Time
	tokens    int
	lim       *Limiter
	cancelTok string

	// Err is set when the Reserve call hit a Redis error (additive vs upstream).
	Err error

	mu       sync.Mutex
	canceled bool
}

// OK reports whether the limiter can provide the events within its constraints.
func (r *Reservation) OK() bool { return r != nil && r.ok }

// Delay is shorthand for DelayFrom(time.Now()).
func (r *Reservation) Delay() time.Duration { return r.DelayFrom(time.Now()) }

// DelayFrom returns the duration the caller must wait, from t, before acting.
// It returns InfDuration if the reservation is not OK. t is on the caller's
// local clock; timeToAct was built as localNow+waitMicros (DESIGN sec 5.2), so
// no server clock leaks in.
func (r *Reservation) DelayFrom(t time.Time) time.Duration {
	if r == nil || !r.ok {
		return InfDuration
	}
	d := r.timeToAct.Sub(t)
	if d < 0 {
		return 0
	}
	return d
}

// Cancel is shorthand for CancelAt(time.Now()).
func (r *Reservation) Cancel() { r.CancelAt(time.Now()) }

// CancelAt returns the reserved credit to the limiter, indicating the caller
// will not perform the reserved action. It is idempotent and a no-op for a
// not-OK reservation or a best-effort algorithm (whose cancel token is empty).
// The t argument is accepted for parity but ignored (the server clock is
// canonical).
func (r *Reservation) CancelAt(_ time.Time) {
	if r == nil || !r.ok {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.canceled || r.cancelTok == "" {
		return
	}
	r.canceled = true
	if err := r.lim.cancel(r.cancelTok, r.tokens); err != nil {
		r.lim.reportErr(err)
	}
}
