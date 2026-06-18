package rate

import (
	"context"
	"reflect"
	"testing"
	"time"

	xrate "golang.org/x/time/rate"
)

// usageSurface is the x/time/rate usage interface that must hold identically for
// our Limiter and the upstream *xrate.Limiter. If either drifts, the build
// breaks. Reserve/ReserveN are excluded here because they
// return each package's own *Reservation; they are covered by the reflection
// test below.
type usageSurface interface {
	Allow() bool
	AllowN(time.Time, int) bool
	Wait(context.Context) error
	WaitN(context.Context, int) error
	SetLimit(xrate.Limit)
	SetLimitAt(time.Time, xrate.Limit)
	SetBurst(int)
	SetBurstAt(time.Time, int)
	Limit() xrate.Limit
	Burst() int
	Tokens() float64
	TokensAt(time.Time) float64
}

var (
	_ usageSurface = (*Limiter)(nil)
	_ usageSurface = (*xrate.Limiter)(nil)
)

// documentedMissing lists upstream methods we intentionally do not mirror.
// Currently empty: we implement the entire usage surface.
var documentedMissing = map[string]bool{}

// TestUpstreamLimiterCoverage fails loudly when xrate.Limiter gains a method we
// do not implement — the early-warning signal that upstream has moved.
func TestUpstreamLimiterCoverage(t *testing.T) {
	ours := reflect.TypeOf(&Limiter{})
	up := reflect.TypeOf(&xrate.Limiter{})
	for i := 0; i < up.NumMethod(); i++ {
		name := up.Method(i).Name
		if documentedMissing[name] {
			continue
		}
		if _, ok := ours.MethodByName(name); !ok {
			t.Errorf("xrate.Limiter has method %q not implemented by our Limiter", name)
		}
	}
}

// TestUpstreamReservationCoverage does the same for Reservation.
func TestUpstreamReservationCoverage(t *testing.T) {
	ours := reflect.TypeOf(&Reservation{})
	up := reflect.TypeOf(&xrate.Reservation{})
	for i := 0; i < up.NumMethod(); i++ {
		name := up.Method(i).Name
		if _, ok := ours.MethodByName(name); !ok {
			t.Errorf("xrate.Reservation has method %q not implemented by our Reservation", name)
		}
	}
}

// TestReExportedTypes confirms the semantic types are literally upstream's.
func TestReExportedTypes(t *testing.T) {
	if reflect.TypeOf(Limit(0)) != reflect.TypeOf(xrate.Limit(0)) {
		t.Fatal("Limit must be the upstream type")
	}
	if Inf != xrate.Inf {
		t.Fatal("Inf must equal upstream Inf")
	}
	if Every(time.Second) != xrate.Every(time.Second) {
		t.Fatal("Every must match upstream")
	}
}
