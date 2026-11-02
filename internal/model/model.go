// Package model defines the venue-neutral market-data schema used across marketmux.
//
// Prices and sizes are fixed-point int64 with 8 implied decimals (1e-8 units). They are parsed
// directly from the venue's decimal text (JSON strings or JSON numbers) without going through
// float64, so a price like "77825.90" round-trips exactly and equal prices from different venues
// compare equal.
package model

import (
	"errors"
	"strconv"
)

// Scale is the fixed-point scale: 1 unit = 1e-8.
const Scale = 100_000_000

// Decimals is the number of implied decimals.
const Decimals = 8

// Fixed is a fixed-point decimal with 8 implied decimals.
type Fixed int64

var (
	errEmpty     = errors.New("empty decimal")
	errSyntax    = errors.New("invalid decimal")
	errPrecision = errors.New("more than 8 decimal places")
	errRange     = errors.New("decimal out of range")
)

// ParseFixed parses a non-negative decimal such as "77825.9", "0.00800000" or "1e-05".
// Exponent notation is accepted because some venues emit JSON numbers. Digits beyond the 8th
// decimal place must be zeros (otherwise errPrecision): silently truncating a price is a bug.
func ParseFixed(b []byte) (Fixed, error) {
	if len(b) == 0 {
		return 0, errEmpty
	}
	var intPart, frac int64
	fracDigits := 0
	i := 0
	sawDigit := false
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		if intPart > (1<<62)/10 {
			return 0, errRange
		}
		intPart = intPart*10 + int64(b[i]-'0')
		sawDigit = true
	}
	if i < len(b) && b[i] == '.' {
		i++
		for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
			sawDigit = true
			if fracDigits < Decimals {
				frac = frac*10 + int64(b[i]-'0')
				fracDigits++
			} else if b[i] != '0' {
				return 0, errPrecision
			}
		}
	}
	if !sawDigit {
		return 0, errSyntax
	}
	for d := fracDigits; d < Decimals; d++ {
		frac *= 10
	}
	if intPart > (1<<62)/Scale {
		return 0, errRange
	}
	v := intPart*Scale + frac
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		exp, err := strconv.Atoi(string(b[i+1:]))
		if err != nil {
			return 0, errSyntax
		}
		for ; exp > 0; exp-- {
			if v > (1<<62)/10 {
				return 0, errRange
			}
			v *= 10
		}
		for ; exp < 0; exp++ {
			if v%10 != 0 {
				return 0, errPrecision
			}
			v /= 10
		}
		return Fixed(v), nil
	}
	if i != len(b) {
		return 0, errSyntax
	}
	return Fixed(v), nil
}

// MustFixed parses s or panics; for tests and constants.
func MustFixed(s string) Fixed {
	f, err := ParseFixed([]byte(s))
	if err != nil {
		panic(err)
	}
	return f
}

// AppendDecimal appends f formatted with exactly `places` decimals (0..8), truncating
// trailing digits beyond `places` (callers choose places >= the value's precision).
func (f Fixed) AppendDecimal(dst []byte, places int) []byte {
	v := int64(f)
	if v < 0 {
		dst = append(dst, '-')
		v = -v
	}
	dst = strconv.AppendInt(dst, v/Scale, 10)
	if places <= 0 {
		return dst
	}
	frac := v % Scale
	for d := Decimals; d > places; d-- {
		frac /= 10
	}
	dst = append(dst, '.')
	var buf [Decimals]byte
	for k := places - 1; k >= 0; k-- {
		buf[k] = byte('0' + frac%10)
		frac /= 10
	}
	return append(dst, buf[:places]...)
}

// String formats with 8 decimals.
func (f Fixed) String() string { return string(f.AppendDecimal(nil, Decimals)) }

// Float64 converts for display and statistics only.
func (f Fixed) Float64() float64 { return float64(f) / Scale }

// Venue identifies a trading venue.
type Venue uint8

// Supported venues.
const (
	Kraken Venue = iota + 1
	Coinbase
	Bitstamp
	Gemini
	Synthetic // fake venue used by tests and benchmarks
	numVenues
)

// NumVenues is one past the largest venue id.
const NumVenues = int(numVenues)

func (v Venue) String() string {
	switch v {
	case Kraken:
		return "kraken"
	case Coinbase:
		return "coinbase"
	case Bitstamp:
		return "bitstamp"
	case Gemini:
		return "gemini"
	case Synthetic:
		return "synthetic"
	default:
		return "venue(" + strconv.Itoa(int(v)) + ")"
	}
}

// ParseVenue maps a name to a Venue (0 if unknown).
func ParseVenue(s string) Venue {
	for v := Venue(1); v < numVenues; v++ {
		if v.String() == s {
			return v
		}
	}
	return 0
}

// Side of a book level.
type Side uint8

// Book sides.
const (
	Bid Side = iota
	Ask
)

// Level is one price level: Qty == 0 in a delta means "remove the level".
type Level struct {
	Price Fixed
	Qty   Fixed
}

// Kind of normalised event.
type Kind uint8

// Event kinds.
const (
	KindSnapshot Kind = iota + 1 // replaces the venue book for the symbol
	KindDelta                    // absolute level updates (qty 0 = delete)
	KindTrade
	KindHeartbeat
)

// Event is the normalised unit flowing through the pipeline. Bids/Asks slices are owned by the
// decoder's reusable buffers and valid only until the next decode on the same decoder.
type Event struct {
	Venue    Venue
	Kind     Kind
	Symbol   string // canonical, e.g. "BTC-USD"
	Bids     []Level
	Asks     []Level
	Checksum uint32 // venue checksum when provided (Kraken), with HasChecksum
	HasCRC   bool
	Seq      uint64 // venue sequence when provided (Coinbase heartbeat)
	HasSeq   bool
	ExchTsUs int64 // venue timestamp, microseconds since epoch, 0 if absent
	RecvNs   int64 // local monotonic receive time, ns
	// Trade fields.
	TradePx   Fixed
	TradeQty  Fixed
	TradeSide Side
}
