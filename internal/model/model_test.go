package model

import (
	"strconv"
	"testing"
)

func TestParseFixed(t *testing.T) {
	cases := map[string]int64{
		"0": 0, "1": 1e8, "77825.9": 7782590000000, "0.00800000": 800000, "0.00000001": 1,
		"123.45678900": 12345678900, "1e-05": 1000, "7.7825E4": 7782500000000, "10.": 1000000000,
		".5": 50000000,
	}
	for in, want := range cases {
		got, err := ParseFixed([]byte(in))
		if err != nil || int64(got) != want {
			t.Errorf("ParseFixed(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", ".", "abc", "1.2.3", "1.000000001", "-1", "1e", "1x"} {
		if _, err := ParseFixed([]byte(bad)); err == nil {
			t.Errorf("ParseFixed(%q) accepted", bad)
		}
	}
}

func TestAppendDecimal(t *testing.T) {
	f := MustFixed("77825.9")
	if s := string(f.AppendDecimal(nil, 1)); s != "77825.9" {
		t.Fatal(s)
	}
	if s := string(f.AppendDecimal(nil, 8)); s != "77825.90000000" {
		t.Fatal(s)
	}
	if s := string(MustFixed("0.00800000").AppendDecimal(nil, 8)); s != "0.00800000" {
		t.Fatal(s)
	}
	if s := string(MustFixed("12").AppendDecimal(nil, 0)); s != "12" {
		t.Fatal(s)
	}
}

func FuzzParseFixed(f *testing.F) {
	for _, s := range []string{"0", "77825.9", "0.00800000", "1e-05", "9999999999.99999999"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		v, err := ParseFixed([]byte(s))
		if err != nil {
			return
		}
		if v < 0 {
			t.Fatalf("negative from %q", s)
		}
		// Round trip through 8-decimal formatting must be exact.
		back, err := ParseFixed(v.AppendDecimal(nil, 8))
		if err != nil || back != v {
			t.Fatalf("round trip %q -> %d -> %v (%v)", s, v, back, err)
		}
		// Agreement with float parsing for plain decimals within float precision.
		if fv, err := strconv.ParseFloat(s, 64); err == nil && fv < 1e6 {
			if d := fv*Scale - float64(v); d > 1 || d < -1 {
				t.Fatalf("%q: fixed %d vs float %f", s, v, fv*Scale)
			}
		}
	})
}
