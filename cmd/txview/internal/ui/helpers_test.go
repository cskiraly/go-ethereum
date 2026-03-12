// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package ui

import (
	"strconv"
	"strings"
	"testing"

	"github.com/dop251/goja"
)

// loadHelpers creates a goja JS runtime and loads helpers.js into it.
func loadHelpers(t *testing.T) *goja.Runtime {
	t.Helper()

	src, err := Assets.ReadFile("helpers.js")
	if err != nil {
		t.Fatalf("failed to read helpers.js: %v", err)
	}
	vm := goja.New()
	_, err = vm.RunString(string(src))
	if err != nil {
		t.Fatalf("failed to execute helpers.js: %v", err)
	}
	return vm
}

// callHelper evaluates a JS expression and returns the string result.
func callHelper(t *testing.T, vm *goja.Runtime, expr string) string {
	t.Helper()
	v, err := vm.RunString(expr)
	if err != nil {
		t.Fatalf("JS error evaluating %q: %v", expr, err)
	}
	if goja.IsNull(v) || goja.IsUndefined(v) {
		return ""
	}
	return v.String()
}

func TestJSParseTS(t *testing.T) {
	vm := loadHelpers(t)

	tests := []struct {
		input    string
		wantNull bool
	}{
		{`'2024-06-15T12:30:45.123Z'`, false},
		{`'0001-01-01T00:00:00Z'`, true}, // Go zero time
		{`''`, true},                     // empty string
		{`null`, true},                   // null
		{`undefined`, true},              // undefined
		{`'not-a-date'`, true},           // invalid
	}
	for _, tt := range tests {
		result, err := vm.RunString("txviewHelpers.parseTS(" + tt.input + ")")
		if err != nil {
			t.Fatalf("parseTS(%s) error: %v", tt.input, err)
		}
		isNull := goja.IsNull(result) || goja.IsUndefined(result)
		if isNull != tt.wantNull {
			t.Errorf("parseTS(%s): got null=%v, want null=%v", tt.input, isNull, tt.wantNull)
		}
	}

	// Valid timestamp should return a Date with correct getTime().
	// Use a known UTC epoch value.
	ms := callHelper(t, vm, "txviewHelpers.parseTS('1970-01-01T00:00:01.234Z').getTime()")
	if ms != "1234" {
		t.Errorf("parseTS epoch ms: got %s, want 1234", ms)
	}
}

func TestJSMsDelta(t *testing.T) {
	vm := loadHelpers(t)

	tests := []struct {
		deltaMs int
		want    string
	}{
		{0, "0s"},
		{50, "0s"},         // < 100ms
		{99, "0s"},         // < 100ms boundary
		{5300, "5.3s"},     // < 10s
		{45000, "45s"},     // < 60s
		{150000, "2m30s"},  // minutes
		{4500000, "1h15m"}, // hours (4500s = 75m = 1h15m)
	}
	for _, tt := range tests {
		expr := "(function() { var b = new Date(1000000); var t = new Date(1000000 + " +
			strconv.Itoa(tt.deltaMs) + "); return txviewHelpers.msDelta(t, b); })()"
		got := callHelper(t, vm, expr)
		if got != tt.want {
			t.Errorf("msDelta(%dms): got %q, want %q", tt.deltaMs, got, tt.want)
		}
	}

	// Negative delta should clamp to "0s".
	got := callHelper(t, vm, "(function() { var b = new Date(2000000); var t = new Date(1000000); return txviewHelpers.msDelta(t, b); })()")
	if got != "0s" {
		t.Errorf("msDelta(negative): got %q, want %q", got, "0s")
	}
}

func TestJSEscapeHtml(t *testing.T) {
	vm := loadHelpers(t)

	tests := []struct {
		input, want string
	}{
		{"hello", "hello"},
		{"<script>", "&lt;script&gt;"},
		{`a&b"c`, `a&amp;b&quot;c`},
		{"<b>bold</b> & \"quoted\"", "&lt;b&gt;bold&lt;/b&gt; &amp; &quot;quoted&quot;"},
		{"", ""},
	}
	for _, tt := range tests {
		got := callHelper(t, vm, "txviewHelpers.escapeHtml("+jsString(tt.input)+")")
		if got != tt.want {
			t.Errorf("escapeHtml(%q): got %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestJSBuildTxFields(t *testing.T) {
	vm := loadHelpers(t)

	// Full info object.
	_, err := vm.RunString(`
		var testInfo = {
			Status: 'pooled',
			Local: true,
			TxType: 2,
			TxSize: 500,
			From: '0xabc',
			Nonce: 42,
			Gas: 21000,
			GasFeeCap: '1000000000',
			GasTipCap: '2000000',
			Value: '1000000000000000000',
			To: '0xdef',
			FirstSeen: '2024-06-15T12:30:45Z',
			Requested: '2024-06-15T12:30:46Z',
			RequestedFrom: 'peer1',
			Received: '2024-06-15T12:30:47Z',
			Pooled: '2024-06-15T12:30:48Z',
			Included: '0001-01-01T00:00:00Z',
			Finalized: '0001-01-01T00:00:00Z',
			Dropped: '0001-01-01T00:00:00Z',
			DropReason: '',
			Deliverer: 'peer1',
			Announcers: ['peer1', 'peer2'],
			BlockNum: 0,
			BlockHash: '',
			RejectErr: ''
		};
		var testFields = txviewHelpers.buildTxFields('0x1234', testInfo);
	`)
	if err != nil {
		t.Fatalf("setup buildTxFields: %v", err)
	}

	// Should have 26 fields.
	count := callHelper(t, vm, "testFields.length")
	if count != "26" {
		t.Fatalf("buildTxFields: expected 26 fields, got %s", count)
	}

	// Check specific fields.
	checks := []struct {
		index int
		label string
		value string
	}{
		{0, "Hash", "0x1234"},
		{1, "Status", "pooled"},
		{2, "Local", "yes"},
		{4, "Size", "500 bytes"},
		{5, "From", "0xabc"},
		{6, "Nonce", "42"},
		{8, "Gas Fee Cap", "1000000000 wei"},
		{14, "Requested From", "peer1"},
		{21, "Deliverer", "peer1"},
		{22, "Announcers", "peer1, peer2"},
	}
	for _, c := range checks {
		label := callHelper(t, vm, "testFields["+strconv.Itoa(c.index)+"][0]")
		value := callHelper(t, vm, "String(testFields["+strconv.Itoa(c.index)+"][1])")
		if label != c.label {
			t.Errorf("field %d label: got %q, want %q", c.index, label, c.label)
		}
		if value != c.value {
			t.Errorf("field %d (%s) value: got %q, want %q", c.index, c.label, value, c.value)
		}
	}

	// Test with Local=false.
	got := callHelper(t, vm, `txviewHelpers.buildTxFields('0x0', {
		Status: 'announced', Local: false, TxType: 0, TxSize: 100,
		FirstSeen: '0001-01-01T00:00:00Z'
	})[2][1]`)
	if got != "no" {
		t.Errorf("buildTxFields Local=false: got %q, want %q", got, "no")
	}

	// Missing optional fields should default to "-".
	got = callHelper(t, vm, `String(txviewHelpers.buildTxFields('0x0', {
		Status: 'announced', Local: false, TxType: 0, TxSize: 100,
		FirstSeen: '0001-01-01T00:00:00Z'
	})[5][1])`)
	if got != "-" {
		t.Errorf("buildTxFields missing From: got %q, want %q", got, "-")
	}
}

func TestJSFormatTS(t *testing.T) {
	vm := loadHelpers(t)

	// Null/zero input returns "-".
	got := callHelper(t, vm, "txviewHelpers.formatTS('0001-01-01T00:00:00Z', null)")
	if got != "-" {
		t.Errorf("formatTS(zero): got %q, want %q", got, "-")
	}
	got = callHelper(t, vm, "txviewHelpers.formatTS('', null)")
	if got != "-" {
		t.Errorf("formatTS(empty): got %q, want %q", got, "-")
	}

	// With a base timestamp, the delta suffix should appear.
	got = callHelper(t, vm, `(function() {
		var base = txviewHelpers.parseTS('2024-06-15T12:30:45.000Z');
		return txviewHelpers.formatTS('2024-06-15T12:30:50.300Z', base);
	})()`)
	// The exact absolute part depends on locale, but the delta should be present.
	if len(got) == 0 || got == "-" {
		t.Errorf("formatTS with base: got %q, expected non-empty with delta", got)
	}
	// Check that the delta suffix is included.
	if !strings.Contains(got, "(+5.3s)") {
		t.Errorf("formatTS with base: got %q, expected to contain '(+5.3s)'", got)
	}

	// Without base, no delta suffix.
	got = callHelper(t, vm, "txviewHelpers.formatTS('2024-06-15T12:30:45.123Z', null)")
	if got == "-" {
		t.Errorf("formatTS(valid, null): got '-', want formatted time")
	}
	if strings.Contains(got, "(+") {
		t.Errorf("formatTS without base should not have delta suffix, got %q", got)
	}
}

// jsString returns a JS string literal for the given Go string.
func jsString(s string) string {
	// Simple escaping for test inputs (no backslashes or single quotes expected).
	return "'" + s + "'"
}
