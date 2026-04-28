// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Phase-1 date parser: ISO8601 / RFC3339, epoch_millis, epoch_second, plus a
// Joda → Go layout translator covering the common tokens (yyyy, MM, dd, HH,
// mm, ss, SSS, Z/ZZ/XXX). Locale handling is best-effort: only en_US/en are
// supported at parse time. More tokens (era markers, week-of-year, named
// months) are Phase-6 work.
package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledDate struct {
	from          string
	to            string
	formats       []string // raw Joda formats (we translate per attempt)
	outputFormat  string   // "" => RFC3339 millis; "epoch_millis"/"epoch_second" => numeric
	timezone      *time.Location
	ignoreFailure bool
}

func compileDate(p *dsl.DateProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/date: 'from' is required")
	}
	if len(p.Formats) == 0 {
		return nil, errors.New("streamlang/date: 'formats' is required")
	}
	loc := time.UTC
	if p.Timezone != "" {
		l, err := time.LoadLocation(p.Timezone)
		if err != nil {
			return nil, fmt.Errorf("streamlang/date: invalid timezone %q: %w", p.Timezone, err)
		}
		loc = l
	}
	to := p.To
	if to == "" {
		to = "@timestamp"
	}
	return &compiledDate{
		from:          p.From,
		to:            to,
		formats:       p.Formats,
		outputFormat:  p.OutputFormat,
		timezone:      loc,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledDate) Action() string      { return "date" }
func (c *compiledDate) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledDate) Execute(d document.Document) error {
	v, ok := d.Get(c.from)
	if !ok {
		// date follows ES conventions: silently no-op on missing source.
		return nil
	}
	text := v.Str()
	t, err := parseDate(text, c.formats, c.timezone)
	if err != nil {
		return fmt.Errorf("streamlang/date: %w", err)
	}

	switch c.outputFormat {
	case "epoch_millis":
		return d.Set(c.to, document.IntValue(t.UnixMilli()))
	case "epoch_second":
		return d.Set(c.to, document.IntValue(t.Unix()))
	case "":
		return d.Set(c.to, document.StringValue(t.UTC().Format(rfc3339Millis)))
	default:
		layout := jodaToGoLayout(c.outputFormat)
		return d.Set(c.to, document.StringValue(t.Format(layout)))
	}
}

const rfc3339Millis = "2006-01-02T15:04:05.000Z07:00"

func parseDate(text string, formats []string, loc *time.Location) (time.Time, error) {
	// Built-in shortcuts: try first across all entries.
	for _, fmt := range formats {
		switch fmt {
		case "ISO8601":
			if t, err := time.Parse(time.RFC3339Nano, text); err == nil {
				return t, nil
			}
			if t, err := time.Parse(time.RFC3339, text); err == nil {
				return t, nil
			}
		case "epoch_millis":
			if ms, err := strconv.ParseInt(text, 10, 64); err == nil {
				return time.UnixMilli(ms).In(loc), nil
			}
		case "epoch_second":
			if s, err := strconv.ParseInt(text, 10, 64); err == nil {
				return time.Unix(s, 0).In(loc), nil
			}
		}
	}
	// RFC3339 always works regardless of explicit listing.
	if t, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, text); err == nil {
		return t, nil
	}

	for _, f := range formats {
		if f == "ISO8601" || f == "epoch_millis" || f == "epoch_second" {
			continue
		}
		layout := jodaToGoLayout(f)
		if t, err := time.ParseInLocation(layout, text, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse %q with any of %d formats", text, len(formats))
}

// jodaToGoLayout translates a Joda/Java date format string to a Go reference
// layout. Covers the tokens used by the streamlang-consistency test suite and
// the most common ES ingest pipeline date formats.
//
// Reference layout: Mon Jan 2 15:04:05 MST 2006 / 01/02 03:04:05PM '06 -0700
func jodaToGoLayout(joda string) string {
	// Token order matters: longer tokens first.
	tokens := []struct{ from, to string }{
		// Quoted literal 'T' / 'Z' / quoted strings (very simple handling).
		{"'T'", "T"},
		{"'Z'", "Z"},
		// Year
		{"yyyy", "2006"},
		{"yy", "06"},
		// Month
		{"MMMM", "January"},
		{"MMM", "Jan"},
		{"MM", "01"},
		{"M", "1"},
		// Day
		{"dd", "02"},
		{"d", "2"},
		// Hours (24h)
		{"HH", "15"},
		{"H", "15"},
		// Hours (12h) — Go uses 03 for the same token width.
		{"hh", "03"},
		{"h", "3"},
		// Minutes
		{"mm", "04"},
		{"m", "4"},
		// Seconds
		{"ss", "05"},
		{"s", "5"},
		// Fractional seconds
		{"SSS", "000"},
		{"SS", "00"},
		{"S", "0"},
		// AM/PM
		{"a", "PM"},
		// Timezone
		{"XXX", "-07:00"},
		{"XX", "-0700"},
		{"X", "-07"},
		{"ZZZ", "MST"},
		{"ZZ", "-07:00"},
		{"Z", "-0700"},
	}
	out := joda
	// Walk through and replace; naive but works for the common cases.
	for _, tok := range tokens {
		out = strings.ReplaceAll(out, tok.from, tok.to)
	}
	return out
}
