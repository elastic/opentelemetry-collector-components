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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"errors"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/xreceiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/list"
)

//go:embed testdata/profiles.jsonl
var demoProfiles []byte

type profilesGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples *list.LoopingList[pprofile.Profiles]

	stats   Stats
	statsMu sync.Mutex

	consumer xconsumer.Profiles

	cancelFn            context.CancelFunc
	inflightConcurrency sync.WaitGroup
}

func createProfilesReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer xconsumer.Profiles,
) (xreceiver.Profiles, error) {
	genConfig := config.(*Config)

	parser := pprofile.JSONUnmarshaler{}
	var err error
	sampleProfiles := demoProfiles

	if genConfig.Profiles.JsonlFile != "" {
		sampleProfiles, err = os.ReadFile(string(genConfig.Profiles.JsonlFile))
		if err != nil {
			return nil, err
		}
	}

	maxBufferSize := genConfig.Profiles.MaxBufferSize
	if maxBufferSize == 0 {
		maxBufferSize = len(sampleProfiles) + 10 // add some margin
	}

	var items []pprofile.Profiles
	scanner := bufio.NewScanner(bytes.NewReader(sampleProfiles))
	scanner.Buffer(make([]byte, 0, maxBufferSize), maxBufferSize)
	for scanner.Scan() {
		profileBytes := scanner.Bytes()
		lineProfiles, err := parser.UnmarshalProfiles(profileBytes)
		if err != nil {
			return nil, err
		}
		items = append(items, lineProfiles)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &profilesGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  list.NewLoopingList(items, genConfig.Profiles.MaxReplay),
	}, nil
}

func (ar *profilesGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	for i := 0; i < ar.cfg.Concurrency; i++ {
		ar.inflightConcurrency.Add(1)
		go func() {
			defer ar.inflightConcurrency.Done()
			next := pprofile.NewProfiles() // per-worker temporary container to avoid allocs
			for {
				select {
				case <-startCtx.Done():
					return
				default:
				}
				if ar.cfg.DisablePdataReuse || next.IsReadOnly() {
					// As the optimization to reuse pdata is not compatible with fanoutconsumer,
					// i.e. in pipelines where there are more than 1 consumer,
					// as fanoutconsumer will mark the pdata struct as read only and cannot be reused.
					// See https://github.com/open-telemetry/opentelemetry-collector/blob/461a3558086a03ab13ea121d12e28e185a1c79b0/internal/fanoutconsumer/logs.go#L70
					next = pprofile.NewProfiles()
				}
				err := ar.nextProfiles(next)
				if errors.Is(err, list.ErrLoopLimitReached) {
					return
				}
				// For graceful shutdown, use ctx instead of startCtx to shield Consume* from context canceled
				// In other words, Consume* will finish at its own pace, which may take indefinitely long.
				recordCount := next.SampleCount()
				if err := ar.consumer.ConsumeProfiles(ctx, next); err != nil {
					ar.logger.Error(err.Error())
					ar.statsMu.Lock()
					ar.stats.FailedRequests++
					ar.stats.FailedSamples += recordCount
					ar.statsMu.Unlock()
				} else {
					ar.statsMu.Lock()
					ar.stats.Requests++
					ar.stats.Samples += recordCount
					ar.statsMu.Unlock()
				}
			}
		}()
	}
	go func() {
		ar.inflightConcurrency.Wait()
		if ar.cfg.Profiles.doneCh != nil {
			ar.cfg.Profiles.doneCh <- ar.stats
		}
	}()
	return nil
}

func (ar *profilesGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	ar.inflightConcurrency.Wait()
	return nil
}

func (ar *profilesGenerator) nextProfiles(next pprofile.Profiles) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	sample, err := ar.samples.Next()
	if err != nil {
		return err
	}
	sample.CopyTo(next)

	rm := next.ResourceProfiles()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeProfiles().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeProfiles().At(j).Profiles().Len(); k++ {
				profile := rm.At(i).ScopeProfiles().At(j).Profiles().At(k)
				profile.SetTime(now)
			}
		}
	}

	return nil
}
