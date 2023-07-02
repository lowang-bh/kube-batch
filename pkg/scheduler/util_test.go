/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"reflect"
	"testing"

	_ "github.com/kubernetes-sigs/kube-batch/pkg/scheduler/actions"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
)

func TestLoadSchedulerConf(t *testing.T) {
	configuration := `
actions: "allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`

	trueValue := true
	expectedTiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:                framework.PriorityPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
				{
					Name:                framework.GangPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
				{
					Name:                framework.ConformancePlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
			},
		},
		{
			Plugins: []conf.PluginOption{
				{
					Name:                framework.DRFPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
				{
					Name:                framework.PredicatesPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
				{
					Name:                framework.ProportionPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
				{
					Name:                framework.NodeorderPlugin,
					EnabledJobOrder:     &trueValue,
					EnabledJobReady:     &trueValue,
					EnabledJobPipelined: &trueValue,
					EnabledTaskOrder:    &trueValue,
					EnabledPreemptable:  &trueValue,
					EnabledReclaimable:  &trueValue,
					EnabledQueueOrder:   &trueValue,
					EnabledPredicate:    &trueValue,
					EnabledNodeOrder:    &trueValue,
				},
			},
		},
	}

	_, tiers, err := loadSchedulerConf(configuration)
	if err != nil {
		t.Errorf("Failed to load scheduler configuration: %v", err)
	}
	if !reflect.DeepEqual(tiers, expectedTiers) {
		t.Errorf("Failed to set default settings for plugins, expected: %+v, got %+v",
			expectedTiers, tiers)
	}
}
