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

package plugins

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/conformance"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/drf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/gang"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/nodeorder"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/predicates"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/priority"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/proportion"
)

func init() {
	// Plugins for Jobs
	framework.RegisterPluginBuilder(framework.DRFPlugin, drf.New)
	framework.RegisterPluginBuilder(framework.GangPlugin, gang.New)
	framework.RegisterPluginBuilder(framework.PredicatesPlugin, predicates.New)
	framework.RegisterPluginBuilder(framework.PriorityPlugin, priority.New)
	framework.RegisterPluginBuilder(framework.NodeorderPlugin, nodeorder.New)
	framework.RegisterPluginBuilder(framework.ConformancePlugin, conformance.New)

	// Plugins for Queues
	framework.RegisterPluginBuilder(framework.ProportionPlugin, proportion.New)
}
