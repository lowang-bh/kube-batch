/*
Copyright 2018 The Kubernetes Authors.

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

package priority

import (
	"github.com/golang/glog"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
)

type priorityPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return priority plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &priorityPlugin{pluginArguments: arguments}
}

func (pp *priorityPlugin) Name() string {
	return "priority"
}

func (pp *priorityPlugin) OnSessionOpen(ssn *framework.Session) {
	taskOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.TaskInfo)
		rv := r.(*api.TaskInfo)

		glog.V(4).Infof("Priority TaskOrder: <%v/%v> priority is %v, <%v/%v> priority is %v",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority == rv.Priority {
			return 0
		}

		if lv.Priority > rv.Priority {
			return -1
		}

		return 1
	}

	// Add Task Order function
	ssn.AddTaskOrderFn(pp.Name(), taskOrderFn)

	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		glog.V(4).Infof("Priority JobOrderFn: <%v/%v> priority: %d, <%v/%v> priority: %d",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority > rv.Priority {
			return -1
		}

		if lv.Priority < rv.Priority {
			return 1
		}

		return 0
	}

	ssn.AddJobOrderFn(pp.Name(), jobOrderFn)

	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo {
		preemptorJob := ssn.Jobs[preemptor.Job]

		var victims []*api.TaskInfo
		for _, preemptee := range preemptees {
			preempteeJob := ssn.Jobs[preemptee.Job]
			if preempteeJob.UID != preemptorJob.UID {
				if preempteeJob.Priority >= preemptorJob.Priority {
					glog.V(4).Infof("Can not preempt task <%v/%v> because preemptee job has greater or equal job priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preempteeJob.Priority, preemptorJob.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			} else { // same job's diffenrent tasks should compare task's priority
				if preemptee.Priority >= preemptor.Priority {
					glog.V(4).Infof("Can not preempt task <%v/%v> because preemptee task has greater or equal task priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preemptee.Priority, preemptor.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			}
		}

		glog.V(4).Infof("Victims from Priority plugins are %+v", victims)
		return victims
	}

	ssn.AddPreemptableFn(pp.Name(), preemptableFn)
}

func (pp *priorityPlugin) OnSessionClose(ssn *framework.Session) {}
