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

package framework

import (
	"fmt"

	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/metrics"
)

// Statement structure
type Statement struct {
	operations []operation
	ssn        *Session
}

type operation struct {
	name string
	args []interface{}
}

// Evict the pod
func (s *Statement) Evict(reclaimee *api.TaskInfo, reason string) error {
	// Update status in session
	job, found := s.ssn.Jobs[reclaimee.Job]
	if found {
		if err := job.UpdateTaskStatus(reclaimee, api.Releasing); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when evicting in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, api.Releasing, s.ssn.UID, err)
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when evicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// Update task in node.
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	s.operations = append(s.operations, operation{
		name: "evict",
		args: []interface{}{reclaimee, reason},
	})

	return nil
}

func (s *Statement) evict(reclaimee *api.TaskInfo, reason string) error {
	if err := s.ssn.cache.Evict(reclaimee, reason); err != nil {
		if e := s.unevict(reclaimee, reason); err != nil {
			glog.Errorf("Faled to unevict task <%v/%v>: %v.", reclaimee.Namespace, reclaimee.Name, e)
		}
		return err
	}

	return nil
}

func (s *Statement) unevict(reclaimee *api.TaskInfo, reason string) error {
	// Update status in session
	job, found := s.ssn.Jobs[reclaimee.Job]
	if found {
		if err := job.UpdateTaskStatus(reclaimee, api.Running); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when unevicting in Session <%v>: %v",
				reclaimee.Namespace, reclaimee.Name, api.Running, s.ssn.UID, err)
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when unevicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// Update task in node.
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: reclaimee,
			})
		}
	}

	return nil
}

// Pipeline the task for the node
func (s *Statement) Pipeline(task *api.TaskInfo, hostname string) error {
	// Only update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, api.Pipelined); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when pipeline in Session <%v>: %v",
				task.Namespace, task.Name, api.Pipelined, s.ssn.UID, err)
			return err
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when pipeline.", task.Job, s.ssn.UID)
		return fmt.Errorf("failed to find job %s when pipeline", task.Job)
	}

	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			glog.Errorf("Failed to add task <%v/%v> to node <%v> when pipeline in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			return err
		}
		glog.V(3).Infof("After pipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		glog.Errorf("Failed to found Node <%s> in Session <%s> index when pipeline.", hostname, s.ssn.UID)
		return fmt.Errorf("failed to find node %s when pipeline", hostname)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	s.operations = append(s.operations, operation{
		name: "pipeline",
		args: []interface{}{task, hostname},
	})

	return nil
}

func (s *Statement) pipeline(task *api.TaskInfo) {
}

func (s *Statement) unpipeline(task *api.TaskInfo) error {
	// Only update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, api.Pending); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when unpipeline in Session <%v>: %v",
				task.Namespace, task.Name, api.Pending, s.ssn.UID, err)
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when unpipeline.", task.Job, s.ssn.UID)
	}

	hostname := task.NodeName

	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.RemoveTask(task); err != nil {
			glog.Errorf("Failed to remove task <%v/%v> to node <%v> when unpipeline in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
		}
		glog.V(3).Infof("After unpipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		glog.Errorf("Failed to found Node <%s> in Session <%s> index when unpipeline.", hostname, s.ssn.UID)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}

	return nil
}

// Allocate the task to the node
func (s *Statement) Allocate(task *api.TaskInfo, hostname string) error {
	if err := s.ssn.cache.AllocateVolumes(task, hostname); err != nil {
		return err
	}

	// Only update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, api.Allocated); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when allocating in Session <%v>: %v",
				task.Namespace, task.Name, api.Allocated, s.ssn.UID, err)
			return err
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when allocating.", task.Job, s.ssn.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			glog.Errorf("Failed to add task <%v/%v> to node <%v> when allocating in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			return err
		}
		glog.V(3).Infof("After allocated Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		glog.Errorf("Failed to found Node <%s> in Session <%s> index when allocating.", hostname, s.ssn.UID)
		return fmt.Errorf("failed to find node %s when allocating", hostname)
	}

	// Callbacks
	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task: task,
			})
		}
	}

	// Update status in session
	glog.V(3).Infof("Allocating operations for task <%v/%v> ...", task.Namespace, task.Name)
	s.operations = append(s.operations, operation{
		name: "allocate",
		args: []interface{}{task, hostname},
	})

	return nil
}

func (s *Statement) allocate(task *api.TaskInfo, hostname string) error {
	if err := s.ssn.cache.BindVolumes(task); err != nil {
		return err
	}

	if err := s.ssn.cache.Bind(task, task.NodeName); err != nil {
		return err
	}

	// Update status in session
	if job, found := s.ssn.Jobs[task.Job]; found {
		if err := job.UpdateTaskStatus(task, api.Binding); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when binding in Session <%v>: %v",
				task.Namespace, task.Name, api.Binding, s.ssn.UID, err)
			return err
		}
	} else {
		glog.Errorf("Failed to found Job <%s> in Session <%s> index when binding.", task.Job, s.ssn.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	metrics.UpdateTaskScheduleDuration(metrics.Duration(task.Pod.CreationTimestamp.Time))
	return nil
}

// unallocate the pod for task
func (s *Statement) unallocate(task *api.TaskInfo, reason string) error {
	// Update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		if err := job.UpdateTaskStatus(task, api.Pending); err != nil {
			glog.Errorf("Failed to update task <%v/%v> status to %v when unallocating in Session <%v>: %v",
				task.Namespace, task.Name, api.Pending, s.ssn.UID, err)
		}
	} else {
		glog.Errorf("Failed to find Job <%s> in Session <%s> index when unallocating.",
			task.Job, s.ssn.UID)
	}

	if node, found := s.ssn.Nodes[task.NodeName]; found {
		glog.V(3).Infof("Remove Task <%v> on node <%v>", task.Name, task.NodeName)
		err := node.RemoveTask(task)
		if err != nil {
			glog.Errorf("Failed to remove Task <%v> on node <%v> when unallocating: %s", task.Name, task.NodeName, err.Error())
		}
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}
	return nil
}

// Discard operation for evict and pipeline
func (s *Statement) Discard() {
	glog.V(3).Info("Discarding operations ...")
	for i := len(s.operations) - 1; i >= 0; i-- {
		op := s.operations[i]
		switch op.name {
		case "evict":
			err := s.unevict(op.args[0].(*api.TaskInfo), op.args[1].(string))
			if err != nil {
				glog.Errorf("Statement.Discard unevict task failed: %s", err.Error())
			}
		case "pipeline":
			err := s.unpipeline(op.args[0].(*api.TaskInfo))
			if err != nil {
				glog.Errorf("Statement.Discard unpipeline failed: %v", err)
			}
		case "allocate":
			err := s.unallocate(op.args[0].(*api.TaskInfo), op.args[1].(string))
			if err != nil {
				glog.Errorf("Statement.Discard unallocate failed: %v", err)
			}
		}
	}
}

// Commit operation for evict and pipeline
func (s *Statement) Commit() {
	glog.V(3).Info("Committing operations ...")
	for _, op := range s.operations {
		switch op.name {
		case "evict":
			if err := s.evict(op.args[0].(*api.TaskInfo), op.args[1].(string)); err != nil {
				glog.Errorf("Statement.Commit evict failed: %v", err)
			}
		case "pipeline":
			s.pipeline(op.args[0].(*api.TaskInfo))
		case "allocate":
			if err := s.allocate(op.args[0].(*api.TaskInfo), op.args[1].(string)); err != nil {
				glog.Errorf("Statement.Commit allocate failed: %v", err)
			}
		}
	}
}
