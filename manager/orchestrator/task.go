package orchestrator

import (
	"reflect"
	"time"

	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/identity"
	"github.com/docker/swarmkit/manager/allocator"
	"github.com/docker/swarmkit/protobuf/ptypes"
)

// DefaultRestartDelay is the restart delay value to use when none is
// specified.
const DefaultRestartDelay = 5 * time.Second

// NewTask creates a new task.
func NewTask(cluster *api.Cluster, service *api.Service, slot uint64, nodeID string) *api.Task {
	var logDriver *api.Driver
	if service.Spec.Task.LogDriver != nil {
		// use the log driver specific to the task, if we have it.
		logDriver = service.Spec.Task.LogDriver
	} else if cluster != nil {
		// pick up the cluster default, if available.
		logDriver = cluster.Spec.TaskDefaults.LogDriver // nil is okay here.
	}

	taskID := identity.NewID()
	task := api.Task{
		ID:                 taskID,
		ServiceAnnotations: service.Spec.Annotations,
		Spec:               service.Spec.Task,
		SpecVersion:        service.SpecVersion,
		ServiceID:          service.ID,
		Slot:               slot,
		Status: api.TaskStatus{
			State:     api.TaskStateNew,
			Timestamp: ptypes.MustTimestampProto(time.Now()),
			Message:   "created",
		},
		Endpoint: &api.Endpoint{
			Spec: service.Spec.Endpoint.Copy(),
		},
		DesiredState: api.TaskStateRunning,
		LogDriver:    logDriver,
	}

	// In global mode we also set the NodeID
	if nodeID != "" {
		task.NodeID = nodeID
	}

	return &task
}

// RestartCondition returns the restart condition to apply to this task.
func RestartCondition(task *api.Task) api.RestartPolicy_RestartCondition {
	restartCondition := api.RestartOnAny
	if task.Spec.Restart != nil {
		restartCondition = task.Spec.Restart.Condition
	}
	return restartCondition
}

// IsTaskDirty determines whether a task matches the given service's spec.
func IsTaskDirty(s *api.Service, t *api.Task) bool {
	// If the spec version matches, we know the task is not dirty. However,
	// if it does not match, that doesn't mean the task is dirty, since
	// only a portion of the spec is included in the comparison.
	if t.SpecVersion != nil && *s.SpecVersion == *t.SpecVersion {
		return false
	}

	if !reflect.DeepEqual(s.Spec.Task, t.Spec) ||
		(t.Endpoint != nil && !reflect.DeepEqual(s.Spec.Endpoint, t.Endpoint.Spec)) {
		return true
	}

	ingressNetworkNeeded := allocator.IsIngressNetworkNeeded(s)

	// Always prefer NetworkAttachmentConfig in the TaskSpec
	specNetworks := t.Spec.Networks
	if len(specNetworks) == 0 && s != nil && len(s.Spec.Networks) != 0 {
		specNetworks = s.Spec.Networks
	}

	// O(N^2), but probably better than doing memory allocations to create
	// a map. Revisit this if we end up needing to support large numbers of
	// networks on the same service.

	// First check that all networks referenced by the task are part of the
	// service spec.
taskNetworks:
	for _, taskNetwork := range t.Networks {
		if allocator.IsIngressNetwork(taskNetwork.Network) {
			if !ingressNetworkNeeded {
				return true
			}
			ingressNetworkNeeded = false
			continue
		}

		for _, serviceNetwork := range specNetworks {
			if serviceNetwork.Target == taskNetwork.Network.ID {
				continue taskNetworks
			}
		}

		return true
	}

serviceNetworks:
	// Next, check that all networks referenced by the service spec are part
	// of the task.
	for _, serviceNetwork := range specNetworks {
		for _, taskNetwork := range t.Networks {
			if allocator.IsIngressNetwork(taskNetwork.Network) {
				continue
			}
			if serviceNetwork.Target == taskNetwork.Network.ID {
				continue serviceNetworks
			}
		}

		return true
	}

	return false
}

// InvalidNode is true if the node is nil, down, or drained
func InvalidNode(n *api.Node) bool {
	return n == nil ||
		n.Status.State == api.NodeStatus_DOWN ||
		n.Spec.Availability == api.NodeAvailabilityDrain
}
