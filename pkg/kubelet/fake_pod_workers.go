/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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

package kubelet

import (
	"k8s.io/kubernetes/pkg/api"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/types"
)

// fakePodWorkers runs sync pod function in serial, so we can have
// deterministic behaviour in testing.
type fakePodWorkers struct {
	syncPodFn syncPodFnType
	cache     kubecontainer.Cache
	t         TestingInterface
}

func (f *fakePodWorkers) UpdatePod(pod *api.Pod, mirrorPod *api.Pod, updateType kubetypes.SyncPodType, updateComplete func()) {
	status, err := f.cache.Get(pod.UID)
	if err != nil {
		f.t.Errorf("Unexpected error: %v", err)
	}
	if err := f.syncPodFn(pod, mirrorPod, status, kubetypes.SyncPodUpdate); err != nil {
		f.t.Errorf("Unexpected error: %v", err)
	}
}

func (f *fakePodWorkers) ForgetNonExistingPodWorkers(desiredPods map[types.UID]empty) {}

func (f *fakePodWorkers) ForgetWorker(uid types.UID) {}

type TestingInterface interface {
	Errorf(format string, args ...interface{})
}
