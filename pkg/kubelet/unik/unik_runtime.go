/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package unik

import (
	"github.com/layer-x/layerx-commons/lxerrors"
	"github.com/layer-x/unik/types"
	"github.com/layer-x/unik/unik_client"
	"hash/adler32"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

const (
	KUBERNETES_POD_ID        = "KUBERNETES_POD_ID"
	KUBERNETES_POD_NAME      = "KUBERNETES_POD_NAME"
	KUBERNETES_POD_NAMESPACE = "KUBERNETES_POD_NAMESPACE "
)

type UnikRuntime struct {
	version unikVersion
	client  *unik_client.UnikClient
}

func NewUnikRuntime(url string, version int) *UnikRuntime {
	return &UnikRuntime{
		client: unik_client.NewUnikClient(url),
		version: unikVersion{
			version: version,
		},
	}
}

func (r *UnikRuntime) Type() string {
	return "unik"
}

func (r *UnikRuntime) Version() (kubecontainer.Version, error) {
	return r.version, nil
}

func (r *UnikRuntime) APIVersion() (kubecontainer.Version, error) {
	return r.version, nil
}

func (r *UnikRuntime) GetPods(all bool) ([]*kubecontainer.Pod, error) {
	unikInstances, err := r.client.GetUnikInstances()
	if err != nil {
		return nil, lxerrors.New("could not retrieve unik instances from backend", err)
	}
	pods := []*kubecontainer.Pod{}
	for _, unikInstance := range unikInstances {
		podId, ok := unikInstance.Tags[KUBERNETES_POD_ID]
		if !ok {
			//received an instance that isn't ours
			continue
		}
		container := convertInstance(unikInstance)
		if container.State == kubecontainer.ContainerStateExited && !all {
			continue
		}
		podName, ok := unikInstance.Tags[KUBERNETES_POD_NAME]
		if !ok {
			podName = podId
		}
		podNameSpace, ok := unikInstance.Tags[KUBERNETES_POD_NAMESPACE]
		if !ok {
			podNameSpace = "UNIK_NAMESPACE"
		}
		podFound := false
	PodFindLoop:
		for _, pod := range pods {
			if pod.ID == podId {
				pod.Containers = append(pod.Containers, container)
				podFound = true
				break PodFindLoop
			}
		}
		if !podFound {
			newPod := kubecontainer.Pod{
				ID:         podId,
				Name:       podName,
				Namespace:  podNameSpace,
				Containers: []*kubecontainer.Container{container},
			}
			pods = append(pods, newPod)
		}
	}
	return pods, nil
}

func (r *UnikRuntime) GarbageCollect(gcPolicy kubecontainer.ContainerGCPolicy) error {
	return lxerrors.New("not implemented", nil)
}

func convertInstance(unikInstance types.UnikInstance) *kubecontainer.Container {
	hash := adler32.New()
	hashutil.DeepHashObject(hash, *unikInstance)

	containerState := ""
	switch unikInstance.State {
	case "pending":
	case "running":
		containerState = kubecontainer.ContainerStateRunning
	case "shutting-down":
	case "terminated":
		containerState = kubecontainer.ContainerStateExited
	default:
		containerState = kubecontainer.ContainerStateUnknown
	}

	return &kubecontainer.Container{
		ID:      unikInstance.UnikInstanceID,
		Name:    unikInstance.UnikInstanceName,
		Image:   unikInstance.UnikernelName,
		Hash:    uint64(hash.Sum32()),
		Created: unikInstance.Created.Unix(),
		State:   containerState,
	}
}
