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

	"github.com/golang/glog"
	"io"
	"k8s.io/kubernetes/pkg/api"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	kubetypes "k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util"
	"sort"
	"time"
)

const (
	KUBERNETES_POD_ID        = "KUBERNETES_POD_ID"
	KUBERNETES_POD_NAME      = "KUBERNETES_POD_NAME"
	KUBERNETES_POD_NAMESPACE = "KUBERNETES_POD_NAMESPACE "
)

type UnikRuntime struct {
	version         *unikVersion
	client          *unik_client.UnikClient
	livenessManager proberesults.Manager
}

func NewUnikRuntime(url string, version int) *UnikRuntime {
	return &UnikRuntime{
		client:          unik_client.NewUnikClient(url),
		version:         newUnikVersion(version),
		livenessManager: proberesults.NewManager(),
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
	glog.V(4).Infof("Unik is retreving all pods.")
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
			if pod.ID == kubetypes.UID(podId) {
				pod.Containers = append(pod.Containers, container)
				podFound = true
				break PodFindLoop
			}
		}
		if !podFound {
			newPod := kubecontainer.Pod{
				ID:         kubetypes.UID(podId),
				Name:       podName,
				Namespace:  podNameSpace,
				Containers: []*kubecontainer.Container{container},
			}
			pods = append(pods, &newPod)
		}
	}
	return pods, nil
}

func (r *UnikRuntime) GarbageCollect(gcPolicy kubecontainer.ContainerGCPolicy) error {
	glog.V(4).Infof("Unik is running garbage collection.")
	pods, err := r.GetPods(true)
	if err != nil {
		return lxerrors.New("could not get pods", err)
	}
	remainingContainersToDelete := []*kubecontainer.Container{}
	newestGCTime := time.Now().Add(-gcPolicy.MinAge)
	for _, pod := range pods {
		//mark containers that are past due & dead
		podContainersToDelete := []*kubecontainer.Container{}
		for _, kubeContainer := range pod.Containers {
			containerTime := time.Unix(kubeContainer.Created, 0)
			if newestGCTime.Before(containerTime) && kubeContainer.State != kubecontainer.ContainerStateRunning {
				podContainersToDelete = append(podContainersToDelete, kubeContainer)
			}
		}
		//remove N oldest dead containers where N is # past MaxPodPerContainer
		if gcPolicy.MaxPerPodContainer >= 0 {
			toRemove := len(podContainersToDelete) - gcPolicy.MaxPerPodContainer
			sortByOldest(podContainersToDelete)
			if toRemove > 0 {
				for i := 0; i < toRemove; i++ {
					kubeContainer := podContainersToDelete[i]
					err = r.deleteContainer(kubeContainer.ID.ID)
					if err != nil {
						return err
					}
				}
			}
			podContainersToDelete = podContainersToDelete[toRemove:]
		}
		//remove any remaining containers that exceed total MaxContainers
		remainingContainersToDelete = append(remainingContainersToDelete, podContainersToDelete...)
	}
	if gcPolicy.MaxContainers >= 0 && len(remainingContainersToDelete) > gcPolicy.MaxContainers {
		sortByOldest(remainingContainersToDelete)
		toRemove := len(remainingContainersToDelete) - gcPolicy.MaxContainers
		for i := 0; i < toRemove; i++ {
			kubeContainer := remainingContainersToDelete[i]
			err = r.deleteContainer(kubeContainer.ID.ID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *UnikRuntime) SyncPod(pod *api.Pod, podStatus api.PodStatus, internalPodStatus *kubecontainer.PodStatus, pullSecrets []api.Secret, backOff *util.Backoff) (result kubecontainer.PodSyncResult) {
	glog.V(4).Infof("Unik is syncing pod %v.", pod)
	var err error
	defer func() {
		if err != nil {
			result.Fail(err)
		}
	}()

	runningPod := kubecontainer.ConvertPodStatusToRunningPod(internalPodStatus)
	// Add references to all containers.
	unidentifiedContainers := make(map[kubecontainer.ContainerID]*kubecontainer.Container)
	for _, c := range runningPod.Containers {
		unidentifiedContainers[c.ID] = c
	}

	restartPod := false
	for _, container := range pod.Spec.Containers {
		expectedHash := kubecontainer.HashContainer(&container)

		c := runningPod.FindContainerByName(container.Name)
		if c == nil {
			if kubecontainer.ShouldContainerBeRestartedOldVersion(&container, pod, &podStatus) {
				glog.V(3).Infof("Container %+v is dead, but RestartPolicy says that we should restart it.", container)
				// TODO(yifan): Containers in one pod are fate-sharing at this moment, see:
				// https://github.com/appc/spec/issues/276.
				restartPod = true
				break
			}
			continue
		}

		// TODO: check for non-root image directives.  See ../docker/manager.go#SyncPod

		// TODO(yifan): Take care of host network change.
		containerChanged := c.Hash != 0 && c.Hash != expectedHash
		if containerChanged {
			glog.Infof("Pod %q container %q hash changed (%d vs %d), it will be killed and re-created.", format.Pod(pod), container.Name, c.Hash, expectedHash)
			restartPod = true
			break
		}

		liveness, found := r.livenessManager.Get(c.ID)
		if found && liveness != proberesults.Success && pod.Spec.RestartPolicy != api.RestartPolicyNever {
			glog.Infof("Pod %q container %q is unhealthy, it will be killed and re-created.", format.Pod(pod), container.Name)
			restartPod = true
			break
		}

		delete(unidentifiedContainers, c.ID)
	}

	// If there is any unidentified containers, restart the pod.
	if len(unidentifiedContainers) > 0 {
		restartPod = true
	}

	if restartPod {
		// Kill the pod only if the pod is actually running.
		if len(runningPod.Containers) > 0 {
			if err = r.KillPod(pod, runningPod); err != nil {
				return
			}
		}
		if err = r.RunPod(pod, pullSecrets); err != nil {
			return
		}
	}
	return
}

func (r *UnikRuntime) KillPod(pod *api.Pod, runningPod kubecontainer.Pod) error {
	glog.V(4).Infof("Unik is killing pod: name %q.", runningPod.Name)
	for _, kubeContainer := range runningPod.Containers {
		glog.V(4).Infof("Unik is deleting container: name %s.", kubeContainer.ID.ID)
		err := r.deleteContainer(kubeContainer.ID.ID)
		if err != nil {
			return lxerrors.New("failed to kill container for pod "+string(runningPod.ID), err)
		}
	}
	return nil
}

func (r *UnikRuntime) RunPod(pod *api.Pod, pullSecrets []api.Secret) error {
	glog.V(4).Infof("Unik is running pod: name %q.", pod.Name)
	for _, desiredContainer := range pod.Spec.Containers {
		glog.V(4).Infof("Unik is running container %s for pod.", desiredContainer.Name)
		err := r.runContainer(desiredContainer.Image, string(pod.UID), pod.Name, pod.Namespace, desiredContainer.Name)
		if err != nil {
			return lxerrors.New("failed to run container "+desiredContainer.Name+" for pod "+string(pod.UID), err)
		}
	}
	return nil
}

func (r *UnikRuntime) GetPodStatus(uid kubetypes.UID, name, namespace string) (*kubecontainer.PodStatus, error) {
	return nil, lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) PullImage(image kubecontainer.ImageSpec, pullSecrets []api.Secret) error {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) IsImagePresent(image kubecontainer.ImageSpec) (bool, error) {
	return false, lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) ListImages() ([]kubecontainer.Image, error) {
	return []kubecontainer.Image{}, lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) RemoveImage(image kubecontainer.ImageSpec) error {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) GetContainerLogs(pod *api.Pod, containerID kubecontainer.ContainerID, logOptions *api.PodLogOptions, stdout, stderr io.Writer) (err error) {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) RunInContainer(containerID kubecontainer.ContainerID, cmd []string) ([]byte, error) {
	return []byte{}, lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) ExecInContainer(containerID kubecontainer.ContainerID, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool) error {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) PortForward(pod *kubecontainer.Pod, port uint16, stream io.ReadWriteCloser) error {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) AttachContainer(id kubecontainer.ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool) (err error) {
	return lxerrors.New("not implemented", nil)
}

func (r *UnikRuntime) deleteContainer(unikInstanceId string) error {
	err := r.client.DeleteUnikInstance(unikInstanceId)
	if err != nil {
		return lxerrors.New("could not delete unik instance "+unikInstanceId, err)
	}
	return nil
}

func (r *UnikRuntime) runContainer(unikernelName, podId, podName, podNamespace, containerName string) error {
	tags := make(map[string]string)
	tags[KUBERNETES_POD_ID] = podId
	tags[KUBERNETES_POD_NAME] = podName
	tags[KUBERNETES_POD_NAMESPACE] = podNamespace
	err := r.client.RunUnikernel(unikernelName, containerName, 1, tags)
	if err != nil {
		return lxerrors.New("failed to run unikernel "+unikernelName, err)
	}
	return nil
}

func convertInstance(unikInstance *types.UnikInstance) *kubecontainer.Container {
	var containerState kubecontainer.ContainerState
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
		ID: kubecontainer.ContainerID{
			Type: "unik",
			ID:   unikInstance.UnikInstanceID,
		},
		Name:    unikInstance.UnikInstanceName,
		Image:   unikInstance.UnikernelName,
		Hash:    hashUnikInstance(unikInstance),
		Created: unikInstance.Created.Unix(),
		State:   containerState,
	}
}

func hashUnikInstance(unikInstance *types.UnikInstance) uint64 {
	hash := adler32.New()
	hashutil.DeepHashObject(hash, *unikInstance)
	return uint64(hash.Sum32())
}

func (r *UnikRuntime) getUnikInstance(unikInstanceId string) (*types.UnikInstance, error) {
	unikInstances, err := r.client.GetUnikInstances()
	if err != nil {
		return nil, lxerrors.New("could not get unik instance list", err)
	}
	for _, unikInstance := range unikInstances {
		if unikInstance.UnikInstanceID == unikInstanceId {
			return unikInstance, nil
		}
	}
	return nil, lxerrors.New("could not find unik instance with id "+unikInstanceId, nil)
}

func sortByOldest(containers []*kubecontainer.Container) {
	sort.Sort(oldestFirst(containers))
}

type oldestFirst []*kubecontainer.Container

func (a oldestFirst) Len() int      { return len(a) }
func (a oldestFirst) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a oldestFirst) Less(i, j int) bool {
	return time.Unix(a[i].Created, 0).After(time.Unix(a[j].Created, 0))
}
