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
	KUBERNETES_POD_NAMESPACE = "KUBERNETES_POD_NAMESPACE"
)

type UnikRuntime struct {
	version           *unikVersion
	client            *unik_client.UnikClient
	livenessManager   proberesults.Manager
	containerStatuses map[string]*kubecontainer.ContainerStatus
}

func NewUnikRuntime(url string, version int) *UnikRuntime {
	return &UnikRuntime{
		client:            unik_client.NewUnikClient(url),
		version:           newUnikVersion(version),
		livenessManager:   proberesults.NewManager(),
		containerStatuses: make(map[string]*kubecontainer.ContainerStatus),
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
	podStatus := &kubecontainer.PodStatus{ID: uid, Name: name, Namespace: namespace}
	var containerStatuses []*kubecontainer.ContainerStatus
	pods, err := r.GetPods(true)
	if err != nil {
		return nil, lxerrors.New("could not get pods", err)
	}
	for _, pod := range pods {
		if pod.ID == uid {
			for _, kubeContainer := range pod.Containers {
				status := generateContainerStatus(kubeContainer)
				containerStatuses = append(containerStatuses, status)
			}
		}
	}
	podStatus.ContainerStatuses = containerStatuses
	return podStatus, nil
}

func (r *UnikRuntime) PullImage(image kubecontainer.ImageSpec, pullSecrets []api.Secret) error {
	return lxerrors.New("you must run \"unik push "+image.Image+" path/to/source/code in order to make this image available to kubernenetes", nil)
}

func (r *UnikRuntime) IsImagePresent(image kubecontainer.ImageSpec) (bool, error) {
	unikernels, err := r.client.ListUnikernels()
	if err != nil {
		return false, lxerrors.New("failed to retrieve unikernel list", err)
	}
	for _, unikernel := range unikernels {
		if unikernel.UnikernelName == image.Image {
			return true, nil
		}
	}
	return false, nil
}

func (r *UnikRuntime) ListImages() ([]kubecontainer.Image, error) {
	images := []kubecontainer.Image{}
	unikernels, err := r.client.ListUnikernels()
	if err != nil {
		return []kubecontainer.Image{}, lxerrors.New("failed to retrieve unikernel list", err)
	}
	for _, unikernel := range unikernels {
		images = append(images, convertUnikernel(unikernel))
	}
	return images, nil
}

func (r *UnikRuntime) RemoveImage(image kubecontainer.ImageSpec) error {
	return r.client.DeleteUnikernel(image.Image, true)
}

func (r *UnikRuntime) GetContainerLogs(pod *api.Pod, containerID kubecontainer.ContainerID, logOptions *api.PodLogOptions, stdout, stderr io.Writer) (err error) {
	if logOptions.Follow {
		return r.client.FollowUnikInstanceLogs(containerID.ID, stdout)
	} else {
		logStr, err := r.client.GetUnikInstanceLogs(containerID.ID)
		if err != nil {
			return err
		}
		_, err = stdout.Write([]byte(logStr))
		return err
	}
}

func (r *UnikRuntime) RunInContainer(containerID kubecontainer.ContainerID, cmd []string) ([]byte, error) {
	return []byte{}, lxerrors.New("Run in container not supported for unikernels", nil)
}

func (r *UnikRuntime) ExecInContainer(containerID kubecontainer.ContainerID, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool) error {
	return lxerrors.New("Exec in container not supported for unikernels", nil)
}

func (r *UnikRuntime) PortForward(pod *kubecontainer.Pod, port uint16, stream io.ReadWriteCloser) error {
	return lxerrors.New("Port forward not supported for unikernels", nil)
}

func (r *UnikRuntime) AttachContainer(id kubecontainer.ContainerID, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool) (err error) {
	return lxerrors.New("Attach container not supported for unikernels", nil)
}

func generateContainerStatus(kubeContainer *kubecontainer.Container) *kubecontainer.ContainerStatus {
	finishedAt := time.Unix(0, 0)
	if kubeContainer.State == kubecontainer.ContainerStateExited {
		finishedAt = time.Now()
	}
	status := &kubecontainer.ContainerStatus{
		ID:           kubeContainer.ID,
		Name:         kubeContainer.Name,
		State:        kubeContainer.State,
		CreatedAt:    time.Unix(kubeContainer.Created, 0),
		StartedAt:    time.Unix(kubeContainer.Created, 0),
		FinishedAt:   finishedAt, //todo(sw): figure out a way to store exit time of unikernel
		ExitCode:     0,          //todo(sw): figure out a way to determine unikernel exit failure/success
		Image:        kubeContainer.Image,
		ImageID:      "unik://" + kubeContainer.Image,
		Hash:         kubeContainer.Hash,
		RestartCount: 0,                        //todo(sw): figure out a way to store restart count for apps
		Reason:       "because unik said so!",  //todo(sw): figure a way to send reasons? (optional)
		Message:      "unik instance finished", //todo(sw): get the last line of the logs, perhaps?
	}
	return status
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

func convertUnikernel(unikernel *types.Unikernel) kubecontainer.Image {
	return kubecontainer.Image{
		ID:       unikernel.UnikernelName,
		RepoTags: []string{unikernel.AMI},
		Size:     1000, //todo(sw): get ami size from amazon
	}
}

func convertInstance(unikInstance *types.UnikInstance) *kubecontainer.Container {
	containerState := kubecontainer.ContainerStateUnknown
	switch unikInstance.State {
	case "pending":
	case "running":
		containerState = kubecontainer.ContainerStateRunning
	case "shutting-down":
	case "terminated":
		containerState = kubecontainer.ContainerStateExited
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
