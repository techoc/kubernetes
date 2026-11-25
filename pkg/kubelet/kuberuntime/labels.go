/*
Copyright 2016 The Kubernetes Authors.

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

package kuberuntime

import (
	"context"
	"encoding/json"
	"strconv"

	v1 "k8s.io/api/core/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubelet/pkg/types"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

const (
	// TODO: 调整这些标签名以符合 Kubernetes 的格式。
	// 这些标签/注解键名由 kubelet 写入容器运行时，用于重启后重建 Pod 与容器状态。
	podDeletionGracePeriodLabel    = "io.kubernetes.pod.deletionGracePeriod"
	podTerminationGracePeriodLabel = "io.kubernetes.pod.terminationGracePeriod"

	containerHashLabel                     = "io.kubernetes.container.hash"
	containerRestartCountLabel             = "io.kubernetes.container.restartCount"
	containerTerminationMessagePathLabel   = "io.kubernetes.container.terminationMessagePath"
	containerTerminationMessagePolicyLabel = "io.kubernetes.container.terminationMessagePolicy"
	containerPreStopHandlerLabel           = "io.kubernetes.container.preStopHandler"
	containerPortsLabel                    = "io.kubernetes.container.ports"
)

// labeledPodSandboxInfo 是从 Pod sandbox 标签中解析出的信息。
type labeledPodSandboxInfo struct {
	// 来自 v1.Pod 的标签，不含 kubelet 自己的保留标签。
	Labels       map[string]string
	PodName      string
	PodNamespace string
	PodUID       kubetypes.UID
}

// annotatedPodSandboxInfo 是从 Pod sandbox 注解中解析出的信息。
type annotatedPodSandboxInfo struct {
	// 来自 v1.Pod 的注解。
	Annotations map[string]string
}

// labeledContainerInfo 是从容器标签中解析出的信息。
type labeledContainerInfo struct {
	ContainerName string
	PodName       string
	PodNamespace  string
	PodUID        kubetypes.UID
}

// annotatedContainerInfo 是从容器注解中解析出的运行时信息，
// 这些字段在 kubelet 重启后用于恢复容器状态。
type annotatedContainerInfo struct {
	// Hash 是容器 spec 的哈希，用于判断容器是否需要重建。
	Hash                      uint64
	RestartCount              int
	PodDeletionGracePeriod    *int64
	PodTerminationGracePeriod *int64
	TerminationMessagePath    string
	TerminationMessagePolicy  v1.TerminationMessagePolicy
	PreStopHandler            *v1.LifecycleHandler
	ContainerPorts            []v1.ContainerPort
}

// newPodLabels 根据 v1.Pod 构造写入 Pod sandbox 的标签。
func newPodLabels(pod *v1.Pod) map[string]string {
	labels := map[string]string{}

	// 先复制 v1.Pod 自带的标签，再写入 kubelet 用于识别 Pod 的保留标签。
	for k, v := range pod.Labels {
		labels[k] = v
	}

	labels[types.KubernetesPodNameLabel] = pod.Name
	labels[types.KubernetesPodNamespaceLabel] = pod.Namespace
	labels[types.KubernetesPodUIDLabel] = string(pod.UID)

	return labels
}

// newPodAnnotations 根据 v1.Pod 构造写入 Pod sandbox 的注解。
func newPodAnnotations(pod *v1.Pod) map[string]string {
	return pod.Annotations
}

// newContainerLabels 根据 v1.Container 和 v1.Pod 构造容器标签。
func newContainerLabels(container *v1.Container, pod *v1.Pod) map[string]string {
	labels := map[string]string{}
	labels[types.KubernetesPodNameLabel] = pod.Name
	labels[types.KubernetesPodNamespaceLabel] = pod.Namespace
	labels[types.KubernetesPodUIDLabel] = string(pod.UID)
	labels[types.KubernetesContainerNameLabel] = container.Name

	return labels
}

// newContainerAnnotations 根据 v1.Container 和 v1.Pod 构造容器注解，
// 记录容器哈希、重启次数、优雅终止参数以及 preStop 钩子和端口等信息。
func newContainerAnnotations(ctx context.Context, container *v1.Container, pod *v1.Pod, restartCount int, opts *kubecontainer.RunContainerOptions) map[string]string {
	logger := klog.FromContext(ctx)
	annotations := map[string]string{}

	// 当设备插件注解与 kubelet 注解冲突时，kubelet 始终覆盖前者。
	for _, a := range opts.Annotations {
		annotations[a.Name] = a.Value
	}

	annotations[containerHashLabel] = strconv.FormatUint(kubecontainer.HashContainer(container), 16)
	annotations[containerRestartCountLabel] = strconv.Itoa(restartCount)
	annotations[containerTerminationMessagePathLabel] = container.TerminationMessagePath
	annotations[containerTerminationMessagePolicyLabel] = string(container.TerminationMessagePolicy)

	if pod.DeletionGracePeriodSeconds != nil {
		annotations[podDeletionGracePeriodLabel] = strconv.FormatInt(*pod.DeletionGracePeriodSeconds, 10)
	}
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		annotations[podTerminationGracePeriodLabel] = strconv.FormatInt(*pod.Spec.TerminationGracePeriodSeconds, 10)
	}

	if container.Lifecycle != nil && container.Lifecycle.PreStop != nil {
		// 使用 JSON 编码，写入注解后仍可还原出可读的 PreStop 处理器对象。
		rawPreStop, err := json.Marshal(container.Lifecycle.PreStop)
		if err != nil {
			logger.Error(err, "Unable to marshal lifecycle PreStop handler for container", "containerName", container.Name, "pod", klog.KObj(pod))
		} else {
			annotations[containerPreStopHandlerLabel] = string(rawPreStop)
		}
	}

	// 端口列表同样用 JSON 编码，便于运行时重启后重建端口映射。
	if len(container.Ports) > 0 {
		rawContainerPorts, err := json.Marshal(container.Ports)
		if err != nil {
			logger.Error(err, "Unable to marshal container ports for container", "containerName", container.Name, "pod", klog.KObj(pod))
		} else {
			annotations[containerPortsLabel] = string(rawContainerPorts)
		}
	}

	return annotations
}

// getPodSandboxInfoFromLabels 从标签中解析出 labeledPodSandboxInfo。
func getPodSandboxInfoFromLabels(ctx context.Context, labels map[string]string) *labeledPodSandboxInfo {
	logger := klog.FromContext(ctx)
	podSandboxInfo := &labeledPodSandboxInfo{
		Labels:       make(map[string]string),
		PodName:      getStringValueFromLabel(logger, labels, types.KubernetesPodNameLabel),
		PodNamespace: getStringValueFromLabel(logger, labels, types.KubernetesPodNamespaceLabel),
		PodUID:       kubetypes.UID(getStringValueFromLabel(logger, labels, types.KubernetesPodUIDLabel)),
	}

	// 只保留来自 v1.Pod 的标签，过滤掉 kubelet 用于识别的保留标签。
	for k, v := range labels {
		if k != types.KubernetesPodNameLabel && k != types.KubernetesPodNamespaceLabel && k != types.KubernetesPodUIDLabel {
			podSandboxInfo.Labels[k] = v
		}
	}

	return podSandboxInfo
}

// getPodSandboxInfoFromAnnotations 从注解中解析出 annotatedPodSandboxInfo。
func getPodSandboxInfoFromAnnotations(annotations map[string]string) *annotatedPodSandboxInfo {
	return &annotatedPodSandboxInfo{
		Annotations: annotations,
	}
}

// getContainerInfoFromLabels 从标签中解析出 labeledContainerInfo。
func getContainerInfoFromLabels(ctx context.Context, labels map[string]string) *labeledContainerInfo {
	logger := klog.FromContext(ctx)
	return &labeledContainerInfo{
		PodName:       getStringValueFromLabel(logger, labels, types.KubernetesPodNameLabel),
		PodNamespace:  getStringValueFromLabel(logger, labels, types.KubernetesPodNamespaceLabel),
		PodUID:        kubetypes.UID(getStringValueFromLabel(logger, labels, types.KubernetesPodUIDLabel)),
		ContainerName: getStringValueFromLabel(logger, labels, types.KubernetesContainerNameLabel),
	}
}

// getContainerInfoFromAnnotations 从注解中解析出 annotatedContainerInfo。
func getContainerInfoFromAnnotations(ctx context.Context, annotations map[string]string) *annotatedContainerInfo {
	logger := klog.FromContext(ctx)
	var err error
	containerInfo := &annotatedContainerInfo{
		TerminationMessagePath:   getStringValueFromLabel(logger, annotations, containerTerminationMessagePathLabel),
		TerminationMessagePolicy: v1.TerminationMessagePolicy(getStringValueFromLabel(logger, annotations, containerTerminationMessagePolicyLabel)),
	}

	// 哈希以十六进制存储；解析失败只记录错误，不阻断容器信息重建。
	if containerInfo.Hash, err = getUint64ValueFromLabel(ctx, annotations, containerHashLabel); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", containerHashLabel, "annotations", annotations)
	}
	// 重启次数以十进制存储。
	if containerInfo.RestartCount, err = getIntValueFromLabel(logger, annotations, containerRestartCountLabel); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", containerRestartCountLabel, "annotations", annotations)
	}
	if containerInfo.PodDeletionGracePeriod, err = getInt64PointerFromLabel(annotations, podDeletionGracePeriodLabel); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", podDeletionGracePeriodLabel, "annotations", annotations)
	}
	if containerInfo.PodTerminationGracePeriod, err = getInt64PointerFromLabel(annotations, podTerminationGracePeriodLabel); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", podTerminationGracePeriodLabel, "annotations", annotations)
	}

	// preStop 处理器是可选信息，只有注解存在时才反序列化。
	preStopHandler := &v1.LifecycleHandler{}
	if found, err := getJSONObjectFromLabel(annotations, containerPreStopHandlerLabel, preStopHandler); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", containerPreStopHandlerLabel, "annotations", annotations)
	} else if found {
		containerInfo.PreStopHandler = preStopHandler
	}

	// 端口列表同样是可选信息。
	containerPorts := []v1.ContainerPort{}
	if found, err := getJSONObjectFromLabel(annotations, containerPortsLabel, &containerPorts); err != nil {
		logger.Error(err, "Unable to get label value from annotations", "label", containerPortsLabel, "annotations", annotations)
	} else if found {
		containerInfo.ContainerPorts = containerPorts
	}

	return containerInfo
}

// getStringValueFromLabel 读取字符串标签值；标签缺失时返回空字符串而不是错误。
func getStringValueFromLabel(logger klog.Logger, labels map[string]string, label string) string {
	if value, found := labels[label]; found {
		return value
	}
	// 不返回错误，因为现存大量旧容器没有该标签。
	logger.V(3).Info("Container doesn't have requested label, it may be an old or invalid container", "label", label)
	// 对这类容器返回空字符串，调用方会通过其他途径获取值。
	return ""
}

// getIntValueFromLabel 以十进制读取整型标签值；标签缺失时返回 0 而不是错误。
func getIntValueFromLabel(logger klog.Logger, labels map[string]string, label string) (int, error) {
	if strValue, found := labels[label]; found {
		intValue, err := strconv.Atoi(strValue)
		if err != nil {
			// 这种情况理论上不应出现，用 0 兜底处理异常数据。
			return 0, err
		}
		return intValue, nil
	}
	// 不返回错误，因为现存大量旧容器没有该标签。
	logger.V(3).Info("Container doesn't have requested label, it may be an old or invalid container", "label", label)
	// 对缺失标签统一按 0 处理。
	return 0, nil
}

// getUint64ValueFromLabel 以十六进制读取 uint64 标签值；标签缺失时返回 0 而不是错误。
func getUint64ValueFromLabel(ctx context.Context, labels map[string]string, label string) (uint64, error) {
	logger := klog.FromContext(ctx)
	if strValue, found := labels[label]; found {
		// 容器哈希以十六进制字符串写入，因此这里按 16 进制解析。
		intValue, err := strconv.ParseUint(strValue, 16, 64)
		if err != nil {
			// 这种情况理论上不应出现，用 0 兜底处理异常数据。
			return 0, err
		}
		return intValue, nil
	}
	// 不返回错误，因为现存大量旧容器没有该标签。
	logger.V(3).Info("Container doesn't have requested label, it may be an old or invalid container", "label", label)
	// 对缺失标签统一按 0 处理。
	return 0, nil
}

// getInt64PointerFromLabel 以十进制读取指向 int64 的标签值；
// 标签缺失时返回 nil 指针，以区分 "未设置" 和 "值为 0"。
func getInt64PointerFromLabel(labels map[string]string, label string) (*int64, error) {
	if strValue, found := labels[label]; found {
		int64Value, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			return nil, err
		}
		return &int64Value, nil
	}
	// 标签不存在时返回 nil 指针，保留 "未设置" 的语义。
	return nil, nil
}

// getJSONObjectFromLabel 反序列化 JSON 标签值，返回的 bool 表示标签是否存在。
func getJSONObjectFromLabel(labels map[string]string, label string, value interface{}) (bool, error) {
	if strValue, found := labels[label]; found {
		err := json.Unmarshal([]byte(strValue), value)
		return found, err
	}
	// 标签不存在时返回 false。
	return false, nil
}
