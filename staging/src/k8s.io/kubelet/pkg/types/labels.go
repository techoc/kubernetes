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

package types

// 这些是容器运行时上报给 kubelet 的元数据 key，
// 调用方不要直接写字面量，统一走常量以免拼写漂移。
const (
	KubernetesPodNameLabel       = "io.kubernetes.pod.name"
	KubernetesPodNamespaceLabel  = "io.kubernetes.pod.namespace"
	KubernetesPodUIDLabel        = "io.kubernetes.pod.uid"
	KubernetesContainerNameLabel = "io.kubernetes.container.name"
)

const (
	// infra 容器不代表业务负载，用固定 "POD" 标记便于上层识别并跳过统计。
	PodInfraContainerName = "POD"
)

// key 不存在时返回空串，让调用方用统一的“缺失即空”语义处理运行时标签。
func GetContainerName(labels map[string]string) string {
	return labels[KubernetesContainerNameLabel]
}

// 直接读 map，缺失时得到空串，避免为可选元数据引入错误分支。
func GetPodName(labels map[string]string) string {
	return labels[KubernetesPodNameLabel]
}

// UID 是跨同名 Pod 重建的唯一标识，缺失时同样返回空串。
func GetPodUID(labels map[string]string) string {
	return labels[KubernetesPodUIDLabel]
}

// 命名空间与名字共同定位 Pod，缺失时返回空串交由调用方判断。
func GetPodNamespace(labels map[string]string) string {
	return labels[KubernetesPodNamespaceLabel]
}
