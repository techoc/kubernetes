/*
Copyright 2015 The Kubernetes Authors.

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

import (
	"net/http"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cri-client/pkg/logs"
)

// TODO: Reconcile custom types in kubelet/types and this subpackage

// HTTPDoer 抽象 http.Do 能力。
type HTTPDoer interface {
	// Do 发送 HTTP 请求并返回响应。
	Do(req *http.Request) (*http.Response, error)
}

// Timestamp 封装 time.Time，并提供 RFC3339Nano 格式的格式化与解析工具。
type Timestamp struct {
	// time 是底层时间值。
	time time.Time
}

// NewTimestamp 使用当前时间创建 Timestamp。
func NewTimestamp() *Timestamp {
	return &Timestamp{time.Now()}
}

// ConvertToTimestamp 使用 RFC3339NanoLenient 布局解析字符串并转换为 Timestamp。
func ConvertToTimestamp(timeString string) *Timestamp {
	// 解析失败时返回零值时间，保持与调用方约定的宽松行为。
	parsed, _ := time.Parse(logs.RFC3339NanoLenient, timeString)
	return &Timestamp{parsed}
}

// Get 返回底层的 time.Time。
func (t *Timestamp) Get() time.Time {
	return t.time
}

// GetString 使用 RFC3339NanoFixed 布局返回时间字符串。
func (t *Timestamp) GetString() string {
	return t.time.Format(logs.RFC3339NanoFixed)
}

// SortedContainerStatuses 用于按容器名称对容器状态排序。
type SortedContainerStatuses []v1.ContainerStatus

// Len 返回列表长度。
func (s SortedContainerStatuses) Len() int { return len(s) }

// Swap 交换两个元素。
func (s SortedContainerStatuses) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 按容器名称字典序比较。
func (s SortedContainerStatuses) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

// SortInitContainerStatuses 把状态按 InitContainer 在 Pod 规格中的顺序排列。
// 该函数假设状态列表中没有重名容器。
func SortInitContainerStatuses(p *v1.Pod, statuses []v1.ContainerStatus) {
	containers := p.Spec.InitContainers
	// current 指向下一个待放置的位置，已匹配的状态保持稳定。
	current := 0
	for _, container := range containers {
		for j := current; j < len(statuses); j++ {
			if container.Name == statuses[j].Name {
				statuses[current], statuses[j] = statuses[j], statuses[current]
				current++
				break
			}
		}
	}
}

// SortStatusesOfInitContainers 按 Pod 规格中的顺序返回 InitContainer 的状态。
func SortStatusesOfInitContainers(p *v1.Pod, statusMap map[string]*v1.ContainerStatus) []v1.ContainerStatus {
	containers := p.Spec.InitContainers
	statuses := []v1.ContainerStatus{}
	for _, container := range containers {
		if status, found := statusMap[container.Name]; found {
			statuses = append(statuses, *status)
		}
	}
	return statuses
}

// Reservation 表示为非 Pod 组件预留的资源。
type Reservation struct {
	// System 是预留给非 Kubernetes 组件的资源。
	System v1.ResourceList
	// Kubernetes 是预留给 Kubernetes 系统组件的资源。
	Kubernetes v1.ResourceList
}

// ResolvedPodUID 是已转换为 kubelet 内部表示形式的 Pod UID。
type ResolvedPodUID types.UID

// MirrorPodUID 是镜像 Pod 的 UID。
type MirrorPodUID types.UID
