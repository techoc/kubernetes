/*
Copyright 2017 The Kubernetes Authors.

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

package state

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/utils/cpuset"
)

// ContainerCPUAssignments 是 CPU manager 状态中使用的容器 CPU 分配
// （未启用原地垂直扩缩容）。
type ContainerCPUAssignments map[string]map[string]cpuset.CPUSet

// Clone 返回 ContainerCPUAssignments 的拷贝。
func (as ContainerCPUAssignments) Clone() ContainerCPUAssignments {
	ret := make(ContainerCPUAssignments, len(as))
	for pod := range as {
		ret[pod] = make(map[string]cpuset.CPUSet, len(as[pod]))
		maps.Copy(ret[pod], as[pod])
	}
	return ret
}

// String 返回 ContainerCPUAssignments 的字符串表示。
// Pod 与容器名按字母排序以保证输出确定；该方法只用于日志与调试，排序开销可接受。
func (as ContainerCPUAssignments) String() string {
	var sb strings.Builder
	sb.WriteString("{")

	for i, pod := range slices.Sorted(maps.Keys(as)) {
		if i > 0 {
			sb.WriteString(",")
		}
		containerMap := as[pod]
		fmt.Fprintf(&sb, "%q:{", pod)

		for j, container := range slices.Sorted(maps.Keys(containerMap)) {
			if j > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "%q:%q", container, containerMap[container].String())
		}
		sb.WriteString("}")
	}
	sb.WriteString("}")
	return sb.String()
}

// PodCPUAssignments 保存 Pod 级 CPU 分配。
type PodCPUAssignments map[string]PodEntry

// PodEntry 表示 Pod 级 CPU 分配。
type PodEntry struct {
	CPUSet cpuset.CPUSet `json:"cpuSet,omitempty"`
}

// podEntryJSON 是 PodEntry 的 JSON 序列化形态，CPUSet 以字符串存储。
type podEntryJSON struct {
	CPUSet string `json:"cpuSet,omitempty"`
}

// MarshalJSON 实现 json.Marshaler。
// 因为 CPUSet 在 checkpoint JSON 中是字符串，而内部状态中是结构体。
func (p PodEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal(podEntryJSON{
		CPUSet: p.CPUSet.String(),
	})
}

// UnmarshalJSON 实现 json.Unmarshaler。
// 因为 CPUSet 在 checkpoint JSON 中是字符串，而内部状态中是结构体。
func (p *PodEntry) UnmarshalJSON(b []byte) error {
	var entry podEntryJSON
	if err := json.Unmarshal(b, &entry); err != nil {
		return err
	}
	// 解析失败时返回带上下文的错误，避免状态静默损坏。
	cset, err := cpuset.Parse(entry.CPUSet)
	if err != nil {
		return fmt.Errorf("failed to parse cpuset: %w", err)
	}
	p.CPUSet = cset
	return nil
}

// Clone 返回 PodCPUAssignments 的拷贝。
func (a PodCPUAssignments) Clone() PodCPUAssignments {
	clone := make(PodCPUAssignments, len(a))
	maps.Copy(clone, a)
	return clone
}

// ContainerCPUAllocation 跟踪当前分配状态；
// ContainerCPUBaselines 跟踪准入时的分配状态。
// 启用原地垂直扩缩容时，需要准入时的状态才能正确记账；
// 否则该字段未使用，只关心当前状态。

// ContainerCPUBaseline 保存单个容器的准入时 CPU 基线。
type ContainerCPUBaseline struct {
	Baseline cpuset.CPUSet
}

// ContainerCPUBaselines 保存 Pod -> 容器 -> 基线的映射。
type ContainerCPUBaselines map[string]map[string]ContainerCPUBaseline

// Clone 返回 ContainerCPUBaselines 的深拷贝。
func (as ContainerCPUBaselines) Clone() ContainerCPUBaselines {
	ret := make(ContainerCPUBaselines, len(as))
	for pod := range as {
		ret[pod] = make(map[string]ContainerCPUBaseline, len(as[pod]))
		for container, orig := range as[pod] {
			ret[pod][container] = ContainerCPUBaseline{
				Baseline: orig.Baseline.Clone(),
			}
		}
	}
	return ret
}

// Reader 接口用于读取当前 CPU/Pod 分配状态。
type Reader interface {
	// 返回指定容器的 CPU 分配及是否存在。
	GetCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool)
	// 返回默认（共享池）CPU 集合。
	GetDefaultCPUSet() cpuset.CPUSet
	// 返回容器 CPU 分配，不存在时回退到默认集合。
	GetCPUSetOrDefault(podUID string, containerName string) cpuset.CPUSet
	// 返回所有容器的 CPU 分配。
	GetCPUAssignments() ContainerCPUAssignments
	// 返回 Pod 级 CPU 分配。
	GetPodCPUSet(podUID string) (cpuset.CPUSet, bool)
	// 返回所有 Pod 级 CPU 分配。
	GetPodCPUAssignments() PodCPUAssignments
	// 返回所有容器在准入时（任何 resize 之前）记录的 CPU 分配。
	GetCPUBaselines() ContainerCPUBaselines
	// 返回指定容器在准入时（任何 resize 之前）记录的 CPU 分配。
	GetBaselineCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool)
}

// writer 接口定义状态写入操作（未导出，仅由 State 组合使用）。
type writer interface {
	// 保存指定容器的 CPU 分配。
	SetCPUSet(podUID string, containerName string, cpuset cpuset.CPUSet)
	// 保存默认（共享池）CPU 集合。
	SetDefaultCPUSet(cpuset cpuset.CPUSet)
	// 用参数整体替换 CPU 分配。
	SetCPUAssignments(ContainerCPUAssignments)
	// 删除指定容器的 CPU 分配。
	Delete(podUID string, containerName string)
	// 清空所有 CPU 分配状态。
	ClearState()
	// 保存 Pod 级 CPU 分配。
	SetPodCPUSet(podUID string, cpuset cpuset.CPUSet)
	// 用参数整体替换 PodCPUAssignments。
	SetPodCPUAssignments(PodCPUAssignments)
	// 删除指定 Pod 的 Pod 级 CPU 分配。
	DeletePod(podUID string)
	// 保存所有容器在准入时（任何 resize 之前）记录的 CPU 分配。
	SetCPUBaselines(ContainerCPUBaselines)
}

// State 接口提供跟踪与设置 CPU/Pod 分配的方法。
type State interface {
	Reader
	writer
}
