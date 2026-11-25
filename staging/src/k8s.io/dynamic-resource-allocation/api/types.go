/*
Copyright 2024 The Kubernetes Authors.

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

package api

import (
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceSlice 是 DRA 资源切片的内部表示，仅用于进程内的比较与日志打印。
// TypeMeta 为不参与编码的内嵌类型元数据，ObjectMeta 为内嵌对象元数据，Spec 为资源切片的规格。
// 各字段的 JSON tag 用于生成可读的 klog 输出和 diff：内部字段使用大写名称，真实 API 字段使用小写名称。
// 这些 tag 有意与正常 ResourceSlice 的编码方式不兼容，以免误把该内部类型用于 API Server 请求。
type ResourceSlice struct {
	metav1.TypeMeta `json:"-"` // 不需要，且设置不一致。
	metav1.ObjectMeta

	Spec ResourceSliceSpec
}

// ResourceSliceSpec 描述资源切片的规格。
// Driver 为驱动名称；Pool 为所属资源池；NodeName、NodeSelector、AllNodes 共同描述切片适用的节点范围；
// Devices 为设备列表；PerDeviceNodeSelection 表示是否按设备分别选择节点；SharedCounters 为共享计数器集合列表；
// PartitionTypeAttribute 为分区类型属性；SkipNodeOperations 为要跳过的节点操作列表。
type ResourceSliceSpec struct {
	Driver                 UniqueString
	Pool                   ResourcePool
	NodeName               *string                         `json:",omitempty"`
	NodeSelector           *v1.NodeSelector                `json:",omitempty"`
	AllNodes               bool                            `json:",omitempty"`
	Devices                []Device                        `json:",omitempty"`
	PerDeviceNodeSelection *bool                           `json:",omitempty"`
	SharedCounters         []CounterSet                    `json:",omitempty"`
	PartitionTypeAttribute *resourceapi.FullyQualifiedName `json:",omitempty"`
	SkipNodeOperations     []resourceapi.SkipNodeOperation `json:",omitempty"`
}

// CounterSet 表示一组共享计数器。
// Name 为计数器集合名称，Counters 为计数器名称到计数器值的映射。
type CounterSet struct {
	Name     UniqueString
	Counters map[string]resourceapi.Counter `json:",omitempty"`
}

// ResourcePool 描述资源池的元信息。
// Name 为资源池名称，Generation 为资源池代际号，ResourceSliceCount 为池中的资源切片数量。
type ResourcePool struct {
	Name               UniqueString
	Generation         int64
	ResourceSliceCount int64
}

// Device 描述一个设备及其分配相关属性。
// Name 为设备名称；Attributes 为设备属性映射；Capacity 为设备容量映射；
// ConsumesCounters 为该设备消耗的计数器集合；NodeName、NodeSelector、AllNodes 描述设备适用的节点；
// Taints 为设备污点；BindsToNode 表示是否绑定到节点；BindingConditions 与 BindingFailureConditions
// 分别为绑定条件与绑定失败条件；AllowMultipleAllocations 表示是否允许多次分配；
// NodeAllocatableResources 为节点可分配资源映射。
type Device struct {
	Name                     UniqueString
	Attributes               map[resourceapi.QualifiedName]resourceapi.DeviceAttribute `json:",omitempty"`
	Capacity                 map[resourceapi.QualifiedName]resourceapi.DeviceCapacity  `json:",omitempty"`
	ConsumesCounters         []DeviceCounterConsumption                                `json:",omitempty"`
	NodeName                 *string                                                   `json:",omitempty"`
	NodeSelector             *v1.NodeSelector                                          `json:",omitempty"`
	AllNodes                 *bool                                                     `json:",omitempty"`
	Taints                   []resourceapi.DeviceTaint                                 `json:",omitempty"`
	BindsToNode              bool                                                      `json:",omitempty"`
	BindingConditions        []string                                                  `json:",omitempty"`
	BindingFailureConditions []string                                                  `json:",omitempty"`
	AllowMultipleAllocations *bool                                                     `json:",omitempty"`
	NodeAllocatableResources map[v1.ResourceName]resourceapi.NodeAllocatableResource   `json:",omitempty"`
}

// DeviceCounterConsumption 描述设备对一组计数器的消耗。
// CounterSet 为所引用的计数器集合名称，Counters 为消耗的计数器映射，CompatibilityGroups 为兼容性组列表。
type DeviceCounterConsumption struct {
	CounterSet          UniqueString
	Counters            map[string]resourceapi.Counter `json:",omitempty"`
	CompatibilityGroups []string                       `json:",omitempty"`
}
