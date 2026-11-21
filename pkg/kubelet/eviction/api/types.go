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

package api

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Signal 表示可触发节点上 Pod 驱逐的指标信号。
type Signal string

const (
	// SignalMemoryAvailable 是可用内存（capacity - workingSet），单位为字节。
	SignalMemoryAvailable Signal = "memory.available"
	// SignalNodeFsAvailable 是 kubelet 用于卷、守护进程日志等的文件系统可用空间。
	SignalNodeFsAvailable Signal = "nodefs.available"
	// SignalNodeFsInodesFree 是 kubelet 文件系统上可用的 inode 数量。
	SignalNodeFsInodesFree Signal = "nodefs.inodesFree"
	// SignalImageFsAvailable 是容器运行时存储镜像层所用文件系统的可用空间。
	// 若容器文件系统与镜像文件系统未分离，
	// imagefs 同时存放镜像层和可写层。
	SignalImageFsAvailable Signal = "imagefs.available"
	// SignalImageFsInodesFree 是容器运行时镜像文件系统上可用的 inode 数量。
	// 若容器文件系统与镜像文件系统未分离，
	// imagefs 同时存放镜像层和可写层。
	SignalImageFsInodesFree Signal = "imagefs.inodesFree"
	// SignalContainerFsAvailable 是容器运行时存放容器可写层所用文件系统的可用空间。
	// 单一文件系统时 containerfs=nodefs；
	// 存在独立镜像文件系统时 containerfs=imagefs。
	// kubelet 会根据配置覆盖用户设置，将其指向 imagefs 或 nodefs。
	SignalContainerFsAvailable Signal = "containerfs.available"
	// SignalContainerFsInodesFree 是容器运行时存放容器可写层所用文件系统上可用的 inode 数量。
	// 单一文件系统时 containerfs=nodefs；
	// 存在独立镜像文件系统时 containerfs=imagefs。
	// kubelet 会根据配置覆盖用户设置，将其指向 imagefs 或 nodefs。
	SignalContainerFsInodesFree Signal = "containerfs.inodesFree"
	// SignalAllocatableMemoryAvailable 是可供 Pod 分配的内存（allocatable - Pod 的 workingSet），单位为字节。
	SignalAllocatableMemoryAvailable Signal = "allocatableMemory.available"
	// SignalPIDAvailable 是可供 Pod 分配的 PID 数量。
	SignalPIDAvailable Signal = "pid.available"
)

// ThresholdOperator 是描述阈值比较关系的操作符。
type ThresholdOperator string

const (
	// OpLessThan 表示「小于」比较。
	OpLessThan ThresholdOperator = "LessThan"
)

// OpForSignal 把信号映射到阈值操作符。
// 目前只支持 "LessThan"，未来可能扩展（例如新增 "consumed" 类信号）。
// 阈值的方向性由信号类型隐含决定：对给定信号，驱逐只会在从某一方向
// 越过阈值时触发，不会双向触发。因此无需在 kubelet 公开 API 中暴露操作符，
// 而是在内部完成信号类型到操作符的映射。
var OpForSignal = map[Signal]ThresholdOperator{
	SignalMemoryAvailable:            OpLessThan,
	SignalNodeFsAvailable:            OpLessThan,
	SignalNodeFsInodesFree:           OpLessThan,
	SignalImageFsAvailable:           OpLessThan,
	SignalImageFsInodesFree:          OpLessThan,
	SignalContainerFsAvailable:       OpLessThan,
	SignalContainerFsInodesFree:      OpLessThan,
	SignalAllocatableMemoryAvailable: OpLessThan,
	SignalPIDAvailable:               OpLessThan,
}

// ThresholdValue 抽象绝对值与百分比两种阈值表示。
type ThresholdValue struct {
	// 以下字段互斥，只使用最靠前的非零字段。

	// Quantity 是与信号关联、用于与操作符比较的绝对数值。
	Quantity *resource.Quantity
	// Percentage 表示相对于总资源的使用百分比。
	Percentage float32
}

// Threshold 定义触发驱逐的指标阈值。
type Threshold struct {
	// Signal 是被测量的信号。
	Signal Signal
	// Operator 表示信号与阈值之间的关系。
	Operator ThresholdOperator
	// Value 是资源评估所用的阈值。
	Value ThresholdValue
	// GracePeriod 表示阈值需持续满足多久才触发驱逐。
	GracePeriod time.Duration
	// MinReclaim 表示满足阈值时至少需要回收的资源量。
	MinReclaim *ThresholdValue
}

// GetThresholdQuantity 计算阈值对应的绝对数量。
func GetThresholdQuantity(value ThresholdValue, capacity *resource.Quantity) *resource.Quantity {
	if value.Quantity != nil {
		// 绝对值直接深拷贝返回，避免调用方修改原值。
		res := value.Quantity.DeepCopy()
		return &res
	}
	// 百分比按总容量换算为绝对数量。
	return resource.NewQuantity(int64(float64(capacity.Value())*float64(value.Percentage)), resource.BinarySI)
}
