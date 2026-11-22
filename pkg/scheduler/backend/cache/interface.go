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

package cache

import (
	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Cache 收集 Pod 信息并提供节点级别的聚合信息，供通用调度器高效查询。
// 操作以 Pod 为中心，根据 Pod 事件增量更新。
// Pod 事件经网络传递，无法保证送达：我们使用 Reflector 从远端 list/watch，
// 而 Reflector 可能变慢并执行 relist，从而漏掉部分事件。
//
// 调度器缓存中 Pod 事件的状态机：
//
//	+-------------------------------------------+  +----+
//	|                            Add            |  |    |
//	|                                           |  |    | Update
//	+      Assume                Add            v  v    |
//
// Initial +--------> Assumed +----------- ---> Added <--+
//
//	^                    +                        +
//	|                    |                        |
//	|                    |                        | Remove
//	|                    |                        |
//	|                    |                        v
//	+--------------------+                     Deleted
//	      Forget
//
// 注意 "Initial" 与 "Deleted" 状态的 Pod 并不真实存在于缓存中。
// 根据现有使用场景，我们做如下假设：
//   - 同一个 Pod 不会被 Assume 两次
//   - Pod 可能绕过调度器直接被创建，此时只会看到 Add 事件而没有 Assume 事件
//   - 未被 Add 的 Pod 不会被 Remove 或 Update
type Cache interface {
	// NodeCount 返回缓存中的节点数。
	// 仅限测试使用。
	NodeCount() int

	// PodCount 返回缓存中的 Pod 数（含已删除节点上的 Pod）。
	// 仅限测试使用。
	PodCount() (int, error)

	// GetNode 返回缓存中节点的副本。
	// 仅限测试使用。
	GetNode(name string) (*framework.NodeInfo, error)

	// AssumePod 假定 Pod 已调度，并将其信息聚合到所属节点上。
	AssumePod(logger klog.Logger, pod *v1.Pod) error

	// ForgetPod 从缓存中撤销一个 Assume 的 Pod，作为 AssumePod 的逆向操作，
	// 仅在 Pod 仍存在时调用。
	ForgetPod(logger klog.Logger, pod *v1.Pod) error

	// RemoveAssumedPod 从缓存中移除一个 Assume 的 Pod，在集群中该 Pod 已被删除时调用，
	// 以正确清理内部状态。
	RemoveAssumedPod(logger klog.Logger, pod *v1.Pod) error

	// AddPod 确认一个 Assume 的 Pod，或把一个新分配的 Pod 加入缓存。
	AddPod(logger klog.Logger, pod *v1.Pod) error

	// UpdatePod 移除 oldPod 的信息并加入 newPod 的信息。
	UpdatePod(logger klog.Logger, oldPod, newPod *v1.Pod) error

	// RemovePod 移除 Pod，并从其所在节点上扣减该 Pod 的信息。
	RemovePod(logger klog.Logger, pod *v1.Pod) error

	// GetPod 从缓存中返回与指定 Pod 同名同命名空间的 Pod。
	GetPod(pod *v1.Pod) (*v1.Pod, error)

	// IsAssumedPod 判断 Pod 是否处于 Assume 状态。
	IsAssumedPod(pod *v1.Pod) (bool, error)

	// AddNode 加入节点的整体信息。
	AddNode(logger klog.Logger, node *v1.Node)

	// UpdateNode 更新节点的整体信息。
	UpdateNode(logger klog.Logger, oldNode, newNode *v1.Node)

	// RemoveNode 移除节点的整体信息。
	RemoveNode(logger klog.Logger, node *v1.Node) error

	// UpdateSnapshot 将传入的快照更新到缓存当前内容。
	// 节点信息包含该节点上已调度（含 Assume）Pod 的聚合信息。
	// 快照只包含调用时未被删除的节点，且其中每个节点的 nodeinfo.Node() 保证非 nil。
	UpdateSnapshot(logger klog.Logger, nodeSnapshot *Snapshot) error

	// Dump 导出当前缓存内容。
	Dump() *Dump

	// BindPod 通过向 dispatcher 添加一个 bind API 调用来处理 Pod 绑定。
	// 仅在启用 SchedulerAsyncAPICalls 特性门控时使用。
	BindPod(binding *v1.Binding) (<-chan error, error)

	// PodGroupStates 返回 PodGroupStateLister。
	PodGroupStates() fwk.PodGroupStateLister

	// PodGroups 返回用于访问缓存中 PodGroup 对象的 PodGroupLister。
	PodGroups() fwk.PodGroupLister

	// CompositePodGroupStates 返回 CompositePodGroupStateLister。
	CompositePodGroupStates() fwk.CompositePodGroupStateLister

	// CompositePodGroups 返回用于访问缓存中 CompositePodGroup 对象的 lister。
	CompositePodGroups() fwk.CompositePodGroupLister

	// AddPodGroupMember 将未分配、未 Assume 的 Pod 加入其 PodGroup 状态。
	AddPodGroupMember(pod *v1.Pod)

	// UpdatePodGroupMember 更新 PodGroup 状态中的某个 Pod。
	UpdatePodGroupMember(logger klog.Logger, oldPod, newPod *v1.Pod)

	// RemovePodGroupMember 从 PodGroup 状态中移除某个 Pod。
	RemovePodGroupMember(pod *v1.Pod)

	// AddPodGroup 将 PodGroup 对象加入缓存。
	AddPodGroup(podGroup *schedulingv1beta1.PodGroup)

	// UpdatePodGroup 更新缓存中的 PodGroup 对象。
	UpdatePodGroup(logger klog.Logger, oldPodGroup, newPodGroup *schedulingv1beta1.PodGroup)

	// RemovePodGroup 从缓存中移除 PodGroup 对象。
	RemovePodGroup(logger klog.Logger, podGroup *schedulingv1beta1.PodGroup)

	// AddCompositePodGroup 将 CompositePodGroup 加入缓存。
	AddCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup)

	// UpdateCompositePodGroup 更新缓存中的 CompositePodGroup 对象。
	UpdateCompositePodGroup(logger klog.Logger, oldPodGroup, newPodGroup *schedulingv1alpha3.CompositePodGroup)

	// RemoveCompositePodGroup 从缓存中移除 CompositePodGroup。
	RemoveCompositePodGroup(logger klog.Logger, cpg *schedulingv1alpha3.CompositePodGroup)

	// BuildHierarchySnapshotFromPod 返回给定 Pod 所属 PodGroup 层级的快照。
	BuildHierarchySnapshotFromPod(pod *v1.Pod) (fwk.PodGroupManager, error)

	// GetRootKeyForGroup 返回给定 EntityKey 所在层级的根键。
	GetRootKeyForGroup(key fwk.EntityKey) (fwk.EntityKey, bool, error)
}

// Dump 表示缓存状态的一份快照导出。
type Dump struct {
	AssumedPods sets.Set[string]
	Nodes       map[string]*framework.NodeInfo
}
