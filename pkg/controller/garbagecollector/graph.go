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

package garbagecollector

import (
	"fmt"
	"sync"

	"github.com/go-logr/logr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// objectReference 唯一标识一个对象。
// 在 OwnerReference 之外额外保存 Namespace，是因为动态客户端查询对象时必须知道命名空间，
// 而 OwnerReference 本身并不携带该字段。
type objectReference struct {
	metav1.OwnerReference
	// Namespace 是动态客户端定位对象所必需的信息，OwnerReference 中不含该字段。
	Namespace string
}

// String 提供文本日志下的可读表示。
func (s objectReference) String() string {
	return fmt.Sprintf("[%s/%s, namespace: %s, name: %s, uid: %s]", s.APIVersion, s.Kind, s.Namespace, s.Name, s.UID)
}

// MarshalLog 提供结构化（JSON）日志下的字段化表示。
func (s objectReference) MarshalLog() interface{} {
	return struct {
		Name       string    `json:"name"`
		Namespace  string    `json:"namespace"`
		APIVersion string    `json:"apiVersion"`
		UID        types.UID `json:"uid"`
	}{
		Namespace:  s.Namespace,
		Name:       s.Name,
		APIVersion: s.APIVersion,
		UID:        s.UID,
	}
}

// 编译期断言：objectReference 同时满足文本与结构化日志接口，防止接口实现被意外破坏。
var _ fmt.Stringer = objectReference{}
var _ logr.Marshaler = objectReference{}

// GraphBuilder.processGraphChanges() 是单线程的，也是对 nodes 的唯一写入者；
// 而 GarbageCollector.attemptToDeleteItem() 是多线程的，会并发读取 nodes。
// 注意：node 对不同字段使用不同的锁，getter/setter 各用对应的锁，
// 因此同一次 getter 调用序列返回的值之间可能并不一致（例如被并发的协调循环更新）。
type node struct {
	identity objectReference
	// dependents 会被 orphan() 例程读取，因此需要单独的锁保护。
	dependentsLock sync.RWMutex
	// dependents 是 ownerReference 指向本节点（identity）的所有节点，即本对象的属主关系子图。
	dependents map[*node]struct{}
	// 当对象带有 DeletionTimestamp 且设置了 FinalizerDeleteDependents 时，
	// 由 processGraphChanges() 标记为 true，表示正在等待依赖者先行删除。
	deletingDependents     bool
	deletingDependentsLock sync.RWMutex
	// beingDeleted 记录对象是否已被观测到非空的 deletionTimestamp。
	beingDeleted     bool
	beingDeletedLock sync.RWMutex
	// virtual 表示节点是根据属主引用虚拟构造出来的，从未通过 informer 事件真正观测到；
	// 一旦收到真实事件就会被 markObserved 清除该标记。
	virtual     bool
	virtualLock sync.RWMutex
	// 处理 Update 事件时，需要用 owners 与图中已记录的属主做比较，
	// 以判断属主关系是否发生变化。
	owners     []metav1.OwnerReference
	ownersLock sync.RWMutex
}

// clone() 只能在单线程的 processGraphChanges() 中调用：
// 它不加锁地读取所有字段，依赖"唯一写者"这一前提保证数据一致。
func (n *node) clone() *node {
	c := &node{
		identity:           n.identity,
		dependents:         make(map[*node]struct{}, len(n.dependents)),
		deletingDependents: n.deletingDependents,
		beingDeleted:       n.beingDeleted,
		virtual:            n.virtual,
		owners:             make([]metav1.OwnerReference, 0, len(n.owners)),
	}
	for dep := range n.dependents {
		c.dependents[dep] = struct{}{}
	}
	for _, owner := range n.owners {
		c.owners = append(c.owners, owner)
	}
	return c
}

// 对象一旦开始删除就是单向走向最终删除，不存在撤销删除的场景，
// 因此只提供将其置为 true 的 setter，不提供清除方法。
func (n *node) markBeingDeleted() {
	n.beingDeletedLock.Lock()
	defer n.beingDeletedLock.Unlock()
	n.beingDeleted = true
}

// isBeingDeleted 报告该节点是否已被观测到进入删除流程。
func (n *node) isBeingDeleted() bool {
	n.beingDeletedLock.RLock()
	defer n.beingDeletedLock.RUnlock()
	return n.beingDeleted
}

// markObserved 清除 virtual 标记：节点已通过 informer 事件被真实观测到。
func (n *node) markObserved() {
	n.virtualLock.Lock()
	defer n.virtualLock.Unlock()
	n.virtual = false
}

// isObserved 报告节点是否已被真实事件观测到（virtual 为 false）。
func (n *node) isObserved() bool {
	n.virtualLock.RLock()
	defer n.virtualLock.RUnlock()
	return !n.virtual
}

// markDeletingDependents 标记本对象正在等待其依赖者被删除。
func (n *node) markDeletingDependents() {
	n.deletingDependentsLock.Lock()
	defer n.deletingDependentsLock.Unlock()
	n.deletingDependents = true
}

// isDeletingDependents 报告是否处于等待依赖者删除的状态。
func (n *node) isDeletingDependents() bool {
	n.deletingDependentsLock.RLock()
	defer n.deletingDependentsLock.RUnlock()
	return n.deletingDependents
}

// setOwners 更新节点记录的属主引用列表。
func (n *node) setOwners(owners []metav1.OwnerReference) {
	n.ownersLock.Lock()
	defer n.ownersLock.Unlock()
	n.owners = owners
}

// getOwners 返回当前记录的属主引用列表。
func (n *node) getOwners() []metav1.OwnerReference {
	n.ownersLock.RLock()
	defer n.ownersLock.RUnlock()
	return n.owners
}

// addDependent 在属主的依赖者集合中加入一个节点。
func (n *node) addDependent(dependent *node) {
	n.dependentsLock.Lock()
	defer n.dependentsLock.Unlock()
	n.dependents[dependent] = struct{}{}
}

// deleteDependent 从属主的依赖者集合中移除一个节点。
func (n *node) deleteDependent(dependent *node) {
	n.dependentsLock.Lock()
	defer n.dependentsLock.Unlock()
	delete(n.dependents, dependent)
}

// dependentsLength 返回当前依赖者数量。
func (n *node) dependentsLength() int {
	n.dependentsLock.RLock()
	defer n.dependentsLock.RUnlock()
	return len(n.dependents)
}

// 注意：该函数不提供任何同步保证——返回的列表只是快照，
// 函数返回后 dependents 集合仍可能被并发增删。
func (n *node) getDependents() []*node {
	n.dependentsLock.RLock()
	defer n.dependentsLock.RUnlock()
	var ret []*node
	for dep := range n.dependents {
		ret = append(ret, dep)
	}
	return ret
}

// blockingDependents 返回那些阻止 n 被删除的依赖者：
// 这些依赖者的 ownerReference 指向 n，且该引用的 BlockOwnerDeletion 为 true，
// 意味着前台删除必须等它们先被删除。
// 与 getDependents 一样，返回结果不保证与后续状态一致。
func (n *node) blockingDependents() []*node {
	dependents := n.getDependents()
	var ret []*node
	for _, dep := range dependents {
		for _, owner := range dep.getOwners() {
			if owner.UID == n.identity.UID && owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
				ret = append(ret, dep)
			}
		}
	}
	return ret
}

// ownerReferenceCoordinates 只保留属主引用的定位字段（uid/name/kind/apiVersion），
// 去掉 controller、blockOwnerDeletion 等语义字段，便于比较对象身份是否相同。
func ownerReferenceCoordinates(ref metav1.OwnerReference) metav1.OwnerReference {
	return metav1.OwnerReference{
		UID:        ref.UID,
		Name:       ref.Name,
		Kind:       ref.Kind,
		APIVersion: ref.APIVersion,
	}
}

// ownerReferenceMatchesCoordinates 判断两个属主引用的定位字段是否完全一致，
// 用于确认它们指向的是同一个对象。
func ownerReferenceMatchesCoordinates(a, b metav1.OwnerReference) bool {
	return a.UID == b.UID && a.Name == b.Name && a.Kind == b.Kind && a.APIVersion == b.APIVersion
}

// String 用 fmt 输出节点内容用于调试。
// 这里需要依次获取所有读锁，因为 %#v 会反射遍历所有字段；
// 不加锁会与并发写入产生数据竞争。
func (n *node) String() string {
	n.beingDeletedLock.RLock()
	defer n.beingDeletedLock.RUnlock()

	n.virtualLock.RLock()
	defer n.virtualLock.RUnlock()

	n.dependentsLock.RLock()
	defer n.dependentsLock.RUnlock()

	n.ownersLock.RLock()
	defer n.ownersLock.RUnlock()

	return fmt.Sprintf("%#v", n)
}

// concurrentUIDToNode 是 UID 到节点的并发安全映射，供多线程的 GC worker 查询图节点。
type concurrentUIDToNode struct {
	uidToNodeLock sync.RWMutex
	uidToNode     map[types.UID]*node
}

// Write 按节点 UID 写入或覆盖映射中的节点。
func (m *concurrentUIDToNode) Write(node *node) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()
	m.uidToNode[node.identity.UID] = node
}

// Read 按 UID 查询节点，第二个返回值表示是否存在。
func (m *concurrentUIDToNode) Read(uid types.UID) (*node, bool) {
	m.uidToNodeLock.RLock()
	defer m.uidToNodeLock.RUnlock()
	n, ok := m.uidToNode[uid]
	return n, ok
}

// Delete 按 UID 从映射中移除节点。
func (m *concurrentUIDToNode) Delete(uid types.UID) {
	m.uidToNodeLock.Lock()
	defer m.uidToNodeLock.Unlock()
	delete(m.uidToNode, uid)
}
