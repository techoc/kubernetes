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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

// BaseControllerRefManager 封装控制器认领/释放对象所需的基础状态：控制器对象、
// 标签选择器，以及只会执行一次的 CanAdopt 前置检查。
type BaseControllerRefManager struct {
	Controller metav1.Object
	Selector   labels.Selector

	canAdoptErr  error
	canAdoptOnce sync.Once
	CanAdoptFunc func(ctx context.Context) error
}

// CanAdopt 执行（至多一次）是否可以认领对象的前置检查，失败错误会被缓存复用。
func (m *BaseControllerRefManager) CanAdopt(ctx context.Context) error {
	m.canAdoptOnce.Do(func() {
		if m.CanAdoptFunc != nil {
			m.canAdoptErr = m.CanAdoptFunc(ctx)
		}
	})
	return m.canAdoptErr
}

// ClaimObject 尝试让本控制器取得对象的所有权：match 返回 true 时认领孤儿对象，
// 返回 false 时释放已拥有的对象。
//
// 若尝试了调和但失败，返回非 nil 错误，调用方通常应稍后重试。若返回 nil，
// 表示调和成功或无需调和；布尔返回值表示当前是否拥有该对象。
//
// 控制器正在被删除时不会执行任何认领/释放操作（已拥有且仍匹配的对象除外：
// 确认所有权本身不需要写操作）。
func (m *BaseControllerRefManager) ClaimObject(ctx context.Context, obj metav1.Object, match func(metav1.Object) bool, adopt, release func(context.Context, metav1.Object) error) (bool, error) {
	controllerRef := metav1.GetControllerOfNoCopy(obj)
	if controllerRef != nil {
		if controllerRef.UID != m.Controller.GetUID() {
			// 属主是其他控制器，直接忽略。
			return false, nil
		}
		if match(obj) {
			// 已拥有且选择器匹配：无需任何写操作，即使控制器正在删除也仍可确认所有权，
			// 因此先返回 true，不再检查删除时间戳。
			return true, nil
		}
		// Owned by us but selector doesn't match.
		// Try to release, unless we're being deleted.
		if m.Controller.GetDeletionTimestamp() != nil {
			return false, nil
		}
		if err := release(ctx, obj); err != nil {
			// 对象已被删除时忽略认领错误。
			if errors.IsNotFound(err) {
				return false, nil
			}
			// 可能是其他控制器抢先释放，也可能是瞬时错误；调用方应重新入队稍后重试。
			return false, err
		}
		// 释放成功，当前不再拥有该对象。
		return false, nil
	}

	// 对象没有控制器属主，是孤儿对象。
	if m.Controller.GetDeletionTimestamp() != nil || !match(obj) {
		// 控制器正在删除或选择器不匹配时，忽略该孤儿对象。
		return false, nil
	}
	if obj.GetDeletionTimestamp() != nil {
		// 对象自身正在删除时，认领没有意义。
		return false, nil
	}

	if len(m.Controller.GetNamespace()) > 0 && m.Controller.GetNamespace() != obj.GetNamespace() {
		// 命名空间作用域控制器不能跨命名空间认领对象。
		return false, nil
	}

	// 各项检查通过，尝试认领该孤儿对象。
	if err := adopt(ctx, obj); err != nil {
		// 对象已被删除时忽略认领错误。
		if errors.IsNotFound(err) {
			return false, nil
		}
		// 可能被其他控制器抢先认领，也可能是瞬时错误；调用方应重新入队稍后重试。
		return false, err
	}
	// 认领成功。
	return true, nil
}

// PodControllerRefManager 管理 Pod 的 controllerRef，通过 PodControlInterface 打补丁完成认领与释放。
type PodControllerRefManager struct {
	BaseControllerRefManager
	controllerKind schema.GroupVersionKind
	podControl     PodControlInterface
	finalizers     []string
}

// NewPodControllerRefManager 构造管理 Pod controllerRef 的 PodControllerRefManager。
//
// canAdopt 可用于在首次认领前执行开销较大的检查（例如向 API Server 实时 GET），
// 仅在实际尝试认领时调用（至多一次）；返回非 nil 错误时所有认领都会失败。
//
// 注意：同一个 manager 实例调用过 CanAdopt() 后不会再次调用。若需要在新一轮同步中
// 重新检查（例如控制器状态已变化），应创建新的实例。
func NewPodControllerRefManager(
	podControl PodControlInterface,
	controller metav1.Object,
	selector labels.Selector,
	controllerKind schema.GroupVersionKind,
	canAdopt func(ctx context.Context) error,
	finalizers ...string,
) *PodControllerRefManager {
	return &PodControllerRefManager{
		BaseControllerRefManager: BaseControllerRefManager{
			Controller:   controller,
			Selector:     selector,
			CanAdoptFunc: canAdopt,
		},
		controllerKind: controllerKind,
		podControl:     podControl,
		finalizers:     finalizers,
	}
}

// ClaimPods 尝试认领一组 Pod：选择器匹配时认领孤儿 Pod，不再匹配时释放已拥有的 Pod。
// 若提供了 filters，只有所有过滤器都返回 true 的 Pod 才会被认领。
// 返回当前由本控制器拥有的 Pod 列表；若调和过程出现失败，返回聚合错误，调用方应稍后重试。
func (m *PodControllerRefManager) ClaimPods(ctx context.Context, pods []*v1.Pod, filters ...func(*v1.Pod) bool) ([]*v1.Pod, error) {
	var claimed []*v1.Pod
	var errlist []error

	match := func(obj metav1.Object) bool {
		pod := obj.(*v1.Pod)
		// 先做开销较低的选择器匹配，过滤器只对可能匹配的 Pod 执行。
		if !m.Selector.Matches(labels.Set(pod.Labels)) {
			return false
		}
		for _, filter := range filters {
			if !filter(pod) {
				return false
			}
		}
		return true
	}
	adopt := func(ctx context.Context, obj metav1.Object) error {
		return m.AdoptPod(ctx, obj.(*v1.Pod))
	}
	release := func(ctx context.Context, obj metav1.Object) error {
		return m.ReleasePod(ctx, obj.(*v1.Pod))
	}

	for _, pod := range pods {
		ok, err := m.ClaimObject(ctx, pod, match, adopt, release)
		if err != nil {
			errlist = append(errlist, err)
			continue
		}
		if ok {
			claimed = append(claimed, pod)
		}
	}
	return claimed, utilerrors.NewAggregate(errlist)
}

// AdoptPod 发送补丁，将本控制器设置为 Pod 的控制器属主。
func (m *PodControllerRefManager) AdoptPod(ctx context.Context, pod *v1.Pod) error {
	if err := m.CanAdopt(ctx); err != nil {
		return fmt.Errorf("can't adopt Pod %v/%v (%v): %v", pod.Namespace, pod.Name, pod.UID, err)
	}
	// 注意：若 Pod 上已有 controller=true 的属主，ValidateOwnerReferences() 会拒绝该补丁，
	// 因此无需在客户端再做竞态检查。

	patchBytes, err := ownerRefControllerPatch(m.Controller, m.controllerKind, pod.UID, m.finalizers...)
	if err != nil {
		return err
	}
	return m.podControl.PatchPod(ctx, pod.Namespace, pod.Name, patchBytes)
}

// ReleasePod 发送补丁，移除 Pod 上指向本控制器的 controllerRef。404 与 422 错误会被忽略。
func (m *PodControllerRefManager) ReleasePod(ctx context.Context, pod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Patching pod to remove its controllerRef", "pod", klog.KObj(pod), "gvk", m.controllerKind, "controller", m.Controller.GetName())
	patchBytes, err := GenerateDeleteOwnerRefStrategicMergeBytes(pod.UID, []types.UID{m.Controller.GetUID()}, m.finalizers...)
	if err != nil {
		return err
	}
	err = m.podControl.PatchPod(ctx, pod.Namespace, pod.Name, patchBytes)
	if err != nil {
		if errors.IsNotFound(err) {
			// Pod 已被删除，无需释放。
			return nil
		}
		if errors.IsInvalid(err) {
			// 422 在两种情况下出现：Pod 已无属主，或 Pod 的 UID 不匹配（即被删除后重建）；
			// 两种情况都说明目标状态已达成，可以忽略。

			// TODO: If the pod has owner references, but none of them
			// has the owner.UID, server will silently ignore the patch.
			// Investigate why.
			return nil
		}
	}
	return err
}

// ReplicaSetControllerRefManager 管理 ReplicaSet 的 controllerRef，提供分类、认领与释放方法。
type ReplicaSetControllerRefManager struct {
	BaseControllerRefManager
	controllerKind schema.GroupVersionKind
	rsControl      RSControlInterface
}

// NewReplicaSetControllerRefManager 构造管理 ReplicaSet controllerRef 的管理器。
// canAdopt 仅在首次实际认领时调用（至多一次），可用来做实时 GET 等开销较大的检查；
// 返回错误则所有认领失败。同一实例不会重复调用 CanAdopt()，需要重新检查时请新建实例。
func NewReplicaSetControllerRefManager(
	rsControl RSControlInterface,
	controller metav1.Object,
	selector labels.Selector,
	controllerKind schema.GroupVersionKind,
	canAdopt func(ctx context.Context) error,
) *ReplicaSetControllerRefManager {
	return &ReplicaSetControllerRefManager{
		BaseControllerRefManager: BaseControllerRefManager{
			Controller:   controller,
			Selector:     selector,
			CanAdoptFunc: canAdopt,
		},
		controllerKind: controllerKind,
		rsControl:      rsControl,
	}
}

// ClaimReplicaSets 认领选择器匹配的孤儿 ReplicaSet，释放已拥有但不再匹配的 ReplicaSet，
// 返回当前拥有的 ReplicaSet 列表及调和过程中出现的聚合错误。
func (m *ReplicaSetControllerRefManager) ClaimReplicaSets(ctx context.Context, sets []*apps.ReplicaSet) ([]*apps.ReplicaSet, error) {
	var claimed []*apps.ReplicaSet
	var errlist []error

	match := func(obj metav1.Object) bool {
		return m.Selector.Matches(labels.Set(obj.GetLabels()))
	}
	adopt := func(ctx context.Context, obj metav1.Object) error {
		return m.AdoptReplicaSet(ctx, obj.(*apps.ReplicaSet))
	}
	release := func(ctx context.Context, obj metav1.Object) error {
		return m.ReleaseReplicaSet(ctx, obj.(*apps.ReplicaSet))
	}

	for _, rs := range sets {
		ok, err := m.ClaimObject(ctx, rs, match, adopt, release)
		if err != nil {
			errlist = append(errlist, err)
			continue
		}
		if ok {
			claimed = append(claimed, rs)
		}
	}
	return claimed, utilerrors.NewAggregate(errlist)
}

// AdoptReplicaSet 发送补丁，将本控制器设置为 ReplicaSet 的控制器属主。
func (m *ReplicaSetControllerRefManager) AdoptReplicaSet(ctx context.Context, rs *apps.ReplicaSet) error {
	if err := m.CanAdopt(ctx); err != nil {
		return fmt.Errorf("can't adopt ReplicaSet %v/%v (%v): %v", rs.Namespace, rs.Name, rs.UID, err)
	}
	// 注意：若 Pod 上已有 controller=true 的属主，ValidateOwnerReferences() 会拒绝该补丁，
	// 因此无需在客户端再做竞态检查。
	patchBytes, err := ownerRefControllerPatch(m.Controller, m.controllerKind, rs.UID)
	if err != nil {
		return err
	}
	return m.rsControl.PatchReplicaSet(ctx, rs.Namespace, rs.Name, patchBytes)
}

// ReleaseReplicaSet 发送补丁，移除 ReplicaSet 上指向本控制器的属主引用；404 与 422 错误会被忽略。
func (m *ReplicaSetControllerRefManager) ReleaseReplicaSet(ctx context.Context, replicaSet *apps.ReplicaSet) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Patching ReplicaSet to remove its controllerRef", "replicaSet", klog.KObj(replicaSet), "gvk", m.controllerKind, "controller", m.Controller.GetName())
	patchBytes, err := GenerateDeleteOwnerRefStrategicMergeBytes(replicaSet.UID, []types.UID{m.Controller.GetUID()})
	if err != nil {
		return err
	}
	err = m.rsControl.PatchReplicaSet(ctx, replicaSet.Namespace, replicaSet.Name, patchBytes)
	if err != nil {
		if errors.IsNotFound(err) {
			// ReplicaSet 已被删除，无需释放。
			return nil
		}
		if errors.IsInvalid(err) {
			// 422 表示 ReplicaSet 已无属主或 UID 不匹配（删除后重建），两种情况均可忽略。
			return nil
		}
	}
	return err
}

// RecheckDeletionTimestamp 返回一个 CanAdopt 函数：通过 getObject 获取控制器最新状态，
// 若其已进入删除流程（DeletionTimestamp 非空）则拒绝认领，避免为即将消失的控制器绑定对象。
func RecheckDeletionTimestamp(getObject func(context.Context) (metav1.Object, error)) func(context.Context) error {
	return func(ctx context.Context) error {
		obj, err := getObject(ctx)
		if err != nil {
			return fmt.Errorf("can't recheck DeletionTimestamp: %v", err)
		}
		if obj.GetDeletionTimestamp() != nil {
			return fmt.Errorf("%v/%v has just been deleted at %v", obj.GetNamespace(), obj.GetName(), obj.GetDeletionTimestamp())
		}
		return nil
	}
}

// ControllerRevisionControllerRefManager 管理 ControllerRevision 的 controllerRef，提供分类、认领与释放方法。
type ControllerRevisionControllerRefManager struct {
	BaseControllerRefManager
	controllerKind schema.GroupVersionKind
	crControl      ControllerRevisionControlInterface
}

// NewControllerRevisionControllerRefManager 构造管理 ControllerRevision controllerRef 的管理器。
// canAdopt 仅在首次实际认领时调用（至多一次），可用于实时 GET 等开销较大的检查；
// 返回错误则所有认领失败。同一实例不会重复调用，需重新检查时请新建实例。
func NewControllerRevisionControllerRefManager(
	crControl ControllerRevisionControlInterface,
	controller metav1.Object,
	selector labels.Selector,
	controllerKind schema.GroupVersionKind,
	canAdopt func(ctx context.Context) error,
) *ControllerRevisionControllerRefManager {
	return &ControllerRevisionControllerRefManager{
		BaseControllerRefManager: BaseControllerRefManager{
			Controller:   controller,
			Selector:     selector,
			CanAdoptFunc: canAdopt,
		},
		controllerKind: controllerKind,
		crControl:      crControl,
	}
}

// ClaimControllerRevisions 认领选择器匹配的孤儿 ControllerRevision，释放已拥有但不再匹配的对象，
// 返回当前拥有的 ControllerRevision 列表及聚合错误。
func (m *ControllerRevisionControllerRefManager) ClaimControllerRevisions(ctx context.Context, histories []*apps.ControllerRevision) ([]*apps.ControllerRevision, error) {
	var claimed []*apps.ControllerRevision
	var errlist []error

	match := func(obj metav1.Object) bool {
		return m.Selector.Matches(labels.Set(obj.GetLabels()))
	}
	adopt := func(ctx context.Context, obj metav1.Object) error {
		return m.AdoptControllerRevision(ctx, obj.(*apps.ControllerRevision))
	}
	release := func(ctx context.Context, obj metav1.Object) error {
		return m.ReleaseControllerRevision(ctx, obj.(*apps.ControllerRevision))
	}

	for _, h := range histories {
		ok, err := m.ClaimObject(ctx, h, match, adopt, release)
		if err != nil {
			errlist = append(errlist, err)
			continue
		}
		if ok {
			claimed = append(claimed, h)
		}
	}
	return claimed, utilerrors.NewAggregate(errlist)
}

// AdoptControllerRevision 发送补丁，将本控制器设置为 ControllerRevision 的控制器属主。
func (m *ControllerRevisionControllerRefManager) AdoptControllerRevision(ctx context.Context, history *apps.ControllerRevision) error {
	if err := m.CanAdopt(ctx); err != nil {
		return fmt.Errorf("can't adopt ControllerRevision %v/%v (%v): %v", history.Namespace, history.Name, history.UID, err)
	}
	// 注意：若 Pod 上已有 controller=true 的属主，ValidateOwnerReferences() 会拒绝该补丁，
	// 因此无需在客户端再做竞态检查。
	patchBytes, err := ownerRefControllerPatch(m.Controller, m.controllerKind, history.UID)
	if err != nil {
		return err
	}
	return m.crControl.PatchControllerRevision(ctx, history.Namespace, history.Name, patchBytes)
}

// ReleaseControllerRevision 发送补丁，移除 ControllerRevision 上指向本控制器的属主引用；404 与 422 错误会被忽略。
func (m *ControllerRevisionControllerRefManager) ReleaseControllerRevision(ctx context.Context, history *apps.ControllerRevision) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Patching ControllerRevision to remove its controllerRef", "controllerRevision", klog.KObj(history), "gvk", m.controllerKind, "controller", m.Controller.GetName())
	patchBytes, err := GenerateDeleteOwnerRefStrategicMergeBytes(history.UID, []types.UID{m.Controller.GetUID()})
	if err != nil {
		return err
	}

	err = m.crControl.PatchControllerRevision(ctx, history.Namespace, history.Name, patchBytes)
	if err != nil {
		if errors.IsNotFound(err) {
			// ControllerRevision 已被删除，无需释放。
			return nil
		}
		if errors.IsInvalid(err) {
			// 422 表示对象已无属主或 UID 不匹配（删除后重建），两种情况均可忽略。
			return nil
		}
	}
	return err
}

// objectForAddOwnerRefPatch 及 objectMetaForPatch 描述认领时使用的 strategic-merge 补丁结构。
type objectForAddOwnerRefPatch struct {
	Metadata objectMetaForPatch `json:"metadata"`
}

type objectMetaForPatch struct {
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
	UID             types.UID               `json:"uid"`
	Finalizers      []string                `json:"finalizers,omitempty"`
}

// ownerRefControllerPatch 生成添加控制器属主引用（controller=true、blockOwnerDeletion=true）的补丁；
// uid 字段作为乐观并发校验，对象在读取后被替换时补丁会被拒绝。
func ownerRefControllerPatch(controller metav1.Object, controllerKind schema.GroupVersionKind, uid types.UID, finalizers ...string) ([]byte, error) {
	blockOwnerDeletion := true
	isController := true
	addControllerPatch := objectForAddOwnerRefPatch{
		Metadata: objectMetaForPatch{
			UID: uid,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         controllerKind.GroupVersion().String(),
					Kind:               controllerKind.Kind,
					Name:               controller.GetName(),
					UID:                controller.GetUID(),
					Controller:         &isController,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
			Finalizers: finalizers,
		},
	}
	patchBytes, err := json.Marshal(&addControllerPatch)
	if err != nil {
		return nil, err
	}
	return patchBytes, nil
}

// 以下类型描述释放属主引用时使用的 strategic-merge 补丁结构。
type objectForDeleteOwnerRefStrategicMergePatch struct {
	Metadata objectMetaForMergePatch `json:"metadata"`
}

type objectMetaForMergePatch struct {
	UID              types.UID           `json:"uid"`
	OwnerReferences  []map[string]string `json:"ownerReferences"`
	DeleteFinalizers []string            `json:"$deleteFromPrimitiveList/finalizers,omitempty"`
}

// GenerateDeleteOwnerRefStrategicMergeBytes 生成删除指定属主引用的 strategic-merge 补丁，
// 并可通过 $deleteFromPrimitiveList 一并移除 finalizers；dependentUID 用于并发校验。
func GenerateDeleteOwnerRefStrategicMergeBytes(dependentUID types.UID, ownerUIDs []types.UID, finalizers ...string) ([]byte, error) {
	var ownerReferences []map[string]string
	for _, ownerUID := range ownerUIDs {
		ownerReferences = append(ownerReferences, ownerReference(ownerUID, "delete"))
	}
	patch := objectForDeleteOwnerRefStrategicMergePatch{
		Metadata: objectMetaForMergePatch{
			UID:              dependentUID,
			OwnerReferences:  ownerReferences,
			DeleteFinalizers: finalizers,
		},
	}
	patchBytes, err := json.Marshal(&patch)
	if err != nil {
		return nil, err
	}
	return patchBytes, nil
}

// ownerReference 构造 strategic-merge 补丁中的单个属主引用操作项，按 UID 定位要删除的引用。
func ownerReference(uid types.UID, patchType string) map[string]string {
	return map[string]string{
		"$patch": patchType,
		"uid":    string(uid),
	}
}
