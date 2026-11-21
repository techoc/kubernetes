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
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

// resourceDefaultNamespace 对命名空间级资源返回 defaultNamespace；
// 集群级资源没有命名空间，必须返回空串，否则元数据客户端会构造出错误的请求路径。
func resourceDefaultNamespace(namespaced bool, defaultNamespace string) string {
	if namespaced {
		return defaultNamespace
	}
	return ""
}

// apiResource 通过 RESTMapper 把 <apiVersion, kind> 解析为资源名并判断作用域。
// 同时返回 namespaced 标志，供调用方决定是否需要带上命名空间坐标。
func (gc *GarbageCollector) apiResource(apiVersion, kind string) (schema.GroupVersionResource, bool, error) {
	fqKind := schema.FromAPIVersionAndKind(apiVersion, kind)
	mapping, err := gc.restMapper.RESTMapping(fqKind.GroupKind(), fqKind.Version)
	if err != nil {
		return schema.GroupVersionResource{}, false, newRESTMappingError(kind, apiVersion)
	}
	return mapping.Resource, mapping.Scope == meta.RESTScopeNamespace, nil
}

// deleteObject 删除对象，并使用 UID（以及可选的 resourceVersion）作为前置条件，
// 防止在 GC 读取对象与发出删除之间对象被替换时误删同名的新对象。
// 若因 resourceVersion 冲突失败且所有者引用未变化，则去掉该前置条件重试，
// 避免与所有者引用无关的频繁更新使 GC 一直饥饿。
// deleteObject 删除对象，并使用 UID（以及可选的 resourceVersion）作为前置条件，
// 防止在 GC 读取对象与发出删除之间对象被替换时误删同名的新对象。
// 若因 resourceVersion 冲突失败且所有者引用未变化，则去掉该前置条件重试，
// 避免与所有者引用无关的频繁更新使 GC 一直饥饿。
func (gc *GarbageCollector) deleteObject(item objectReference, resourceVersion string, ownersAtResourceVersion []metav1.OwnerReference, policy *metav1.DeletionPropagation) error {
	resource, namespaced, err := gc.apiResource(item.APIVersion, item.Kind)
	if err != nil {
		return err
	}
	uid := item.UID
	preconditions := metav1.Preconditions{UID: &uid}
	if len(resourceVersion) > 0 {
		preconditions.ResourceVersion = &resourceVersion
	}
	deleteOptions := metav1.DeleteOptions{Preconditions: &preconditions, PropagationPolicy: policy}
	resourceClient := gc.metadataClient.Resource(resource).Namespace(resourceDefaultNamespace(namespaced, item.Namespace))
	err = resourceClient.Delete(context.TODO(), item.Name, deleteOptions)
	if errors.IsConflict(err) && len(resourceVersion) > 0 {
		// 检查冲突期间 ownerReferences 是否变化，决定是否可安全重试。
		liveObject, liveErr := resourceClient.Get(context.TODO(), item.Name, metav1.GetOptions{})
		if errors.IsNotFound(liveErr) {
			// 目标对象已消失，返回 NotFound 让调用方按统一的"对象不存在"路径处理。
			return liveErr
		}
		if liveErr == nil && liveObject.UID == item.UID && liveObject.ResourceVersion != resourceVersion && reflect.DeepEqual(liveObject.OwnerReferences, ownersAtResourceVersion) {
			// 对象发生了与 ownerReferences 无关的更新导致冲突；去掉 resourceVersion
			// 前置条件重试，避免高频无关更新让 GC 永远无法删除该对象。
			return gc.deleteObject(item, "", nil, policy)
		}
	}
	return err
}

// getObject 读取对象的元数据（含 UID、resourceVersion、ownerReferences、finalizers）。
// GC 依赖这些字段判定依赖关系与执行状态。
// getObject 读取对象的元数据（含 UID、resourceVersion、ownerReferences、finalizers）。
// GC 依赖这些字段判定依赖关系与执行状态。
func (gc *GarbageCollector) getObject(item objectReference) (*metav1.PartialObjectMetadata, error) {
	resource, namespaced, err := gc.apiResource(item.APIVersion, item.Kind)
	if err != nil {
		return nil, err
	}
	namespace := resourceDefaultNamespace(namespaced, item.Namespace)
	if namespaced && len(namespace) == 0 {
		// 类型是命名空间级的但没有命名空间坐标：只可能是集群级对象把它当作所有者，
		// 这本身就是非法引用，直接返回特定错误而不是发出一个必然失败的请求。
		return nil, namespacedOwnerOfClusterScopedObjectErr
	}
	return gc.metadataClient.Resource(resource).Namespace(namespace).Get(context.TODO(), item.Name, metav1.GetOptions{})
}

// patchObject 对指定对象打补丁；用于移除 finalizer 时把 GC 从对象的清理流程中摘除。
// patchObject 对指定对象打补丁；用于移除 finalizer 时把 GC 从对象的清理流程中摘除。
func (gc *GarbageCollector) patchObject(item objectReference, patch []byte, pt types.PatchType) (*metav1.PartialObjectMetadata, error) {
	resource, namespaced, err := gc.apiResource(item.APIVersion, item.Kind)
	if err != nil {
		return nil, err
	}
	return gc.metadataClient.Resource(resource).Namespace(resourceDefaultNamespace(namespaced, item.Namespace)).Patch(context.TODO(), item.Name, pt, patch, metav1.PatchOptions{})
}

// removeFinalizer 从所有者的 finalizer 列表中移除目标 finalizer。
// 使用 RetryOnConflict 重试：读取与打补丁之间可能有其他写入者修改对象，
// 补丁中带上读到的 resourceVersion 可在冲突时重新读取最新状态再试。
// 对象已不存在时无需清理，视为成功。
// removeFinalizer 从所有者的 finalizer 列表中移除目标 finalizer。
// 使用 RetryOnConflict 重试：读取与打补丁之间可能有其他写入者修改对象，
// 补丁中带上读到的 resourceVersion 可在冲突时重新读取最新状态再试。
// 对象已不存在时无需清理，视为成功。
func (gc *GarbageCollector) removeFinalizer(logger klog.Logger, owner *node, targetFinalizer string) error {
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ownerObject, err := gc.getObject(owner.identity)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot finalize owner %s, because cannot get it: %v. The garbage collector will retry later", owner.identity, err)
		}
		accessor, err := meta.Accessor(ownerObject)
		if err != nil {
			return fmt.Errorf("cannot access the owner object %v: %v. The garbage collector will retry later", ownerObject, err)
		}
		finalizers := accessor.GetFinalizers()
		var newFinalizers []string
		found := false
		for _, f := range finalizers {
			if f == targetFinalizer {
				found = true
				continue
			}
			newFinalizers = append(newFinalizers, f)
		}
		if !found {
			logger.V(5).Info("finalizer already removed from object", "finalizer", targetFinalizer, "object", owner.identity)
			return nil
		}

		// 构造移除目标 finalizer 的合并补丁。
		patch, err := json.Marshal(&objectForFinalizersPatch{
			ObjectMetaForFinalizersPatch: ObjectMetaForFinalizersPatch{
				ResourceVersion: accessor.GetResourceVersion(),
				Finalizers:      newFinalizers,
			},
		})
		if err != nil {
			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", owner.identity, err)
		}
		_, err = gc.patchObject(owner.identity, patch, types.MergePatchType)
		return err
	})
	// 重试耗尽后返回可操作的错误，说明 GC 会在后续循环中再次尝试。
	// 重试耗尽后返回可操作的错误，说明 GC 会在后续循环中再次尝试。
	if errors.IsConflict(err) {
		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for owner %v", retry.DefaultBackoff.Steps, owner.identity)
	}
	return err
}
