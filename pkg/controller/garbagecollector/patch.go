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
	"slices"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metaonly"
)

// getMetadata 优先从本地缓存读取对象元数据；
// 缓存不存在或不是最新版本时，回退到向 apiserver 发送 GET 请求。
func (gc *GarbageCollector) getMetadata(apiVersion, kind, namespace, name string) (metav1.Object, error) {
	apiResource, _, err := gc.apiResource(apiVersion, kind)
	if err != nil {
		return nil, err
	}
	// 读取 monitors 期间持读锁，避免 informer 重建 monitor 时并发读写 map。
	gc.dependencyGraphBuilder.monitorLock.RLock()
	defer gc.dependencyGraphBuilder.monitorLock.RUnlock()
	m, ok := gc.dependencyGraphBuilder.monitors[apiResource]
	if !ok || m == nil {
		// 该资源没有对应的本地缓存，直接向 API server 发 GET 请求。
		return gc.metadataClient.Resource(apiResource).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	}
	key := name
	// store 使用 namespace/name 作为命名空间对象的键。
	if len(namespace) != 0 {
		key = namespace + "/" + name
	}
	raw, exist, err := m.store.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exist {
		// 缓存中没有该对象，向 API server 发 GET 请求。
		return gc.metadataClient.Resource(apiResource).Namespace(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	}
	obj, ok := raw.(runtime.Object)
	if !ok {
		return nil, fmt.Errorf("expect a runtime.Object, got %v", raw)
	}
	return meta.Accessor(obj)
}

// objectForFinalizersPatch 把元数据包装成带 metadata 字段的补丁对象，
// 使 json.Marshal 的结果符合 Kubernetes PATCH 请求体格式。
type objectForFinalizersPatch struct {
	ObjectMetaForFinalizersPatch `json:"metadata"`
}

// ObjectMetaForFinalizersPatch 定义 finalizers 补丁操作所需的元数据字段。
type ObjectMetaForFinalizersPatch struct {
	ResourceVersion string   `json:"resourceVersion"`
	Finalizers      []string `json:"finalizers"`
}

// objectForPatch 把元数据包装成带 metadata 字段的补丁对象，
// 使 json.Marshal 的结果符合 Kubernetes PATCH 请求体格式。
type objectForPatch struct {
	ObjectMetaForPatch `json:"metadata"`
}

// ObjectMetaForPatch 定义 ownerReferences 补丁操作所需的元数据字段。
type ObjectMetaForPatch struct {
	ResourceVersion string                  `json:"resourceVersion"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
}

// jsonMergePatchFunc 定义用于构造修改 ownerReference 数组的 JSON merge patch 的函数类型。
type jsonMergePatchFunc func(*node) ([]byte, error)

// patch 先尝试 strategic merge patch；目标资源不支持时（415）回退到 JSON merge patch。
func (gc *GarbageCollector) patch(item *node, smp []byte, jmp jsonMergePatchFunc) (*metav1.PartialObjectMetadata, error) {
	// 优先使用 strategic merge patch，让 API server 按字段语义合并数组。
	smpResult, err := gc.patchObject(item.identity, smp, types.StrategicMergePatchType)
	if err == nil {
		return smpResult, nil
	}
	// 仅对 415 媒体类型不支持的错误降级，其他错误（如冲突）必须原样返回。
	if !errors.IsUnsupportedMediaType(err) {
		return nil, err
	}
	// 目标资源不支持 StrategicMergePatch，改用 JSON merge patch。
	patch, err := jmp(item)
	if err != nil {
		return nil, err
	}
	return gc.patchObject(item.identity, patch, types.MergePatchType)
}

// deleteOwnerRefJSONMergePatch 生成移除匹配 ownerUIDs 的 ownerReferences 的 JSON merge patch。
func (gc *GarbageCollector) deleteOwnerRefJSONMergePatch(item *node, ownerUIDs ...types.UID) ([]byte, error) {
	accessor, err := gc.getMetadata(item.identity.APIVersion, item.identity.Kind, item.identity.Namespace, item.identity.Name)
	if err != nil {
		return nil, err
	}
	expectedObjectMeta := ObjectMetaForPatch{}
	// 带上 resourceVersion 使补丁成为乐观并发更新，防止覆盖并发修改。
	expectedObjectMeta.ResourceVersion = accessor.GetResourceVersion()
	refs := accessor.GetOwnerReferences()
	// JSON merge patch 中数组是整体替换的，因此需显式列出所有要保留的引用。
	for _, ref := range refs {
		if !slices.Contains(ownerUIDs, ref.UID) {
			expectedObjectMeta.OwnerReferences = append(expectedObjectMeta.OwnerReferences, ref)
		}
	}
	return json.Marshal(objectForPatch{expectedObjectMeta})
}

// unblockOwnerReferencesStrategicMergePatch 生成把 node 所有 ownerReferences 的
// BlockOwnerDeletion 字段置为 false 的 strategic merge patch。
func (n *node) unblockOwnerReferencesStrategicMergePatch() ([]byte, error) {
	// strategic merge patch 只需列出要改动的引用，故用轻量对象承载差异字段。
	var dummy metaonly.MetadataOnlyObject
	var blockingRefs []metav1.OwnerReference
	falseVar := false
	for _, owner := range n.getOwners() {
		// 只收集真正阻塞删除的引用，避免生成无变化的补丁。
		if owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
			ref := owner
			ref.BlockOwnerDeletion = &falseVar
			blockingRefs = append(blockingRefs, ref)
		}
	}
	dummy.ObjectMeta.SetOwnerReferences(blockingRefs)
	dummy.ObjectMeta.UID = n.identity.UID
	return json.Marshal(dummy)
}

// unblockOwnerReferencesJSONMergePatch 生成把 node 所有 ownerReferences 的
// BlockOwnerDeletion 字段置为 false 的 JSON merge patch。
func (gc *GarbageCollector) unblockOwnerReferencesJSONMergePatch(n *node) ([]byte, error) {
	accessor, err := gc.getMetadata(n.identity.APIVersion, n.identity.Kind, n.identity.Namespace, n.identity.Name)
	if err != nil {
		return nil, err
	}
	expectedObjectMeta := ObjectMetaForPatch{}
	// 带上 resourceVersion 使补丁成为乐观并发更新，防止覆盖并发修改。
	expectedObjectMeta.ResourceVersion = accessor.GetResourceVersion()
	// JSON merge patch 中数组是整体替换的，必须提交全部 ownerReferences。
	var expectedOwners []metav1.OwnerReference
	falseVar := false
	for _, owner := range n.getOwners() {
		owner.BlockOwnerDeletion = &falseVar
		expectedOwners = append(expectedOwners, owner)
	}
	expectedObjectMeta.OwnerReferences = expectedOwners
	return json.Marshal(objectForPatch{expectedObjectMeta})
}
