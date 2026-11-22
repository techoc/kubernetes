/*
Copyright 2020 The Kubernetes Authors.

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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// lastAppliedUpdater 包装另一个 Manager，在服务端 apply 后同步客户端 apply 注解。
//
// 目的：kubectl 的客户端 apply 依赖 kubectl.kubernetes.io/last-applied-configuration
// 计算三方合并；当同一个对象改用服务端 apply 后，该注解若不更新就会过期。
type lastAppliedUpdater struct {
	fieldManager Manager
}

var _ Manager = &lastAppliedUpdater{}

// NewLastAppliedUpdater sets the client-side apply annotation up to date with
// server-side apply managed fields
func NewLastAppliedUpdater(fieldManager Manager) Manager {
	return &lastAppliedUpdater{
		fieldManager: fieldManager,
	}
}

// Update implements Manager.
//
// Update 请求不涉及 last-applied 注解，直接委托给被包装的 Manager。
func (f *lastAppliedUpdater) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	return f.fieldManager.Update(liveObj, newObj, managed, manager)
}

// Apply implements Manager.
//
// server-side apply managed fields
//
// 先执行服务端 apply，成功后再按需刷新 last-applied 注解。
// 注解只在以下条件同时满足时更新：manager 是 "kubectl"，且对象上已存在该注解。
// 这样既能兼容从客户端 apply 升级的场景，又不会给纯服务端 apply 的对象凭空加注解。
func (f *lastAppliedUpdater) Apply(liveObj, newObj runtime.Object, managed Managed, manager string, force bool) (runtime.Object, Managed, error) {
	liveObj, managed, err := f.fieldManager.Apply(liveObj, newObj, managed, manager, force)
	if err != nil {
		return liveObj, managed, err
	}

	// Sync the client-side apply annotation only from kubectl server-side apply.
	// To opt-out of this behavior, users may specify a different field manager.
	//
	// If the client-side apply annotation doesn't exist,
	// then continue because we have no annotation to update
	if manager == "kubectl" && hasLastApplied(liveObj) {
		lastAppliedValue, err := buildLastApplied(newObj)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build last-applied annotation: %v", err)
		}
		err = SetLastApplied(liveObj, lastAppliedValue)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to set last-applied annotation: %v", err)
		}
	}
	return liveObj, managed, err
}

// hasLastApplied 判断对象是否带有非空的 last-applied 注解。
func hasLastApplied(obj runtime.Object) bool {
	var accessor, err = meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}
	var annotations = accessor.GetAnnotations()
	if annotations == nil {
		return false
	}
	lastApplied, ok := annotations[LastAppliedConfigAnnotation]
	return ok && len(lastApplied) > 0
}

// buildLastApplied 把对象序列化为 last-applied 注解的值。
func buildLastApplied(obj runtime.Object) (string, error) {
	// 深拷贝：下面会删除注解，不能影响调用方持有的对象。
	obj = obj.DeepCopyObject()

	var accessor, err = meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}

	// Remove the annotation from the object before encoding the object
	// 注解本身不能包含自己，否则会无限膨胀。
	var annotations = accessor.GetAnnotations()
	delete(annotations, LastAppliedConfigAnnotation)
	accessor.SetAnnotations(annotations)

	lastApplied, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return "", fmt.Errorf("couldn't encode object into last applied annotation: %v", err)
	}
	return string(lastApplied), nil
}
