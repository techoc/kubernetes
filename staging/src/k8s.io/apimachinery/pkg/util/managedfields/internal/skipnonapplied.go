/*
Copyright 2019 The Kubernetes Authors.

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
	"math/rand"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

// skipNonAppliedManager 是兼容性垫片：历史对象没有 managed fields，
// 直接接管会把这些字段全部判给第一个写请求者，造成冲突。
// 因此只在对象首次被 apply 之后才开始跟踪。
type skipNonAppliedManager struct {
	fieldManager           Manager
	objectCreater          runtime.ObjectCreater
	beforeApplyManagerName string
	probability            float32
}

var _ Manager = &skipNonAppliedManager{}

// NewSkipNonAppliedManager 创建包装的 FieldManager，仅在首次 apply 后开始跟踪管理器。
func NewSkipNonAppliedManager(fieldManager Manager, objectCreater runtime.ObjectCreater) Manager {
	return NewProbabilisticSkipNonAppliedManager(fieldManager, objectCreater, 0.0)
}

// NewProbabilisticSkipNonAppliedManager 创建包装的 FieldManager，在首次 apply 后开始跟踪管理器，
// 或以 p 的概率从创建时开始跟踪。
//
// 概率参数用于灰度：对新建对象以 p 的概率提前开始跟踪，便于逐步铺开而不必等下一次 apply。
func NewProbabilisticSkipNonAppliedManager(fieldManager Manager, objectCreater runtime.ObjectCreater, p float32) Manager {
	return &skipNonAppliedManager{
		fieldManager:           fieldManager,
		objectCreater:          objectCreater,
		beforeApplyManagerName: "before-first-apply",
		probability:            p,
	}
}

// Update 实现 Manager 接口。
//
// 三种情况：
//   - 已有 managed fields：正常委托；
//   - 无 managed fields 且是 create（UID 为空）：按概率决定是否跟踪；
//   - 无 managed fields 且是 update：确定跳过，因为无法追溯历史归属。
func (f *skipNonAppliedManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	accessor, err := meta.Accessor(liveObj)
	if err != nil {
		return newObj, managed, nil
	}

	//如果托管字段为空，需要判断是否跳过跟踪。
	if len(managed.Fields()) == 0 {
		//通过检查 lastObj 的 UID 是否为空判断操作是否为 create。
		//如果操作是 create，跟踪托管字段的概率为 f.probability；
		//如果操作是 update，已知托管字段为空，因此跳过跟踪。
		if len(accessor.GetUID()) == 0 {
			if f.probability <= rand.Float32() {
				return newObj, managed, nil
			}
		} else {
			return newObj, managed, nil
		}
	}
	return f.fieldManager.Update(liveObj, newObj, managed, manager)
}

// Apply 实现 Manager 接口。
//
// 首次 apply 时，先以 "before-first-apply" 的名义把现存字段登记给一个虚拟 manager，
// 再从空对象出发执行真正的 apply，从而只把本次 apply 显式设置的字段算给用户。
func (f *skipNonAppliedManager) Apply(liveObj, appliedObj runtime.Object, managed Managed, fieldManager string, force bool) (runtime.Object, Managed, error) {
	if len(managed.Fields()) == 0 {
		gvk := appliedObj.GetObjectKind().GroupVersionKind()
		emptyObj, err := f.objectCreater.New(gvk)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create empty object of type %v: %v", gvk, err)
		}
		// unstructured 对象的 GVK 需要在创建后显式设置，否则后续类型转换会失败。
		if unstructured, isUnstructured := emptyObj.(runtime.Unstructured); isUnstructured {
			unstructured.GetObjectKind().SetGroupVersionKind(gvk)
		}
		liveObj, managed, err = f.fieldManager.Update(emptyObj, liveObj, managed, f.beforeApplyManagerName)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create manager for existing fields: %v", err)
		}
	}
	return f.fieldManager.Apply(liveObj, appliedObj, managed, fieldManager, force)
}
