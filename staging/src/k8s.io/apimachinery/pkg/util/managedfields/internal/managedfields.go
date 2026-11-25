/*
Copyright 2018 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// ManagedInterface 将 fieldpath.ManagedFields 与各操作的时间戳组合在一起。
type ManagedInterface interface {
	//Fields 获取 fieldpath.ManagedFields。
	Fields() fieldpath.ManagedFields

	//Times 获取各操作关联的时间戳。
	Times() map[string]*metav1.Time
}

type managedStruct struct {
	fields fieldpath.ManagedFields
	times  map[string]*metav1.Time
}

var _ ManagedInterface = &managedStruct{}

// Fields 实现 ManagedInterface 接口。
func (m *managedStruct) Fields() fieldpath.ManagedFields {
	return m.fields
}

// Times 实现 ManagedInterface 接口。
func (m *managedStruct) Times() map[string]*metav1.Time {
	return m.times
}

// NewEmptyManaged 创建空的 ManagedInterface。
func NewEmptyManaged() ManagedInterface {
	return NewManaged(fieldpath.ManagedFields{}, map[string]*metav1.Time{})
}

// NewManaged 根据 fieldpath.ManagedFields 和各操作的时间戳创建 ManagedInterface。
func NewManaged(f fieldpath.ManagedFields, t map[string]*metav1.Time) ManagedInterface {
	return &managedStruct{
		fields: f,
		times:  t,
	}
}

// RemoveObjectManagedFields 在合并前从对象中移除 ManagedFields，
// 避免它递归出现在结果的 ManagedFields 中。
// 如果对象无法提供元数据则 panic，因为这表示编程错误而非输入错误。
func RemoveObjectManagedFields(obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}
	accessor.SetManagedFields(nil)
}

// EncodeObjectManagedFields 转换 fieldpath.ManagedFields 并写入对象的 ManagedFields。
func EncodeObjectManagedFields(obj runtime.Object, managed ManagedInterface) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		panic(fmt.Sprintf("couldn't get accessor: %v", err))
	}

	encodedManagedFields, err := encodeManagedFields(managed)
	if err != nil {
		return fmt.Errorf("failed to convert back managed fields to API: %v", err)
	}
	accessor.SetManagedFields(encodedManagedFields)

	return nil
}

// DecodeManagedFields 将 ManagedFields 从线上格式（API 格式）转换为
// sigs.k8s.io/structured-merge-diff 使用的格式。
func DecodeManagedFields(encodedManagedFields []metav1.ManagedFieldsEntry) (ManagedInterface, error) {
	managed := managedStruct{}
	managed.fields = make(fieldpath.ManagedFields, len(encodedManagedFields))
	managed.times = make(map[string]*metav1.Time, len(encodedManagedFields))

	for i, encodedVersionedSet := range encodedManagedFields {
		//只有 Apply 和 Update 操作可以持久化。
		switch encodedVersionedSet.Operation {
		case metav1.ManagedFieldsOperationApply, metav1.ManagedFieldsOperationUpdate:
		default:
			return nil, fmt.Errorf("operation must be `Apply` or `Update`")
		}
		//每个条目都必须标明记录时使用的 API 版本。
		if len(encodedVersionedSet.APIVersion) < 1 {
			return nil, fmt.Errorf("apiVersion must not be empty")
		}
		//此实现只理解 FieldsV1 格式；空值表示条目格式错误，
		//其他格式均不受支持。
		switch encodedVersionedSet.FieldsType {
		case "FieldsV1":
			//有效情况。
		case "":
			return nil, fmt.Errorf("missing fieldsType in managed fields entry %d", i)
		default:
			return nil, fmt.Errorf("invalid fieldsType %q in managed fields entry %d", encodedVersionedSet.FieldsType, i)
		}
		manager, err := BuildManagerIdentifier(&encodedVersionedSet)
		if err != nil {
			return nil, fmt.Errorf("error decoding manager from %v: %v", encodedVersionedSet, err)
		}
		managed.fields[manager], err = decodeVersionedSet(&encodedVersionedSet)
		if err != nil {
			return nil, fmt.Errorf("error decoding versioned set from %v: %v", encodedVersionedSet, err)
		}
		managed.times[manager] = encodedVersionedSet.Time
	}
	return &managed, nil
}

// BuildManagerIdentifier 从 ManagedFieldsEntry 创建管理器标识符字符串。
func BuildManagerIdentifier(encodedManager *metav1.ManagedFieldsEntry) (manager string, err error) {
	encodedManagerCopy := *encodedManager

	//标识符在多次 apply 间必须稳定，因此排除易变的字段
	//（fields、type、time）。
	//管理器标识符绝不包含 fields 类型。
	encodedManagerCopy.FieldsType = ""

	//管理器标识符绝不包含字段内容。
	encodedManagerCopy.FieldsV1 = nil

	//管理器标识符绝不包含时间。
	encodedManagerCopy.Time = nil

	//对于 apply 操作，不在管理器标识符中包含 APIVersion，
	//使每次 apply 都得到相同的标识符。
	if encodedManager.Operation == metav1.ManagedFieldsOperationApply {
		encodedManagerCopy.APIVersion = ""
	}

	//使用剩余字段构建管理器标识符。
	b, err := json.Marshal(&encodedManagerCopy)
	if err != nil {
		return "", fmt.Errorf("error marshalling manager identifier: %v", err)
	}

	return string(b), nil
}

func decodeVersionedSet(encodedVersionedSet *metav1.ManagedFieldsEntry) (versionedSet fieldpath.VersionedSet, err error) {
	fields := EmptyFields
	if encodedVersionedSet.FieldsV1 != nil {
		fields = *encodedVersionedSet.FieldsV1
	}
	set, err := FieldsToSet(fields)
	if err != nil {
		return nil, fmt.Errorf("error decoding set: %v", err)
	}
	return fieldpath.NewVersionedSet(&set, fieldpath.APIVersion(encodedVersionedSet.APIVersion), encodedVersionedSet.Operation == metav1.ManagedFieldsOperationApply), nil
}

// encodeManagedFields 将 ManagedFields 从 sigs.k8s.io/structured-merge-diff 格式转换为
// 线上格式（API 格式）。
func encodeManagedFields(managed ManagedInterface) (encodedManagedFields []metav1.ManagedFieldsEntry, err error) {
	if len(managed.Fields()) == 0 {
		return nil, nil
	}
	encodedManagedFields = []metav1.ManagedFieldsEntry{}
	for manager := range managed.Fields() {
		versionedSet := managed.Fields()[manager]
		v, err := encodeManagerVersionedSet(manager, versionedSet)
		if err != nil {
			return nil, fmt.Errorf("error encoding versioned set for %v: %v", manager, err)
		}
		if t, ok := managed.Times()[manager]; ok {
			v.Time = t
		}
		encodedManagedFields = append(encodedManagedFields, *v)
	}
	return sortEncodedManagedFields(encodedManagedFields)
}

func sortEncodedManagedFields(encodedManagedFields []metav1.ManagedFieldsEntry) (sortedManagedFields []metav1.ManagedFieldsEntry, err error) {
	//确定性顺序使序列化对象保持稳定，
	//避免无意义更新和 goroutine 差异干扰。
	sort.Slice(encodedManagedFields, func(i, j int) bool {
		p, q := encodedManagedFields[i], encodedManagedFields[j]

		if p.Operation != q.Operation {
			return p.Operation < q.Operation
		}

		pSeconds, qSeconds := int64(0), int64(0)
		if p.Time != nil {
			pSeconds = p.Time.Unix()
		}
		if q.Time != nil {
			qSeconds = q.Time.Unix()
		}
		if pSeconds != qSeconds {
			return pSeconds < qSeconds
		}

		if p.Manager != q.Manager {
			return p.Manager < q.Manager
		}

		if p.APIVersion != q.APIVersion {
			return p.APIVersion < q.APIVersion
		}
		return p.Subresource < q.Subresource
	})

	return encodedManagedFields, nil
}

func encodeManagerVersionedSet(manager string, versionedSet fieldpath.VersionedSet) (encodedVersionedSet *metav1.ManagedFieldsEntry, err error) {
	encodedVersionedSet = &metav1.ManagedFieldsEntry{}

	//尽量从管理器标识符中提取字段。
	err = json.Unmarshal([]byte(manager), encodedVersionedSet)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling manager identifier %v: %v", manager, err)
	}

	//从 VersionedSet 获取 APIVersion、Operation 和 Fields。
	encodedVersionedSet.APIVersion = string(versionedSet.APIVersion())
	if versionedSet.Applied() {
		encodedVersionedSet.Operation = metav1.ManagedFieldsOperationApply
	}
	encodedVersionedSet.FieldsType = "FieldsV1"
	fields, err := SetToFields(*versionedSet.Set())
	if err != nil {
		return nil, fmt.Errorf("error encoding set: %v", err)
	}
	encodedVersionedSet.FieldsV1 = &fields

	return encodedVersionedSet, nil
}
