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

package internal_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/managedfields/managedfieldstest"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	"sigs.k8s.io/yaml"
)

// TestManagedFieldsUpdateDoesModifyTime 验证 update 实际改变字段时，对应 manager 的
// ManagedFields 时间戳会被刷新——这是“该 manager 最近一次改动”的可见依据。
func TestManagedFieldsUpdateDoesModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = updateObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()

	time.Sleep(time.Second)

	err = updateObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()

	if previousManagedFields[0].Time.Equal(newManagedFields[0].Time) {
		t.Errorf("ManagedFields time has not been updated:\n%v", newManagedFields)
	}
}

// TestManagedFieldsApplyDoesModifyTime 与 update 用例对称：apply 实际改变字段时，
// 时间戳同样应刷新。
func TestManagedFieldsApplyDoesModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = applyObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()

	time.Sleep(time.Second)

	err = applyObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()

	if previousManagedFields[0].Time.Equal(newManagedFields[0].Time) {
		t.Errorf("ManagedFields time has not been updated:\n%v", newManagedFields)
	}
}

// TestManagedFieldsUpdateWithoutChangesDoesNotModifyTime 验证 update 内容未变时
// 时间戳保持不变：避免无意义的写入造成 managedFields 抖动与额外的 etcd 写。
func TestManagedFieldsUpdateWithoutChangesDoesNotModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = updateObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()

	time.Sleep(time.Second)

	err = updateObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()

	if !previousManagedFields[0].Time.Equal(newManagedFields[0].Time) {
		t.Errorf("ManagedFields time has changed:\nBefore:\n%v\nAfter:\n%v", previousManagedFields, newManagedFields)
	}
}

// TestManagedFieldsApplyWithoutChangesDoesNotModifyTime 与 update 用例对称：
// apply 内容未变时时间戳也不变。
func TestManagedFieldsApplyWithoutChangesDoesNotModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = applyObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()

	time.Sleep(time.Second)

	err = applyObject(f, "fieldmanager_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()

	if !previousManagedFields[0].Time.Equal(newManagedFields[0].Time) {
		t.Errorf("ManagedFields time has changed:\nBefore:\n%v\nAfter:\n%v", previousManagedFields, newManagedFields)
	}
}

// TestNonManagedFieldsUpdateDoesNotModifyTime 验证一个 manager update 时，不会影响
// 其它 manager（此处为 fieldmanager_b_test）的 ManagedFields 条目。
func TestNonManagedFieldsUpdateDoesNotModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = updateObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	err = updateObject(f, "fieldmanager_b_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_b": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()
	previousEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range previousManagedFields {
		previousEntries[entry.Manager] = entry
	}

	time.Sleep(time.Second)

	err = updateObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "value",
			"key_b": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()
	newEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range newManagedFields {
		newEntries[entry.Manager] = entry
	}

	if _, ok := newEntries["fieldmanager_b_test"]; ok {
		t.Errorf("FieldManager B ManagedFields has changed:\n%v", newEntries["fieldmanager_b_test"])
	}
}

// TestNonManagedFieldsApplyDoesNotModifyTime 与 update 用例对称：一个 manager apply 时，
// 其它 manager 的时间戳应保持不变。
func TestNonManagedFieldsApplyDoesNotModifyTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = applyObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	err = applyObject(f, "fieldmanager_b_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_b": "value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()
	previousEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range previousManagedFields {
		previousEntries[entry.Manager] = entry
	}

	time.Sleep(time.Second)

	err = applyObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()
	newEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range newManagedFields {
		newEntries[entry.Manager] = entry
	}

	if !previousEntries["fieldmanager_b_test"].Time.Equal(newEntries["fieldmanager_b_test"].Time) {
		t.Errorf("FieldManager B ManagedFields time changed:\nBefore:\n%v\nAfter:\n%v",
			previousEntries["fieldmanager_b_test"], newEntries["fieldmanager_b_test"])
	}
}

// TestTakingOverManagedFieldsDuringUpdateDoesNotModifyPreviousManagerTime 验证 update
// 接管他人字段时，只更新接管者的条目，不刷新被接管者原有条目的时间戳。
func TestTakingOverManagedFieldsDuringUpdateDoesNotModifyPreviousManagerTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = updateObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "value",
			"key_b": value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()
	previousEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range previousManagedFields {
		previousEntries[entry.Manager] = entry
	}

	time.Sleep(time.Second)

	err = updateObject(f, "fieldmanager_b_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_b": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()
	newEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range newManagedFields {
		newEntries[entry.Manager] = entry
	}

	if !previousEntries["fieldmanager_a_test"].Time.Equal(newEntries["fieldmanager_a_test"].Time) {
		t.Errorf("FieldManager A ManagedFields time has been updated:\nBefore:\n%v\nAfter:\n%v",
			previousEntries["fieldmanager_a_test"], newEntries["fieldmanager_a_test"])
	}
}

// TestTakingOverManagedFieldsDuringApplyDoesNotModifyPreviousManagerTime 与 update 用例
// 对称：apply 接管字段时也不刷新被接管者的时间戳。
func TestTakingOverManagedFieldsDuringApplyDoesNotModifyPreviousManagerTime(t *testing.T) {
	var err error
	f := managedfieldstest.NewTestFieldManager(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "ConfigMap"))

	err = applyObject(f, "fieldmanager_a_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_a": "value",
			"key_b": value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	previousManagedFields := f.ManagedFields()
	previousEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range previousManagedFields {
		previousEntries[entry.Manager] = entry
	}

	time.Sleep(time.Second)

	err = applyObject(f, "fieldmanager_b_test", []byte(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "configmap"
		},
		"data": {
			"key_b": "new-value"
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	newManagedFields := f.ManagedFields()
	newEntries := map[string]v1.ManagedFieldsEntry{}
	for _, entry := range newManagedFields {
		newEntries[entry.Manager] = entry
	}

	if !previousEntries["fieldmanager_a_test"].Time.Equal(newEntries["fieldmanager_a_test"].Time) {
		t.Errorf("FieldManager A ManagedFields time has been updated:\nBefore:\n%v\nAfter:\n%v",
			previousEntries["fieldmanager_a_test"], newEntries["fieldmanager_a_test"])
	}
}

// NoopManager 是一个什么都不做的下层 manager，用于隔离 ManagedFieldsUpdater 自身的行为。
type NoopManager struct{}

// Apply 返回 nil 对象，表示下层未做任何改动。
func (NoopManager) Apply(liveObj, appliedObj runtime.Object, managed internal.Managed, fieldManager string, force bool) (runtime.Object, internal.Managed, error) {
	return nil, managed, nil
}

// Update 返回 nil 对象与 nil managed，表示下层未做任何改动。
func (NoopManager) Update(liveObj, newObj runtime.Object, managed internal.Managed, manager string) (runtime.Object, internal.Managed, error) {
	return nil, nil, nil
}

func updateObject(f managedfieldstest.TestFieldManager, fieldManagerName string, object []byte) error {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal(object, &obj.Object); err != nil {
		return fmt.Errorf("error decoding YAML: %v", err)
	}
	if err := f.Update(obj, fieldManagerName); err != nil {
		return fmt.Errorf("failed to update object: %v", err)
	}
	return nil
}

// applyObject 把 YAML/JSON 文本解码为 unstructured 并执行 Apply（force=true）。
func applyObject(f managedfieldstest.TestFieldManager, fieldManagerName string, object []byte) error {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal(object, &obj.Object); err != nil {
		return fmt.Errorf("error decoding YAML: %v", err)
	}
	if err := f.Apply(obj, fieldManagerName, true); err != nil {
		return fmt.Errorf("failed to apply object: %v", err)
	}
	return nil
}

// TestNilNewObjectReplacedWithDeepCopyExcludingManagedFields 确保当 ManagedFieldsUpdater
// 从下层 manager 链拿到 nil 值（表示操作是 no-op）时，它自己会返回输入 live 对象的副本，
// 并去掉该副本中的 managedFields。
func TestNilNewObjectReplacedWithDeepCopyExcludingManagedFields(t *testing.T) {
	// 初始化一个带 managedFields 的“live object”
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal([]byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
		"metadata": {
			"name": "pod",
			"labels": {"app": "nginx"},
			"managedFields": [
				{
					"apiVersion": "v1",
					"fieldsType": "FieldsV1",
					"fieldsV1": {
						"f:metadata": {
							"f:labels": {
								"f:app": {}
							}
						}
					},
					"manager": "fieldmanager_test",
					"operation": "Apply",
					"time": "2021-11-11T18:41:17Z"
				}
			]
		}
	}`), &obj.Object); err != nil {
		t.Fatalf("error decoding YAML: %v", err)
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("couldn't get accessor: %v", err)
	}

	// 从 live object 中解码 managedFields，因为它不允许出现在 patch 中。
	managed, err := internal.DecodeManagedFields(accessor.GetManagedFields())
	if err != nil {
		t.Fatalf("failed to decode managed fields: %v", err)
	}

	updater := internal.NewManagedFieldsUpdater(NoopManager{})

	newObject, _, err := updater.Apply(obj, obj.DeepCopyObject(), managed, "some_manager", false)
	if err != nil {
		t.Fatalf("failed to apply configuration %v", err)
	}

	if newObject == obj {
		t.Fatalf("returned newObject must not be the same instance as the passed in liveObj")
	}

	// 去掉 live 对象的 managedFields，检查它与返回的 newObject 深度相等。
	liveWithoutManaged := obj.DeepCopyObject()
	internal.RemoveObjectManagedFields(liveWithoutManaged)

	if !reflect.DeepEqual(liveWithoutManaged, newObject) {
		t.Fatalf("returned newObject must be deeply equal to the input live object, without managed fields")
	}
}
