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

package internal_test

import (
	"encoding/json"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	internaltesting "k8s.io/apimachinery/pkg/util/managedfields/internal/testing"
	"sigs.k8s.io/yaml"
)

// TestNoUpdateBeforeFirstApply 验证 SkipNonAppliedManager 下的首次 apply：
// 不依赖此前的 update 记录，直接产生一条 apply 的 managedFields 条目。
func TestNoUpdateBeforeFirstApply(t *testing.T) {
	// 用 SkipNonAppliedManager 包装底层 Manager，模拟 “首次 apply 前无字段记录” 的场景。
	f := internaltesting.NewTestFieldManagerImpl(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "Pod"), "", func(m internal.Manager) internal.Manager {
		return internal.NewSkipNonAppliedManager(m, &internaltesting.FakeObjectCreater{})
	})

	appliedObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal([]byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
		"metadata": {
			"name": "pod",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"containers": [{
				"name":  "nginx",
				"image": "nginx:latest"
			}]
        }
	}`), &appliedObj.Object); err != nil {
		t.Fatalf("error decoding YAML: %v", err)
	}

	if err := f.Apply(appliedObj, "fieldmanager_test_apply", false); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}

	if e, a := 1, len(f.ManagedFields()); e != a {
		t.Fatalf("exected %v entries in managedFields, but got %v: %#v", e, a, f.ManagedFields())
	}

	if e, a := "fieldmanager_test_apply", f.ManagedFields()[0].Manager; e != a {
		t.Fatalf("exected manager name to be %v, but got %v: %#v", e, a, f.ManagedFields())
	}
}

// TestUpdateBeforeFirstApply 验证首次 apply 之前先有 update 时的冲突语义：
// 首次 apply 不强制会与 "before-first-apply" 记录冲突，force 后两个 manager 并存。
func TestUpdateBeforeFirstApply(t *testing.T) {
	f := internaltesting.NewTestFieldManagerImpl(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "Pod"), "", func(m internal.Manager) internal.Manager {
		return internal.NewSkipNonAppliedManager(m, &internaltesting.FakeObjectCreater{})
	})

	updatedObj := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(`{"kind": "Pod", "apiVersion": "v1", "metadata": {"labels": {"app": "my-nginx"}}}`), updatedObj); err != nil {
		t.Fatalf("Failed to unmarshal object: %v", err)
	}

	if err := f.Update(updatedObj, "fieldmanager_test_update"); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}

	// update 不应单独产生 managedFields 条目。
	if m := f.ManagedFields(); len(m) != 0 {
		t.Fatalf("managedFields were tracked on update only: %v", m)
	}

	appliedObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal([]byte(`{
		"apiVersion": "v1",
		"kind": "Pod",
		"metadata": {
			"name": "pod",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"containers": [{
				"name":  "nginx",
				"image": "nginx:latest"
			}]
        }
	}`), &appliedObj.Object); err != nil {
		t.Fatalf("error decoding YAML: %v", err)
	}

	// 非强制 apply 应因 labels.app 与 before-first-apply 记录冲突而失败。
	err := f.Apply(appliedObj, "fieldmanager_test_apply", false)
	apiStatus, _ := err.(apierrors.APIStatus)
	if err == nil || !apierrors.IsConflict(err) || len(apiStatus.Status().Details.Causes) != 1 {
		t.Fatalf("Expecting to get one conflict but got %v", err)
	}

	if e, a := ".metadata.labels.app", apiStatus.Status().Details.Causes[0].Field; e != a {
		t.Fatalf("Expecting to conflict on field %q but conflicted on field %q: %v", e, a, err)
	}

	if e, a := "before-first-apply", apiStatus.Status().Details.Causes[0].Message; !strings.Contains(a, e) {
		t.Fatalf("Expecting conflict message to contain %q but got %q: %v", e, a, err)
	}

	// 强制 apply 后应成功，并保留两个 manager。
	if err := f.Apply(appliedObj, "fieldmanager_test_apply", true); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}

	if e, a := 2, len(f.ManagedFields()); e != a {
		t.Fatalf("exected %v entries in managedFields, but got %v: %#v", e, a, f.ManagedFields())
	}

	if e, a := "fieldmanager_test_apply", f.ManagedFields()[0].Manager; e != a {
		t.Fatalf("exected first manager name to be %v, but got %v: %#v", e, a, f.ManagedFields())
	}

	if e, a := "before-first-apply", f.ManagedFields()[1].Manager; e != a {
		t.Fatalf("exected second manager name to be %v, but got %v: %#v", e, a, f.ManagedFields())
	}
}
