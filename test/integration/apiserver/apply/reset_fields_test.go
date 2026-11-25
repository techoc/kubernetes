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

package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/integration/apiserver/apidefinitions"
)

// noConflicts 是不会产生冲突的资源集合。
var noConflicts = map[string]struct{}{
	// CSR 的 spec 和 status 都会被清空，没有字段受管理，跳过。
	"certificatesigningrequests": {},
	// storageversions 的 spec 为空，不可能产生冲突。
	"storageversions": {},
	// servicecidrs 的 spec 不可变，不可能产生冲突。
	"servicecidrs": {},
	// namespaces 只有 spec.finalizers 字段，该字段同样被跳过，因此不会冲突。
	"namespaces": {},
}

// TestApplyResetFields 确保 fieldManager 不会持有被 storage 策略重置的字段。
// 构造 spec 和 status 各相差一个字段的 obj1 与 obj2，分别 apply 到 spec 和 status 端点；
// 无冲突说明 fieldmanager1 的 status 所有权和 fieldmanager2 的 spec 所有权已被清除。
// 随后再次将 obj2 apply 到 spec 端点，预期因冲突失败。
func TestApplyResetFields(t *testing.T) {
	apidefinitions.TestAllDefinitions(t, "reset-fields-test", func(t *testing.T, api apidefinitions.Definition) {
		if !api.HasStatus() {
			t.Skip()
		}
		if !api.HasVerb("patch") || !api.HasVerb("get") || !api.HasVerb("update") {
			t.Skip("Resource does not support patch, get, and update")
		}

		// 构造第一个对象。
		status := api.StorageData.GetStatusStub()
		obj1 := apidefinitions.TestObj(t, api.StorageData.Stub, status, api.Mapping.GroupVersionKind)
		name := obj1.GetName()

		rsc := api.ResourceClient()

		// 只 apply 第一个对象的 spec。
		_, err := rsc.Apply(context.TODO(), name, obj1, metav1.ApplyOptions{FieldManager: "fieldmanager1"})
		if err != nil {
			t.Fatalf("Failed to apply obj1: %v", err)
		}

		// 构造第二个对象。
		status2 := api.StorageData.GetMutatedStatusStub()
		differentSpec := api.StorageData.MutatedStub
		if differentSpec == "" {
			t.Skipf("No conflicting spec data defined for %v to test reset fields", api.Mapping.Resource)
		}

		obj2 := apidefinitions.TestObj(t, api.StorageData.Stub, status2, api.Mapping.GroupVersionKind)
		if err := json.Unmarshal([]byte(differentSpec), &obj2.Object); err != nil {
			t.Fatal(err)
		}
		obj2.SetName(name)

		if reflect.DeepEqual(obj1, obj2) {
			t.Fatalf("obj1 and obj2 should not be equal %v", obj2)
		}

		// 只 apply 第二个对象的 status。
		_, err = rsc.ApplyStatus(context.TODO(), name, obj2, metav1.ApplyOptions{FieldManager: "fieldmanager2"})
		if err != nil {
			t.Fatalf("Failed to apply obj2: %v", err)
		}

		if _, ok := noConflicts[api.Mapping.Resource.Resource]; !ok {
			// 再次 apply 到 spec 端点，验证所有权已被清除。
			_, err = rsc.Apply(context.TODO(), name, obj2, metav1.ApplyOptions{FieldManager: "fieldmanager2"})
			if err := expectConflict(nil, err, rsc, name); err != nil {
				t.Fatalf("Did not get expected conflict on spec apply: %v", err)
			}
		}

		if err := rsc.Delete(context.TODO(), name, *metav1.NewDeleteOptions(0)); err != nil {
			t.Fatalf("deleting final object failed: %v", err)
		}
	})
}

func expectConflict(objRet *unstructured.Unstructured, err error, rsc dynamic.ResourceInterface, name string) error {
	if err != nil && strings.Contains(err.Error(), "conflict") {
		return nil
	}
	which := "returned"
	// 出现意外情况时，不能假设 objRet==nil 与 err!=nil 严格对应，需另行获取对象。
	if objRet == nil {
		which = "subsequently fetched"
		var err2 error
		objRet, err2 = rsc.Get(context.TODO(), name, metav1.GetOptions{})
		if err2 != nil {
			return fmt.Errorf("instead got error %w, and failed to Get object: %v", err, err2)
		}
	}
	marshBytes, marshErr := json.Marshal(objRet)
	var gotten string
	if marshErr == nil {
		gotten = string(marshBytes)
	} else {
		gotten = fmt.Sprintf("<failed to json.Marshall(%#+v): %v>", objRet, marshErr)
	}
	return fmt.Errorf("instead got error %w; %s object is %s", err, which, gotten)
}
