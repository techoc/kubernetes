/*
Copyright The Kubernetes Authors.

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

package apidefinitions

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestGenerationManagement 测试资源更新时 metadata.generation 的管理方式。
//
// 该测试确保：
// 1. Generation 初始化为 1。
// 2. 每次 spec 更新时 Generation 单调递增。
// 3. status 更新时 Generation 不增加。
func TestGenerationManagement(t *testing.T) {

	// 请勿在此添加新条目。
	// 这里记录的是有 status 但不管理 generation 的资源。
	generationExempt := sets.New(
		"apiservices.v1.apiregistration.k8s.io",
		"certificatesigningrequests.v1.certificates.k8s.io",
		"namespaces.v1",
		"nodes.v1",
		"persistentvolumeclaims.v1",
		"persistentvolumes.v1",
		"resourceclaims.v1.resource.k8s.io",
		"resourceclaims.v1beta1.resource.k8s.io",
		"resourceclaims.v1beta2.resource.k8s.io",
		"resourcequotas.v1",
		"servicecidrs.v1.networking.k8s.io",
		"services.v1",
		"storageversions.v1alpha1.internal.apiserver.k8s.io",
		"volumeattachments.v1.storage.k8s.io",
	)

	TestAllDefinitions(t, "generation-namespace", func(t *testing.T, api Definition) {
		if !api.HasStatus() {
			t.Skip()
		}
		if !api.HasVerb("patch") || !api.HasVerb("get") || !api.HasVerb("update") {
			t.Skip("Resource does not support patch, get, and update")
		}

		differentSpec := api.StorageData.MutatedStub
		if differentSpec == "" {
			t.Skipf("No conflicting spec data defined for %v to test generation bump", api.Mapping.Resource)
		}

		status := api.StorageData.GetStatusStub()
		obj := TestObj(t, api.StorageData.Stub, status, api.Mapping.GroupVersionKind)
		name := obj.GetName()
		rsc := api.ResourceClient()

		_, err := rsc.Create(context.TODO(), obj, metav1.CreateOptions{FieldManager: "spec-manager"})
		if err != nil {
			t.Fatalf("Failed to create via SSA: %v", err)
		}

		baseline, err := rsc.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get baseline: %v", err)
		}

		// 验证 generation 初始化为 1。
		if matchesException(api.Mapping.Resource, generationExempt) {
			if baseline.GetGeneration() != 0 {
				t.Errorf("Expected generation exempt resource always have generation 0, but got %v", baseline.GetGeneration())
			}
		} else if baseline.GetGeneration() != int64(1) {
			t.Errorf("Expected generation to be %v on create, got %v", 1, baseline.GetGeneration())
		}

		// 验证更新 status 不会增加 generation。
		update := api.StorageData.GetMutatedStatusStub()
		statusObj := TestObj(t, api.StorageData.Stub, update, api.Mapping.GroupVersionKind)
		statusObj.SetName(name)

		_, err = rsc.ApplyStatus(context.TODO(), name, statusObj, metav1.ApplyOptions{FieldManager: "status-manager", Force: true})
		if err != nil {
			t.Fatalf("Failed to apply status via SSA: %v", err)
		}

		afterStatus, err := rsc.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get after status update: %v", err)
		}

		if afterStatus.GetGeneration() != baseline.GetGeneration() {
			t.Errorf("Expected generation to remain %v after status update, but got %v", baseline.GetGeneration(), afterStatus.GetGeneration())
		}

		// 验证更新 spec 会增加 generation。
		result, err := rsc.Patch(context.TODO(), name, types.MergePatchType, []byte(differentSpec), metav1.PatchOptions{})
		if err != nil {
			if !errors.IsInvalid(err) {
				t.Fatalf("Patch to main endpoint failed with unexpected error: %v", err)
			}
			return
		}
		if result.GetGeneration() <= afterStatus.GetGeneration() {
			if matchesException(api.Mapping.Resource, generationExempt) {
				if result.GetGeneration() != 0 {
					t.Errorf("Expected generation exempt resource always have generation 0, but got %v", result.GetGeneration())
				}
			} else {
				t.Errorf("Expected generation to monotonically increase after spec update (was %v, got %v)", afterStatus.GetGeneration(), result.GetGeneration())
			}
		}
	})
}
