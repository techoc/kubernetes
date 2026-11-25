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
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestFieldsWipingConsistency(t *testing.T) {
	// 每个 allowlist 记录的是未实现预期字段擦除行为的既有 API。
	// 请勿向这些列表添加新条目。

	// 通过主端点创建时不会擦除 status 的 API。
	createDoesNotWipeStatus := sets.New(
		// Node 允许在创建时设置所有字段，包括 status。
		"nodes.v1",
	)

	// 通过 /status 端点更新时不会擦除 metadata 的 API。
	// 所有新 API 都应使用 ResetObjectMetaForStatus。
	statusDoesNotWipeMetadata := sets.New(
		// https://github.com/kubernetes/kubernetes/issues/137681
		"customresourcedefinitions.v1.apiextensions.k8s.io",

		// 未使用 ResetObjectMetaForStatus 的 API：
		"certificatesigningrequests.v1.certificates.k8s.io",
		"cronjobs.v1.batch",
		"daemonsets.v1.apps",
		"horizontalpodautoscalers.v1.autoscaling",
		"horizontalpodautoscalers.v2.autoscaling",
		"ingresses.v1.networking.k8s.io",
		"jobs.v1.batch",
		"namespaces.v1",
		"nodes.v1",
		"persistentvolumeclaims.v1",
		"persistentvolumes.v1",
		"poddisruptionbudgets.v1.policy",
		"pods.v1",
		"replicasets.v1.apps",
		"replicationcontrollers.v1",
		"resourcequotas.v1",
		"services.v1",
		"statefulsets.v1.apps",
	)

	TestAllDefinitions(t, "reset-fields-test", func(t *testing.T, api Definition) {
		if !api.HasStatus() {
			t.Skip()
		}
		if !api.HasVerb("patch") || !api.HasVerb("get") || !api.HasVerb("update") {
			t.Skip("Resource does not support patch, get, and update")
		}

		status := api.StorageData.GetStatusStub()
		obj := TestObj(t, api.StorageData.Stub, status, api.Mapping.GroupVersionKind)
		name := obj.GetName()

		rsc := api.ResourceClient()

		// 创建资源并检查创建时 status 是否被擦除。
		created, err := rsc.Apply(context.TODO(), name, obj, metav1.ApplyOptions{FieldManager: "spec-manager"})
		if err != nil {
			t.Fatalf("Failed to create via SSA: %v", err)
		}
		createWipesStatus := !checkPatch(t, status, "status", created.Object)

		// 从创建出的对象的 managedFields 推断主端点的 GetResetFields 行为，
		// 要在下面的 status apply（Force: true）抢占 status 字段所有权
		// 并破坏证据之前完成。
		ssaMainResetsStatus := true
		for _, mf := range created.GetManagedFields() {
			if mf.Manager == "spec-manager" && mf.Subresource == "" {
				ssaMainResetsStatus = !managedFieldsOwnTopLevelField(t, mf.FieldsV1, "status")
			}
		}

		// 使用修改后的 spec、status 和 metadata 字段 apply 到 /status 端点。
		statusObj := TestObj(t, api.StorageData.Stub, status, api.Mapping.GroupVersionKind)
		if api.StorageData.MutatedStub != "" {
			if err := json.Unmarshal([]byte(api.StorageData.MutatedStub), &statusObj.Object); err != nil {
				t.Fatal(err)
			}
		}
		statusObj.SetName(name)
		statusLabels := statusObj.GetLabels()
		if statusLabels == nil {
			statusLabels = map[string]string{}
		}
		statusLabels["test-status-ssa"] = "true"
		statusObj.SetLabels(statusLabels)
		_, err = rsc.ApplyStatus(context.TODO(), name, statusObj, metav1.ApplyOptions{FieldManager: "status-manager", Force: true})
		if err != nil {
			t.Fatalf("Failed to apply status via SSA: %v", err)
		}

		// 写入后读取，以观察字段擦除行为和 managedField 状态。
		baseline, err := rsc.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Failed to get baseline: %v", err)
		}
		baselineStatus := baseline.Object["status"]
		baselineSpec := baseline.Object["spec"]

		// 从 managedFields 推断 GetResetFields 行为。
		ssaStatusResetsSpec := true
		ssaStatusResetsMetadata := true
		for _, mf := range baseline.GetManagedFields() {
			if mf.Manager == "status-manager" && mf.Subresource == "status" {
				ssaStatusResetsSpec = !managedFieldsOwnTopLevelField(t, mf.FieldsV1, "spec")
				ssaStatusResetsMetadata = !managedFieldsOwnLabel(t, mf.FieldsV1, "test-status-ssa")
			}
		}

		// 检查 / PrepareForUpdate 的 status 擦除
		var mainWipesStatus bool
		if baselineStatus != nil {
			differentStatus := api.StorageData.GetMutatedStatusStub()
			result, err := rsc.Patch(context.TODO(), name, types.MergePatchType, []byte(differentStatus), metav1.PatchOptions{})
			if err != nil {
				t.Fatalf("Failed to patch main endpoint with different status: %v", err)
			}
			mainWipesStatus = !checkPatch(t, differentStatus, "status", result.Object)
		} else {
			mainWipesStatus = true
		}

		// 检查 /status PrepareForUpdate 的 spec 擦除
		var statusWipesSpec bool
		differentSpec := api.StorageData.MutatedStub
		if baselineSpec != nil && differentSpec != "" {
			result, err := rsc.Patch(context.TODO(), name, types.MergePatchType, []byte(differentSpec), metav1.PatchOptions{}, "status")
			if err != nil {
				if !errors.IsInvalid(err) {
					t.Fatalf("Unexpected error from Patch to status endpoint with different spec: %v", err)
				}
				statusWipesSpec = true
			} else {
				statusWipesSpec = !checkPatch(t, differentSpec, "spec", result.Object)
			}
		} else {
			statusWipesSpec = true
		}

		// 检查 /status PrepareForUpdate 的 metadata 擦除
		var statusWipesMetadata bool
		labelPatch := []byte(`{"metadata": {"labels": {"test-wipe-label": "test-value"}}}`)
		result, err := rsc.Patch(context.TODO(), name, types.MergePatchType, labelPatch, metav1.PatchOptions{}, "status")
		if err != nil {
			t.Fatalf("Failed to patch status endpoint with different metadata labels: %v", err)
		}
		statusWipesMetadata = !checkPatch(t, string(labelPatch), "metadata", result.Object)

		if ssaMainResetsStatus != mainWipesStatus {
			t.Errorf("Main endpoint: SSA ResetStatus (%v) and PrepareForUpdate wipeStatus (%v) behaviors do not match.", ssaMainResetsStatus, mainWipesStatus)
		}
		if ssaStatusResetsSpec != statusWipesSpec {
			t.Errorf("Status endpoint: SSA ResetSpec (%v) and PrepareForUpdate wipeSpec (%v) behaviors do not match.", ssaStatusResetsSpec, statusWipesSpec)
		}
		if ssaStatusResetsMetadata != statusWipesMetadata {
			t.Errorf("Status endpoint: SSA ResetMetadata (%v) and PrepareForUpdate wipeMetadata (%v) behaviors do not match.", ssaStatusResetsMetadata, statusWipesMetadata)
		}

		// 强制校验字段擦除行为，并通过 allowlist 豁免既有例外。
		assertWiped(t, api.Mapping.Resource, "create must wipe status", createWipesStatus, createDoesNotWipeStatus)
		assertWiped(t, api.Mapping.Resource, "update must wipe status", mainWipesStatus, sets.New[string]())      // 此项没有豁免，今后也不要添加。
		assertWiped(t, api.Mapping.Resource, "status update must wipe spec", statusWipesSpec, sets.New[string]()) // 此项没有豁免，今后也不要添加。
		assertWiped(t, api.Mapping.Resource, "status update must wipe metadata", statusWipesMetadata, statusDoesNotWipeMetadata)

		if err := rsc.Delete(context.TODO(), name, *metav1.NewDeleteOptions(0)); err != nil {
			t.Logf("Failed to delete %v: %v", name, err)
		}
	})
}

// checkPatch 检查 objData 中 fieldScope（例如 spec、status、metdata）下的字段值
// 是否与 applyManifest 中的值一致。
func checkPatch(t *testing.T, applyManifest string, fieldScope string, objData map[string]interface{}) bool {
	t.Helper()
	var applyObj map[string]interface{}
	if err := json.Unmarshal([]byte(applyManifest), &applyObj); err != nil {
		t.Fatalf("Failed to parse apply JSON: %v", err)
	}
	applyValue, ok := applyObj[fieldScope]
	if !ok {
		return false
	}
	objValue, ok := objData[fieldScope]
	if !ok {
		return false
	}
	return containsAll(applyValue, objValue)
}

// managedFieldsOwnTopLevelField 检查 FieldsV1 集合是否包含给定的顶层字段。
func managedFieldsOwnTopLevelField(t *testing.T, fieldsV1 *metav1.FieldsV1, field string) bool {
	t.Helper()
	if fieldsV1 == nil {
		return false
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(fieldsV1.GetRawBytes(), &fields); err != nil {
		t.Logf("Failed to unmarshal FieldsV1: %v", err)
		return false
	}
	_, ok := fields["f:"+field]
	return ok
}

// assertWiped 检查字段擦除行为是否成立，并通过 allowlist 豁免已知例外。
func assertWiped(t *testing.T, gvr schema.GroupVersionResource, msg string, wiped bool, allowed sets.Set[string]) {
	t.Helper()
	name := ResourceString(gvr)
	if matchesException(gvr, allowed) {
		if wiped {
			t.Errorf("%s: %s unexpectedly wiped. Remove it from the exception list.", name, msg)
		}
	} else {
		if !wiped {
			t.Errorf("%s: %s", name, msg)
		}
	}
}

// managedFieldsOwnLabel 检查 FieldsV1 集合是否包含某个 metadata label。
func managedFieldsOwnLabel(t *testing.T, fieldsV1 *metav1.FieldsV1, labelKey string) bool {
	t.Helper()
	if fieldsV1 == nil {
		return false
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(fieldsV1.GetRawBytes(), &fields); err != nil {
		t.Logf("Failed to unmarshal FieldsV1: %v", err)
		return false
	}
	metadata, ok := fields["f:metadata"].(map[string]interface{})
	if !ok {
		return false
	}
	labels, ok := metadata["f:labels"].(map[string]interface{})
	if !ok {
		return false
	}
	_, ok = labels["f:"+labelKey]
	return ok
}
