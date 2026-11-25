/*
Copyright 2021 The Kubernetes Authors.

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
	"path"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	deploymentstorage "k8s.io/kubernetes/pkg/registry/apps/deployment/storage"
	replicasetstorage "k8s.io/kubernetes/pkg/registry/apps/replicaset/storage"
	statefulsetstorage "k8s.io/kubernetes/pkg/registry/apps/statefulset/storage"
	replicationcontrollerstorage "k8s.io/kubernetes/pkg/registry/core/replicationcontroller/storage"
)

type scaleTest struct {
	kind     string
	resource string
	path     string
	validObj string
}

func TestScaleAllResources(t *testing.T) {
	client, closeFn := setup(t)
	defer closeFn()

	tests := []scaleTest{
		{
			kind:     "Deployment",
			resource: "deployments",
			path:     "/apis/apps/v1",
			validObj: validAppsV1("Deployment"),
		},
		{
			kind:     "StatefulSet",
			resource: "statefulsets",
			path:     "/apis/apps/v1",
			validObj: validAppsV1("StatefulSet"),
		},
		{
			kind:     "ReplicaSet",
			resource: "replicasets",
			path:     "/apis/apps/v1",
			validObj: validAppsV1("ReplicaSet"),
		},
		{
			kind:     "ReplicationController",
			resource: "replicationcontrollers",
			path:     "/api/v1",
			validObj: validV1ReplicationController(),
		},
	}

	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			validObject := []byte(test.validObj)

			// 创建对象
			_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				Name("test").
				Param("fieldManager", "apply_test").
				Body(validObject).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Failed to create object using apply: %v", err)
			}
			obj := retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 1)
			assertReplicasOwnership(t, obj, "apply_test")

			// 调用 scale 子资源来更新 replicas
			_, err = client.CoreV1().RESTClient().
				Patch(types.MergePatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				Name("test").
				SubResource("scale").
				Param("fieldManager", "scale_test").
				Body([]byte(`{"spec":{"replicas": 5}}`)).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Failed to scale object: %v", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 5)
			assertReplicasOwnership(t, obj, "scale_test")

			// 重新 apply 原始对象，因为 replicas 已改变，应当因冲突而失败
			_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				Name("test").
				Param("fieldManager", "apply_test").
				Body(validObject).
				Do(context.TODO()).Get()
			if !apierrors.IsConflict(err) {
				t.Fatalf("Expected conflict when re-applying the original object, but got: %v", err)
			}

			// 强制重新 apply 这些变更应当成功
			_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				Name("test").
				Param("fieldManager", "apply_test").
				Param("force", "true").
				Body(validObject).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Error force-updating: %v", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 1)
			assertReplicasOwnership(t, obj, "apply_test")

			// 使用 replicas 数量不同的 scale 对象执行 "Apply"，应当产生冲突。
			_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				SubResource("scale").
				Name("test").
				Param("fieldManager", "apply_scale").
				Body([]byte(`{"kind":"Scale","apiVersion":"autoscaling/v1","metadata":{"name":"test","namespace":"default"},"spec":{"replicas":17}}`)).
				Do(context.TODO()).Get()
			if !apierrors.IsConflict(err) {
				t.Fatalf("Expected conflict error but got: %v", err)
			}
			if !strings.Contains(err.Error(), "apply_test") {
				t.Fatalf("Expected conflict with `apply_test` manager when but got: %v", err)
			}

			// 与之前相同但使用 force。此时只有新 manager 应拥有 .spec.replicas
			_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				SubResource("scale").
				Name("test").
				Param("fieldManager", "apply_scale").
				Param("force", "true").
				Body([]byte(`{"kind":"Scale","apiVersion":"autoscaling/v1","metadata":{"name":"test","namespace":"default"},"spec":{"replicas":17}}`)).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Error updating object by applying scale and forcing: %v ", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 17)
			assertReplicasOwnership(t, obj, "apply_scale")

			// 替换 scale 对象
			_, err = client.CoreV1().RESTClient().Put().
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				SubResource("scale").
				Name("test").
				Param("fieldManager", "replace_test").
				Body([]byte(`{"kind":"Scale","apiVersion":"autoscaling/v1","metadata":{"name":"test","namespace":"default"},"spec":{"replicas":7}}`)).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Error replacing object: %v", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 7)
			assertReplicasOwnership(t, obj, "replace_test")

			// apply 相同的 replicas 数量，此时两个 manager 都应拥有该字段
			_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				SubResource("scale").
				Name("test").
				Param("fieldManager", "co_owning_test").
				Body([]byte(`{"kind":"Scale","apiVersion":"autoscaling/v1","metadata":{"name":"test","namespace":"default"},"spec":{"replicas":7}}`)).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Error updating object: %v", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 7)
			assertReplicasOwnership(t, obj, "replace_test", "co_owning_test")

			// 再次 scale 后，该 manager 应成为 replicas 的唯一拥有者
			_, err = client.CoreV1().RESTClient().Patch(types.MergePatchType).
				AbsPath(test.path).
				Namespace("default").
				Resource(test.resource).
				SubResource("scale").
				Name("test").
				Param("fieldManager", "scale_test").
				Body([]byte(`{"spec":{"replicas": 5}}`)).
				Do(context.TODO()).Get()
			if err != nil {
				t.Fatalf("Error scaling object: %v", err)
			}
			obj = retrieveObject(t, client, test.path, test.resource)
			assertReplicasValue(t, obj, 5)
			assertReplicasOwnership(t, obj, "scale_test")
		})
	}
}

func TestScaleUpdateOnlyStatus(t *testing.T) {
	client, closeFn := setup(t)
	defer closeFn()

	resource := "deployments"
	path := "/apis/apps/v1"
	validObject := []byte(validAppsV1("Deployment"))

	// 创建对象
	_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
		AbsPath(path).
		Namespace("default").
		Resource(resource).
		Name("test").
		Param("fieldManager", "apply_test").
		Body(validObject).
		Do(context.TODO()).Get()
	if err != nil {
		t.Fatalf("Failed to create object using apply: %v", err)
	}
	obj := retrieveObject(t, client, path, resource)
	assertReplicasValue(t, obj, 1)
	assertReplicasOwnership(t, obj, "apply_test")

	// 调用 scale 子资源来更新 replicas
	_, err = client.CoreV1().RESTClient().
		Patch(types.MergePatchType).
		AbsPath(path).
		Namespace("default").
		Resource(resource).
		Name("test").
		SubResource("scale").
		Param("fieldManager", "scale_test").
		Body([]byte(`{"status":{"replicas": 42}}`)).
		Do(context.TODO()).Get()
	if err != nil {
		t.Fatalf("Failed to scale object: %v", err)
	}
	obj = retrieveObject(t, client, path, resource)
	assertReplicasValue(t, obj, 1)
	assertReplicasOwnership(t, obj, "apply_test")
}

func TestAllKnownVersionsAreInMappings(t *testing.T) {
	cases := []struct {
		groupKind schema.GroupKind
		mappings  managedfields.ResourcePathMappings
	}{
		{
			groupKind: schema.GroupKind{Group: "apps", Kind: "ReplicaSet"},
			mappings:  replicasetstorage.ReplicasPathMappings(),
		},
		{
			groupKind: schema.GroupKind{Group: "apps", Kind: "StatefulSet"},
			mappings:  statefulsetstorage.ReplicasPathMappings(),
		},
		{
			groupKind: schema.GroupKind{Group: "apps", Kind: "Deployment"},
			mappings:  deploymentstorage.ReplicasPathMappings(),
		},
		{
			groupKind: schema.GroupKind{Group: "", Kind: "ReplicationController"},
			mappings:  replicationcontrollerstorage.ReplicasPathMappings(),
		},
	}
	for _, c := range cases {
		knownVersions := scheme.Scheme.VersionsForGroupKind(c.groupKind)
		for _, version := range knownVersions {
			if _, ok := c.mappings[version.String()]; !ok {
				t.Errorf("missing version %v for %v mappings", version, c.groupKind)
			}
		}

		if len(knownVersions) != len(c.mappings) {
			t.Errorf("%v mappings has extra items: %v vs %v", c.groupKind, c.mappings, knownVersions)
		}
	}
}

func validAppsV1(kind string) string {
	return fmt.Sprintf(`{
	    "apiVersion": "apps/v1",
	    "kind": "%s",
	    "metadata": {
	      "name": "test"
	    },
	    "spec": {
	      "replicas": 1,
	      "selector": {
	        "matchLabels": {
	           "app": "nginx"
	        }
	      },
	      "template": {
	        "metadata": {
	          "labels": {
	            "app": "nginx"
	          }
	        },
	        "spec": {
	          "containers": [{
	            "name":  "nginx",
	            "image": "nginx:latest"
	          }]
	        }
	      }
	    }
	  }`, kind)
}

func validV1ReplicationController() string {
	return `{
	    "apiVersion": "v1",
	    "kind": "ReplicationController",
	    "metadata": {
	      "name": "test"
	    },
	    "spec": {
	      "replicas": 1,
	      "selector": {
          "app": "nginx"
	      },
	      "template": {
	        "metadata": {
	          "labels": {
	            "app": "nginx"
	          }
	        },
	        "spec": {
	          "containers": [{
	            "name":  "nginx",
	            "image": "nginx:latest"
	          }]
	        }
	      }
	    }
	  }`
}

func retrieveObject(t *testing.T, client clientset.Interface, prefix, resource string) *unstructured.Unstructured {
	t.Helper()

	urlPath := path.Join(prefix, "namespaces", "default", resource, "test")
	bytes, err := client.CoreV1().RESTClient().Get().AbsPath(urlPath).DoRaw(context.TODO())
	if err != nil {
		t.Fatalf("Failed to retrieve object: %v", err)
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(bytes, obj); err != nil {
		t.Fatalf("Error unmarshalling the retrieved object: %v", err)
	}
	return obj
}

func assertReplicasValue(t *testing.T, obj *unstructured.Unstructured, value int) {
	actualValue, found, err := unstructured.NestedInt64(obj.Object, "spec", "replicas")

	if err != nil {
		t.Fatalf("Error when retrieving replicas field: %v", err)
	}
	if !found {
		t.Fatalf("Replicas field not found")
	}

	if int(actualValue) != value {
		t.Fatalf("Expected replicas field value to be %d but got %d", value, actualValue)
	}
}

func assertReplicasOwnership(t *testing.T, obj *unstructured.Unstructured, fieldManagers ...string) {
	t.Helper()

	accessor, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("Failed to get meta accessor for object: %v", err)
	}

	seen := make(map[string]bool)
	for _, m := range fieldManagers {
		seen[m] = false
	}

	for _, managedField := range accessor.GetManagedFields() {
		var entryJSON map[string]interface{}
		if err := json.Unmarshal(managedField.FieldsV1.GetRawBytes(), &entryJSON); err != nil {
			t.Fatalf("failed to read into json")
		}

		spec, ok := entryJSON["f:spec"].(map[string]interface{})
		if !ok {
			// 继续处理下一个 managedField，因为该字段不包含 spec 条目
			continue
		}

		if _, ok := spec["f:replicas"]; !ok {
			// 继续处理下一个 managedField，因为该字段不包含 spec.replicas 条目
			continue
		}

		// 检查该 manager 是否属于我们预期的 manager 之一
		if _, ok := seen[managedField.Manager]; !ok {
			t.Fatalf("Unexpected field manager, found %q, expected to be in: %v", managedField.Manager, seen)
		}

		seen[managedField.Manager] = true
	}

	var missingManagers []string
	for manager, managerSeen := range seen {
		if !managerSeen {
			missingManagers = append(missingManagers, manager)
		}
	}
	if len(missingManagers) > 0 {
		t.Fatalf("replicas fields should be owned by %v", missingManagers)
	}
}
