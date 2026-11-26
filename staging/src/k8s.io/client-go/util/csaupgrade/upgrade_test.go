/*
Copyright 2022 The Kubernetes Authors.

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

package csaupgrade_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/util/csaupgrade"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// TestFindOwners 验证 FindFieldsOwners 能按 operation + 字段路径找出拥有者，
// 覆盖根字段、嵌套字段、多字段、部分匹配与 Apply/Update 两种 operation。
func TestFindOwners(t *testing.T) {
	testCases := []struct {
		Name              string
		ManagedFieldsYAML string
		Operation         metav1.ManagedFieldsOperationType
		Fields            *fieldpath.Set
		Expectation       []string
	}{
		{
			// 根字段路径的所有者。
			Name: "Basic",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-22T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("data")),
			Expectation: []string{"kubectl-client-side-apply"},
		},
		{
			// 查找嵌套在另一字段内的 fieldpath。
			Name: "Nested",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-22T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")),
			Expectation: []string{"kubectl-client-side-apply"},
		},
		{
			// 查找在某个条目中找不到、但在另一个中存在的 operation/fieldpath 组合。
			// axes
			Name: "NotFound",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl
        operation: Apply
        time: "2022-08-23T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")),
			Expectation: []string{},
		},
		{
			// 使用 apply 操作测试。
			Name: "ApplyOperation",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl
        operation: Apply
        time: "2022-08-23T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationApply,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")),
			Expectation: []string{"kubectl"},
		},
		{
			// 多个 field manager 中只匹配一个。
			Name: "OneOfMultiple",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-23T23:08:23Z"
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
        manager: kubectl
        operation: Apply
        time: "2022-08-23T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")),
			Expectation: []string{"kubectl-client-side-apply"},
		},
		{
			// 存在多个 field manager，且匹配其中多个而非全部。
			Name: "ManyOfMultiple",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-23T23:08:23Z"
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
        manager: kubectl
        operation: Apply
        time: "2022-08-23T23:08:23Z"
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
        manager: kubectl-client-side-apply2
        operation: Update
        time: "2022-08-23T23:08:23Z"
      `,
			Operation:   metav1.ManagedFieldsOperationUpdate,
			Fields:      fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")),
			Expectation: []string{"kubectl-client-side-apply", "kubectl-client-side-apply2"},
		},
		{
			// 用多个字段测试匹配。
			Name: "BasicMultipleFields",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
          f:data:
            .: {}
            f:key: {}
            f:legacy: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-23T23:08:23Z"
      `,
			Operation: metav1.ManagedFieldsOperationUpdate,
			Fields: fieldpath.NewSet(
				fieldpath.MakePathOrDie("data", "key"),
				fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration"),
			),
			Expectation: []string{"kubectl-client-side-apply"},
		},
		{
			// 用多个字段测试匹配，但 manager 缺少其中一个字段。
			// 请求因此不匹配。
			Name: "MissingOneField",
			ManagedFieldsYAML: `
      managedFields:
      - apiVersion: v1
        fieldsType: FieldsV1
        fieldsV1:
          f:metadata:
            f:annotations:
              .: {}
              f:kubectl.kubernetes.io/last-applied-configuration: {}
          f:data:
            .: {}
            f:legacy: {}
        manager: kubectl-client-side-apply
        operation: Update
        time: "2022-08-23T23:08:23Z"
      `,
			Operation: metav1.ManagedFieldsOperationUpdate,
			Fields: fieldpath.NewSet(
				fieldpath.MakePathOrDie("data", "key"),
				fieldpath.MakePathOrDie("metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration"),
			),
			Expectation: []string{},
		},
	}
	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			var entries struct {
				ManagedFields []metav1.ManagedFieldsEntry `json:"managedFields"`
			}
			// 从 YAML 片段解析出 managedFields，测试数据与真实对象结构保持一致。
			err := yaml.Unmarshal([]byte(tcase.ManagedFieldsYAML), &entries)
			require.NoError(t, err)

			result := csaupgrade.FindFieldsOwners(entries.ManagedFields, tcase.Operation, tcase.Fields)

			// 比较 owner 名字，因为它们唯一标识选中的条目。
			// (given that the operation is provided)
			// 在 operation 已限定的前提下，manager 名即可唯一标识条目。
			ownerNames := []string{}
			for _, entry := range result {
				ownerNames = append(ownerNames, entry.Manager)
				require.Equal(t, tcase.Operation, entry.Operation)
			}
			require.ElementsMatch(t, tcase.Expectation, ownerNames)
		})
	}
}

// TestUpgradeCSA 验证把 client-side apply 的 managedFields 迁移为 server-side apply 的整体行为：
// 包括直接改名、与已有 SSA 条目合并、版本不匹配时丢弃、无操作、多 manager 合并及 subresource。
func TestUpgradeCSA(t *testing.T) {

	cases := []struct {
		Name           string
		CSAManagers    []string
		SSAManager     string
		Options        []csaupgrade.Option
		OriginalObject []byte
		ExpectedObject []byte
	}{
		{
			// CSA 条目存在给定名字的 manager，但 SSA 条目中不存在。
			// 找到时，期望 CSA 条目被转换为 SSA 条目。
			// 并被重命名。
			Name:        "csa-basic-direct-conversion",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 这是首次使用 kubectl --server-side 的场景。
			// 服务端创建了重复的 managed fields 条目：一个来自 Update，另一个来自初次 SSA。
			Name:        "csa-combine-with-ssa-duplicate-keys",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 这是首次使用 kubectl --server-side 的场景，
			// 某个 key 被删除。缺陷场景是 key 留在 CSA 条目中但 SSA 条目已无，导致它不会被清理。
			// 这里展示升级此类对象后后续行为正确。
			// time SSA applier
			// 期望最终对象包含两个条目中 key 的并集。
			Name:        "csa-combine-with-ssa-additional-keys",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 对象上有多个 CSA 版本与 apply 条目的版本不匹配；展示它们会被直接丢弃而不合并。
			Name:        "csa-no-applicable-version",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key2: {}
      f:metadata:
        f:annotations:
          f:hello2: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v2
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key3: {}
      f:metadata:
        f:annotations:
          f:hello3: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v3
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key4: {}
      f:metadata:
        f:annotations:
          f:hello3: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v4
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key5: {}
      f:metadata:
        f:annotations:
          f:hello4: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 对象上有多个 CSA 版本与 apply 条目的版本不匹配，其中一个匹配；展示匹配的 CSA
			// 条目会被合并进 SSA 条目。
			Name:        "csa-single-applicable-version",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key2: {}
      f:metadata:
        f:annotations:
          f:hello2: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v2
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key3: {}
      f:metadata:
        f:annotations:
          f:hello3: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v3
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key4: {}
      f:metadata:
        f:annotations:
          f:hello4: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  - apiVersion: v4
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        f:key5: {}
      f:metadata:
        f:annotations:
          f:hello5: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-08-22T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:key2: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:hello2: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 对无需迁移且没有现有 SSA manager 的对象不做任何处理。
			Name:        "noop",
			CSAManagers: []string{"kubectl-client-side-apply"},
			SSAManager:  "not-already-in-object",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v5
    fieldsType: FieldsV1
    fieldsV1:
      f:data:
        .: {}
        f:key: {}
        f:legacy: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
    manager: kubectl
    operation: Apply
    time: "2022-08-23T23:08:23Z"
  name: test
  namespace: default
`),
		},
		{
			// 期望多个 target 被合并进已有的 ssa manager。
			Name:        "multipleTargetsExisting",
			CSAManagers: []string{"kube-scheduler", "kubectl-client-side-apply"},
			SSAManager:  "kubectl",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:labels:
          f:name: {}
      f:spec:
        f:containers:
          k:{"name":"kubernetes-pause"}:
            .: {}
            f:image: {}
            f:name: {}
    manager: kubectl
    operation: Apply
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:status:
        f:conditions:
          .: {}
          k:{"type":"PodScheduled"}:
            .: {}
            f:lastProbeTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
            f:type: {}
    manager: kube-scheduler
    operation: Update
    time: "2022-11-03T23:22:40Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
        f:labels:
          .: {}
          f:name: {}
      f:spec:
        f:containers:
          k:{"name":"kubernetes-pause"}:
            .: {}
            f:image: {}
            f:imagePullPolicy: {}
            f:name: {}
            f:resources: {}
            f:terminationMessagePath: {}
            f:terminationMessagePolicy: {}
        f:dnsPolicy: {}
        f:enableServiceLinks: {}
        f:restartPolicy: {}
        f:schedulerName: {}
        f:securityContext: {}
        f:terminationGracePeriodSeconds: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-11-03T23:22:40Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:status:
        f:conditions:
          .: {}
          k:{"type":"PodScheduled"}:
            .: {}
            f:lastProbeTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
            f:type: {}
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
        f:labels:
          .: {}
          f:name: {}
      f:spec:
        f:containers:
          k:{"name":"kubernetes-pause"}:
            .: {}
            f:image: {}
            f:imagePullPolicy: {}
            f:name: {}
            f:resources: {}
            f:terminationMessagePath: {}
            f:terminationMessagePolicy: {}
        f:dnsPolicy: {}
        f:enableServiceLinks: {}
        f:restartPolicy: {}
        f:schedulerName: {}
        f:securityContext: {}
        f:terminationGracePeriodSeconds: {}
    manager: kubectl
    operation: Apply
  name: test
  namespace: default
`),
		},
		{
			// 期望多个 target 被合并进新的 ssa manager。
			Name:        "multipleTargetsNewInsertion",
			CSAManagers: []string{"kubectl-client-side-apply", "kube-scheduler"},
			SSAManager:  "newly-inserted-manager",
			OriginalObject: []byte(`
apiVersion: v1
data: {}
kind: ConfigMap
metadata:
  resourceVersion: "1"
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
      {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
  creationTimestamp: "2022-08-22T23:08:23Z"
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:labels:
          f:name: {}
      f:spec:
        f:containers:
          k:{"name":"kubernetes-pause"}:
            .: {}
            f:image: {}
            f:name: {}
    manager: kubectl
    operation: Apply
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:status:
        f:conditions:
          .: {}
          k:{"type":"PodScheduled"}:
            .: {}
            f:lastProbeTime: {}
            f:lastTransitionTime: {}
            f:message: {}
            f:reason: {}
            f:status: {}
            f:type: {}
    manager: kube-scheduler
    operation: Update
    time: "2022-11-03T23:22:40Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          .: {}
          f:kubectl.kubernetes.io/last-applied-configuration: {}
        f:labels:
          .: {}
          f:name: {}
      f:spec:
        f:containers:
          k:{"name":"kubernetes-pause"}:
            .: {}
            f:image: {}
            f:imagePullPolicy: {}
            f:name: {}
            f:resources: {}
            f:terminationMessagePath: {}
            f:terminationMessagePolicy: {}
        f:dnsPolicy: {}
        f:enableServiceLinks: {}
        f:restartPolicy: {}
        f:schedulerName: {}
        f:securityContext: {}
        f:terminationGracePeriodSeconds: {}
    manager: kubectl-client-side-apply
    operation: Update
    time: "2022-11-03T23:22:40Z"
  name: test
  namespace: default
`),
			ExpectedObject: []byte(`
  apiVersion: v1
  data: {}
  kind: ConfigMap
  metadata:
    resourceVersion: "1"
    annotations:
      kubectl.kubernetes.io/last-applied-configuration: |
        {"apiVersion":"v1","data":{"key":"value","legacy":"unused"},"kind":"ConfigMap","metadata":{"annotations":{},"name":"test","namespace":"default"}}
    creationTimestamp: "2022-08-22T23:08:23Z"
    managedFields:
    - apiVersion: v1
      fieldsType: FieldsV1
      fieldsV1:
        f:metadata:
          f:labels:
            f:name: {}
        f:spec:
          f:containers:
            k:{"name":"kubernetes-pause"}:
              .: {}
              f:image: {}
              f:name: {}
      manager: kubectl
      operation: Apply
    - apiVersion: v1
      fieldsType: FieldsV1
      fieldsV1:
        f:metadata:
          f:annotations:
            .: {}
            f:kubectl.kubernetes.io/last-applied-configuration: {}
          f:labels:
            .: {}
            f:name: {}
        f:spec:
          f:containers:
            k:{"name":"kubernetes-pause"}:
              .: {}
              f:image: {}
              f:imagePullPolicy: {}
              f:name: {}
              f:resources: {}
              f:terminationMessagePath: {}
              f:terminationMessagePolicy: {}
          f:dnsPolicy: {}
          f:enableServiceLinks: {}
          f:restartPolicy: {}
          f:schedulerName: {}
          f:securityContext: {}
          f:terminationGracePeriodSeconds: {}
        f:status:
          f:conditions:
            .: {}
            k:{"type":"PodScheduled"}:
              .: {}
              f:lastProbeTime: {}
              f:lastTransitionTime: {}
              f:message: {}
              f:reason: {}
              f:status: {}
              f:type: {}
      manager: newly-inserted-manager
      operation: Apply
      time: "2022-11-03T23:22:40Z"
    name: test
    namespace: default
`),
		},
		{
			// 期望多个 target 被合并进新的 ssa manager。
			Name:        "subresource",
			CSAManagers: []string{"kube-controller-manager"},
			SSAManager:  "kube-controller-manager",
			Options:     []csaupgrade.Option{csaupgrade.Subresource("status")},
			OriginalObject: []byte(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  annotations:
    pv.kubernetes.io/bind-completed: "yes"
    pv.kubernetes.io/bound-by-controller: "yes"
    volume.beta.kubernetes.io/storage-provisioner: openshift-storage.cephfs.csi.ceph.com
    volume.kubernetes.io/storage-provisioner: openshift-storage.cephfs.csi.ceph.com
  creationTimestamp: "2024-02-24T15:24:31Z"
  finalizers:
  - kubernetes.io/pvc-protection
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:spec:
        f:accessModes: {}
        f:resources:
          f:requests:
            .: {}
            f:storage: {}
        f:storageClassName: {}
        f:volumeMode: {}
    manager: Mozilla
    operation: Update
    time: "2024-02-24T15:24:31Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          .: {}
          f:pv.kubernetes.io/bind-completed: {}
          f:pv.kubernetes.io/bound-by-controller: {}
          f:volume.beta.kubernetes.io/storage-provisioner: {}
          f:volume.kubernetes.io/storage-provisioner: {}
      f:spec:
        f:volumeName: {}
    manager: kube-controller-manager
    operation: Update
    time: "2024-02-24T15:24:32Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:status:
        f:accessModes: {}
        f:capacity:
          .: {}
          f:storage: {}
        f:phase: {}
    manager: kube-controller-manager
    operation: Update
    subresource: status
    time: "2024-02-24T15:24:32Z"
  name: test
  namespace: default
  resourceVersion: "948647140"
  uid: f0692a61-0ffe-4fd5-b00f-0b95f3654fb9
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: ocs-storagecluster-cephfs
  volumeMode: Filesystem
  volumeName: pvc-f0692a61-0ffe-4fd5-b00f-0b95f3654fb9
status:
  accessModes:
  - ReadWriteOnce
  capacity:
    storage: 1Gi
  phase: Bound
`),
			ExpectedObject: []byte(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  annotations:
    pv.kubernetes.io/bind-completed: "yes"
    pv.kubernetes.io/bound-by-controller: "yes"
    volume.beta.kubernetes.io/storage-provisioner: openshift-storage.cephfs.csi.ceph.com
    volume.kubernetes.io/storage-provisioner: openshift-storage.cephfs.csi.ceph.com
  creationTimestamp: "2024-02-24T15:24:31Z"
  finalizers:
  - kubernetes.io/pvc-protection
  managedFields:
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:spec:
        f:accessModes: {}
        f:resources:
          f:requests:
            .: {}
            f:storage: {}
        f:storageClassName: {}
        f:volumeMode: {}
    manager: Mozilla
    operation: Update
    time: "2024-02-24T15:24:31Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:metadata:
        f:annotations:
          .: {}
          f:pv.kubernetes.io/bind-completed: {}
          f:pv.kubernetes.io/bound-by-controller: {}
          f:volume.beta.kubernetes.io/storage-provisioner: {}
          f:volume.kubernetes.io/storage-provisioner: {}
      f:spec:
        f:volumeName: {}
    manager: kube-controller-manager
    operation: Update
    time: "2024-02-24T15:24:32Z"
  - apiVersion: v1
    fieldsType: FieldsV1
    fieldsV1:
      f:status:
        f:accessModes: {}
        f:capacity:
          .: {}
          f:storage: {}
        f:phase: {}
    manager: kube-controller-manager
    operation: Apply
    subresource: status
    time: "2024-02-24T15:24:32Z"
  name: test
  namespace: default
  resourceVersion: "948647140"
  uid: f0692a61-0ffe-4fd5-b00f-0b95f3654fb9
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: ocs-storagecluster-cephfs
  volumeMode: Filesystem
  volumeName: pvc-f0692a61-0ffe-4fd5-b00f-0b95f3654fb9
status:
  accessModes:
  - ReadWriteOnce
  capacity:
    storage: 1Gi
  phase: Bound
`),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			initialObject := unstructured.Unstructured{}
			err := yaml.Unmarshal(testCase.OriginalObject, &initialObject.Object)
			if err != nil {
				t.Fatal(err)
			}

			// 在副本上执行升级，保留 initialObject 供后续 patch 测试使用。
			upgraded := initialObject.DeepCopy()
			err = csaupgrade.UpgradeManagedFields(
				upgraded,
				sets.New(testCase.CSAManagers...),
				testCase.SSAManager,
				testCase.Options...,
			)

			if err != nil {
				t.Fatal(err)
			}

			expectedObject := unstructured.Unstructured{}
			err = yaml.Unmarshal(testCase.ExpectedObject, &expectedObject.Object)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(&expectedObject, upgraded) {
				t.Fatal(cmp.Diff(&expectedObject, upgraded))
			}

			// 展示 UpgradeManagedFieldsPatch 生成的 patch 恰好让对象等于预期输出，不多也不少。
			// of UpgradeManagedFields
			// 验证补丁路径与直接修改路径结果一致：应用补丁后的对象应等于 upgraded。
			initialCopy := initialObject.DeepCopyObject()
			patchBytes, err := csaupgrade.UpgradeManagedFieldsPatch(
				initialCopy, sets.New(testCase.CSAManagers...), testCase.SSAManager, testCase.Options...)

			if err != nil {
				t.Fatal(err)
			} else if patchBytes != nil {
				patch, err := jsonpatch.DecodePatch(patchBytes)
				if err != nil {
					t.Fatal(err)
				}

				initialJSON, err := json.Marshal(initialObject.Object)
				if err != nil {
					t.Fatal(err)
				}

				patchedBytes, err := patch.Apply(initialJSON)
				if err != nil {
					t.Fatal(err)
				}

				var patched unstructured.Unstructured
				if err := json.Unmarshal(patchedBytes, &patched.Object); err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(&patched, upgraded) {
					t.Fatalf("expected patch to produce an upgraded object: %v", cmp.Diff(patched, upgraded))
				}
			}
		})
	}
}
