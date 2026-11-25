/*
Copyright 2025 The Kubernetes Authors.

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

package dra

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/testapigroup/install"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	testapigrouprest "k8s.io/kubernetes/pkg/registry/testapigroup/rest"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

func init() {
	install.Install(legacyscheme.Scheme)
}

func TestApply(t *testing.T) {
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	apiServerFlags := framework.DefaultTestServerFlags()
	runtimeConfigs := []string{"testapigroup.apimachinery.k8s.io/v1=true"}
	apiServerFlags = append(apiServerFlags, "--runtime-config="+strings.Join(runtimeConfigs, ","))

	// 健全性检查。此处未针对并发访问做保护，
	// 但没关系：集成测试也会在竞态检测下运行，
	// 届时会捕获这类问题。
	if controlplane.AdditionalStorageProvidersForTests != nil {
		t.Fatal("cannot set AdditionalStorageProvidersForTests, already set")
	}
	t.Cleanup(func() {
		controlplane.AdditionalStorageProvidersForTests = nil
	})
	controlplane.AdditionalStorageProvidersForTests = func(client *kubernetes.Clientset) []controlplaneapiserver.RESTStorageProvider {
		return []controlplaneapiserver.RESTStorageProvider{
			testapigrouprest.RESTStorageProvider{NamespaceClient: client.CoreV1().Namespaces()},
		}
	}

	server := kubeapiservertesting.StartTestServerOrDie(t, apiServerOptions, apiServerFlags, etcdOptions)
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Log("Stopping the apiserver...")
		server.TearDownFn()
	})
	tCtx = tCtx.WithRESTConfig(server.ClientConfig)

	// 这里可以添加更多子测试，目前只有一个。
	tCtx.Run("optional-list-map-key", testOptionalListMapKey)
}

func testOptionalListMapKey(tCtx ktesting.TContext) {
	requireManagedFields := func(what string, obj *unstructured.Unstructured, expectedManagedFields any) {
		tCtx.Helper()
		actualManagedFields, _, _ := unstructured.NestedFieldCopy(obj.Object, "metadata", "managedFields")
		if actualManagedFields != nil {
			managers := actualManagedFields.([]any)
			for i := range managers {
				// 去掉不确定的 time 字段。
				unstructured.RemoveNestedField(managers[i].(map[string]any), "time")
			}
			// 按 manager 排序。每个 manager 最多只有一个条目，
			// 因此无需额外的排序依据。顺序在语义上无关紧要，
			// 所以这里不需要指定某个特定的顺序。
			//
			// 没有这一步的话，顺序会变得不确定（导致测试不稳定！）。
			slices.SortFunc(managers, func(a, b any) int {
				return strings.Compare(a.(map[string]any)["manager"].(string), b.(map[string]any)["manager"].(string))
			})
		}
		require.Equal(tCtx, dump(expectedManagedFields), dump(actualManagedFields), "%s:\n%s", what, dump(obj))
	}

	requireInfos := func(what string, obj *unstructured.Unstructured, expectedInfos []testapigroupv1.CarpInfo) {
		tCtx.Helper()
		actualInfos, _, _ := unstructured.NestedFieldCopy(obj.Object, "status", "infos")
		// 列表条目的顺序可能变化，按 manager 排序。
		if actualInfos, ok := actualInfos.([]map[string]any); ok {
			slices.SortFunc(actualInfos, func(a, b map[string]any) int {
				managerA, _, _ := unstructured.NestedString(a, "manager")
				managerB, _, _ := unstructured.NestedString(b, "manager")
				return strings.Compare(managerA, managerB)
			})
		}
		require.Equal(tCtx, dump(expectedInfos), dump(actualInfos), "%s:\n%s", what, dump(obj))
	}

	carp := &unstructured.Unstructured{}
	name := "test-carp"
	namespace := createTestNamespace(tCtx, nil)
	carp.SetName(name)
	carp.SetNamespace(namespace)
	client := tCtx.Dynamic().Resource(testapigroupv1.SchemeGroupVersion.WithResource("carps")).Namespace(namespace)

	// 创建时不设置 spec 中的任何字段，此时 managedFields 仍为空。
	carp, err := client.Create(tCtx, carp, metav1.CreateOptions{FieldManager: "creator"})
	tCtx.ExpectNoError(err, "create carp")
	requireManagedFields("after creation", carp, nil)

	apply := func(what, manager, applyconfig, expectedManagedFields string, expectedInfos []testapigroupv1.CarpInfo) {
		tCtx.Helper()

		carp, err := client.ApplyStatus(tCtx, name, parseObj(tCtx, applyconfig), metav1.ApplyOptions{FieldManager: manager})
		tCtx.ExpectNoError(err, what)
		requireManagedFields(what, carp, parseAny(tCtx, expectedManagedFields))
		requireInfos(what, carp, expectedInfos)
	}

	// 设置 "A: 1, B: x" 和 "A: 2, B: x" 的 infos。
	apply("add status #1", "status1", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
status:
  infos:
  - a: 1
    b: "x"
    data: status1_a1_bx
  - a: 2
    b: "x"
    data: status1_a2_bx
`,
		`
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
`,
		[]testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 2, B: "x", Data: "status1_a2_bx"}},
	)

	// 第二个状态，包含 "A: 1, B: y" 和 "A: 2, B: y" 的 infos。
	apply("add status #2", "status2", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
status:
  infos:
  - a: 1
    b: "y"
    data: status2_a1_by
  - a: 2
    b: "y"
    data: status2_a2_by
`, `
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status2
  operation: Apply
  subresource: status
`,
		[]testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 2, B: "x", Data: "status1_a2_bx"}, {A: 1, B: "y", Data: "status2_a1_by"}, {A: 2, B: "y", Data: "status2_a2_by"}},
	)

	// 删除第一个 field manager 的一个条目。
	apply("remove status #1", "status1", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
status:
  infos:
  - a: 1
    b: "x"
    data: status1_a1_bx
`, `
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status2
  operation: Apply
  subresource: status
`,
		[]testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 1, B: "y", Data: "status2_a1_by"}, {A: 2, B: "y", Data: "status2_a2_by"}},
	)

	// 更新第二个 field manager 的一个条目。
	apply("update status #2", "status2", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
status:
  infos:
  - a: 1
    b: "y"
    data: status2_a1_by
  - a: 2
    b: "y"
    data: status2_a2_by_updated
`, `
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status2
  operation: Apply
  subresource: status
`, []testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 1, B: "y", Data: "status2_a1_by"}, {A: 2, B: "y", Data: "status2_a2_by_updated"}},
	)

	// 添加 C 为空和非空的条目。
	apply("add with C", "status1", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
status:
  infos:
  - a: 1
    b: "x"
    data: status1_a1_bx
  - a: 1
    b: "x"
    c: ""
    data: status1_a1_bx_c
  - a: 1
    b: "x"
    c: "z"
    data: status1_a1_bx_cz
`, `
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":1,"b":"x","c":""}:
          .: {}
          f:a: {}
          f:b: {}
          f:c: {}
          f:data: {}
        k:{"a":1,"b":"x","c":"z"}:
          .: {}
          f:a: {}
          f:b: {}
          f:c: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":2,"b":"y"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
  manager: status2
  operation: Apply
  subresource: status
`, []testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 1, B: "y", Data: "status2_a1_by"}, {A: 2, B: "y", Data: "status2_a2_by_updated"}, {A: 1, B: "x", C: ptr.To(""), Data: "status1_a1_bx_c"}, {A: 1, B: "x", C: ptr.To("z"), Data: "status1_a1_bx_cz"}},
	)

	// 清除第二个 manager 的所有条目。
	apply("remove second manager", "status2", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
`, `
- apiVersion: testapigroup.apimachinery.k8s.io/v1
  fieldsType: FieldsV1
  fieldsV1:
    f:status:
      f:infos:
        k:{"a":1,"b":"x"}:
          .: {}
          f:a: {}
          f:b: {}
          f:data: {}
        k:{"a":1,"b":"x","c":""}:
          .: {}
          f:a: {}
          f:b: {}
          f:c: {}
          f:data: {}
        k:{"a":1,"b":"x","c":"z"}:
          .: {}
          f:a: {}
          f:b: {}
          f:c: {}
          f:data: {}
  manager: status1
  operation: Apply
  subresource: status
`, []testapigroupv1.CarpInfo{{A: 1, B: "x", Data: "status1_a1_bx"}, {A: 1, B: "x", C: ptr.To(""), Data: "status1_a1_bx_c"}, {A: 1, B: "x", C: ptr.To("z"), Data: "status1_a1_bx_cz"}},
	)

	// 清除第一个 manager 的所有条目。
	apply("remove first manager", "status1", `
kind: Carp
apiVersion: testapigroup.apimachinery.k8s.io/v1
`,
		``, nil,
	)
}

// createTestNamespace 创建的命名空间名称派生自
// 当前的测试名称：
// - 非字母数字字符替换为连字符。
// - 在中间截断，使其足够短以适用于 GenerateName。
// - 由 apiserver 添加连字符和随机后缀。
func createTestNamespace(tCtx ktesting.TContext, labels map[string]string) string {
	tCtx.Helper()
	name := regexp.MustCompile(`[^[:alnum:]_-]`).ReplaceAllString(tCtx.Name(), "-")
	name = strings.ToLower(name)
	if len(name) > 63 {
		name = name[:30] + "--" + name[len(name)-30:]
	}
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: name + "-"}}
	ns.Labels = labels
	ns, err := tCtx.Client().CoreV1().Namespaces().Create(tCtx, ns, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create test namespace")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.ExpectNoError(tCtx.Client().CoreV1().Namespaces().Delete(tCtx, ns.Name, metav1.DeleteOptions{}), "delete test namespace")
	})
	return ns.Name
}

func dump(in any) string {
	out, err := yaml.Marshal(in)
	if err != nil {
		return err.Error()
	}
	return string(out)
}

func parseAs[T any](tCtx ktesting.TContext, data string) T {
	tCtx.Helper()

	var obj T
	err := yaml.Unmarshal([]byte(data), &obj)
	tCtx.ExpectNoError(err, data)
	return obj
}

func parseAny(tCtx ktesting.TContext, data string) any {
	tCtx.Helper()

	return parseAs[any](tCtx, data)
}

func parseObj(tCtx ktesting.TContext, data string) *unstructured.Unstructured {
	tCtx.Helper()

	obj := parseAs[unstructured.Unstructured](tCtx, data)
	return &obj
}
