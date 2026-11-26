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

package resource_test

import (
	"bytes"
	"fmt"

	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes/scheme"
)

// exampleManifest 覆盖三类输入形态：单对象、内嵌 List、以及 core v1 List，
// 用来验证 Flatten 能把嵌套列表都展开成独立对象。
var exampleManifest = `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: mutating1
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfigurationList
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingWebhookConfiguration
  metadata:
    name: mutating2
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingWebhookConfiguration
  metadata:
    name: mutating3
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: validating1
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfigurationList
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: validating2
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: validating3
---
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingWebhookConfiguration
  metadata:
    name: mutating4
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: validating4
---
`

// 示例：用本地 resource builder 从清单读取带类型的资源。
//
// 示例演示典型的 CLI 读取流程：本地 builder -> 强类型 scheme -> Stream 输入 ->
// Flatten 展开 -> ContinueOnError 尽量多读，最后用 Infos 取回全部对象。
func ExampleNewLocalBuilder() {
	// 创建本地 builder。
	builder := resource.NewLocalBuilder().
		// 配置 scheme，以取得 scheme 注册版本中带类型的对象；
		// 也可改用 Unstructured() 取得 unstructured 对象。
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		// 通过 Reader 提供输入。
		// 也可调用 Path(false, "/path/to/file") 从文件读取。
		Stream(bytes.NewBufferString(exampleManifest), "input").
		// 展开 List 对象中的条目。
		Flatten().
		// 尽量累积更多条目。
		ContinueOnError()

	// 执行 builder。
	result := builder.Do()

	if err := result.Err(); err != nil {
		fmt.Println("builder error:", err)
		return
	}

	items, err := result.Infos()
	if err != nil {
		fmt.Println("infos error:", err)
		return
	}

	for _, item := range items {
		fmt.Printf("%s (%T)\n", item.String(), item.Object)
	}

	// Output:
	// Name: "mutating1", Namespace: "" (*v1.MutatingWebhookConfiguration)
	// Name: "mutating2", Namespace: "" (*v1.MutatingWebhookConfiguration)
	// Name: "mutating3", Namespace: "" (*v1.MutatingWebhookConfiguration)
	// Name: "validating1", Namespace: "" (*v1.ValidatingWebhookConfiguration)
	// Name: "validating2", Namespace: "" (*v1.ValidatingWebhookConfiguration)
	// Name: "validating3", Namespace: "" (*v1.ValidatingWebhookConfiguration)
	// Name: "mutating4", Namespace: "" (*v1.MutatingWebhookConfiguration)
	// Name: "validating4", Namespace: "" (*v1.ValidatingWebhookConfiguration)
}
