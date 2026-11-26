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

// 注意：该示例只适用于同一 release/branch 内的代码。
//
// 该示例演示用 dynamic 客户端对 Deployment 完成创建、更新、列举、删除。
// 对象以 unstructured 形式构造，字段名与 apps/v1 的 JSON 结构一一对应。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/retry"
	//
	// 取消注释可加载所有认证插件。
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// 或取消注释以加载指定的认证插件。
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

func main() {
	// 默认读取 ~/.kube/config；home 目录不可用时留空，交由调用者显式传入路径。
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// 示例代码用 panic 简化错误处理；生产代码应向上返回错误。
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	// 动态客户端以 GVR 定位资源，无需导入 apps/v1 的 Go 类型。
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	// Deployment 以 map 形式描述；字段名与 apps/v1 的 JSON 结构一致。
	// 注意后续用 SetNestedField 修改数值字段时必须传 int64。
	deployment := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "demo-deployment",
			},
			"spec": map[string]interface{}{
				"replicas": 2,
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": "demo",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "demo",
						},
					},

					"spec": map[string]interface{}{
						"containers": []map[string]interface{}{
							{
								"name":  "web",
								"image": "nginx:1.12",
								"ports": []map[string]interface{}{
									{
										"name":          "http",
										"protocol":      "TCP",
										"containerPort": 80,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 创建 Deployment。
	// Namespace(apiv1.NamespaceDefault) 显式指定命名空间；返回结果中的 metadata
	// 已由服务端补全（含 resourceVersion、uid 等）。
	fmt.Println("Creating deployment...")
	result, err := client.Resource(deploymentRes).Namespace(apiv1.NamespaceDefault).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment %q.\n", result.GetName())

	// Update Deployment
	prompt()
	fmt.Println("Updating deployment...")
	// 更新 Deployment 有两种方式：
	//
	// 1. 修改 "deployment" 变量后调用 Update(deployment)，类似 "kubectl replace"，
	//    但会覆盖掉 Create 与 Update 之间其他客户端的改动。
	// 2. 修改 Get() 返回的 "result" 并重试 Update(result) 直到不再冲突，可保留
	//    其他客户端的改动。下面采用此方案，并用 client-go 自带的 retry 包实现（推荐）。
	//
	// 更多信息：
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
	//
	// 下面采用方案 2：冲突时重新读取最新对象再改，避免覆盖他人写入。

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// 更新前先读取最新版本；RetryOnConflict 使用指数退避，避免打满 apiserver。
		result, getErr := client.Resource(deploymentRes).Namespace(apiv1.NamespaceDefault).Get(context.TODO(), "demo-deployment", metav1.GetOptions{})
		if getErr != nil {
			panic(fmt.Errorf("failed to get latest version of Deployment: %v", getErr))
		}

		// 把副本数改为 1。
		if err := unstructured.SetNestedField(result.Object, int64(1), "spec", "replicas"); err != nil {
			panic(fmt.Errorf("failed to set replica value: %v", err))
		}

		// 取出 spec.containers。
		containers, found, err := unstructured.NestedSlice(result.Object, "spec", "template", "spec", "containers")
		if err != nil || !found || containers == nil {
			panic(fmt.Errorf("deployment containers not found or error in spec: %v", err))
		}

		// 更新 container[0] 的镜像。
		// 先改副本再写回整个 containers 切片，保持嵌套结构一致。
		if err := unstructured.SetNestedField(containers[0].(map[string]interface{}), "nginx:1.13", "image"); err != nil {
			panic(err)
		}
		if err := unstructured.SetNestedField(result.Object, containers, "spec", "template", "spec", "containers"); err != nil {
			panic(err)
		}

		_, updateErr := client.Resource(deploymentRes).Namespace(apiv1.NamespaceDefault).Update(context.TODO(), result, metav1.UpdateOptions{})
		return updateErr
	})
	if retryErr != nil {
		panic(fmt.Errorf("update failed: %v", retryErr))
	}
	fmt.Println("Updated deployment...")

	// 列举 Deployment。
	prompt()
	fmt.Printf("Listing deployments in namespace %q:\n", apiv1.NamespaceDefault)
	list, err := client.Resource(deploymentRes).Namespace(apiv1.NamespaceDefault).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, d := range list.Items {
		// replicas 在 unstructured 中为 int64，因此用 NestedInt64 读取。
		replicas, found, err := unstructured.NestedInt64(d.Object, "spec", "replicas")
		if err != nil || !found {
			fmt.Printf("Replicas not found for deployment %s: error=%s", d.GetName(), err)
			continue
		}
		fmt.Printf(" * %s (%d replicas)\n", d.GetName(), replicas)
	}

	// 删除 Deployment。
	// 使用 Foreground 删除：等待依赖对象清理完毕后再返回，便于示例串行演示。
	prompt()
	fmt.Println("Deleting deployment...")
	deletePolicy := metav1.DeletePropagationForeground
	deleteOptions := metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}
	if err := client.Resource(deploymentRes).Namespace(apiv1.NamespaceDefault).Delete(context.TODO(), "demo-deployment", deleteOptions); err != nil {
		panic(err)
	}

	fmt.Println("Deleted deployment.")
}

// prompt 暂停等待用户按回车，便于逐步观察每个阶段的输出。
func prompt() {
	fmt.Printf("-> Press Return key to continue.")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		break
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fmt.Println()
}
