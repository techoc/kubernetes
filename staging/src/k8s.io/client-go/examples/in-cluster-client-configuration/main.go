/*
Copyright 2016 The Kubernetes Authors.

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

// Note: the example only works with the code within the same release/branch.
//
// 示例程序演示如何在 Pod 内（集群内）用 rest.InClusterConfig 获取配置并创建 clientset。
// 它只能在集群内运行，因为依赖注入的 service account token 与 CA 文件路径；
// 注意示例仅与同一 release/branch 的代码兼容。
package main

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	//
	// Uncomment to load all auth plugins
	// 取消注释可加载全部认证插件（会注册所有内置提供者）。
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// 或只加载特定认证插件，减少二进制体积与副作用。
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

func main() {
	// creates the in-cluster config
	// 从 Pod 环境（service account token、service 环境变量、CA 证书）构造配置。
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	// 基于配置创建类型化 clientset，连接信息复用同一个 HTTP 客户端。
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	for {
		// get pods in all the namespaces by omitting namespace
		// Or specify namespace to get pods in particular namespace
		// 空命名空间表示列举所有命名空间中的 Pod。
		pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		// Examples for error handling:
		// - Use helper functions e.g. errors.IsNotFound()
		// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
		// 演示两种错误处理方式：errors.IsNotFound 判断语义类型，
		// 或断言 *errors.StatusError 读取服务端返回的详细信息。
		_, err = clientset.CoreV1().Pods("default").Get(context.TODO(), "example-xxxxx", metav1.GetOptions{})
		if errors.IsNotFound(err) {
			fmt.Printf("Pod example-xxxxx not found in default namespace\n")
		} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
			fmt.Printf("Error getting pod %v\n", statusError.ErrStatus.Message)
		} else if err != nil {
			panic(err.Error())
		} else {
			fmt.Printf("Found example-xxxxx pod in default namespace\n")
		}

		// 简单轮询；真实场景应使用 informer 并结合退避策略。
		time.Sleep(10 * time.Second)
	}
}
