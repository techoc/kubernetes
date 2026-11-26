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

// 注意：本示例只适用于同一 release/branch 中的代码。
// release/branch 的 client-go 配套使用，跨版本复制时 API 可能不兼容。
package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	//
	// 取消注释以加载全部认证插件。
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// 或者取消注释以加载指定的认证插件。
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)

func main() {
	var kubeconfig *string
	// 中文：优先使用 $HOME/.kube/config 作为默认路径；若无家目录（如某些容器环境），
	// 则要求用户显式传入绝对路径。
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// 使用 kubeconfig 中的 current-context：masterURL 传空字符串，表示完全依赖 kubeconfig。
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// 创建 clientset：NewForConfig 会深拷贝并补全默认值（QPS、限流器等），可安全复用。
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	for {
		// 中文：空 namespace 表示跨所有命名空间列举 Pod。
		pods, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		// 错误处理示例：
		// - 使用 errors.IsNotFound() 等辅助函数。
		// - 或转换为 StatusError 并使用其属性，如 ErrStatus.Message。
		//
		// 中文：展示两种错误处理方式——用 IsNotFound 做语义判断，或断言 *StatusError
		// 读取服务端返回的 Status.Message。
		namespace := "default"
		pod := "example-xxxxx"
		_, err = clientset.CoreV1().Pods(namespace).Get(context.TODO(), pod, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
		} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
			fmt.Printf("Error getting pod %s in namespace %s: %v\n",
				pod, namespace, statusError.ErrStatus.Message)
		} else if err != nil {
			panic(err.Error())
		} else {
			fmt.Printf("Found pod %s in namespace %s\n", pod, namespace)
		}

		// 中文：示例为演示轮询，生产代码应使用 informer 或 watch 而非固定间隔的 List。
		time.Sleep(10 * time.Second)
	}
}
