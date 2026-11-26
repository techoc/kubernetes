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

package clientgo_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Example_inClusterConfiguration 演示使用 Pod 的服务账号在集群内创建 clientset。
// 运行在集群外时请改用 kubeconfig。
func Example_inClusterConfiguration() {
	// 本例演示为运行在 Kubernetes 集群内的应用创建 clientset，使用 Pod 的
	// ServiceAccount 完成认证。

	// rest.InClusterConfig() 返回的配置对象可直接用于创建 clientset，是集群内应用的
	// 推荐配置方式。
	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 创建 clientset。
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 使用 clientset 与 API 交互。

	pods, err := clientset.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	fmt.Printf("There are %d pods in the default namespace\n", len(pods.Items))
}

// Example_outOfClusterConfiguration 演示通过 kubeconfig 在集群外创建 clientset，
// 这是本地开发和命令行工具的常见方式。
func Example_outOfClusterConfiguration() {
	// 本例演示通过 kubeconfig 为集群外应用创建 clientset，这是本地开发与命令行
	// 工具的标准做法。

	// kubeconfig 默认位于用户主目录。
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	// 从 kubeconfig 文件创建客户端配置。
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 配置客户端限流。
	config.QPS = 50
	config.Burst = 100

	// clientset 包含集群支持的全部 API 组与版本的客户端。
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 使用 clientset 与 API 交互。

	pods, err := clientset.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	fmt.Printf("There are %d pods in the default namespace\n", len(pods.Items))
}

// Example_handlingAPIErrors 演示如何用 apimachinery 的 errors 包区分常见 API 错误。
// 这里手工构造 NotFound，避免依赖真实集群。
func Example_handlingAPIErrors() {
	// 本例演示常见 API 错误的处理方式。先构造一个 NotFound 错误模拟请求失败。
	err := errors.NewNotFound(schema.GroupResource{Group: "v1", Resource: "pods"}, "my-pod")

	// errors.IsNotFound() 用于判断错误是否为 NotFound。
	if errors.IsNotFound(err) {
		fmt.Println("Pod not found")
	}

	// errors 包还提供其他常见 API 错误的判断函数，例如：
	// - errors.IsAlreadyExists(err)
	// - errors.IsConflict(err)
	// - errors.IsServerTimeout(err)

	// Output:
	// Pod not found
}

// Example_usingDynamicClient 演示用 dynamic client 访问没有 Go 类型定义的资源
// （典型场景是 Custom Resource）。
func Example_usingDynamicClient() {
	// 本例演示创建并使用动态客户端操作自定义资源等没有 Go 类型定义的对象。

	// 配置客户端（本例为集群外场景）。
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 创建动态客户端。
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 定义要访问对象的 GroupVersionResource；Pod 对应
	// {Group: "", Version: "v1", Resource: "pods"}。
	// 核心组的 Group 为空字符串，资源名使用复数小写形式。
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	// 用动态客户端列出 default 命名空间下的所有 Pod，结果是 UnstructuredList。
	list, err := dynamicClient.Resource(gvr).Namespace("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 遍历列表并打印每个 Pod 的名称。
	for _, item := range list.Items {
		name, found, err := unstructured.NestedString(item.Object, "metadata", "name")
		if err != nil || !found {
			fmt.Printf("Could not find name for pod: %v\n", err)
			continue
		}
		fmt.Printf("Pod Name: %s\n", name)
	}
}

// Example_usingInformers 演示 informer 的基本用法：共享缓存、事件回调、
// 双 channel 优雅退出以及等待首次同步完成。
func Example_usingInformers() {
	// 本例演示用 Informer 监听 Pod 变化的基本模式；仅作概念演示，真实控制器还需
	// 更健壮的逻辑和 workqueue。

	// 配置客户端（本例为集群外场景）。
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// SharedInformerFactory 为多个 informer 提供共享缓存，减少内存与网络开销。
	// 第二个参数是 resync 周期：即使对象没有变化，也会按该周期重放一次 Update 事件。
	factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)
	podInformer := factory.Core().V1().Pods().Informer()

	_, err = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				log.Printf("Pod ADDED: %s", key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				log.Printf("Pod UPDATED: %s", key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				log.Printf("Pod DELETED: %s", key)
			}
		},
	})
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 优雅退出需要两个 channel 配合。
	//
	// `sigCh` 供 `signal` 包传递操作系统信号（如 Ctrl+C），类型必须是 `chan os.Signal`。
	//
	// `stopCh` 用于通知 InformerFactory 停止：`Start` 方法接受 `<-chan struct{}`，
	// 该 channel 关闭即停止。
	//
	// 下面的 goroutine 充当“转换器”：在 `sigCh` 上收到信号后关闭 `stopCh`，
	// 从而让 InformerFactory 退出。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	stopCh := make(chan struct{})
	go func() {
		<-sigCh
		close(stopCh)
	}()

	// 启动 Informer。
	factory.Start(stopCh)

	// 等待首次缓存同步完成。
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
		log.Println("Timed out waiting for caches to sync")
		return
	}

	log.Println("Informer has synced. Watching for Pod events...")

	// 等待停止信号。
	<-stopCh
	log.Println("Shutting down...")
}

// Example_serverSideApply 演示用 applyconfiguration 构造声明式对象并执行
// Server-Side Apply（SSA），这是控制器和 operator 的推荐写法。
func Example_serverSideApply() {
	// 本例演示用 Server-Side Apply 声明式管理 Deployment，这是控制器和 Operator
	// 管理对象的推荐方式。

	// 配置客户端（本例为集群外场景）。
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// 用 applyconfigurations 包定义 Deployment 的期望状态，以类型化的方式构造 patch。
	deploymentName := "my-app"
	replicas := int32(2)
	image := "nginx:1.14.2"

	// FieldManager 是必填字段，用于标识管理该对象状态的控制器。
	// 不同 FieldManager 各自拥有自己写入的字段，字段冲突会在 Apply 时被服务端拒绝。
	fieldManager := "my-controller"

	// 构造 apply 配置。
	deploymentApplyConfig := appsv1ac.Deployment(deploymentName, "default").
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(replicas).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(map[string]string{"app": "my-app"})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(map[string]string{"app": "my-app"}).
				WithSpec(corev1ac.PodSpec().
					WithContainers(corev1ac.Container().
						WithName("nginx").
						WithImage(image),
					),
				),
			),
		)

	// 执行 Server-Side Apply。PatchType 必须是 types.ApplyPatchType，
	// context、名称、apply 配置及 patch 选项均为必填。
	result, err := clientset.AppsV1().Deployments("default").Apply(
		context.TODO(),
		deploymentApplyConfig,
		metav1.ApplyOptions{FieldManager: fieldManager},
	)

	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	fmt.Printf("Deployment %q applied successfully.\n", result.Name)
}

// Example_leaderElection 演示副本间的主备选举：只有拿到 Lease 的副本执行实际工作，
// 失去租约时通过回调停止工作。
func Example_leaderElection() {
	// 本例演示 leader election：多副本控制器借此保证同一时刻只有一个副本处于活跃状态。

	// 配置客户端（本例为集群外场景）。
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error building kubeconfig: %s\n", err.Error())
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error building clientset: %s\n", err.Error())
		return
	}

	// 写入 Lease 对象的控制器名称，必须唯一。
	id := "my-controller"

	// Lease 对象所在的命名空间与名称。
	leaseNamespace := "default"
	leaseName := "my-controller-lease"

	// 创建可取消的 context，用于停止 leader election。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置信号处理，在进程退出时取消 context。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received termination signal, shutting down...")
		cancel()
	}()

	// 创建锁对象。
	lock, err := resourcelock.New(resourcelock.LeasesResourceLock,
		leaseNamespace,
		leaseName,
		clientset.CoreV1(),
		clientset.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity: id,
		})
	if err != nil {
		fmt.Printf("Error creating lock: %v\n", err)
		return
	}

	// 创建 leader elector。
	// 三个时间参数必须满足 LeaseDuration > RenewDeadline > RetryPeriod*1.2，
	// 否则 NewLeaderElector 会直接返回错误。
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// 控制器当选 leader 时调用，应在此启动控制器主逻辑。
				log.Println("Became leader, starting controller.")
				// 这里仅用占位实现代表控制器的工作。
				for {
					select {
					case <-time.After(5 * time.Second):
						log.Println("Doing controller work...")
					case <-ctx.Done():
						log.Println("Controller stopped.")
						return
					}
				}
			},
			OnStoppedLeading: func() {
				// 控制器失去 leader 身份时调用，应停止正在执行的工作并优雅退出。
				log.Printf("Lost leadership, shutting down.")
			},
			OnNewLeader: func(identity string) {
				// 新的 leader 当选时调用。
				if identity != id {
					log.Printf("New leader elected: %s", identity)
				}
			},
		},
	})
	if err != nil {
		fmt.Printf("Error creating leader elector: %v\n", err)
		return
	}

	// 启动 leader election 循环。
	elector.Run(ctx)
}
