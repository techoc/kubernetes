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
	// This example demonstrates how to create a clientset for an application
	// running inside a Kubernetes cluster. It uses the pod's service account
	// for authentication.

	// rest.InClusterConfig() returns a configuration object that can be used to
	// create a clientset. It is the recommended way to configure a client for
	// in-cluster applications.
	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Create the clientset.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Use the clientset to interact with the API.

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
	// This example demonstrates how to create a clientset for an application
	// running outside of a Kubernetes cluster, using a kubeconfig file. This is
	// the standard approach for local development and command-line tools.

	// The default location for the kubeconfig file is in the user's home directory.
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	// Create the client configuration from the kubeconfig file.
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Configure client-side rate limiting.
	config.QPS = 50
	config.Burst = 100

	// A clientset contains clients for all the API groups and versions supported
	// by the cluster.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Use the clientset to interact with the API.

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
	// This example demonstrates how to handle common API errors.
	// Create a NotFound error to simulate a failed API request.
	err := errors.NewNotFound(schema.GroupResource{Group: "v1", Resource: "pods"}, "my-pod")

	// The errors.IsNotFound() function checks if an error is a NotFound error.
	if errors.IsNotFound(err) {
		fmt.Println("Pod not found")
	}

	// The errors package provides functions for other common API errors, such as:
	// - errors.IsAlreadyExists(err)
	// - errors.IsConflict(err)
	// - errors.IsServerTimeout(err)

	// Output:
	// Pod not found
}

// Example_usingDynamicClient 演示用 dynamic client 访问没有 Go 类型定义的资源
// （典型场景是 Custom Resource）。
func Example_usingDynamicClient() {
	// This example demonstrates how to create and use a dynamic client to work
	// with Custom Resources or other objects without needing their Go type
	// definitions.

	// Configure the client (out-of-cluster for this example).
	var kubeconfig string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Create a dynamic client.
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Define the GroupVersionResource for the object you want to access.
	// For Pods, this is {Group: "", Version: "v1", Resource: "pods"}.
	// 核心组的 Group 为空字符串，资源名使用复数小写形式。
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	// Use the dynamic client to list all pods in the "default" namespace.
	// The result is an UnstructuredList.
	list, err := dynamicClient.Resource(gvr).Namespace("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error encountered: %v\n", err)
		return
	}

	// Iterate over the list and print the name of each pod.
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
	// This example demonstrates the basic pattern for using an informer to watch
	// for changes to Pods. This is a conceptual example; a real controller would
	// have more robust logic and a workqueue.

	// Configure the client (out-of-cluster for this example).
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

	// A SharedInformerFactory provides a shared cache for multiple informers,
	// which reduces memory and network overhead.
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

	// Graceful shutdown requires a two-channel pattern.
	//
	// The first channel, `sigCh`, is used by the `signal` package to send us
	// OS signals (e.g., Ctrl+C). This channel must be of type `chan os.Signal`.
	//
	// The second channel, `stopCh`, is used to tell the informer factory to
	// stop. The informer factory's `Start` method expects a channel of type
	// `<-chan struct{}`. It will stop when this channel is closed.
	//
	// The goroutine below is the "translator" that connects these two channels.
	// It waits for a signal on `sigCh`, and when it receives one, it closes
	// `stopCh`, which in turn tells the informer factory to shut down.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	stopCh := make(chan struct{})
	go func() {
		<-sigCh
		close(stopCh)
	}()

	// Start the informer.
	factory.Start(stopCh)

	// Wait for the initial cache sync.
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
		log.Println("Timed out waiting for caches to sync")
		return
	}

	log.Println("Informer has synced. Watching for Pod events...")

	// Wait for the stop signal.
	<-stopCh
	log.Println("Shutting down...")
}

// Example_serverSideApply 演示用 applyconfiguration 构造声明式对象并执行
// Server-Side Apply（SSA），这是控制器和 operator 的推荐写法。
func Example_serverSideApply() {
	// This example demonstrates how to use Server-Side Apply to declaratively
	// manage a Deployment object. Server-Side Apply is the recommended approach
	// for controllers and operators to manage objects.

	// Configure the client (out-of-cluster for this example).
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

	// Define the desired state of the Deployment using the applyconfigurations package.
	// This provides a typed, structured way to build the patch.
	deploymentName := "my-app"
	replicas := int32(2)
	image := "nginx:1.14.2"

	// The FieldManager is a required field that identifies the controller managing
	// this object's state.
	// 不同 FieldManager 各自拥有自己写入的字段，字段冲突会在 Apply 时被服务端拒绝。
	fieldManager := "my-controller"

	// Build the apply configuration.
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

	// Perform the Server-Side Apply patch. The PatchType must be types.ApplyPatchType.
	// The context, name, apply configuration, and patch options are required.
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
	// This example demonstrates the leader election pattern. A controller running
	// multiple replicas uses leader election to ensure that only one replica is
	// active at a time.

	// Configure the client (out-of-cluster for this example).
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

	// The unique name of the controller writing to the Lease object.
	id := "my-controller"

	// The namespace and name of the Lease object.
	leaseNamespace := "default"
	leaseName := "my-controller-lease"

	// Create a context that can be cancelled to stop the leader election.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a signal handler to cancel the context on termination.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received termination signal, shutting down...")
		cancel()
	}()

	// Create the lock object.
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

	// Create the leader elector.
	// 三个时间参数必须满足 LeaseDuration > RenewDeadline > RetryPeriod*1.2，
	// 否则 NewLeaderElector 会直接返回错误。
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// This function is called when the controller becomes the leader.
				// You would start your controller's main logic here.
				log.Println("Became leader, starting controller.")
				// This is a simple placeholder for the controller's work.
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
				// This function is called when the controller loses leadership.
				// You should stop any active work and gracefully shut down.
				log.Printf("Lost leadership, shutting down.")
			},
			OnNewLeader: func(identity string) {
				// This function is called when a new leader is elected.
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

	// Start the leader election loop.
	elector.Run(ctx)
}
