/*
Copyright 2018 The Kubernetes Authors.

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

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// buildConfig 优先使用显式 kubeconfig，否则回退到集群内配置。
// 两种来源的取舍决定了示例既能在本地运行，也能作为 Pod 部署。
func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var leaseLockName string
	var leaseLockNamespace string
	var id string
	// startedLeading 区分“真正当过 leader”与“从未当选”，OnStoppedLeading 依赖它决定是否清理。
	var startedLeading atomic.Bool

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&id, "id", uuid.New().String(), "the holder identity name")
	flag.StringVar(&leaseLockName, "lease-lock-name", "", "the lease lock resource name")
	flag.StringVar(&leaseLockNamespace, "lease-lock-namespace", "", "the lease lock resource namespace")
	flag.Parse()

	// 更完整的示例可能允许配置不同日志后端；这里直接使用 klog 默认值。
	logger := klog.Background()

	if leaseLockName == "" {
		logger.Error(nil, "Unable to get lease lock resource name (missing lease-lock-name flag)")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	if leaseLockNamespace == "" {
		logger.Error(nil, "Unable to get lease lock resource namespace (missing lease-lock-namespace flag)")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	// Leader election 通过写锁对象使用 Kubernetes API。锁可以是 LeaseLock（推荐）、
	// ConfigMap 或已弃用的 Endpoints。冲突写入会被检测到，各客户端自行处理。
	config, err := buildConfig(kubeconfig)
	if err != nil {
		logger.Error(err, "Building Kubernetes client configuration failed")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	client := clientset.NewForConfigOrDie(config)

	run := func(ctx context.Context) {
		// 示例用空 select 永久阻塞，真实控制器应在此运行 reconcile 循环。
		logger.Info("Controller loop...")

		select {}
	}

	// 用 Go context 通知 leader election 何时让出领导权。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听中断或 SIGTERM 信号并取消 context，leader election 检测到后会退出。
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		logger.Info("Received termination, signaling shutdown")
		cancel()
	}()

	// 选用 Lease 锁：Lease 写入频率较低，且集群中监听「所有 Lease」的对象更少。
	//
	// Identity 必须全局唯一，否则多个副本会互相覆盖租约记录。
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: leaseLockNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// 启动 leader election 循环。
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// 重要：受租约保护的代码必须在调用 cancel 之前**全部结束**，否则后台循环可能
		// 仍在运行时另一个进程已当选，违背租约要保证的互斥目标。
		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second,
		RenewDeadline:   15 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// 当选时收到通知，通常在这里启动控制器逻辑。
				startedLeading.Store(true)
				run(ctx)
			},
			OnStoppedLeading: func() {
				// 可在此清理资源，但该回调在 LeaderElector 退出时总会触发，即使从未当选。
				// 因此先确认确实当过 leader，避免误清理。
				logger.Info("Leader lost", "identity", id)

				// 示例：仅在确实当选过时才清理。
				if startedLeading.Load() {
					// 在这里执行清理，例如释放资源、关闭连接等。
					logger.Info("Performing cleanup operations...")
				} else {
					logger.Info("Skipping cleanup because we never started leading")
				}
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				// 新 leader 当选时收到通知。
				if identity == id {
					// 我刚拿到锁。
					return
				}
				logger.Info("New leader elected", "identity", identity)
			},
		},
	})
}
