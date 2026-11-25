/*
Copyright 2017 The Kubernetes Authors.

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

// 本文件是 sample-controller 的进程入口：装配客户端与 informer，然后把控制权交给 Controller.Run。
// main 只处理“启动前”的失败，运行期的收敛逻辑全在 controller.go 里。

package main

import (
	"flag"
	"time"

	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/sample-controller/pkg/signals"
	// 取消下一行注释以加载 GCP 插件（仅对 GKE 集群认证需要）。
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	informers "k8s.io/sample-controller/pkg/generated/informers/externalversions"
)

var (
	// 命令行参数先收在包级变量里，由 init 注册标志、main 中统一读取。
	// --master 覆盖 kubeconfig 里的 server 地址，无需改动配置文件。
	// --kubeconfig 只在集群外运行时需要；集群内走 service account 凭据。
	masterURL  string
	kubeconfig string
)

func main() {
	// 先注册 klog 自己的标志再 Parse，否则 --v 等参数不会被识别。
	klog.InitFlags(nil)
	flag.Parse()

	// ctx 贯穿整个程序，SIGTERM/SIGINT 一到就能让 informer 和 worker 一起收尾。
	ctx := signals.SetupSignalHandler()
	logger := klog.FromContext(ctx)

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		// 配置都建不起来，后续的 client 和 informer 无从谈起，直接退出。
		logger.Error(err, "Error building kubeconfig")
		// 用 FlushAndExit 而不是 os.Exit，保证日志不会在退出时丢缓冲区里的内容。
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "Error building kubernetes clientset")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	exampleClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		// 自定义资源的 client 失败也要立即停：缺了它控制器无法读写 Foo。
		logger.Error(err, "Error building kubernetes clientset")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	// 30 秒 resync 作为兜底：即使漏了某个事件，也能靠周期性全量通知重新收敛。
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	exampleInformerFactory := informers.NewSharedInformerFactory(exampleClient, time.Second*30)

	// 两个 informer factory 必须都传给控制器，才能同时监听 Deployment 和 Foo。
	controller := NewController(ctx, kubeClient, exampleClient,
		kubeInformerFactory.Apps().V1().Deployments(),
		exampleInformerFactory.Samplecontroller().V1alpha1().Foos())

	// Start 非阻塞，并在专用 goroutine 中运行已注册的 informer，无须再包一层 goroutine。
	// 先 Start 再 Run：Run 内部会等缓存同步，若 informer 没启动就会一直阻塞在等待上。
	kubeInformerFactory.Start(ctx.Done())
	exampleInformerFactory.Start(ctx.Done())

	// 两个 worker 仅为示例；真实控制器可按负载调整，workqueue 会保证任务不重复处理。
	if err = controller.Run(ctx, 2); err != nil {
		logger.Error(err, "Error running controller")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
}

func init() {
	// init 里注册标志，确保 flag.Parse 之前完成绑定。
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
}
