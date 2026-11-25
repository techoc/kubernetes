/*
Copyright 2023 The Kubernetes Authors.

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
	"syscall"
	"time"

	"k8s.io/kms/pkg/service"
	"k8s.io/kms/pkg/util"
	"k8s.io/kms/plugins/mock/pkcs11"
)

// 命令行参数：监听地址、gRPC 超时与 SoftHSM 配置文件路径。
var (
	listenAddr     = flag.String("listen-addr", "unix:///tmp/kms.socket", "gRPC listen address")
	timeout        = flag.Duration("timeout", 5*time.Second, "gRPC timeout")
	configFilePath = flag.String("config-file-path", "/etc/softhsm-config.json", "SoftHSM config file path")
)

// main 是 mock KMS 插件的入口：解析端点、创建基于 SoftHSM 的后端服务，
// 启动 gRPC 服务，并在收到终止信号后优雅关闭。
func main() {
	flag.Parse()

	// 把 unix:// 形式的端点解析为 socket 路径。
	addr, err := util.ParseEndpoint(*listenAddr)
	if err != nil {
		panic("failed to parse endpoint: " + err.Error())
	}

	// 用配置文件创建基于 PKCS#11（SoftHSM）的远程 KMS 实现。
	remoteKMSService, err := pkcs11.NewPKCS11RemoteService(*configFilePath, "kms-test")
	if err != nil {
		panic("failed to create remote service: " + err.Error())
	}

	// 创建一个能响应终止信号的 context，用于控制服务生命周期。
	ctx := withShutdownSignal(context.Background())
	grpcService := service.NewGRPCService(
		addr,
		*timeout,
		remoteKMSService,
	)

	// gRPC 服务阻塞运行，放到 goroutine 中。
	go func() {
		if err := grpcService.ListenAndServe(); err != nil {
			panic("failed to serve: " + err.Error())
		}
	}()

	// 等待终止信号，然后优雅关闭服务。
	<-ctx.Done()
	grpcService.Shutdown()
}

// withShutdownSignal 返回派生自父上下文的新上下文；进程收到终止信号时取消该上下文。
func withShutdownSignal(ctx context.Context) context.Context {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)

	nctx, cancel := context.WithCancel(ctx)

	go func() {
		<-signalChan
		cancel()
	}()
	return nctx
}
