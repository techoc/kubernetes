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

package services

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
)

// terminationSignals 是在支持的平台（linux、darwin、windows）上
// 会导致程序退出的信号。
var terminationSignals = []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// waitForTerminationSignal 等待终止信号。
func waitForTerminationSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, terminationSignals...)
	<-sig
}

// readinessCheck 通过提供的健康检查 URL 检查服务是否就绪。
// 一旦 errCh 中出现错误，函数停止等待
// 并返回该错误。
func readinessCheck(name string, urls []string, errCh <-chan error) error {
	klog.Infof("Running readiness check for service %q", name)

	endTime := time.Now().Add(*serverStartTimeout)
	blockCh := make(chan error)
	defer close(blockCh)
	for endTime.After(time.Now()) {
		select {
		// 只要 channel 上没有错误，我们*总是*要运行健康检查。
		// 使用 systemd 时，从 errCh 读取会返回 nil，因为 cmd.Run()
		// 等待的是 systemd-run，而不是服务进程。systemd-run 很快
		// 以状态 0 退出，导致 channel 无错误地关闭。这种情况下
		// 应等待健康检查完成，而不是 channel 一关闭
		// 就从 readinessCheck 返回。
		case err, ok := <-errCh:
			if ok { // The channel is not closed, this is a real error
				if err != nil { // If there is an error, return it
					return err
				}
				// 如果尚未就绪，继续检查。
			} else { // The channel is closed, this is only a zero value.
				// 把 errCh 换成 blockCh，避免忙循环，
				// 并继续检查就绪状态。
				errCh = blockCh
			}
		case <-time.After(time.Second):
			ready := true
			for _, url := range urls {
				if !e2enode.HealthCheck(url) {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
	}
	return fmt.Errorf("e2e service %q readiness check timeout %v", name, *serverStartTimeout)
}
