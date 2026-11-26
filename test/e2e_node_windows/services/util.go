//go:build windows

/*
Copyright The Kubernetes Authors.

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
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
)

// 在支持的平台（linux、darwin、windows）上使程序退出的信号。
// terminationSignals 是这些信号的集合
var terminationSignals = []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// waitForTerminationSignal 等待终止信号。
func waitForTerminationSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, terminationSignals...)
	<-sig
}

// readinessCheck 通过给定的健康检查 URL 校验服务是否就绪。
// errCh 一旦出现错误，函数就停止等待
// 并返回该错误。
func readinessCheck(name string, urls []string, errCh <-chan error) error {
	klog.Infof("Running readiness check for service %q", name)

	insecureTransport := http.DefaultTransport.(*http.Transport).Clone()
	insecureTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	insecureHTTPClient := &http.Client{
		Transport: insecureTransport,
	}

	endTime := time.Now().Add(*serverStartTimeout)
	blockCh := make(chan error)
	defer close(blockCh)
	for endTime.After(time.Now()) {
		select {
		// channel 上没有错误时，我们*总是*要执行健康检查。
		// systemd 场景下，从 errCh 读到 nil，因为 cmd.Run() 等待的是
		// systemd-run，而不是服务进程。systemd-run 很快以状态 0
		// 退出，导致 channel 无错误关闭。此时应等待健康检查完成，
		// 而不是 channel 一关闭就从 readinessCheck 返回。
		//
		case err, ok := <-errCh:
			if ok { // channel 未关闭，这是真实错误
				if err != nil { // 有错误则返回
					return err
				}
				// 否则继续检查就绪状态。
			} else { // channel 已关闭，这只是零值。
				// 把 errCh 换成 blockCh，避免忙循环，
				// 并继续检查就绪状态。
				errCh = blockCh
			}
		case <-time.After(time.Second):
			ready := true
			for _, url := range urls {
				if !healthCheck(insecureHTTPClient, url) {
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

// 执行健康检查。非 200 响应一律视为失败。
// 只返回不可恢复的错误。
func healthCheck(client *http.Client, url string) bool {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", framework.TestContext.BearerToken))
	resp, err := client.Do(req)
	if err != nil {
		klog.Warningf("Health check on %q failed, error=%v", url, err)
	} else if resp.StatusCode != http.StatusOK {
		klog.Warningf("Health check on %q failed, status=%d", url, resp.StatusCode)
	}
	return err == nil && resp.StatusCode == http.StatusOK
}
