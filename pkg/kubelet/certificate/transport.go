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

package certificate

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/connrotation"
)

// UpdateTransport 为 restconfig 注入一个动态使用 manager 所提供证书进行 TLS 客户端认证的 transport。
// 配置中不能已经提供显式 transport。
// 返回的函数可强制关闭所有活动连接。
//
// 返回的 transport 会周期性检查 manager 以判断证书是否变化；若变化则关闭所有现有客户端连接，
// 强制客户端与服务器重新握手并使用新证书。
//
// 若设置了 exitAfter，当证书不可用（磁盘上被删除或损坏）或证书已过期且服务器可达时，
// 会终止当前进程，让父进程或引导凭据有机会获取新的初始证书。
//
// stopCh 用于表示 transport 不再使用、无需继续检查 manager。
func UpdateTransport(logger klog.Logger, stopCh <-chan struct{}, clientConfig *restclient.Config, clientCertificateManager certificate.Manager, exitAfter time.Duration) (func(), error) {
	return updateTransport(logger, stopCh, 10*time.Second, clientConfig, clientCertificateManager, exitAfter)
}

// updateTransport 是内部方法，暴露了检查客户端证书变化的周期。
func updateTransport(logger klog.Logger, stopCh <-chan struct{}, period time.Duration, clientConfig *restclient.Config, clientCertificateManager certificate.Manager, exitAfter time.Duration) (func(), error) {
	// 已有 transport 或 dialer 时拒绝覆盖，避免配置冲突。
	if clientConfig.Transport != nil || clientConfig.Dial != nil {
		return nil, fmt.Errorf("there is already a transport or dialer configured")
	}

	// 可轮转的 dialer 能在证书更新时统一关闭旧连接。
	d := connrotation.NewDialer((&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext)

	// 有证书管理器时启用证书轮转；否则只设置 dialer。
	if clientCertificateManager != nil {
		if err := addCertRotation(logger, stopCh, period, clientConfig, clientCertificateManager, exitAfter, d); err != nil {
			return nil, err
		}
	} else {
		clientConfig.Dial = d.DialContext
	}

	return d.CloseAll, nil
}

// addCertRotation 配置 TLS 客户端证书回调，并在证书变化时关闭所有连接。
func addCertRotation(logger klog.Logger, stopCh <-chan struct{}, period time.Duration, clientConfig *restclient.Config, clientCertificateManager certificate.Manager, exitAfter time.Duration, d *connrotation.Dialer) error {
	tlsConfig, err := restclient.TLSConfigFor(clientConfig)
	if err != nil {
		return fmt.Errorf("unable to configure TLS for the rest client: %v", err)
	}
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}

	// 清空静态证书，强制使用动态回调。
	tlsConfig.Certificates = nil
	// 每次握手都从 manager 读取最新证书。
	tlsConfig.GetClientCertificate = func(requestInfo *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		cert := clientCertificateManager.Current()
		if cert == nil {
			return &tls.Certificate{Certificate: nil}, nil
		}
		return cert, nil
	}

	// 记录最后一次看到有效证书的时间，用于 exitAfter 判断。
	lastCertAvailable := time.Now()
	lastCert := clientCertificateManager.Current()

	var hasCert atomic.Bool
	hasCert.Store(lastCert != nil)

	// 检查逻辑可能被多个 goroutine 触发，用互斥锁串行化。
	checkLock := &sync.Mutex{}
	checkNewCertificateAndRotate := func() {
		// don't run concurrently
		checkLock.Lock()
		defer checkLock.Unlock()

		curr := clientCertificateManager.Current()

		// 配置了退出期限时，检测证书缺失或过期并按服务器可达性决定是否退出。
		if exitAfter > 0 {
			now := time.Now()
			// 证书缺失：超过期限且服务器可达时退出，让上层重新引导。
			if curr == nil {
				// the certificate has been deleted from disk or is otherwise corrupt
				if now.After(lastCertAvailable.Add(exitAfter)) {
					if clientCertificateManager.ServerHealthy() {
						logger.Error(nil, "No valid client certificate is found and the server is responsive, exiting.", "lastCertificateAvailabilityTime", lastCertAvailable, "shutdownThreshold", exitAfter)
						os.Exit(1)
					} else {
						logger.Error(nil, "No valid client certificate is found but the server is not responsive. A restart may be necessary to retrieve new initial credentials.", "lastCertificateAvailabilityTime", lastCertAvailable, "shutdownThreshold", exitAfter)
					}
				}
			} else {
				// the certificate is expired
				// 证书已过期：服务器可达时退出。
				if now.After(curr.Leaf.NotAfter) {
					if clientCertificateManager.ServerHealthy() {
						logger.Error(nil, "The currently active client certificate has expired and the server is responsive, exiting.")
						os.Exit(1)
					} else {
						logger.Error(nil, "The currently active client certificate has expired, but the server is not responsive. A restart may be necessary to retrieve new initial credentials.")
					}
				}
				lastCertAvailable = now
			}
		}

		// 证书未变化，无需处理。
		if curr == nil || lastCert == curr {
			// Cert hasn't been rotated.
			return
		}
		lastCert = curr
		hasCert.Store(lastCert != nil)

		logger.Info("Certificate rotation detected, shutting down client connections to start using new credentials")
		// 证书已轮转，关闭所有现有连接，强制客户端用新证书重新握手；
		// 参见 https://github.com/kubernetes-incubator/bootkube/pull/663#issuecomment-318506493。
		d.CloseAll()
	}

	// start long-term check
	// 周期性检查证书变化，直到 stopCh 关闭。
	go wait.Until(checkNewCertificateAndRotate, period, stopCh)

	if !hasCert.Load() {
		// 尚未拿到初始证书时，以 1 秒间隔快速轮询。
		go wait.PollUntil(time.Second, func() (bool, error) {
			checkNewCertificateAndRotate()
			return hasCert.Load(), nil
		}, stopCh)
	}

	clientConfig.Transport = utilnet.SetTransportDefaults(&http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
		MaxIdleConnsPerHost: 25,
		DialContext:         d.DialContext,
	})

	// 清空已有的 TLS 配置，统一由新 transport 强制执行。
	clientConfig.CertData = nil
	clientConfig.KeyData = nil
	clientConfig.CertFile = ""
	clientConfig.KeyFile = ""
	clientConfig.CAData = nil
	clientConfig.CAFile = ""
	clientConfig.Insecure = false
	clientConfig.NextProtos = nil

	return nil
}
