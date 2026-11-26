/*
Copyright 2020 The Kubernetes Authors.

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

package transport

import (
	"bytes"
	"crypto/tls"
	"reflect"
	"sync"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/connrotation"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const workItemKey = "key"

// 轮询证书回调的周期；导出以便集成测试调小等待时间。
var CertCallbackRefreshDuration = 5 * time.Minute

// reloadFunc 由 GetClientCertificate 调用，返回最新的客户端证书。
type reloadFunc func(*tls.CertificateRequestInfo) (*tls.Certificate, error)

// dynamicClientCert 实现 TLS 客户端证书的周期性重载，并在证书变化时关闭旧连接以促成重新握手。
type dynamicClientCert struct {
	logger     klog.Logger
	clientCert *tls.Certificate
	certMtx    sync.RWMutex

	reload     reloadFunc
	connDialer *connrotation.Dialer

	// 队列中始终只有一项；复用它是为了获得现成的限速退避重试语义。
	queue workqueue.TypedRateLimitingInterface[string]
}

// certRotatingDialer 构造动态证书控制器，并包装 dial 以支持在证书轮换时关闭所有连接。
func certRotatingDialer(logger klog.Logger, reload reloadFunc, dial utilnet.DialFunc) *dynamicClientCert {
	d := &dynamicClientCert{
		logger:     logger,
		reload:     reload,
		connDialer: connrotation.NewDialer(connrotation.DialFunc(dial)),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "DynamicClientCertificate"},
		),
	}

	return d
}

// loadClientCert 调用回调获取证书，必要时轮换连接。
func (c *dynamicClientCert) loadClientCert() (*tls.Certificate, error) {
	cert, err := c.reload(nil)
	if err != nil {
		return nil, err
	}

	// 检查证书是否变化；相同则不做任何事。
	c.certMtx.RLock()
	haveCert := c.clientCert != nil
	if certsEqual(c.clientCert, cert) {
		c.certMtx.RUnlock()
		return c.clientCert, nil
	}
	c.certMtx.RUnlock()

	c.certMtx.Lock()
	c.clientCert = cert
	c.certMtx.Unlock()

	// 首次获取证书时没有旧连接需要关闭，避免无谓地打断启动阶段的连接。
	if !haveCert {
		return cert, nil
	}

	c.logger.V(1).Info("Certificate rotation detected, shutting down client connections to start using new credentials")
	// 仅替换证书不够：已有连接仍使用旧证书，必须关闭以触发新连接在握手时使用新证书。
	c.connDialer.CloseAll()

	return cert, nil
}

// certsEqual 比较 tls.Certificate，忽略可能被动态填充的 Leaf 字段。
func certsEqual(left, right *tls.Certificate) bool {
	if left == nil || right == nil {
		return left == right
	}

	if !byteMatrixEqual(left.Certificate, right.Certificate) {
		return false
	}

	// 用 DeepEqual 比较私钥：私钥可能是 *rsa.PrivateKey 等含内部状态的类型，不宜逐字节比较。
	if !reflect.DeepEqual(left.PrivateKey, right.PrivateKey) {
		return false
	}

	if !byteMatrixEqual(left.SignedCertificateTimestamps, right.SignedCertificateTimestamps) {
		return false
	}

	if !bytes.Equal(left.OCSPStaple, right.OCSPStaple) {
		return false
	}

	return true
}

// byteMatrixEqual 比较二维字节切片，长度或任一元素不同即视为不同。
func byteMatrixEqual(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

// run 启动控制器并阻塞，直到 stopCh 关闭。
func (c *dynamicClientCert) run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrashWithLogger(c.logger)
	defer c.queue.ShutDown()

	c.logger.V(3).Info("Starting client certificate rotation controller")
	defer c.logger.V(3).Info("Shutting down client certificate rotation controller")

	// worker 从队列取出重载任务；轮询器周期性投递任务。
	go wait.Until(c.runWorker, time.Second, stopCh)

	go wait.PollImmediateUntil(CertCallbackRefreshDuration, func() (bool, error) {
		c.queue.Add(workItemKey)
		return false, nil
	}, stopCh)

	<-stopCh
}

// runWorker 持续处理队列中的重载任务，直到队列被关闭。
func (c *dynamicClientCert) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 取出一个重载任务；成功则忘记重试计数，失败则按限速策略重新入队。
func (c *dynamicClientCert) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	_, err := c.loadClientCert()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleErrorWithLogger(c.logger, err, "Loading client cert failed", "key", dsKey)
	c.queue.AddRateLimited(dsKey)

	return true
}

// GetClientCertificate 满足 tls.Config.GetClientCertificate 回调签名：每次握手时按需重载证书。
// 这样即使后台轮询尚未触发，新建连接也能拿到最新证书。
func (c *dynamicClientCert) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return c.loadClientCert()
}
