/*
Copyright 2019 The Kubernetes Authors.

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

package dynamiccertificates

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const workItemKey = "key"

// DynamicServingCertificateController dynamically loads certificates and provides a golang tls compatible dynamic GetCertificate func.
//
// 控制器把“静态 tls.Config 模板”与“动态证书内容”分离：baseTLSConfig 只复制一次，
// 每次内容变化时在副本上重建 Certificates/ClientCAs/NameToCertificate，再通过
// atomic.Value 原子替换，因而 TLS 握手无需加锁即可读到一致快照。
type DynamicServingCertificateController struct {
	// baseTLSConfig is the static portion of the tlsConfig for serving to clients.  It is copied and the copy is mutated
	// based on the dynamic cert state.
	baseTLSConfig *tls.Config

	// clientCA provides the very latest content of the ca bundle
	clientCA CAContentProvider
	// servingCert provides the very latest content of the default serving certificate
	servingCert CertKeyContentProvider
	// sniCerts are a list of CertKeyContentProvider with associated names used for SNI
	sniCerts []SNICertKeyContentProvider

	// currentlyServedContent holds the original bytes that we are serving. This is used to decide if we need to set a
	// new atomic value. The types used for efficient TLSConfig preclude using the processed value.
	currentlyServedContent *dynamicCertificateContent
	// currentServingTLSConfig holds a *tls.Config that will be used to serve requests
	currentServingTLSConfig atomic.Value

	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue         workqueue.TypedRateLimitingInterface[string]
	eventRecorder events.EventRecorder
}

var _ Listener = &DynamicServingCertificateController{}

// NewDynamicServingCertificateController returns a controller that can be used to keep a TLSConfig up to date.
//
// clientCA 可以为 nil（例如不需要客户端证书的服务端认证）；servingCert 与
// sniCerts 中的证书内容为空时，syncCerts 会报错而不是让服务端无证书可用。
func NewDynamicServingCertificateController(
	baseTLSConfig *tls.Config,
	clientCA CAContentProvider,
	servingCert CertKeyContentProvider,
	sniCerts []SNICertKeyContentProvider,
	eventRecorder events.EventRecorder,
) *DynamicServingCertificateController {
	c := &DynamicServingCertificateController{
		baseTLSConfig: baseTLSConfig,
		clientCA:      clientCA,
		servingCert:   servingCert,
		sniCerts:      sniCerts,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "DynamicServingCertificateController"},
		),
		eventRecorder: eventRecorder,
	}

	return c
}

// GetConfigForClient is an implementation of tls.Config.GetConfigForClient
//
// 每次握手都从 atomic.Value 取出当前配置的副本，保证多个连接拿到的是
// 同一份快照，而不是正在被重建的共享结构。
func (c *DynamicServingCertificateController) GetConfigForClient(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
	uncastObj := c.currentServingTLSConfig.Load()
	if uncastObj == nil {
		// 首次同步尚未完成；此时不能接受连接。
		return nil, errors.New("dynamiccertificates: configuration not ready")
	}
	tlsConfig, ok := uncastObj.(*tls.Config)
	if !ok {
		return nil, errors.New("dynamiccertificates: unexpected config type")
	}

	tlsConfigCopy := tlsConfig.Clone()

	// if the client set SNI information, just use our "normal" SNI flow
	if len(clientHello.ServerName) > 0 {
		return tlsConfigCopy, nil
	}

	// if the client didn't set SNI, then we need to inspect the requested IP so that we can choose
	// a certificate from our list if we specifically handle that IP.  This can happen when an IP is specifically mapped by name.
	// 未带 SNI 时无法按主机名选证书，只能退而根据本端 IP 查表；查不到就用默认证书。
	host, _, err := net.SplitHostPort(clientHello.Conn.LocalAddr().String())
	if err != nil {
		return tlsConfigCopy, nil
	}

	ipCert, ok := tlsConfigCopy.NameToCertificate[host]
	if !ok {
		return tlsConfigCopy, nil
	}
	tlsConfigCopy.Certificates = []tls.Certificate{*ipCert}
	tlsConfigCopy.NameToCertificate = nil

	return tlsConfigCopy, nil
}

// newTLSContent determines the next set of content for overriding the baseTLSConfig.
//
// 这里只收集原始 PEM 字节，不做解析；是否真正变化由 syncCerts 通过 Equal 判断，
// 避免内容未变时重复解析证书。
func (c *DynamicServingCertificateController) newTLSContent() (*dynamicCertificateContent, error) {
	newContent := &dynamicCertificateContent{}

	if c.clientCA != nil {
		currClientCABundle := c.clientCA.CurrentCABundleContent()
		// we allow removing all client ca bundles because the server is still secure when this happens. it just means
		// that there isn't a hint to clients about which client-cert to used.  this happens when there is no client-ca
		// yet known for authentication, which can happen in aggregated apiservers and some kube-apiserver deployment modes.
		newContent.clientCA = caBundleContent{caBundle: currClientCABundle}
	}

	if c.servingCert != nil {
		currServingCert, currServingKey := c.servingCert.CurrentCertKeyContent()
		if len(currServingCert) == 0 || len(currServingKey) == 0 {
			return nil, fmt.Errorf("not loading an empty serving certificate from %q", c.servingCert.Name())
		}

		newContent.servingCert = certKeyContent{cert: currServingCert, key: currServingKey}
	}

	for i, sniCert := range c.sniCerts {
		currCert, currKey := sniCert.CurrentCertKeyContent()
		if len(currCert) == 0 || len(currKey) == 0 {
			return nil, fmt.Errorf("not loading an empty SNI certificate from %d/%q", i, sniCert.Name())
		}

		newContent.sniCerts = append(newContent.sniCerts, sniCertKeyContent{certKeyContent: certKeyContent{cert: currCert, key: currKey}, sniNames: sniCert.SNINames()})
	}

	return newContent, nil
}

// syncCerts gets newTLSContent, if it has changed from the existing, the content is parsed and stored for usage in
// GetConfigForClient.
//
// 失败时保留旧配置继续服务，由调用方（worker）按限速队列重试，避免短暂的文件
// 读取失败直接中断 TLS 服务。
func (c *DynamicServingCertificateController) syncCerts() error {
	newContent, err := c.newTLSContent()
	if err != nil {
		return err
	}
	// if the content is the same as what we currently have, we can simply skip it.  This works because we are single
	// threaded.  If you ever make this multi-threaded, add a lock.
	if newContent.Equal(c.currentlyServedContent) {
		return nil
	}

	// make a shallow copy and override the dynamic pieces which have changed.
	newTLSConfigCopy := c.baseTLSConfig.Clone()

	// parse new content to add to TLSConfig
	if len(newContent.clientCA.caBundle) > 0 {
		newClientCAPool := x509.NewCertPool()
		newClientCAs, err := cert.ParseCertsPEM(newContent.clientCA.caBundle)
		if err != nil {
			return fmt.Errorf("unable to load client CA file %q: %v", string(newContent.clientCA.caBundle), err)
		}
		for i, cert := range newClientCAs {
			klog.V(2).InfoS("Loaded client CA", "index", i, "certName", c.clientCA.Name(), "certDetail", GetHumanCertDetail(cert))
			if c.eventRecorder != nil {
				c.eventRecorder.Eventf(&corev1.ObjectReference{Name: c.clientCA.Name()}, nil, corev1.EventTypeWarning, "TLSConfigChanged", "CACertificateReload", "loaded client CA [%d/%q]: %s", i, c.clientCA.Name(), GetHumanCertDetail(cert))
			}

			newClientCAPool.AddCert(cert)
		}

		newTLSConfigCopy.ClientCAs = newClientCAPool
	}

	if len(newContent.servingCert.cert) > 0 && len(newContent.servingCert.key) > 0 {
		cert, err := tls.X509KeyPair(newContent.servingCert.cert, newContent.servingCert.key)
		if err != nil {
			return fmt.Errorf("invalid serving cert keypair: %v", err)
		}

		x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return fmt.Errorf("invalid serving cert: %v", err)
		}

		klog.V(2).InfoS("Loaded serving cert", "certName", c.servingCert.Name(), "certDetail", GetHumanCertDetail(x509Cert))
		if c.eventRecorder != nil {
			c.eventRecorder.Eventf(&corev1.ObjectReference{Name: c.servingCert.Name()}, nil, corev1.EventTypeWarning, "TLSConfigChanged", "ServingCertificateReload", "loaded serving cert [%q]: %s", c.servingCert.Name(), GetHumanCertDetail(x509Cert))
		}

		newTLSConfigCopy.Certificates = []tls.Certificate{cert}
	}

	if len(newContent.sniCerts) > 0 {
		newTLSConfigCopy.NameToCertificate, err = c.BuildNamedCertificates(newContent.sniCerts)
		if err != nil {
			return fmt.Errorf("unable to build named certificate map: %v", err)
		}

		// append all named certs. Otherwise, the go tls stack will think no SNI processing
		// is necessary because there is only one cert anyway.
		// Moreover, if servingCert is not set, the first SNI
		// cert will become the default cert. That's what we expect anyway.
		for _, sniCert := range newTLSConfigCopy.NameToCertificate {
			newTLSConfigCopy.Certificates = append(newTLSConfigCopy.Certificates, *sniCert)
		}
	}

	// store new values of content for serving.
	c.currentServingTLSConfig.Store(newTLSConfigCopy)
	c.currentlyServedContent = newContent // this is single threaded, so we have no locking issue

	return nil
}

// RunOnce runs a single sync step to ensure that we have a valid starting configuration.
func (c *DynamicServingCertificateController) RunOnce() error {
	return c.syncCerts()
}

// Run starts the kube-apiserver and blocks until stopCh is closed.
//
// 与其它控制器不同，这里只启动一个 worker：证书重建本身很轻，且 syncCerts
// 依赖“单线程”假设来避免对 currentlyServedContent 加锁。
func (c *DynamicServingCertificateController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.InfoS("Starting DynamicServingCertificateController")
	defer klog.InfoS("Shutting down DynamicServingCertificateController")

	// synchronously load once.  We will trigger again, so ignoring any error is fine
	_ = c.RunOnce()

	// doesn't matter what workers say, only start one.
	go wait.Until(c.runWorker, time.Second, stopCh)

	// start timer that rechecks every minute, just in case.  this also serves to prime the controller quickly.
	go wait.Until(func() {
		c.Enqueue()
	}, 1*time.Minute, stopCh)

	<-stopCh
}

func (c *DynamicServingCertificateController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一个同步请求；失败时进入限速重试队列。
func (c *DynamicServingCertificateController) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.syncCerts()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// Enqueue a method to allow separate control loops to cause the certificate controller to trigger and read content.
//
// 队列中永远只有同一个 key，重复入队会被去重；这足以表达“需要重新同步”。
func (c *DynamicServingCertificateController) Enqueue() {
	c.queue.Add(workItemKey)
}
