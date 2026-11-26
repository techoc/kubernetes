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

// DynamicServingCertificateController 动态加载证书，提供可供 Go tls 使用的 GetCertificate 回调。
//
// 控制器把“静态 tls.Config 模板”与“动态证书内容”分离：baseTLSConfig 只复制一次，
// 每次内容变化时在副本上重建 Certificates/ClientCAs/NameToCertificate，再通过
// atomic.Value 原子替换，因而 TLS 握手无需加锁即可读到一致快照。
type DynamicServingCertificateController struct {
	// 对外服务 TLS 配置中的静态部分；每次复制一份再按动态证书状态修改副本。
	baseTLSConfig *tls.Config

	// 提供最新的客户端 CA bundle。
	clientCA CAContentProvider
	// 提供最新的默认服务证书。
	servingCert CertKeyContentProvider
	// SNI 用的名称到证书提供者的列表。
	sniCerts []SNICertKeyContentProvider

	// 保存当前实际服务的原始字节，用于判断是否需要替换 atomic 值；TLSConfig 使用的高效类型无法直接做比较。
	currentlyServedContent *dynamicCertificateContent
	// 当前用于服务请求的 *tls.Config。
	currentServingTLSConfig atomic.Value

	// 队列中始终只有一项，但借用其错误退避与重试语义。
	queue         workqueue.TypedRateLimitingInterface[string]
	eventRecorder events.EventRecorder
}

var _ Listener = &DynamicServingCertificateController{}

// NewDynamicServingCertificateController 构造一个持续更新 TLSConfig 的控制器。
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

// GetConfigForClient 实现 tls.Config.GetConfigForClient 回调。
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

	// 客户端带了 SNI 信息时走常规 SNI 流程。
	if len(clientHello.ServerName) > 0 {
		return tlsConfigCopy, nil
	}

	// 客户端未提供 SNI 时，需要检查目标 IP，以便按显式 IP 映射选择证书。
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

// newTLSContent 计算用于覆盖基础 TLSConfig 的下一份内容。
//
// 这里只收集原始 PEM 字节，不做解析；是否真正变化由 syncCerts 通过 Equal 判断，
// 避免内容未变时重复解析证书。
func (c *DynamicServingCertificateController) newTLSContent() (*dynamicCertificateContent, error) {
	newContent := &dynamicCertificateContent{}

	if c.clientCA != nil {
		currClientCABundle := c.clientCA.CurrentCABundleContent()
		// 允许清空所有客户端 CA：服务端仍然安全，只是少了对客户端该用哪张证书的提示。
		// 聚合 apiserver 及部分部署模式下，初始尚无用于认证的 client CA，就会出现这种情况。
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

// syncCerts 获取新内容，有变化时才解析并保存，供 GetConfigForClient 使用。
//
// 失败时保留旧配置继续服务，由调用方（worker）按限速队列重试，避免短暂的文件
// 读取失败直接中断 TLS 服务。
func (c *DynamicServingCertificateController) syncCerts() error {
	newContent, err := c.newTLSContent()
	if err != nil {
		return err
	}
	// 内容未变则直接跳过；这依赖单线程前提，改成多线程必须加锁。
	if newContent.Equal(c.currentlyServedContent) {
		return nil
	}

	// 浅拷贝静态配置，再覆盖发生变化的动态部分。
	newTLSConfigCopy := c.baseTLSConfig.Clone()

	// 解析新内容并加入 TLSConfig。
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

		// 全部以 NamedCertificates 形式追加，否则 Go tls 看到只有一张证书就会跳过 SNI 处理。
		// 若未设置默认服务证书，第一张 SNI 证书自然成为默认证书，这正是期望行为。
		for _, sniCert := range newTLSConfigCopy.NameToCertificate {
			newTLSConfigCopy.Certificates = append(newTLSConfigCopy.Certificates, *sniCert)
		}
	}

	// 保存本次服务所用的新内容。
	c.currentServingTLSConfig.Store(newTLSConfigCopy)
	c.currentlyServedContent = newContent // this is single threaded, so we have no locking issue

	return nil
}

// RunOnce 执行一次同步，确保启动时配置有效。
func (c *DynamicServingCertificateController) RunOnce() error {
	return c.syncCerts()
}

// Run 启动控制器，直到 ctx 结束才返回。
//
// 与其它控制器不同，这里只启动一个 worker：证书重建本身很轻，且 syncCerts
// 依赖“单线程”假设来避免对 currentlyServedContent 加锁。
func (c *DynamicServingCertificateController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.InfoS("Starting DynamicServingCertificateController")
	defer klog.InfoS("Shutting down DynamicServingCertificateController")

	// 同步加载一次；后面还会再次触发，这里忽略错误无妨。
	_ = c.RunOnce()

	// 只处理一个对象，worker 数传什么都只启动一个。
	go wait.Until(c.runWorker, time.Second, stopCh)

	// 周期性兜底重查，防止漏 watch 事件；同时让控制器启动后立即加载一次。
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

// Enqueue 供其他控制循环触发证书重载。
//
// 队列中永远只有同一个 key，重复入队会被去重；这足以表达“需要重新同步”。
func (c *DynamicServingCertificateController) Enqueue() {
	c.queue.Add(workItemKey)
}
