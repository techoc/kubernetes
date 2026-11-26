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

// Package certificate 管理客户端证书的签发、持久化与轮换。
//
// Manager 通过 CertificateSigningRequest API 申请证书，在证书到期前按
// 70%-90% 的抖动阈值自动轮换，并把新证书写入 Store；它被 kubelet 等
// 需要长期自举凭据的组件使用。
package certificate

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"

	certificates "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate/csr"
	"k8s.io/client-go/util/keyutil"
)

var (
	// certificateWaitTimeout 控制单轮等待证书签发的最长时间。
	certificateWaitTimeout = 15 * time.Minute

	kubeletServingUsagesWithEncipherment = []certificates.KeyUsage{
		// https://tools.ietf.org/html/rfc5280#section-4.2.1.3
		//
		// 数字签名用途：允许证书校验 TLS 握手中使用的数字签名。
		certificates.UsageDigitalSignature,
		// 密钥加密用途：允许该证书/密钥对加密密钥，包括 TLS 协商出的对称密钥。
		certificates.UsageKeyEncipherment,
		// 服务端认证用途：允许 TLS 服务端用它向客户端证明身份。
		certificates.UsageServerAuth,
	}
	kubeletServingUsagesNoEncipherment = []certificates.KeyUsage{
		// https://tools.ietf.org/html/rfc5280#section-4.2.1.3
		//
		// 数字签名用途：允许证书校验 TLS 握手中使用的数字签名。
		certificates.UsageDigitalSignature,
		// 服务端认证用途：允许 TLS 服务端用它向客户端证明身份。
		certificates.UsageServerAuth,
	}
	// DefaultKubeletServingGetUsages 返回 kubelet serving 证书所需的 KeyUsage。
	//
	// RSA 私钥需要 KeyEncipherment 参与 TLS 密钥交换；ECDSA 不需要，加上反而
	// 可能被某些 signer 拒绝，因此按密钥类型区分。
	DefaultKubeletServingGetUsages = func(privateKey interface{}) []certificates.KeyUsage {
		switch privateKey.(type) {
		case *rsa.PrivateKey:
			return kubeletServingUsagesWithEncipherment
		default:
			return kubeletServingUsagesNoEncipherment
		}
	}
	kubeletClientUsagesWithEncipherment = []certificates.KeyUsage{
		// https://tools.ietf.org/html/rfc5280#section-4.2.1.3
		//
		// 数字签名用途：允许证书校验 TLS 握手中使用的数字签名。
		certificates.UsageDigitalSignature,
		// 密钥加密用途：允许该证书/密钥对加密密钥，包括 TLS 协商出的对称密钥。
		certificates.UsageKeyEncipherment,
		// 客户端认证用途：允许 TLS 客户端用它向服务端证明身份。
		certificates.UsageClientAuth,
	}
	kubeletClientUsagesNoEncipherment = []certificates.KeyUsage{
		// https://tools.ietf.org/html/rfc5280#section-4.2.1.3
		//
		// 数字签名用途：允许证书校验 TLS 握手中使用的数字签名。
		certificates.UsageDigitalSignature,
		// 客户端认证用途：允许 TLS 客户端用它向服务端证明身份。
		certificates.UsageClientAuth,
	}
	// DefaultKubeletClientGetUsages 返回 kubelet client 证书所需的 KeyUsage，
	// RSA 与 ECDSA 的区别同理：仅 RSA 需要 KeyEncipherment。
	DefaultKubeletClientGetUsages = func(privateKey interface{}) []certificates.KeyUsage {
		switch privateKey.(type) {
		case *rsa.PrivateKey:
			return kubeletClientUsagesWithEncipherment
		default:
			return kubeletClientUsagesNoEncipherment
		}
	}
)

// Manager 维护并更新当前使用的证书，后台与 API server 通信，为即将过期的证书申请新证书。
type Manager interface {
	// 启动与 API server 的状态同步循环。
	Start()
	// 停止证书管理循环。
	Stop()
	// Current 返回当前选定的证书及其 PEM 格式的证书/私钥数据。
	Current() *tls.Certificate
	// ServerHealthy 表示 manager 是否认为能与服务端通信。判断可能很保守：只有近期确实通信过才返回 true。
	ServerHealthy() bool
}

// Config 是创建新 Manager 可用的一组配置参数。
type Config struct {
	// ClientsetFn 用于创建 clientset，以便轮换密钥时创建/获取 CSR；该函数绝不会并行调用，
	// 调用时会传入当前客户端证书（若存在）。
	ClientsetFn ClientsetFunc
	// Template 是轮换时为新密钥生成 CSR 所用的模板，规则同标准库 crypto.x509.CreateCertificateRequest 的模板参数。
	Template *x509.CertificateRequest
	// GetTemplate 动态返回 CSR 模板，规则同标准库 crypto.x509.CreateCertificateRequest 的模板参数。
	// 返回 nil 表示不申请证书；若设置则优先于 Template。
	GetTemplate func() *x509.CertificateRequest
	// SignerName 是应为 manager 生成证书签名的 signer 名称。
	SignerName string
	// RequestedCertificateLifetime 是期望的有效期，可选，会写入 CSR 的 spec.expirationSeconds。
	// signer 可以忽略该请求，因此无法保证实际有效期。
	RequestedCertificateLifetime *time.Duration
	// Usages 指定证书的用途列表，与 GetUsages 互斥。
	Usages []certificates.KeyUsage
	// GetUsages 动态返回证书用途，与 Usages 互斥：两者只能设置其一。
	GetUsages func(privateKey interface{}) []certificates.KeyUsage
	// CertificateStore 持久化保存当前证书/私钥及后续生成的密钥对。
	//
	// 该 Store 同时也承担重启后恢复证书的职责，因此实现必须落盘。
	CertificateStore Store
	// BootstrapCertificatePEM 是 Store 中暂无证书且尚未从 API 获取到新证书时由 Manager 返回的自举证书。
	// 若 Store 已有证书则忽略。Start 后 Manager 会立即申请新证书，以便组件首次启动时先用通用证书
	// 完成初始化，再尽快换成专属证书。
	BootstrapCertificatePEM []byte
	// BootstrapKeyPEM 是 Store 中暂无证书时由 Manager 返回的自举私钥；Store 已有证书则忽略。
	// 使用自举证书/私钥后会尽快轮换，远早于到期，从而让组件首启时用通用凭据初始化，再换成专属凭据。
	BootstrapKeyPEM []byte `datapolicy:"security-key"`
	// CertificateRotation 记录证书轮换前的存活秒数。用直方图保存轮换节奏的历史，便于监控异常轮换行为并跟踪趋势。
	CertificateRotation Histogram
	// CertifcateRenewFailure 记录证书续期失败次数。
	CertificateRenewFailure Counter
	// GenerateKey 可选，用于为新 CSR 生成私钥；未设置时生成 ECDSA P-256 密钥。
	// 目前只支持 *ecdsa.PrivateKey 与 *rsa.PrivateKey，且强度必须达标（RSA >= 2048 位，ECDSA 曲线 >= 256 位），
	// 否则生成 CSR 时会报错。
	GenerateKey func() (crypto.Signer, error)
	// Name 可选，用于 logger.WithName 与错误信息中的组件名。未设置时依次回退到 SignerName、
	// "client auth"（若含客户端认证用途）或 "server"。
	Name string
	// Ctx 可选。取消它等价于调用 Stop；若其中带有 logger 则使用它，否则用 klog.Background()。
	//
	// 在 Ctx 中携带的 logger 会贯穿轮换循环，便于按组件区分日志。
	Ctx *context.Context
}

// Store 负责获取与更新当前证书，具体后端由实现决定。
type Store interface {
	// Current 返回当前证书及 PEM 数据；暂无证书时应返回 NoCertKeyError，让 Manager 用自举证书申请新证书。
	Current() (*tls.Certificate, error)
	// Update 接收证书/私钥的 PEM 数据，把它设为后续 Current() 返回的当前证书对。
	Update(cert, key []byte) (*tls.Certificate, error)
}

// Gauge 在每次更新时记录证书的剩余有效期。
type Gauge interface {
	Set(float64)
}

// Histogram 记录轮换下来的证书此前被使用了多久。
type Histogram interface {
	Observe(float64)
}

// Counter 包装带标签的计数器。
type Counter interface {
	Inc()
}

// NoCertKeyError 表示当前没有可用的证书/私钥。
type NoCertKeyError string

// ClientsetFunc 返回用于探测 CSR API 与申请 CSR 的 clientset；存在有效证书时会把它传入。
type ClientsetFunc func(current *tls.Certificate) (clientset.Interface, error)

func (e *NoCertKeyError) Error() string { return string(*e) }

// manager 是 Manager 的唯一实现，负责串起模板、CSR、等待签发与落盘。
type manager struct {
	getTemplate func() *x509.CertificateRequest

	// lastRequestLock 保护 lastRequestCancel 与 lastRequest。
	lastRequestLock   sync.Mutex
	lastRequestCancel context.CancelFunc
	lastRequest       *x509.CertificateRequest

	// dynamicTemplate 表示模板由 GetTemplate 动态提供，需要额外的模板变更监视线程。
	dynamicTemplate              bool
	signerName                   string
	requestedCertificateLifetime *time.Duration
	getUsages                    func(privateKey interface{}) []certificates.KeyUsage
	generateKey                  func() (crypto.Signer, error)
	forceRotation                bool

	certStore Store

	certificateRotation     Histogram
	certificateRenewFailure Counter

	// 以下变量只能在持有 certAccessLock 时访问。
	certAccessLock sync.RWMutex
	cert           *tls.Certificate
	serverHealth   bool

	// 后台 goroutine 使用的 context 与取消函数。
	ctx    context.Context
	cancel func(err error)

	// clientsetFn 只能在持有 clientAccessLock 时访问，也就是必须串行调用：
	// 每次轮换都会重建 clientset。
	clientAccessLock sync.Mutex
	clientsetFn      ClientsetFunc

	// 默认取 time.Now，测试中可替换为假时钟。
	now func() time.Time
}

// NewManager 创建证书管理器，作为 Kubelet 中证书的权威来源并处理轮换更新。
func NewManager(config *Config) (Manager, error) {

	// 未提供动态模板时退化为固定模板，两种情况共享同一 getTemplate 入口。
	getTemplate := config.GetTemplate
	if getTemplate == nil {
		getTemplate = func() *x509.CertificateRequest { return config.Template }
	}

	// Usages 与 GetUsages 二选一：前者是静态列表，后者按私钥类型动态计算。
	if config.GetUsages != nil && config.Usages != nil {
		return nil, errors.New("cannot specify both GetUsages and Usages")
	}
	if config.GetUsages == nil && config.Usages == nil {
		return nil, errors.New("either GetUsages or Usages should be specified")
	}
	var getUsages func(interface{}) []certificates.KeyUsage
	if config.GetUsages != nil {
		getUsages = config.GetUsages
	} else {
		getUsages = func(interface{}) []certificates.KeyUsage { return config.Usages }
	}
	m := manager{
		clientsetFn:                  config.ClientsetFn,
		getTemplate:                  getTemplate,
		dynamicTemplate:              config.GetTemplate != nil,
		signerName:                   config.SignerName,
		requestedCertificateLifetime: config.RequestedCertificateLifetime,
		getUsages:                    getUsages,
		generateKey:                  config.GenerateKey,
		certStore:                    config.CertificateStore,
		certificateRotation:          config.CertificateRotation,
		certificateRenewFailure:      config.CertificateRenewFailure,
		now:                          time.Now,
	}

	// 确定该 manager 实例在日志输出中使用的名称。
	// 优先级：显式 Name > SignerName > 按用途推断，便于在多证书组件中区分日志。
	name := config.Name
	if len(name) == 0 {
		name = m.signerName
	}
	if len(name) == 0 {
		usages := getUsages(nil)
		switch {
		case hasKeyUsage(usages, certificates.UsageClientAuth):
			name = string(certificates.UsageClientAuth)
		default:
			name = "certificate"
		}
	}

	// 该名称会通过上下文日志带入。
	logger := klog.Background()
	if config.Ctx != nil {
		logger = klog.FromContext(*config.Ctx)
	}
	logger = klog.LoggerWithName(logger, name)

	cert, forceRotation, err := getCurrentCertificateOrBootstrap(
		logger,
		config.CertificateStore,
		config.BootstrapCertificatePEM,
		config.BootstrapKeyPEM)
	if err != nil {
		return nil, err
	}
	m.cert, m.forceRotation = cert, forceRotation

	// cancel 由 Stop 调用；外部 ctx.Done 用作停止信号。
	m.ctx, m.cancel = context.WithCancelCause(context.Background())
	// 外部 context 只用于传递取消信号；把它的 Done 映射到 Stop，避免两套停止机制。
	if config.Ctx != nil && (*config.Ctx).Done() != nil {
		ctx := *config.Ctx
		// 传入的 context 若带 Done channel，就把它的取消映射到本管理器的 Done。
		go func() {
			<-ctx.Done()
			m.Stop()
		}()
	}
	m.ctx = klog.NewContext(m.ctx, logger)

	return &m, nil
}

// Current 返回 manager 当前选定的证书。初始化时没有证书、尚未从 CSR 客户端收到证书，或当前证书过期时返回 nil。
func (m *manager) Current() *tls.Certificate {
	m.certAccessLock.RLock()
	defer m.certAccessLock.RUnlock()
	// 过期证书视为不可用，返回 nil 促使调用方走自举/重新申请路径。
	if m.cert != nil && m.cert.Leaf != nil && m.now().After(m.cert.Leaf.NotAfter) {
		klog.FromContext(m.ctx).V(2).Info("Current certificate is expired")
		return nil
	}
	return m.cert
}

// ServerHealthy 表示 cert manager 是否认为服务端当前可用。
func (m *manager) ServerHealthy() bool {
	m.certAccessLock.RLock()
	defer m.certAccessLock.RUnlock()
	return m.serverHealth
}

// Stop 终止 manager。
func (m *manager) Stop() {
	m.cancel(errors.New("asked to stop"))
}

// Start 启动后台证书轮换工作。
func (m *manager) Start() {
	go m.run()
}

// 与 Start 不同，run 在 manager 运行期间阻塞，并等待所有 goroutine 停止。
func (m *manager) run() {
	logger := klog.FromContext(m.ctx)
	// 证书轮换依赖 API server 的签发 API，没有 clientset 时不应启动管理器。
	if m.clientsetFn == nil {
		logger.V(2).Info("Certificate rotation is not enabled, no connection to the apiserver")
		return
	}
	logger.V(2).Info("Certificate rotation is enabled")

	var wg sync.WaitGroup
	// 返回前等待所有轮换/模板监视线程退出，保证 Stop 之后没有残留写者。
	defer wg.Wait()

	templateChanged := make(chan struct{})
	rotate := func(ctx context.Context) {
		deadline := m.nextRotationDeadline(logger)
		if sleepInterval := deadline.Sub(m.now()); sleepInterval > 0 {
			logger.V(2).Info("Waiting for next certificate rotation", "sleep", sleepInterval)

			timer := time.NewTimer(sleepInterval)
			defer timer.Stop()

			select {
			case <-timer.C:
				// 截止时间到期时解除阻塞。
			case <-templateChanged:
				// 模板变化通知：若当前待签发的请求已匹配新模板，继续等待即可。
				_, lastRequestTemplate := m.getLastRequest()
				if reflect.DeepEqual(lastRequestTemplate, m.getTemplate()) {
					// 若模板已与最近一次请求一致，则重新开始轮换截止时间循环。
					return
				}
				logger.V(2).Info("Certificate template changed, rotating")
			}
		}

		// 尚无可用模板时不进入 rotateCerts，避免触发退避。
		if m.getTemplate() == nil {
			return
		}

		// 先指数退避重试，仍失败则转入无限轮询：证书轮换不能永久放弃。
		backoff := wait.Backoff{
			Duration: 2 * time.Second,
			Factor:   2,
			Jitter:   0.1,
			Steps:    5,
		}
		if err := wait.ExponentialBackoffWithContext(ctx, backoff, m.rotateCerts); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Reached backoff limit, still unable to rotate certs")
			wait.PollInfiniteWithContext(ctx, 32*time.Second, m.rotateCerts)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		wait.UntilWithContext(m.ctx, rotate, time.Second)
	}()

	if m.dynamicTemplate {
		template := func(ctx context.Context) {
			// 检查当前模板是否与最近一次请求一致。
			lastRequestCancel, lastRequestTemplate := m.getLastRequest()

			if !m.certSatisfiesTemplate(logger) && !reflect.DeepEqual(lastRequestTemplate, m.getTemplate()) {
				// 模板变化时排队中断轮换截止时间循环；若处理中断时请求已与新模板一致，则忽略该中断。
				if lastRequestCancel != nil {
					// 旧请求已不符合当前模板，取消等待以免阻塞在新的签发请求上。
					// 若正在等待的已提交请求已不符合当前需求，就停止等待。
					lastRequestCancel()
				}
				select {
				case templateChanged <- struct{}{}:
				case <-ctx.Done():
				}
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait.UntilWithContext(m.ctx, template, time.Second)
		}()
	}
}

// getCurrentCertificateOrBootstrap 返回可用的当前证书，或在无可用证书时使用自举证书。
//
// 第二个返回值表示是否应立即触发轮换：自举证书是临时的，必须在首次启动后
// 尽快替换为唯一证书。
func getCurrentCertificateOrBootstrap(
	logger klog.Logger,
	store Store,
	bootstrapCertificatePEM []byte,
	bootstrapKeyPEM []byte) (cert *tls.Certificate, shouldRotate bool, errResult error) {

	currentCert, err := store.Current()
	if err == nil {
		// 当前证书已过期时退回自举证书。
		if currentCert.Leaf != nil && time.Now().Before(currentCert.Leaf.NotAfter) {
			return currentCert, false, nil
		}
	} else {
		// 只有“尚无证书”是可恢复的；其他 Store 错误直接上抛。
		if _, ok := err.(*NoCertKeyError); !ok {
			return nil, false, err
		}
	}

	// 没有任何自举材料时返回 nil 证书，但仍标记需要轮换。
	if bootstrapCertificatePEM == nil || bootstrapKeyPEM == nil {
		return nil, true, nil
	}

	bootstrapCert, err := tls.X509KeyPair(bootstrapCertificatePEM, bootstrapKeyPEM)
	if err != nil {
		return nil, false, err
	}
	if len(bootstrapCert.Certificate) < 1 {
		return nil, false, fmt.Errorf("no cert/key data found")
	}

	// 自举证书必须解析出 Leaf，后续轮换判断依赖其中的 Subject/SAN 字段。
	certs, err := x509.ParseCertificates(bootstrapCert.Certificate[0])
	if err != nil {
		return nil, false, fmt.Errorf("unable to parse certificate data: %w", err)
	}
	if len(certs) < 1 {
		return nil, false, fmt.Errorf("no cert data found")
	}
	bootstrapCert.Leaf = certs[0]

	// 把自举证书写入 Store，使其在进程重启后仍可继续使用直到被轮换。
	if _, err := store.Update(bootstrapCertificatePEM, bootstrapKeyPEM); err != nil {
		utilruntime.HandleErrorWithLogger(logger, err, "Unable to set the cert/key pair to the bootstrap certificate")
	}

	return &bootstrapCert, true, nil
}

func (m *manager) getClientset() (clientset.Interface, error) {
	current := m.Current()
	// ClientsetFn 的契约要求串行调用；先取锁再调用可避免并发重建。
	m.clientAccessLock.Lock()
	defer m.clientAccessLock.Unlock()
	return m.clientsetFn(current)
}

// RotateCerts 仅供测试使用，不属于公开接口。证书被更换时返回 true，否则 false；仅在异常情况下返回错误。
func (m *manager) RotateCerts() (bool, error) {
	return m.rotateCerts(m.ctx)
}

// rotateCerts 尝试向服务端申请客户端证书，等待签发后更新磁盘证书；拿不到证书时返回 false，
// 仅在异常情况下返回错误。过程中还会根据各次调用的响应推断「服务端健康状态」。
// TODO：改为返回错误，让调用方正确处理与记录。
func (m *manager) rotateCerts(ctx context.Context) (bool, error) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Rotating certificates")

	// 每次轮换都生成全新的私钥与 CSR，不复用旧密钥。
	template, csrPEM, keyPEM, privateKey, err := m.generateCSR()
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to generate a certificate signing request")
		if m.certificateRenewFailure != nil {
			m.certificateRenewFailure.Inc()
		}
		return false, nil
	}

	// 每次都重新获取 client：客户端凭据可能刚轮换过，重新构造 clientset 可避免使用失效证书。
	clientSet, err := m.getClientset()
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to load a client to request certificates")
		if m.certificateRenewFailure != nil {
			m.certificateRenewFailure.Inc()
		}
		return false, nil
	}

	getUsages := m.getUsages
	if m.getUsages == nil {
		getUsages = DefaultKubeletClientGetUsages
	}
	usages := getUsages(privateKey)
	// 调用 CSR API 为新私钥申请证书。
	reqName, reqUID, err := csr.RequestCertificateWithContext(ctx, clientSet, csrPEM, "", m.signerName, m.requestedCertificateLifetime, usages, privateKey)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed while requesting a signed certificate from the control plane")
		if m.certificateRenewFailure != nil {
			m.certificateRenewFailure.Inc()
		}
		// 只有服务器侧错误才可能影响健康判断，其余轮换失败仅计数。
		return false, m.updateServerError(err)
	}

	// 等待签发设置上限，避免无限挂起阻塞后续轮换。
	ctx, cancel := context.WithTimeout(ctx, certificateWaitTimeout)
	defer cancel()

	// CSR 成功提交后记下本次请求：保存 cancel 与模板，模板监视线程可据此取消已过时的等待。
	m.setLastRequest(cancel, template)

	// 等待证书签发。该接口及内部超时是旧设计的遗留：当时用原始 watch 加退避实现。
	crtPEM, err := csr.WaitForCertificate(ctx, clientSet, reqName, reqUID)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Certificate request was not signed")
		if m.certificateRenewFailure != nil {
			m.certificateRenewFailure.Inc()
		}
		return false, nil
	}

	cert, err := m.certStore.Update(crtPEM, keyPEM)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to store the new cert/key pair")
		if m.certificateRenewFailure != nil {
			m.certificateRenewFailure.Inc()
		}
		return false, nil
	}

	// 记录旧证书的实际存活时长，用于观察轮换节奏是否异常。
	if old := m.updateCached(cert); old != nil && m.certificateRotation != nil {
		m.certificateRotation.Observe(m.now().Sub(old.Leaf.NotBefore).Seconds())
	}

	return true, nil
}

// 检查磁盘上当前证书是否满足当前模板的要求。
// 证书中模板未要求的额外 SAN/org 不会触发续期。调用前必须持有 certAccessLock。
func (m *manager) certSatisfiesTemplateLocked(logger klog.Logger) bool {
	if m.cert == nil {
		return false
	}

	if template := m.getTemplate(); template != nil {
		// 只检查“模板要求但证书缺失”的字段；证书多出的 SAN/org 不触发续期。
		if template.Subject.CommonName != m.cert.Leaf.Subject.CommonName {
			logger.V(2).Info("Current certificate CN does not match requested CN", "currentName", m.cert.Leaf.Subject.CommonName, "requestedName", template.Subject.CommonName)
			return false
		}

		currentDNSNames := sets.NewString(m.cert.Leaf.DNSNames...)
		desiredDNSNames := sets.NewString(template.DNSNames...)
		missingDNSNames := desiredDNSNames.Difference(currentDNSNames)
		if len(missingDNSNames) > 0 {
			logger.V(2).Info("Current certificate is missing requested DNS names", "dnsNames", missingDNSNames.List())
			return false
		}

		currentIPs := sets.NewString()
		for _, ip := range m.cert.Leaf.IPAddresses {
			currentIPs.Insert(ip.String())
		}
		desiredIPs := sets.NewString()
		for _, ip := range template.IPAddresses {
			desiredIPs.Insert(ip.String())
		}
		missingIPs := desiredIPs.Difference(currentIPs)
		if len(missingIPs) > 0 {
			logger.V(2).Info("Current certificate is missing requested IP addresses", "IPs", missingIPs.List())
			return false
		}

		currentOrgs := sets.NewString(m.cert.Leaf.Subject.Organization...)
		desiredOrgs := sets.NewString(template.Subject.Organization...)
		missingOrgs := desiredOrgs.Difference(currentOrgs)
		if len(missingOrgs) > 0 {
			logger.V(2).Info("Current certificate is missing requested orgs", "orgs", missingOrgs.List())
			return false
		}
	}

	return true
}

func (m *manager) certSatisfiesTemplate(logger klog.Logger) bool {
	m.certAccessLock.RLock()
	defer m.certAccessLock.RUnlock()
	return m.certSatisfiesTemplateLocked(logger)
}

// nextRotationDeadline 返回当前证书的轮换阈值，约为有效期的 80%±10%。
func (m *manager) nextRotationDeadline(logger klog.Logger) time.Time {
	// forceRotation 不受锁保护：自举证书需要立即可轮换，因此这里返回 now 并清除标记。
	if m.forceRotation {
		m.forceRotation = false
		return m.now()
	}

	m.certAccessLock.RLock()
	defer m.certAccessLock.RUnlock()

	// 证书不再满足模板时立即轮换，而不是等到 80% 生命周期。
	if !m.certSatisfiesTemplateLocked(logger) {
		return m.now()
	}

	notAfter := m.cert.Leaf.NotAfter
	totalDuration := float64(notAfter.Sub(m.cert.Leaf.NotBefore))
	deadline := m.cert.Leaf.NotBefore.Add(jitteryDuration(totalDuration))

	logger.V(2).Info("Certificate rotation deadline determined", "expiration", notAfter, "deadline", deadline)
	return deadline
}

// jitteryDuration 加抖动设置轮换阈值，使各节点在证书生命周期的 70%-90% 左右轮换。
// 抖动可避免同一批加入集群的节点（如建集群时）此后总在同一时刻轮换。
// 该函数定义为变量，便于测试替换。
var jitteryDuration = func(totalDuration float64) time.Duration {
	return wait.Jitter(time.Duration(totalDuration), 0.2) - time.Duration(totalDuration*0.3)
}

// updateCached 更新最近获取的证书并返回旧证书，同时把服务端标记为健康。
func (m *manager) updateCached(cert *tls.Certificate) *tls.Certificate {
	m.certAccessLock.Lock()
	defer m.certAccessLock.Unlock()
	// 成功拿到新证书即视为与服务器通信正常。
	m.serverHealth = true
	old := m.cert
	m.cert = cert
	return old
}

// updateServerError 根据服务端返回的错误推断其健康状态：错误无需立即终止等待循环时返回 nil，否则返回该错误。
func (m *manager) updateServerError(err error) error {
	m.certAccessLock.Lock()
	defer m.certAccessLock.Unlock()
	switch {
	case apierrors.IsUnauthorized(err):
		// 终结 SSL 的代理可能代替 master 报出该错误：401 往往来自中间代理而非控制面，因此仍认为服务器可达。
		m.serverHealth = true
	case apierrors.IsUnexpectedServerError(err):
		// 通常表示代理或负载均衡器出问题，而非 master 本身。
		m.serverHealth = false
	default:
		// 识别证书请求中预期可能出现、表明一切正常的已知错误：
		// 404/403 是 CSR 流程的正常业务错误，说明 API Server 本身健康。
		m.serverHealth = apierrors.IsNotFound(err) || apierrors.IsForbidden(err)
	}
	return nil
}

// generateKeyFunc 是默认私钥生成函数，测试中可替换。
var generateKeyFunc = generateKeyFuncImpl

// generateKeyFuncImpl 默认生成 ECDSA P-256 私钥：比 RSA 更快且更小。
func generateKeyFuncImpl() (crypto.Signer, error) {
	return ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
}

// generateCSR 生成新私钥、PEM 编码私钥以及基于当前模板的 CSR。
func (m *manager) generateCSR() (template *x509.CertificateRequest, csrPEM []byte, keyPEM []byte, key interface{}, err error) {
	generateKey := m.generateKey
	if generateKey == nil {
		generateKey = generateKeyFunc
	}
	// 生成新私钥。
	privateKey, err := generateKey()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to generate a new private key: %w", err)
	}
	// 强度校验防止调用方注入弱密钥，签发流程一旦成功就难以补救。
	if err = validateKeyStrength(privateKey); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("the new key is insecure: %w", err)
	}
	keyPEM, err = keyutil.MarshalPrivateKeyToPEM(privateKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to marshal the new key to PEM: %w", err)
	}
	template = m.getTemplate()
	if template == nil {
		return nil, nil, nil, nil, errors.New("unable to create a csr, no template available")
	}
	csrPEM, err = cert.MakeCSRFromTemplate(privateKey, template)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to create a csr from the private key: %w", err)
	}
	return template, csrPEM, keyPEM, privateKey, nil
}

// getLastRequest 返回最近一次提交的 CSR 的取消函数与模板。
func (m *manager) getLastRequest() (context.CancelFunc, *x509.CertificateRequest) {
	m.lastRequestLock.Lock()
	defer m.lastRequestLock.Unlock()
	return m.lastRequestCancel, m.lastRequest
}

// setLastRequest 记录最近一次提交的 CSR；调用方在等待签发前调用。
func (m *manager) setLastRequest(cancel context.CancelFunc, r *x509.CertificateRequest) {
	m.lastRequestLock.Lock()
	defer m.lastRequestLock.Unlock()
	m.lastRequestCancel = cancel
	m.lastRequest = r
}

// hasKeyUsage 判断 usages 中是否包含指定用途。
func hasKeyUsage(usages []certificates.KeyUsage, usage certificates.KeyUsage) bool {
	for _, u := range usages {
		if u == usage {
			return true
		}
	}
	return false
}

// validateKeyStrength 检查密钥强度是否足以签发证书：RSA 至少 2048 位，ECDSA 曲线至少 256 位。
func validateKeyStrength(key crypto.Signer) error {
	const (
		minRSAKeyBits   = 2048
		minECDSAKeyBits = 256
	)
	switch k := key.(type) {
	case *rsa.PrivateKey:
		if bits := k.N.BitLen(); bits < minRSAKeyBits {
			return fmt.Errorf("RSA key size %d bits is below the minimum of %d",
				bits, minRSAKeyBits)
		}
	case *ecdsa.PrivateKey:
		if bits := k.Curve.Params().BitSize; bits < minECDSAKeyBits {
			return fmt.Errorf("ECDSA key curve %s (%d bits) is below the minimum of %d",
				k.Curve.Params().Name, bits, minECDSAKeyBits)
		}
	}
	return nil
}
