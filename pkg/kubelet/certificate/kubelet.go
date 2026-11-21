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
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math"
	"net"
	"sort"
	"sync/atomic"
	"time"

	certificates "k8s.io/api/certificates/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/certificate"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	netutils "k8s.io/utils/net"
)

// newGetTemplateFn 返回生成服务证书 CSR 模板的函数；
// 没有可用地址时返回 nil，让证书管理器跳过请求。
func newGetTemplateFn(nodeName types.NodeName, getAddresses func() []v1.NodeAddress) func() *x509.CertificateRequest {
	return func() *x509.CertificateRequest {
		// 把节点地址拆分为 DNS 名和 IP，去重并排序，保证 CSR 内容稳定。
		hostnames, ips := addressesToHostnamesAndIPs(getAddresses())
		// 默认要求至少有一个 IP 才申请服务证书。
		hasRequiredAddresses := len(ips) > 0

		// 特性开启时允许仅凭 DNS 名申请服务证书。
		if utilfeature.DefaultFeatureGate.Enabled(features.AllowDNSOnlyNodeCSR) {
			hasRequiredAddresses = hasRequiredAddresses || len(hostnames) > 0
		}

		// 没有可用于申请的地址时返回 nil。
		if !hasRequiredAddresses {
			return nil
		}
		return &x509.CertificateRequest{
			Subject: pkix.Name{
				CommonName:   fmt.Sprintf("system:node:%s", nodeName),
				Organization: []string{"system:nodes"},
			},
			DNSNames:    hostnames,
			IPAddresses: ips,
		}
	}
}

// NewKubeletServerCertificateManager 创建 kubelet 服务证书管理器。
func NewKubeletServerCertificateManager(kubeClient clientset.Interface, kubeCfg *kubeletconfig.KubeletConfiguration, nodeName types.NodeName, getAddresses func() []v1.NodeAddress, certDirectory string) (certificate.Manager, error) {
	var clientsetFn certificate.ClientsetFunc
	if kubeClient != nil {
		clientsetFn = func(current *tls.Certificate) (clientset.Interface, error) {
			return kubeClient, nil
		}
	}
	certificateStore, err := certificate.NewFileStore(
		"kubelet-server",
		certDirectory,
		certDirectory,
		kubeCfg.TLSCertFile,
		kubeCfg.TLSPrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize server certificate store: %v", err)
	}
	var certificateRenewFailure = compbasemetrics.NewCounter(
		&compbasemetrics.CounterOpts{
			Subsystem:      metrics.KubeletSubsystem,
			Name:           "server_expiration_renew_errors",
			Help:           "Counter of certificate renewal errors.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)
	legacyregistry.MustRegister(certificateRenewFailure)

	certificateRotationAge := compbasemetrics.NewHistogram(
		&compbasemetrics.HistogramOpts{
			Subsystem: metrics.KubeletSubsystem,
			Name:      "certificate_manager_server_rotation_seconds",
			Help:      "Histogram of the number of seconds the previous certificate lived before being rotated.",
			Buckets: []float64{
				60,        // 1  minute
				3600,      // 1  hour
				14400,     // 4  hours
				86400,     // 1  day
				604800,    // 1  week
				2592000,   // 1  month
				7776000,   // 3  months
				15552000,  // 6  months
				31104000,  // 1  year
				124416000, // 4  years
			},
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)
	legacyregistry.MustRegister(certificateRotationAge)

	// 准备模板生成函数后交给 client-go 证书管理器，由它负责签发与轮换。
	getTemplate := newGetTemplateFn(nodeName, getAddresses)

	m, err := certificate.NewManager(&certificate.Config{
		ClientsetFn:             clientsetFn,
		GetTemplate:             getTemplate,
		SignerName:              certificates.KubeletServingSignerName,
		GetUsages:               certificate.DefaultKubeletServingGetUsages,
		CertificateStore:        certificateStore,
		CertificateRotation:     certificateRotationAge,
		CertificateRenewFailure: certificateRenewFailure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize server certificate manager: %v", err)
	}
	// 用 GaugeFunc 暴露服务证书的剩余 TTL；无有效证书时为 +INF。
	legacyregistry.RawMustRegister(compbasemetrics.NewGaugeFunc(
		&compbasemetrics.GaugeOpts{
			Subsystem: metrics.KubeletSubsystem,
			Name:      "certificate_manager_server_ttl_seconds",
			Help: "Gauge of the shortest TTL (time-to-live) of " +
				"the Kubelet's serving certificate. The value is in seconds " +
				"until certificate expiry (negative if already expired). If " +
				"serving certificate is invalid or unused, the value will " +
				"be +INF.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		func() float64 {
			if c := m.Current(); c != nil && c.Leaf != nil {
				return math.Trunc(time.Until(c.Leaf.NotAfter).Seconds())
			}
			return math.Inf(1)
		},
	))
	return m, nil
}

// addressesToHostnamesAndIPs 把 NodeAddress 列表拆分为去重的 DNS 名和 IP。
func addressesToHostnamesAndIPs(addresses []v1.NodeAddress) (dnsNames []string, ips []net.IP) {
	seenDNSNames := map[string]bool{}
	seenIPs := map[string]bool{}
	for _, address := range addresses {
		if len(address.Address) == 0 {
			continue
		}

		// HostName 类型既可能是 DNS 也可能是 IP，需要实际解析后归类。
		switch address.Type {
		case v1.NodeHostName:
			if ip := netutils.ParseIPSloppy(address.Address); ip != nil {
				seenIPs[address.Address] = true
			} else {
				seenDNSNames[address.Address] = true
			}
		case v1.NodeExternalIP, v1.NodeInternalIP:
			if ip := netutils.ParseIPSloppy(address.Address); ip != nil {
				seenIPs[address.Address] = true
			}
		case v1.NodeExternalDNS, v1.NodeInternalDNS:
			seenDNSNames[address.Address] = true
		}
	}

	for dnsName := range seenDNSNames {
		dnsNames = append(dnsNames, dnsName)
	}
	for ip := range seenIPs {
		ips = append(ips, netutils.ParseIPSloppy(ip))
	}

	// map 遍历无序，排序后返回，避免每次 CSR 内容变化。
	sort.Strings(dnsNames)
	sort.Slice(ips, func(i, j int) bool { return ips[i].String() < ips[j].String() })

	return dnsNames, ips
}

// NewKubeletClientCertificateManager 创建客户端证书管理器：
// 初始可以没有用于签发/轮换的 client；之后注入 CSR client 即可开始轮换。
func NewKubeletClientCertificateManager(
	certDirectory string,
	nodeName types.NodeName,
	bootstrapCertData []byte,
	bootstrapKeyData []byte,
	certFile string,
	keyFile string,
	clientsetFn certificate.ClientsetFunc,
) (certificate.Manager, error) {

	certificateStore, err := certificate.NewFileStore(
		"kubelet-client",
		certDirectory,
		certDirectory,
		certFile,
		keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client certificate store: %v", err)
	}
	var certificateRenewFailure = compbasemetrics.NewCounter(
		&compbasemetrics.CounterOpts{
			Namespace:      metrics.KubeletSubsystem,
			Subsystem:      "certificate_manager",
			Name:           "client_expiration_renew_errors",
			Help:           "Counter of certificate renewal errors.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)
	legacyregistry.Register(certificateRenewFailure)

	m, err := certificate.NewManager(&certificate.Config{
		ClientsetFn: clientsetFn,
		Template: &x509.CertificateRequest{
			Subject: pkix.Name{
				CommonName:   fmt.Sprintf("system:node:%s", nodeName),
				Organization: []string{"system:nodes"},
			},
		},
		SignerName: certificates.KubeAPIServerClientKubeletSignerName,
		GetUsages:  certificate.DefaultKubeletClientGetUsages,
		// 为兼容旧行为，kubelet 允许用高权限证书作为初始数据，随后立即轮换；
		// kubeadm 在控制平面节点上使用该路径。
		BootstrapCertificatePEM: bootstrapCertData,
		BootstrapKeyPEM:         bootstrapKeyData,

		CertificateStore:        certificateStore,
		CertificateRenewFailure: certificateRenewFailure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client certificate manager: %v", err)
	}

	return m, nil
}

// NewKubeletServerCertificateDynamicFileManager 创建基于文件读取与监听的服务证书管理器。
// 返回值实现 certificate.Manager 接口，可像本包其他管理器一样使用，
// 但完全不与 API server 通信，也不发起证书请求。
func NewKubeletServerCertificateDynamicFileManager(certFile, keyFile string) (certificate.Manager, error) {
	c, err := dynamiccertificates.NewDynamicServingContentFromFiles("kubelet-server-cert-files", certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to set up dynamic certificate manager for kubelet server cert files: %w", err)
	}
	m := &kubeletServerCertificateDynamicFileManager{
		dynamicCertificateContent: c,
		certFile:                  certFile,
		keyFile:                   keyFile,
	}
	// 先同步一次文件内容，再注册监听以接收后续变化。
	m.Enqueue()
	c.AddListener(m)
	return m, nil
}

// kubeletServerCertificateDynamicFileManager 基于证书/私钥文件提供动态内容。
type kubeletServerCertificateDynamicFileManager struct {
	cancelFn                  context.CancelFunc
	certFile                  string
	keyFile                   string
	dynamicCertificateContent *dynamiccertificates.DynamicCertKeyPairContent
	currentTLSCertificate     atomic.Pointer[tls.Certificate]
}

// Enqueue 在服务证书内容变化时被调用，重新加载证书对。
func (m *kubeletServerCertificateDynamicFileManager) Enqueue() {
	// 暂时使用 klog.TODO()，因为当前没有合适的 logger 可传入；
	// 后续重构该函数接收 logger 参数时应替换掉。
	logger := klog.TODO()
	// 读取当前文件内容并尝试组成 TLS 证书对。
	certContent, keyContent := m.dynamicCertificateContent.CurrentCertKeyContent()
	cert, err := tls.X509KeyPair(certContent, keyContent)
	if err != nil {
		logger.Error(err, "invalid certificate and key pair from file", "certFile", m.certFile, "keyFile", m.keyFile)
		return
	}
	// 只有证书和私钥都有效时才替换当前证书，避免加载失败后丢失可用证书。
	m.currentTLSCertificate.Store(&cert)
	logger.V(4).Info("loaded certificate and key pair in kubelet server certificate manager", "certFile", m.certFile, "keyFile", m.keyFile)
}

// Current 返回最近一次从文件成功加载的证书对。
func (m *kubeletServerCertificateDynamicFileManager) Current() *tls.Certificate {
	return m.currentTLSCertificate.Load()
}

// Start 开始监听证书和私钥文件的变化。
func (m *kubeletServerCertificateDynamicFileManager) Start() {
	var ctx context.Context
	// 暂时使用 context.TODO()，因为当前没有合适的 context 可传入；
	// 后续重构该函数接收 context 参数时应替换掉。
	ctx, m.cancelFn = context.WithCancel(context.TODO())
	// 以 1 秒周期轮询文件变化，文件更新后回调 Enqueue。
	go m.dynamicCertificateContent.Run(ctx, 1)
}

// Stop 停止监听证书和私钥文件。
func (m *kubeletServerCertificateDynamicFileManager) Stop() {
	if m.cancelFn != nil {
		m.cancelFn()
	}
}

// ServerHealthy 始终返回 true：文件管理器不与任何服务端通信。
func (m *kubeletServerCertificateDynamicFileManager) ServerHealthy() bool {
	return true
}
