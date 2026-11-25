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

// Package signer 实现基于本地磁盘密钥的 CA 签名控制器。
// 它为不同类型的 CSR（kubelet serving/client、apiserver client、legacy unknown）
// 提供自动签名，并支持 CA 证书与密钥的热重载。
package signer

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"time"

	capi "k8s.io/api/certificates/v1"
	capiv1beta1 "k8s.io/api/certificates/v1beta1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	certificatesinformers "k8s.io/client-go/informers/certificates/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/certificate/csr"
	capihelper "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/controller/certificates"
	"k8s.io/kubernetes/pkg/controller/certificates/authority"
)

// CSRSigningController 组合通用证书控制器与 CA 证书热重载器：
// 前者负责处理 CSR 队列，后者负责监听 CA 文件变化并重新加载密钥。
type CSRSigningController struct {
	certificateController *certificates.CertificateController
	dynamicCertReloader   dynamiccertificates.ControllerRunner
}

// NewKubeletServingCSRSigningController 创建用于 kubelet serving 证书的签名控制器，
// 对应 signerName 为 kubernetes.io/kubelet-serving。
func NewKubeletServingCSRSigningController(
	ctx context.Context,
	client clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	caFile, caKeyFile string,
	certTTL time.Duration,
) (*CSRSigningController, error) {
	return NewCSRSigningController(ctx, "csrsigning-kubelet-serving", capi.KubeletServingSignerName, client, csrInformer, caFile, caKeyFile, certTTL)
}

// NewKubeletClientCSRSigningController 创建用于 kubelet 客户端证书的签名控制器，
// 对应 signerName 为 kubernetes.io/kube-apiserver-client-kubelet。
func NewKubeletClientCSRSigningController(
	ctx context.Context,
	client clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	caFile, caKeyFile string,
	certTTL time.Duration,
) (*CSRSigningController, error) {
	return NewCSRSigningController(ctx, "csrsigning-kubelet-client", capi.KubeAPIServerClientKubeletSignerName, client, csrInformer, caFile, caKeyFile, certTTL)
}

// NewKubeAPIServerClientCSRSigningController 创建用于 apiserver 客户端证书的签名控制器，
// 对应 signerName 为 kubernetes.io/kube-apiserver-client。
func NewKubeAPIServerClientCSRSigningController(
	ctx context.Context,
	client clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	caFile, caKeyFile string,
	certTTL time.Duration,
) (*CSRSigningController, error) {
	return NewCSRSigningController(ctx, "csrsigning-kube-apiserver-client", capi.KubeAPIServerClientSignerName, client, csrInformer, caFile, caKeyFile, certTTL)
}

// NewLegacyUnknownCSRSigningController 创建遗留 unknown signer 的签名控制器，
// 对应 signerName 为 kubernetes.io/legacy-unknown，保持对 v1 的向后兼容。
func NewLegacyUnknownCSRSigningController(
	ctx context.Context,
	client clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	caFile, caKeyFile string,
	certTTL time.Duration,
) (*CSRSigningController, error) {
	return NewCSRSigningController(ctx, "csrsigning-legacy-unknown", capiv1beta1.LegacyUnknownSignerName, client, csrInformer, caFile, caKeyFile, certTTL)
}

// NewCSRSigningController 根据 signerName 构造签名器并创建控制器实例，
// 将 signer.handle 作为通用证书控制器的处理函数。
func NewCSRSigningController(
	ctx context.Context,
	controllerName string,
	signerName string,
	client clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	caFile, caKeyFile string,
	certTTL time.Duration,
) (*CSRSigningController, error) {
	signer, err := newSigner(signerName, caFile, caKeyFile, client, certTTL)
	if err != nil {
		return nil, err
	}

	return &CSRSigningController{
		certificateController: certificates.NewCertificateController(
			ctx,
			controllerName,
			client,
			csrInformer,
			signer.handle,
		),
		dynamicCertReloader: signer.caProvider.caLoader,
	}, nil
}

// Run 同时启动 CA 证书重载器与 CSR 处理控制器，并等待两者退出。
func (c *CSRSigningController) Run(ctx context.Context, workers int) {
	var wg sync.WaitGroup
	wg.Go(func() {
		c.dynamicCertReloader.Run(ctx, workers)
	})
	wg.Go(func() {
		c.certificateController.Run(ctx, workers)
	})
	wg.Wait()
}

// isRequestForSignerFunc 校验 CSR 是否属于某个 signer 以及其属性是否合法：
// 第一个返回值表示是否由该 signer 处理，第二个返回校验错误。
type isRequestForSignerFunc func(req *x509.CertificateRequest, usages []capi.KeyUsage, signerName string) (bool, error)

// signer 实现单个 signerName 的 CSR 校验与签名逻辑。
type signer struct {
	caProvider *caProvider

	client  clientset.Interface
	certTTL time.Duration // 证书最大有效期；请求可通过 spec.expirationSeconds 申请更短的证书

	signerName           string
	isRequestForSignerFn isRequestForSignerFunc
}

// newSigner 根据 signerName 选择对应的校验函数，并加载本地 CA 证书与密钥。
func newSigner(signerName, caFile, caKeyFile string, client clientset.Interface, certificateDuration time.Duration) (*signer, error) {
	isRequestForSignerFn, err := getCSRVerificationFuncForSignerName(signerName)
	if err != nil {
		return nil, err
	}
	caProvider, err := newCAProvider(caFile, caKeyFile)
	if err != nil {
		return nil, err
	}

	ret := &signer{
		caProvider:           caProvider,
		client:               client,
		certTTL:              certificateDuration,
		signerName:           signerName,
		isRequestForSignerFn: isRequestForSignerFn,
	}
	return ret, nil
}

// handle 处理单个 CSR：跳过未批准、已失败或不属于本 signer 的请求；
// 校验通过后签发证书并写回 CSR 状态。
// 校验类错误通过 Failed Condition 反馈给用户，而不是触发反复重试。
func (s *signer) handle(ctx context.Context, csr *capi.CertificateSigningRequest) error {
	// 仅处理已批准且未失败的请求，避免对终态 CSR 做无谓操作。
	if !certificates.IsCertificateRequestApproved(csr) || certificates.HasTrueCondition(csr, capi.CertificateFailed) {
		return nil
	}

	// 快速路径：signerName 不匹配时直接跳过，多个签名控制器共享同一个 CSR informer。
	if csr.Spec.SignerName != s.signerName {
		return nil
	}

	x509cr, err := capihelper.ParseCSR(csr.Spec.Request)
	if err != nil {
		return fmt.Errorf("unable to parse csr %q: %v", csr.Name, err)
	}
	if recognized, err := s.isRequestForSignerFn(x509cr, csr.Spec.Usages, csr.Spec.SignerName); err != nil {
		csr.Status.Conditions = append(csr.Status.Conditions, capi.CertificateSigningRequestCondition{
			Type:           capi.CertificateFailed,
			Status:         v1.ConditionTrue,
			Reason:         "SignerValidationFailure",
			Message:        err.Error(),
			LastUpdateTime: metav1.Now(),
		})
		_, err = s.client.CertificatesV1().CertificateSigningRequests().UpdateStatus(ctx, csr, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("error adding failure condition for csr: %v", err)
		}
		return nil
	} else if !recognized {
		// 其他 Kubernetes 内置 signer 由各自的控制器处理，这里忽略。
		return nil
	}
	cert, err := s.sign(x509cr, csr.Spec.Usages, csr.Spec.ExpirationSeconds, nil)
	if err != nil {
		return fmt.Errorf("error auto signing csr: %v", err)
	}
	csr.Status.Certificate = cert
	_, err = s.client.CertificatesV1().CertificateSigningRequests().UpdateStatus(ctx, csr, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating signature for csr: %v", err)
	}
	return nil
}

// sign 使用当前 CA 对 CSR 签名并返回 PEM 编码的证书。
// 参数 now 便于测试注入固定时间。
func (s *signer) sign(x509cr *x509.CertificateRequest, usages []capi.KeyUsage, expirationSeconds *int32, now func() time.Time) ([]byte, error) {
	currCA, err := s.caProvider.currentCA()
	if err != nil {
		return nil, err
	}
	der, err := currCA.Sign(x509cr.Raw, authority.PermissiveSigningPolicy{
		TTL:      s.duration(expirationSeconds),
		Usages:   usages,
		Backdate: 5 * time.Minute, // 必须小于用户可申请的最小 TTL（见下方 duration 中的下限检查）
		Short:    8 * time.Hour,   // 5 分钟回溯大约占 8 小时的 1%，用于容忍时钟偏差
		Now:      now,
	})
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

// duration 计算证书有效期：遵循请求的 expirationSeconds，但限定在 [10 分钟, certTTL] 区间。
// 10 分钟下限是回溯时间（5 分钟）的两倍，防止回溯导致证书刚签发即接近过期。
func (s *signer) duration(expirationSeconds *int32) time.Duration {
	if expirationSeconds == nil {
		return s.certTTL
	}

	// 仅在请求的有效期小于默认 TTL 时才采纳请求值；
	// 10 分钟作为合理性下限，是上面硬编码回溯时间的两倍。
	const min = 10 * time.Minute
	switch requestedDuration := csr.ExpirationSecondsToDuration(*expirationSeconds); {
	case requestedDuration > s.certTTL:
		return s.certTTL

	case requestedDuration < min:
		return min

	default:
		return requestedDuration
	}
}

// getCSRVerificationFuncForSignerName 集中维护 signerName 到校验函数的映射，
// 避免后续接线时出现错配。
func getCSRVerificationFuncForSignerName(signerName string) (isRequestForSignerFunc, error) {
	switch signerName {
	case capi.KubeletServingSignerName:
		return isKubeletServing, nil
	case capi.KubeAPIServerClientKubeletSignerName:
		return isKubeletClient, nil
	case capi.KubeAPIServerClientSignerName:
		return isKubeAPIServerClient, nil
	case capiv1beta1.LegacyUnknownSignerName:
		return isLegacyUnknown, nil
	default:
		// TODO：为未知 kube signer 定义单独的错误类型，使无签名证书的上报循环能将这些 CSR 标记为终态；
		// 是否这样做取决于对 signerNames 的控制需求。
		return nil, fmt.Errorf("unrecognized signerName: %q", signerName)
	}
}

// isKubeletServing 校验 kubelet serving 证书的 CSR 属性。
func isKubeletServing(req *x509.CertificateRequest, usages []capi.KeyUsage, signerName string) (bool, error) {
	if signerName != capi.KubeletServingSignerName {
		return false, nil
	}
	return true, capihelper.ValidateKubeletServingCSR(req, usagesToSet(usages))
}

// isKubeletClient 校验 kubelet 客户端证书的 CSR 属性。
func isKubeletClient(req *x509.CertificateRequest, usages []capi.KeyUsage, signerName string) (bool, error) {
	if signerName != capi.KubeAPIServerClientKubeletSignerName {
		return false, nil
	}
	return true, capihelper.ValidateKubeletClientCSR(req, usagesToSet(usages))
}

// isKubeAPIServerClient 校验 apiserver 客户端证书的 CSR 属性，
// 此处只检查 usages，不检查证书请求内容。
func isKubeAPIServerClient(req *x509.CertificateRequest, usages []capi.KeyUsage, signerName string) (bool, error) {
	if signerName != capi.KubeAPIServerClientSignerName {
		return false, nil
	}
	return true, validAPIServerClientUsages(usages)
}

// isLegacyUnknown 识别遗留 unknown signer。
func isLegacyUnknown(req *x509.CertificateRequest, usages []capi.KeyUsage, signerName string) (bool, error) {
	if signerName != capiv1beta1.LegacyUnknownSignerName {
		return false, nil
	}
	// 为保持 v1 向后兼容，legacy-unknown signer 不施加任何限制。
	return true, nil
}

// validAPIServerClientUsages 校验 apiserver 客户端证书的 usages：
// client auth 必填，digital signature 与 key encipherment 可选，其余均视为非法。
func validAPIServerClientUsages(usages []capi.KeyUsage) error {
	hasClientAuth := false
	for _, u := range usages {
		switch u {
		// 这两个 usages 是可选的。
		case capi.UsageDigitalSignature, capi.UsageKeyEncipherment:
		case capi.UsageClientAuth:
			hasClientAuth = true
		default:
			return fmt.Errorf("invalid usage for client certificate: %s", u)
		}
	}
	if !hasClientAuth {
		return fmt.Errorf("missing required usage for client certificate: %s", capi.UsageClientAuth)
	}
	return nil
}

// usagesToSet 将 usages 切片转换为集合，便于校验函数做成员判断。
func usagesToSet(usages []capi.KeyUsage) sets.String {
	result := sets.NewString()
	for _, usage := range usages {
		result.Insert(string(usage))
	}
	return result
}
