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

// Package csr 封装证书签名请求（CSR）的创建与等待签发流程，供需要自行申请
// 客户端/服务端证书的组件（如 kubelet、kube-proxy）使用。
//
// 兼容性：优先使用 certificates.k8s.io/v1，API 不存在时回退到 v1beta1。
package csr

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"reflect"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// RequestCertificate 复用已有 CSR（若本进程曾运行但未完成）或用给定 PEM 编码 CSR
// 新建请求并发送给 API Server。可选的 requestedDuration 用于指定证书有效期。
// 设置 CSR 的 spec.expirationSeconds 以控制签发证书的有效期。但这不保证生效——
// signer 可以选择忽略该请求。
//
// Deprecated: use RequestCertificateWithContext instead.
//
// 已弃用：无法传入 context，无法取消/传递日志上下文；请使用 RequestCertificateWithContext。
func RequestCertificate(client clientset.Interface, csrData []byte, name, signerName string, requestedDuration *time.Duration, usages []certificatesv1.KeyUsage, privateKey interface{}) (reqName string, reqUID types.UID, err error) {
	return RequestCertificateWithContext(context.Background(), client, csrData, name, signerName, requestedDuration, usages, privateKey)
}

// RequestCertificateWithContext 复用已有 CSR（若本进程曾运行但未完成）或用给定 PEM
// 编码 CSR 新建请求并发送给 API Server。可选的 requestedDuration 用于指定证书有效期。
// 设置 CSR 的 spec.expirationSeconds 以控制签发证书的有效期。但这不保证生效——
// signer 可以选择忽略该请求。
func RequestCertificateWithContext(ctx context.Context, client clientset.Interface, csrData []byte, name, signerName string, requestedDuration *time.Duration, usages []certificatesv1.KeyUsage, privateKey interface{}) (reqName string, reqUID types.UID, err error) {
	csr := &certificatesv1.CertificateSigningRequest{
		// Username、UID、Groups 由 API Server 注入。
		TypeMeta: metav1.TypeMeta{Kind: "CertificateSigningRequest"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    csrData,
			Usages:     usages,
			SignerName: signerName,
		},
	}
	// 未指定名字时由 server 生成，便于并发创建多个请求。
	if len(csr.Name) == 0 {
		csr.GenerateName = "csr-"
	}
	if requestedDuration != nil {
		csr.Spec.ExpirationSeconds = DurationToExpirationSeconds(*requestedDuration)
	}

	logger := klog.FromContext(ctx)

	reqName, reqUID, err = create(ctx, client, csr)
	switch {
	case err == nil:
		logger.V(2).Info("Certificate signing request created", "csr", reqName)
		return reqName, reqUID, err

	case apierrors.IsAlreadyExists(err) && len(name) > 0:
		// 进程重启后复用上次未完成的同名 CSR，避免反复堆积；但必须确认请求内容
		// 与当前私钥/主题一致，否则重签会失败或产生错误身份。
		logger.Info("csr for this node already exists, reusing")
		req, err := get(ctx, client, name)
		if err != nil {
			return "", "", formatError("cannot retrieve certificate signing request: %v", err)
		}
		if err := ensureCompatible(req, csr, privateKey); err != nil {
			return "", "", fmt.Errorf("retrieved csr is not compatible: %v", err)
		}
		logger.Info("csr for this node is still valid")
		return req.Name, req.UID, nil

	default:
		return "", "", formatError("cannot create certificate signing request: %v", err)
	}
}

// DurationToExpirationSeconds 把时长转换为 CSR 的 expirationSeconds（整秒）。
func DurationToExpirationSeconds(duration time.Duration) *int32 {
	return ptr.To(int32(duration / time.Second))
}

// ExpirationSecondsToDuration 是 DurationToExpirationSeconds 的逆运算。
func ExpirationSecondsToDuration(expirationSeconds int32) time.Duration {
	return time.Duration(expirationSeconds) * time.Second
}

// get 读取 CSR，v1 不存在时回退 v1beta1 并做字段级转换。
func get(ctx context.Context, client clientset.Interface, name string) (*certificatesv1.CertificateSigningRequest, error) {
	v1req, v1err := client.CertificatesV1().CertificateSigningRequests().Get(ctx, name, metav1.GetOptions{})
	if v1err == nil || !apierrors.IsNotFound(v1err) {
		return v1req, v1err
	}

	v1beta1req, v1beta1err := client.CertificatesV1beta1().CertificateSigningRequests().Get(ctx, name, metav1.GetOptions{})
	if v1beta1err != nil {
		return nil, v1beta1err
	}

	// 只搬运判断兼容性所需的字段；这里不需要完整转换。
	v1req = &certificatesv1.CertificateSigningRequest{
		ObjectMeta: v1beta1req.ObjectMeta,
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request: v1beta1req.Spec.Request,
		},
	}
	if v1beta1req.Spec.SignerName != nil {
		v1req.Spec.SignerName = *v1beta1req.Spec.SignerName
	}
	for _, usage := range v1beta1req.Spec.Usages {
		v1req.Spec.Usages = append(v1req.Spec.Usages, certificatesv1.KeyUsage(usage))
	}
	return v1req, nil
}

// create 优先走 v1；若 API 不存在或使用 legacy-unknown signer，则回退 v1beta1。
func create(ctx context.Context, client clientset.Interface, csr *certificatesv1.CertificateSigningRequest) (reqName string, reqUID types.UID, err error) {
	// 只有在指定了 signerName 与 usages、且没有使用 legacy-unknown signerName 时才尝试走 v1 创建：
	// v1 要求 signerName/usages 必填，且 legacy-unknown 只在 v1beta1 中可表达。
	if len(csr.Spec.Usages) > 0 && len(csr.Spec.SignerName) > 0 && csr.Spec.SignerName != "kubernetes.io/legacy-unknown" {
		v1req, v1err := client.CertificatesV1().CertificateSigningRequests().Create(ctx, csr, metav1.CreateOptions{})
		switch {
		case v1err != nil && apierrors.IsNotFound(v1err):
			// 未找到 v1 CSR API，继续尝试 v1beta1。

		case v1err != nil:
			// 其他创建错误。
			return "", "", v1err

		default:
			// success
			return v1req.Name, v1req.UID, v1err
		}
	}

	// 把相关字段转换为 v1beta1 表示。v1beta1 的 SignerName 是可选指针，必须显式取地址。
	v1beta1csr := &certificatesv1beta1.CertificateSigningRequest{
		ObjectMeta: csr.ObjectMeta,
		Spec: certificatesv1beta1.CertificateSigningRequestSpec{
			SignerName: &csr.Spec.SignerName,
			Request:    csr.Spec.Request,
		},
	}
	for _, usage := range csr.Spec.Usages {
		v1beta1csr.Spec.Usages = append(v1beta1csr.Spec.Usages, certificatesv1beta1.KeyUsage(usage))
	}

	// create v1beta1
	v1beta1req, v1beta1err := client.CertificatesV1beta1().CertificateSigningRequests().Create(ctx, v1beta1csr, metav1.CreateOptions{})
	if v1beta1err != nil {
		return "", "", v1beta1err
	}
	return v1beta1req.Name, v1beta1req.UID, nil
}

// WaitForCertificate 等待证书签发直到超时，或返回错误。
func WaitForCertificate(ctx context.Context, client clientset.Interface, reqName string, reqUID types.UID) (certData []byte, err error) {
	fieldSelector := fields.OneTermEqualSelector("metadata.name", reqName).String()
	logger := klog.FromContext(ctx)

	// 先探测可用 API 版本再建立 watch；期间以固定间隔重试，直到成功或 ctx 结束。
	var lw cache.ListerWatcher
	var obj runtime.Object
	for {
		// 查看 v1 API 是否可用。
		if _, err := client.CertificatesV1().CertificateSigningRequests().List(ctx, metav1.ListOptions{FieldSelector: fieldSelector}); err == nil {
			// watch v1 objects
			obj = &certificatesv1.CertificateSigningRequest{}
			lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					options.FieldSelector = fieldSelector
					return client.CertificatesV1().CertificateSigningRequests().List(ctx, options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					options.FieldSelector = fieldSelector
					return client.CertificatesV1().CertificateSigningRequests().Watch(ctx, options)
				},
			}, client)
			break
		} else {
			logger.V(2).Info("Error fetching v1 certificate signing request", "err", err)
		}

		// 超时则返回。
		if err := ctx.Err(); err != nil {
			return nil, wait.ErrorInterrupted(nil)
		}

		// 查看 v1beta1 API 是否可用。
		if _, err := client.CertificatesV1beta1().CertificateSigningRequests().List(ctx, metav1.ListOptions{FieldSelector: fieldSelector}); err == nil {
			// 监听 v1beta1 对象。
			obj = &certificatesv1beta1.CertificateSigningRequest{}
			lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					options.FieldSelector = fieldSelector
					return client.CertificatesV1beta1().CertificateSigningRequests().List(ctx, options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					options.FieldSelector = fieldSelector
					return client.CertificatesV1beta1().CertificateSigningRequests().Watch(ctx, options)
				},
			}, client)
			break
		} else {
			logger.V(2).Info("Error fetching v1beta1 certificate signing request", "err", err)
		}

		// 超时则返回。
		if err := ctx.Err(); err != nil {
			return nil, wait.ErrorInterrupted(nil)
		}

		// 等待后重试。
		time.Sleep(time.Second)
	}

	var issuedCertificate []byte
	_, err = watchtools.UntilWithSync(
		ctx,
		lw,
		obj,
		nil,
		func(event watch.Event) (bool, error) {
			switch event.Type {
			case watch.Modified, watch.Added:
			case watch.Deleted:
				return false, fmt.Errorf("csr %q was deleted", reqName)
			default:
				return false, nil
			}

			switch csr := event.Object.(type) {
			case *certificatesv1.CertificateSigningRequest:
				// UID 变化说明同名对象被删除重建，旧请求的签发结果不能复用。
				if csr.UID != reqUID {
					return false, fmt.Errorf("csr %q changed UIDs", csr.Name)
				}
				approved := false
				for _, c := range csr.Status.Conditions {
					if c.Type == certificatesv1.CertificateDenied {
						return false, fmt.Errorf("certificate signing request is denied, reason: %v, message: %v", c.Reason, c.Message)
					}
					if c.Type == certificatesv1.CertificateFailed {
						return false, fmt.Errorf("certificate signing request failed, reason: %v, message: %v", c.Reason, c.Message)
					}
					if c.Type == certificatesv1.CertificateApproved {
						approved = true
					}
				}
				if approved {
					if len(csr.Status.Certificate) > 0 {
						logger.V(2).Info("Certificate signing request is issued", "csr", klog.KObj(csr))
						issuedCertificate = csr.Status.Certificate
						return true, nil
					}
					logger.V(2).Info("Certificate signing request is approved, waiting to be issued", "csr", klog.KObj(csr))
				}

			case *certificatesv1beta1.CertificateSigningRequest:
				if csr.UID != reqUID {
					return false, fmt.Errorf("csr %q changed UIDs", csr.Name)
				}
				approved := false
				for _, c := range csr.Status.Conditions {
					if c.Type == certificatesv1beta1.CertificateDenied {
						return false, fmt.Errorf("certificate signing request is denied, reason: %v, message: %v", c.Reason, c.Message)
					}
					if c.Type == certificatesv1beta1.CertificateFailed {
						return false, fmt.Errorf("certificate signing request failed, reason: %v, message: %v", c.Reason, c.Message)
					}
					if c.Type == certificatesv1beta1.CertificateApproved {
						approved = true
					}
				}
				if approved {
					if len(csr.Status.Certificate) > 0 {
						logger.V(2).Info("Certificate signing request is issued", "csr", klog.KObj(csr))
						issuedCertificate = csr.Status.Certificate
						return true, nil
					}
					logger.V(2).Info("Certificate signing request is approved, waiting to be issued", "csr", klog.KObj(csr))
				}

			default:
				return false, fmt.Errorf("unexpected type received: %T", event.Object)
			}

			return false, nil
		},
	)
	// 把 ctx 超时/取消统一为社会约定的 Interrupted 错误，便于调用方判断。
	if wait.Interrupted(err) {
		return nil, wait.ErrorInterrupted(nil)
	}
	if err != nil {
		return nil, formatError("cannot watch on the certificate signing request: %v", err)
	}

	return issuedCertificate, nil
}

// ensureCompatible 确保 CSR 对象与原始 CSR 兼容。
func ensureCompatible(new, orig *certificatesv1.CertificateSigningRequest, privateKey interface{}) error {
	newCSR, err := parseCSR(new.Spec.Request)
	if err != nil {
		return fmt.Errorf("unable to parse new csr: %v", err)
	}
	origCSR, err := parseCSR(orig.Spec.Request)
	if err != nil {
		return fmt.Errorf("unable to parse original csr: %v", err)
	}
	// 主题或 signerName 不一致意味着复用会得到不同身份的证书，必须拒绝。
	if !reflect.DeepEqual(newCSR.Subject, origCSR.Subject) {
		return fmt.Errorf("csr subjects differ: new: %#v, orig: %#v", newCSR.Subject, origCSR.Subject)
	}
	if len(new.Spec.SignerName) > 0 && len(orig.Spec.SignerName) > 0 && new.Spec.SignerName != orig.Spec.SignerName {
		return fmt.Errorf("csr signerNames differ: new %q, orig: %q", new.Spec.SignerName, orig.Spec.SignerName)
	}
	signer, ok := privateKey.(crypto.Signer)
	if !ok {
		return fmt.Errorf("privateKey is not a signer")
	}
	newCSR.PublicKey = signer.Public()
	// 用当前私钥验证原 CSR 的签名，确认 CSR 确实由本进程密钥生成。
	if err := newCSR.CheckSignature(); err != nil {
		return fmt.Errorf("error validating signature new CSR against old key: %v", err)
	}
	if len(new.Status.Certificate) > 0 {
		certs, err := certutil.ParseCertsPEM(new.Status.Certificate)
		if err != nil {
			return fmt.Errorf("error parsing signed certificate for CSR: %v", err)
		}
		now := time.Now()
		for _, cert := range certs {
			if now.After(cert.NotAfter) {
				return fmt.Errorf("one of the certificates for the CSR has expired: %s", cert.NotAfter)
			}
		}
	}
	return nil
}

// formatError 保留 APIStatus 类型（从而保住 Status/Reason 字段），只改写 Message，
// 供上层做 apierrors 判断或展示。它接收单参数格式化字符串，并返回包装后的错误。
func formatError(format string, err error) error {
	if s, ok := err.(apierrors.APIStatus); ok {
		se := &apierrors.StatusError{ErrStatus: s.Status()}
		se.ErrStatus.Message = fmt.Sprintf(format, se.ErrStatus.Message)
		return se
	}
	return fmt.Errorf(format, err)
}

// parseCSR 从 API 对象中提取 CSR 并解码。
func parseCSR(pemData []byte) (*x509.CertificateRequest, error) {
	// 从请求对象中提取 PEM。
	block, _ := pem.Decode(pemData)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("PEM block type must be CERTIFICATE REQUEST")
	}
	return x509.ParseCertificateRequest(block.Bytes)
}
