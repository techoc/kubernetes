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

// Package approver 实现 kubelet 证书的自动审批。
package approver

import (
	"context"
	"crypto/x509"
	"fmt"

	authorization "k8s.io/api/authorization/v1"
	capi "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	certificatesinformers "k8s.io/client-go/informers/certificates/v1"
	clientset "k8s.io/client-go/kubernetes"
	capihelper "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/controller/certificates"
)

// csrRecognizer 将一类 CSR 的识别函数与其所需的授权属性和审批成功信息关联起来。
type csrRecognizer struct {
	recognize      func(csr *capi.CertificateSigningRequest, x509cr *x509.CertificateRequest) bool
	permission     authorization.ResourceAttributes
	successMessage string
}

// sarApprover 通过 SubjectAccessReview 对识别出的 CSR 进行自动审批。
type sarApprover struct {
	client      clientset.Interface
	recognizers []csrRecognizer
}

// NewCSRApprovingController 创建一个新的 CSRApprovingController。
func NewCSRApprovingController(ctx context.Context, client clientset.Interface, csrInformer certificatesinformers.CertificateSigningRequestInformer) *certificates.CertificateController {
	// 具体的识别与授权逻辑委托给 sarApprover，由通用控制器负责事件循环。
	approver := &sarApprover{
		client:      client,
		recognizers: recognizers(),
	}
	return certificates.NewCertificateController(
		ctx,
		"csrapproving",
		client,
		csrInformer,
		approver.handle,
	)
}

// recognizers 返回全部受支持的 CSR 识别器及其对应的授权规则。
func recognizers() []csrRecognizer {
	recognizers := []csrRecognizer{
		// 自注册场景：kubelet 用自己的身份为自己申请客户端证书。
		{
			recognize:      isSelfNodeClientCert,
			permission:     authorization.ResourceAttributes{Group: "certificates.k8s.io", Resource: "certificatesigningrequests", Verb: "create", Subresource: "selfnodeclient", Version: "*"},
			successMessage: "Auto approving self kubelet client certificate after SubjectAccessReview.",
		},
		// 常规场景：由已有凭据代节点申请客户端证书。
		{
			recognize:      isNodeClientCert,
			permission:     authorization.ResourceAttributes{Group: "certificates.k8s.io", Resource: "certificatesigningrequests", Verb: "create", Subresource: "nodeclient", Version: "*"},
			successMessage: "Auto approving kubelet client certificate after SubjectAccessReview.",
		},
	}
	return recognizers
}

// handle 处理 CSR：跳过已签发或已决定的请求，识别并按权限审批新请求。
func (a *sarApprover) handle(ctx context.Context, csr *capi.CertificateSigningRequest) error {
	// 已签发证书的 CSR 无需再审批。
	if len(csr.Status.Certificate) != 0 {
		return nil
	}
	// 审批状态已确定的 CSR 不做二次处理。
	if approved, denied := certificates.GetCertApprovalCondition(&csr.Status); approved || denied {
		return nil
	}
	x509cr, err := capihelper.ParseCSR(csr.Spec.Request)
	if err != nil {
		return fmt.Errorf("unable to parse csr %q: %v", csr.Name, err)
	}

	// 记录尝试过的子资源，供未获授权时给出可诊断的错误信息。
	tried := []string{}

	for _, r := range a.recognizers {
		if !r.recognize(csr, x509cr) {
			continue
		}

		tried = append(tried, r.permission.Subresource)

		approved, err := a.authorize(ctx, csr, r.permission)
		if err != nil {
			return err
		}
		if approved {
			// 仅在内存中追加审批条件，再由 UpdateApproval 统一提交，避免部分写入。
			appendApprovalCondition(csr, r.successMessage)
			_, err = a.client.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, csr.Name, csr, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("error updating approval for csr: %v", err)
			}
			return nil
		}
	}

	// 识别成功但未被授权属于可忽略错误：不自动审批、也不应让控制器反复重试到退避上限。
	if len(tried) != 0 {
		return certificates.IgnorableError("recognized csr %q as %v but subject access review was not approved", csr.Name, tried)
	}

	// 无法识别的 CSR 交给其他审批者处理。
	return nil
}

// authorize 以 CSR 请求者的身份信息构造 SubjectAccessReview，判断其是否具备指定权限。
func (a *sarApprover) authorize(ctx context.Context, csr *capi.CertificateSigningRequest, rattrs authorization.ResourceAttributes) (bool, error) {
	// 将 CSR 中自定义的 extra 字段转换为授权 API 所需的类型。
	extra := make(map[string]authorization.ExtraValue)
	for k, v := range csr.Spec.Extra {
		extra[k] = authorization.ExtraValue(v)
	}

	// 使用 CSR 声明的用户身份而非控制器身份做审查，防止权限放大。
	sar := &authorization.SubjectAccessReview{
		Spec: authorization.SubjectAccessReviewSpec{
			User:               csr.Spec.Username,
			UID:                csr.Spec.UID,
			Groups:             csr.Spec.Groups,
			Extra:              extra,
			ResourceAttributes: &rattrs,
		},
	}
	sar, err := a.client.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return sar.Status.Allowed, nil
}

// appendApprovalCondition 在 CSR 状态上追加一个 Approved 条件。
func appendApprovalCondition(csr *capi.CertificateSigningRequest, message string) {
	csr.Status.Conditions = append(csr.Status.Conditions, capi.CertificateSigningRequestCondition{
		Type:    capi.CertificateApproved,
		Status:  corev1.ConditionTrue,
		Reason:  "AutoApproved",
		Message: message,
	})
}

// isNodeClientCert 判断 CSR 是否由 kube-apiserver 的 kubelet 客户端签名者签发且符合 kubelet 客户端证书要求。
func isNodeClientCert(csr *capi.CertificateSigningRequest, x509cr *x509.CertificateRequest) bool {
	if csr.Spec.SignerName != capi.KubeAPIServerClientKubeletSignerName {
		return false
	}
	return capihelper.IsKubeletClientCSR(x509cr, usagesToSet(csr.Spec.Usages))
}

// isSelfNodeClientCert 判断是否为 kubelet 以自身身份提交的自签名节点客户端证书请求。
func isSelfNodeClientCert(csr *capi.CertificateSigningRequest, x509cr *x509.CertificateRequest) bool {
	// 用户名与证书 CommonName 一致是"自申请"区别于代申请的关键特征。
	if csr.Spec.Username != x509cr.Subject.CommonName {
		return false
	}
	return isNodeClientCert(csr, x509cr)
}

// usagesToSet 将 CSR 的 usages 列表转换为集合，便于按用途做匹配检查。
func usagesToSet(usages []capi.KeyUsage) sets.String {
	result := sets.NewString()
	for _, usage := range usages {
		result.Insert(string(usage))
	}
	return result
}
