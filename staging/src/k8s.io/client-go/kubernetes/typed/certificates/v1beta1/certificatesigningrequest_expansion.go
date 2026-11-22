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

package v1beta1

import (
	"context"

	certificates "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	scheme "k8s.io/client-go/kubernetes/scheme"
)

// CertificateSigningRequestExpansion 补充生成代码之外的手写方法，
// 通过嵌入该接口让 CertificateSigningRequestInterface 暴露出这些方法。
type CertificateSigningRequestExpansion interface {
	UpdateApproval(ctx context.Context, certificateSigningRequest *certificates.CertificateSigningRequest, opts metav1.UpdateOptions) (result *certificates.CertificateSigningRequest, err error)
}

// UpdateApproval 通过 PUT 请求更新 CSRS 的 approval 子资源。
// 审批结果不能走普通 Update：apiserver 只允许通过子资源修改 conditions，
// 因此这里显式指定 SubResource("approval")。传入的整个对象作为请求体。
func (c *certificateSigningRequests) UpdateApproval(ctx context.Context, certificateSigningRequest *certificates.CertificateSigningRequest, opts metav1.UpdateOptions) (result *certificates.CertificateSigningRequest, err error) {
	result = &certificates.CertificateSigningRequest{}
	err = c.GetClient().Put().
		Resource("certificatesigningrequests").
		Name(certificateSigningRequest.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(certificateSigningRequest).
		SubResource("approval").
		Do(ctx).
		Into(result)
	return
}
