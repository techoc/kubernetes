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

package renewal

import (
	"crypto/x509"
	"time"
)

// ExpirationInfo 定义证书的过期信息。
type ExpirationInfo struct {
	// 证书名称。
	// PKI 证书使用 certsphase 包中定义的名称；嵌入 kubeConfig 文件的证书使用 kubeadm 常量包中定义的文件名。
	// 如果使用 Certificates 函数返回的 CertificateRenewHandler，handler.Name 已是正确的值。
	Name string

	// ExpirationDate 定义证书过期日期。
	ExpirationDate time.Time

	// ExternallyManaged 表示证书是否由外部管理，即只提供签名 CA 证书而不提供其私钥。
	// 此时 kubeadm 无法续签该证书。
	ExternallyManaged bool
}

// newExpirationInfo 返回新的 ExpirationInfo。
func newExpirationInfo(name string, cert *x509.Certificate, externallyManaged bool) *ExpirationInfo {
	return &ExpirationInfo{
		Name:              name,
		ExpirationDate:    cert.NotAfter,
		ExternallyManaged: externallyManaged,
	}
}

// ResidualTime 返回距离过期的剩余时间。
func (e *ExpirationInfo) ResidualTime() time.Duration {
	return time.Until(e.ExpirationDate)
}
