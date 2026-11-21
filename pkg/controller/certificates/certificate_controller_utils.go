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

package certificates

import (
	certificates "k8s.io/api/certificates/v1"
	v1 "k8s.io/api/core/v1"
)

// IsCertificateRequestApproved 判断证书请求是否已获批：
// 当请求带有 "Approved" 条件且没有任何 "Denied" 条件时返回 true，否则返回 false。
func IsCertificateRequestApproved(csr *certificates.CertificateSigningRequest) bool {
	approved, denied := GetCertApprovalCondition(&csr.Status)
	// 即便存在 Approved 条件，只要同时被 Denied 就视为未获批，审批必须无冲突。
	return approved && !denied
}

// HasTrueCondition 判断 csr 是否包含指定类型的条件，且其状态为 True 或为空。
// 状态为空按 True 处理，是为了兼容早期未显式填写 status 的条件。
func HasTrueCondition(csr *certificates.CertificateSigningRequest, conditionType certificates.RequestConditionType) bool {
	for _, c := range csr.Status.Conditions {
		if c.Type == conditionType && (len(c.Status) == 0 || c.Status == v1.ConditionTrue) {
			return true
		}
	}
	return false
}

// GetCertApprovalCondition 从证书请求状态中提取审批结果。
// 分别返回是否存在 Approved 条件与是否存在 Denied 条件，不解析条件的具体状态。
func GetCertApprovalCondition(status *certificates.CertificateSigningRequestStatus) (approved bool, denied bool) {
	// 审批决策由条件类型本身表达，因此这里只遍历类型，忽略条件的 status 字段。
	for _, c := range status.Conditions {
		if c.Type == certificates.CertificateApproved {
			approved = true
		}
		if c.Type == certificates.CertificateDenied {
			denied = true
		}
	}
	return
}
