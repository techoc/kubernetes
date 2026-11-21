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

package authority

import (
	"crypto/x509"
	"fmt"
	"sort"
	"time"

	capi "k8s.io/api/certificates/v1"
)

// SigningPolicy 在 CertificateAuthority 签发证书之前校验 CertificateRequest，
// 可以对证书模板做默认值填充或其他修改。
type SigningPolicy interface {
	// apply 未导出，强制签名策略的实现只能位于本包内部。
	apply(template *x509.Certificate, signerNotAfter time.Time) error
}

// PermissiveSigningPolicy 是本地签名器历史上一直使用的签名策略。
//
//   - 透传原始签名请求中的所有 SAN。
//   - 按策略配置设置允许的用途。
//   - 清空所有扩展。
//   - 将 BasicConstraints 设为 true。
//   - 将 IsCA 设为 false。
//   - 校验签名者尚未过期。
//   - 设置 NotBefore 与 NotAfter：
//     所有证书 NotBefore = Now() - Backdate。
//     长有效期证书 NotAfter = Now() + TTL - Backdate。
//     短有效期证书 NotAfter = Now() + TTL。
//     所有证书的 NotAfter 都会被截断到签名者的到期时间。
type PermissiveSigningPolicy struct {
	// TTL 用于按上述规则计算证书的 NotAfter。
	TTL time.Duration

	// Usages 是证书被允许使用的用途列表。
	Usages []capi.KeyUsage

	// Backdate 用于按上述规则回拨证书的 NotBefore。
	Backdate time.Duration

	// Short 是用于判断证书有效期是否算“短”的阈值。
	Short time.Duration

	// Now 默认为 time.Now，测试中可替换为桩函数以固定时间。
	Now func() time.Time
}

// apply 按 PermissiveSigningPolicy 的规则修改证书模板，并在签名者已过期时返回错误。
func (p PermissiveSigningPolicy) apply(tmpl *x509.Certificate, signerNotAfter time.Time) error {
	var now time.Time
	if p.Now != nil {
		now = p.Now()
	} else {
		now = time.Now()
	}

	ttl := p.TTL

	usage, extUsages, err := keyUsagesFromStrings(p.Usages)
	if err != nil {
		return err
	}
	tmpl.KeyUsage = usage
	tmpl.ExtKeyUsage = extUsages

	// 清空请求自带的扩展，避免请求方注入不受控的扩展内容。
	tmpl.ExtraExtensions = nil
	tmpl.Extensions = nil
	tmpl.BasicConstraintsValid = true
	tmpl.IsCA = false

	// 回拨 NotBefore 以容忍时钟偏差。
	tmpl.NotBefore = now.Add(-p.Backdate)

	if ttl < p.Short {
		// 对短有效期证书不回拨结束时间，否则会不必要地缩短本就不长的有效期。
		tmpl.NotAfter = now.Add(ttl)
	} else {
		tmpl.NotAfter = now.Add(ttl - p.Backdate)
	}

	// 证书有效期不能超过签名者自身的有效期。
	if !tmpl.NotAfter.Before(signerNotAfter) {
		tmpl.NotAfter = signerNotAfter
	}

	if !tmpl.NotBefore.Before(signerNotAfter) {
		return fmt.Errorf("the signer has expired: NotAfter=%v", signerNotAfter)
	}

	if !now.Before(signerNotAfter) {
		return fmt.Errorf("refusing to sign a certificate that expired in the past: NotAfter=%v", signerNotAfter)
	}

	return nil
}

// keyUsageDict 将 certificates API 中的用途值映射到 x509 的普通 KeyUsage。
var keyUsageDict = map[capi.KeyUsage]x509.KeyUsage{
	capi.UsageSigning:           x509.KeyUsageDigitalSignature,
	capi.UsageDigitalSignature:  x509.KeyUsageDigitalSignature,
	capi.UsageContentCommitment: x509.KeyUsageContentCommitment,
	capi.UsageKeyEncipherment:   x509.KeyUsageKeyEncipherment,
	capi.UsageKeyAgreement:      x509.KeyUsageKeyAgreement,
	capi.UsageDataEncipherment:  x509.KeyUsageDataEncipherment,
	capi.UsageCertSign:          x509.KeyUsageCertSign,
	capi.UsageCRLSign:           x509.KeyUsageCRLSign,
	capi.UsageEncipherOnly:      x509.KeyUsageEncipherOnly,
	capi.UsageDecipherOnly:      x509.KeyUsageDecipherOnly,
}

// extKeyUsageDict 将 certificates API 中的用途值映射到 x509 的扩展用途 ExtKeyUsage。
var extKeyUsageDict = map[capi.KeyUsage]x509.ExtKeyUsage{
	capi.UsageAny:             x509.ExtKeyUsageAny,
	capi.UsageServerAuth:      x509.ExtKeyUsageServerAuth,
	capi.UsageClientAuth:      x509.ExtKeyUsageClientAuth,
	capi.UsageCodeSigning:     x509.ExtKeyUsageCodeSigning,
	capi.UsageEmailProtection: x509.ExtKeyUsageEmailProtection,
	capi.UsageSMIME:           x509.ExtKeyUsageEmailProtection,
	capi.UsageIPsecEndSystem:  x509.ExtKeyUsageIPSECEndSystem,
	capi.UsageIPsecTunnel:     x509.ExtKeyUsageIPSECTunnel,
	capi.UsageIPsecUser:       x509.ExtKeyUsageIPSECUser,
	capi.UsageTimestamping:    x509.ExtKeyUsageTimeStamping,
	capi.UsageOCSPSigning:     x509.ExtKeyUsageOCSPSigning,
	capi.UsageMicrosoftSGC:    x509.ExtKeyUsageMicrosoftServerGatedCrypto,
	capi.UsageNetscapeSGC:     x509.ExtKeyUsageNetscapeServerGatedCrypto,
}

// keyUsagesFromStrings 把 certificates API 中的用途字符串切片
// （"pkg/apis/certificates".KeyUsage）转换为 x509.KeyUsage 与 x509.ExtKeyUsage。
// 无法识别的用途会汇总成一个错误返回。
func keyUsagesFromStrings(usages []capi.KeyUsage) (x509.KeyUsage, []x509.ExtKeyUsage, error) {
	var keyUsage x509.KeyUsage
	var unrecognized []capi.KeyUsage
	extKeyUsages := make(map[x509.ExtKeyUsage]struct{})
	for _, usage := range usages {
		if val, ok := keyUsageDict[usage]; ok {
			keyUsage |= val
		} else if val, ok := extKeyUsageDict[usage]; ok {
			extKeyUsages[val] = struct{}{}
		} else {
			unrecognized = append(unrecognized, usage)
		}
	}

	// map 遍历顺序不确定，先转成切片再排序，保证输出稳定可测试。
	var sorted sortedExtKeyUsage
	for eku := range extKeyUsages {
		sorted = append(sorted, eku)
	}
	sort.Sort(sorted)

	if len(unrecognized) > 0 {
		return 0, nil, fmt.Errorf("unrecognized usage values: %q", unrecognized)
	}

	return keyUsage, sorted, nil
}

// sortedExtKeyUsage 是可排序的 x509.ExtKeyUsage 切片，实现 sort.Interface。
type sortedExtKeyUsage []x509.ExtKeyUsage

// Len 返回扩展用途切片的长度。
func (s sortedExtKeyUsage) Len() int {
	return len(s)
}

// Swap 交换切片中两个位置的扩展用途。
func (s sortedExtKeyUsage) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less 按枚举值比较两个扩展用途的大小。
func (s sortedExtKeyUsage) Less(i, j int) bool {
	return s[i] < s[j]
}
