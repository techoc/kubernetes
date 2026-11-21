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
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"math/big"
)

var serialNumberLimit = new(big.Int).Lsh(big.NewInt(1), 128)

// CertificateAuthority 实现了一个支持基于策略签名的证书颁发机构（CA），
// 由签名控制器使用。
type CertificateAuthority struct {
	// RawCert 是可选字段，用于检测签名用的证书/密钥对是否发生变化。
	RawCert []byte
	// RawKey 是可选字段，用于检测签名用的证书/密钥对是否发生变化。
	RawKey []byte

	Certificate *x509.Certificate
	PrivateKey  crypto.Signer
}

// Sign 对证书请求进行签名：先解析并校验请求自身的签名，再应用 SigningPolicy
// 生成证书模板，最后用 CA 私钥签发，返回 DER 编码的 X.509 证书。
// 校验请求签名是必要的：否则持有私钥之外身份的请求者可以伪造 CSR 中的公钥和主体。
func (ca *CertificateAuthority) Sign(crDER []byte, policy SigningPolicy) ([]byte, error) {
	cr, err := x509.ParseCertificateRequest(crDER)
	if err != nil {
		return nil, fmt.Errorf("unable to parse certificate request: %v", err)
	}
	// 校验 CSR 签名，确认请求者确实持有对应私钥，防止伪造主体或公钥。
	if err := cr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("unable to verify certificate request signature: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("unable to generate a serial number for %s: %v", cr.Subject.CommonName, err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:       serialNumber,
		Subject:            cr.Subject,
		DNSNames:           cr.DNSNames,
		IPAddresses:        cr.IPAddresses,
		EmailAddresses:     cr.EmailAddresses,
		URIs:               cr.URIs,
		PublicKeyAlgorithm: cr.PublicKeyAlgorithm,
		PublicKey:          cr.PublicKey,
		Extensions:         cr.Extensions,
		ExtraExtensions:    cr.ExtraExtensions,
	}
	// 策略决定有效期和用途等约束；签发时间不得超过 CA 证书自身的有效期。
	if err := policy.apply(tmpl, ca.Certificate.NotAfter); err != nil {
		return nil, err
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Certificate, cr.PublicKey, ca.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign certificate: %v", err)
	}
	return der, nil
}
