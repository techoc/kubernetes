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

package cert

import (
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
)

// MakeCSR generates a PEM-encoded CSR using the supplied private key, subject, and SANs.
// All key types that are implemented via crypto.Signer are supported (This includes *rsa.PrivateKey and *ecdsa.PrivateKey.)
//
// MakeCSR 用给定的私钥、subject 与 SAN 生成 PEM 编码的 CSR。
// 支持所有实现 crypto.Signer 的密钥类型（包括 *rsa.PrivateKey 与 *ecdsa.PrivateKey）。
func MakeCSR(privateKey interface{}, subject *pkix.Name, dnsSANs []string, ipSANs []net.IP) (csr []byte, err error) {
	template := &x509.CertificateRequest{
		Subject:     *subject,
		DNSNames:    dnsSANs,
		IPAddresses: ipSANs,
	}

	return MakeCSRFromTemplate(privateKey, template)
}

// MakeCSRFromTemplate generates a PEM-encoded CSR using the supplied private
// key and certificate request as a template. All key types that are
// implemented via crypto.Signer are supported (This includes *rsa.PrivateKey
// and *ecdsa.PrivateKey.)
//
// MakeCSRFromTemplate 以给定的私钥和证书请求模板生成 PEM 编码的 CSR。
func MakeCSRFromTemplate(privateKey interface{}, template *x509.CertificateRequest) ([]byte, error) {
	// 复制模板，避免修改调用方传入的结构。
	t := *template
	t.SignatureAlgorithm = sigType(privateKey)

	csrDER, err := x509.CreateCertificateRequest(cryptorand.Reader, &t, privateKey)
	if err != nil {
		return nil, err
	}

	csrPemBlock := &pem.Block{
		Type:  CertificateRequestBlockType,
		Bytes: csrDER,
	}

	return pem.EncodeToMemory(csrPemBlock), nil
}

// sigType 根据私钥类型与长度选择签名算法。
// RSA 按密钥长度选择更强的摘要算法；其他类型交给标准库按默认规则处理。
func sigType(privateKey interface{}) x509.SignatureAlgorithm {
	// Customize the signature for RSA keys, depending on the key size
	if privateKey, ok := privateKey.(*rsa.PrivateKey); ok {
		keySize := privateKey.N.BitLen()
		switch {
		case keySize >= 4096:
			return x509.SHA512WithRSA
		case keySize >= 3072:
			return x509.SHA384WithRSA
		default:
			return x509.SHA256WithRSA
		}
	}
	return x509.UnknownSignatureAlgorithm
}
