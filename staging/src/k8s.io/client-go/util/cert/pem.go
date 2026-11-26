/*
Copyright 2014 The Kubernetes Authors.

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
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
)

const (
	// CertificateBlockType 是 pem.Block.Type 的一种取值。
	CertificateBlockType = "CERTIFICATE"
	// CertificateRequestBlockType 是 pem.Block.Type 的一种取值。
	CertificateRequestBlockType = "CERTIFICATE REQUEST"
)

// ParseCertsPEM 返回给定 PEM 字节数组中的 x509 证书，解析失败或不含任何证书时返回错误。
//
// 遇到无法解析的证书立即返回已解析部分与错误；完全没有有效证书时也报错，
// 防止调用方拿着空列表继续建立不安全的连接。
func ParseCertsPEM(pemCerts []byte) ([]*x509.Certificate, error) {
	ok := false
	certs := []*x509.Certificate{}
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		// 只接受没有额外头的 PEM "CERTIFICATE" 块。
		if block.Type != CertificateBlockType || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return certs, err
		}

		certs = append(certs, cert)
		ok = true
	}

	if !ok {
		return certs, errors.New("data does not contain any valid RSA or ECDSA certificates")
	}
	return certs, nil
}

// EncodeCertificates 返回指定证书的 PEM 编码字节数组。
func EncodeCertificates(certs ...*x509.Certificate) ([]byte, error) {
	b := bytes.Buffer{}
	for _, cert := range certs {
		// 统一使用标准 "CERTIFICATE" 类型，确保证书链可被通用 PEM 解析器读取。
		if err := pem.Encode(&b, &pem.Block{Type: CertificateBlockType, Bytes: cert.Raw}); err != nil {
			return []byte{}, err
		}
	}
	return b.Bytes(), nil
}
