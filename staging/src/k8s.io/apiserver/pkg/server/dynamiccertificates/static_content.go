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

package dynamiccertificates

import (
	"crypto/tls"
	"crypto/x509"
)

// staticCAContent 是永不变化的 CAContentProvider；AddListener 为空实现，
// 因为不存在“更新事件”可通知。
type staticCAContent struct {
	name     string
	caBundle *caBundleAndVerifier
}

var _ CAContentProvider = &staticCAContent{}

// NewStaticCAContent returns a CAContentProvider that always returns the same value
func NewStaticCAContent(name string, caBundle []byte) (CAContentProvider, error) {
	caBundleAndVerifier, err := newCABundleAndVerifier(name, caBundle)
	if err != nil {
		return nil, err
	}

	return &staticCAContent{
		name:     name,
		caBundle: caBundleAndVerifier,
	}, nil
}

// Name is just an identifier
func (c *staticCAContent) Name() string {
	return c.name
}

func (c *staticCAContent) AddListener(Listener) {}

// CurrentCABundleContent provides ca bundle byte content
// 返回的切片与内部状态共享，调用方不得修改。
func (c *staticCAContent) CurrentCABundleContent() (cabundle []byte) {
	return c.caBundle.caBundle
}

// VerifyOptions 总是返回 true，因为静态内容在构造时已完成校验。
func (c *staticCAContent) VerifyOptions() (x509.VerifyOptions, bool) {
	return c.caBundle.verifyOptions, true
}

// staticCertKeyContent 是永不轮换的证书/私钥对。
type staticCertKeyContent struct {
	name string
	cert []byte
	key  []byte
}

// NewStaticCertKeyContent returns a CertKeyContentProvider that always returns the same value
func NewStaticCertKeyContent(name string, cert, key []byte) (CertKeyContentProvider, error) {
	// Ensure that the key matches the cert and both are valid
	// 构造时即校验，避免把不匹配的证书/私钥带入运行期，直到握手时才
	// 以晦涩的 TLS 错误暴露。
	_, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	return &staticCertKeyContent{
		name: name,
		cert: cert,
		key:  key,
	}, nil
}

// Name is just an identifier
func (c *staticCertKeyContent) Name() string {
	return c.name
}

func (c *staticCertKeyContent) AddListener(Listener) {}

// CurrentCertKeyContent provides cert and key content
func (c *staticCertKeyContent) CurrentCertKeyContent() ([]byte, []byte) {
	return c.cert, c.key
}

// staticSNICertKeyContent 在静态证书基础上附带 SNI 名称列表，
// 用于为特定 ServerName 选择不同证书。
type staticSNICertKeyContent struct {
	staticCertKeyContent
	sniNames []string
}

// NewStaticSNICertKeyContent returns a SNICertKeyContentProvider that always returns the same value
func NewStaticSNICertKeyContent(name string, cert, key []byte, sniNames ...string) (SNICertKeyContentProvider, error) {
	// Ensure that the key matches the cert and both are valid
	_, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	return &staticSNICertKeyContent{
		staticCertKeyContent: staticCertKeyContent{
			name: name,
			cert: cert,
			key:  key,
		},
		sniNames: sniNames,
	}, nil
}

// SNINames 返回该证书可响应的 SNI 名称；顺序无关，调用方通常做集合匹配。
func (c *staticSNICertKeyContent) SNINames() []string {
	return c.sniNames
}

func (c *staticSNICertKeyContent) AddListener(Listener) {}
