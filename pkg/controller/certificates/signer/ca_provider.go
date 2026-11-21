/*
Copyright 2020 The Kubernetes Authors.

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

package signer

import (
	"bytes"
	"crypto"
	"fmt"
	"sync/atomic"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	"k8s.io/kubernetes/pkg/controller/certificates/authority"
)

// newCAProvider 从证书/私钥文件创建 caProvider，并立即加载一次 CA 以便启动时就能发现问题。
func newCAProvider(caFile, caKeyFile string) (*caProvider, error) {
	caLoader, err := dynamiccertificates.NewDynamicServingContentFromFiles("csr-controller", caFile, caKeyFile)
	if err != nil {
		// 包装错误时带上文件路径，便于定位是哪个 CA 证书文件出了问题。
		return nil, fmt.Errorf("error reading CA cert file %q: %v", caFile, err)
	}

	ret := &caProvider{
		caLoader: caLoader,
	}
	if err := ret.setCA(); err != nil {
		return nil, err
	}

	return ret, nil
}

// caProvider 在内存中缓存解析后的 CA，并借助 caLoader 感知证书/私钥文件的变化。
// caValue 使用 atomic.Value 保存不可变的 CA 快照，读写无需加锁。
type caProvider struct {
	caValue  atomic.Value
	caLoader *dynamiccertificates.DynamicCertKeyPairContent
}

// setCA 无条件用当前证书/私钥内容重新解析并覆盖缓存的 CA。
func (p *caProvider) setCA() error {
	certPEM, keyPEM := p.caLoader.CurrentCertKeyContent()

	certs, err := cert.ParseCertsPEM(certPEM)
	if err != nil {
		return fmt.Errorf("error reading CA cert file %q: %v", p.caLoader.Name(), err)
	}
	// CA 文件必须恰好包含一张证书，否则无法确定应该用哪张来签发。
	if len(certs) != 1 {
		return fmt.Errorf("error reading CA cert file %q: expected 1 certificate, found %d", p.caLoader.Name(), len(certs))
	}

	key, err := keyutil.ParsePrivateKeyPEM(keyPEM)
	if err != nil {
		return fmt.Errorf("error reading CA key file %q: %v", p.caLoader.Name(), err)
	}
	// 只有实现 crypto.Signer 的私钥才能用于签名，先断言避免后续签名时出错。
	priv, ok := key.(crypto.Signer)
	if !ok {
		return fmt.Errorf("error reading CA key file %q: key did not implement crypto.Signer", p.caLoader.Name())
	}

	ca := &authority.CertificateAuthority{
		RawCert: certPEM,
		RawKey:  keyPEM,

		Certificate: certs[0],
		PrivateKey:  priv,
	}
	// 整体替换快照，读者要么看到旧的完整 CA，要么看到新的完整 CA。
	p.caValue.Store(ca)

	return nil
}

// currentCA 返回当前生效的 CA。
// 每次都会检查缓存是否过期；由于只比较小切片的内容，这个检查开销很低。
func (p *caProvider) currentCA() (*authority.CertificateAuthority, error) {
	certPEM, keyPEM := p.caLoader.CurrentCertKeyContent()
	currCA := p.caValue.Load().(*authority.CertificateAuthority)
	// 字节一致说明证书/私钥没有变化，直接复用缓存，避免重复解析。
	if bytes.Equal(currCA.RawCert, certPEM) && bytes.Equal(currCA.RawKey, keyPEM) {
		return currCA, nil
	}

	// 字节不同则先更新缓存再读取；解析失败时仍返回旧 CA，保证有可用的回退值。
	if err := p.setCA(); err != nil {
		return currCA, err
	}
	return p.caValue.Load().(*authority.CertificateAuthority), nil
}
