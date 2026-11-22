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
	"bytes"
)

// dynamicCertificateContent holds the content that overrides the baseTLSConfig
//
// 这是动态证书组件之间比较"是否需要重载 TLS 配置"的快照单元。
type dynamicCertificateContent struct {
	// clientCA holds the content for the clientCA bundle
	clientCA    caBundleContent
	servingCert certKeyContent
	sniCerts    []sniCertKeyContent
}

// caBundleContent holds the content for the clientCA bundle.  Wrapping the bytes makes the Equals work nicely with the
// method receiver.
//
// 用结构体包裹切片，才能为 Equal 定义带 nil 语义的方法（切片本身不能挂方法）。
type caBundleContent struct {
	caBundle []byte
}

// Equal 判断两份动态证书内容是否完全一致。
// nil 与非 nil 被视为不同，只有同时为 nil 才相等；这样"未配置"与
// "配置为空"不会被混为一谈。任一字段变化都返回 false，触发调用方重载。
func (c *dynamicCertificateContent) Equal(rhs *dynamicCertificateContent) bool {
	if c == nil || rhs == nil {
		return c == rhs
	}

	if !c.clientCA.Equal(&rhs.clientCA) {
		return false
	}

	if !c.servingCert.Equal(&rhs.servingCert) {
		return false
	}

	// 先比长度再逐项比较，避免顺序不同导致的误判漏检。
	if len(c.sniCerts) != len(rhs.sniCerts) {
		return false
	}

	for i := range c.sniCerts {
		if !c.sniCerts[i].Equal(&rhs.sniCerts[i]) {
			return false
		}
	}

	return true
}

// Equal 按字节比较两个 CA bundle。证书轮换时内容一旦变化即返回 false。
func (c *caBundleContent) Equal(rhs *caBundleContent) bool {
	if c == nil || rhs == nil {
		return c == rhs
	}

	return bytes.Equal(c.caBundle, rhs.caBundle)
}
