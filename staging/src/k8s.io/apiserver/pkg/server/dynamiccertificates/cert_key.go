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

// certKeyContent 保存证书与私钥的内容。
// 这是热加载证书的内部表示。把 PEM 内容保存为字节切片而非解析后的
// tls.Certificate，是为了能直接按字节比较判断「文件是否真的变了」，
// 避免因时间戳或重复读取造成的无谓重载。
type certKeyContent struct {
	cert []byte
	key  []byte
}

// Equal 比较两份证书/私钥内容是否完全一致。
// nil 与非 nil 视为不相等（c == rhs 对 nil/nil 为 true，对 nil/非 nil 为 false）；
// 只比较 key 与 cert，不涉及任何证书解析，因此可在每次文件变动时廉价调用。
func (c *certKeyContent) Equal(rhs *certKeyContent) bool {
	if c == nil || rhs == nil {
		return c == rhs
	}

	return bytes.Equal(c.key, rhs.key) && bytes.Equal(c.cert, rhs.cert)
}

// sniCertKeyContent 保存证书与私钥的内容，以及任何显式指定的名称。
// 在普通证书内容之外附带显式的 SNI 名称列表。用于证书中未包含
// 所有需要服务的主机名时，由配置补充；因此比较时名称列表也必须参与。
type sniCertKeyContent struct {
	certKeyContent
	sniNames []string
}

// Equal 先比较 SNI 名称列表（长度、逐项、顺序敏感），再比较证书与私钥字节。
// 顺序敏感是有意为之——名称列表通常来自配置顺序，变更顺序也意味着
// 配置变了，重载一次成本可接受，换来实现简单且无歧义。
func (c *sniCertKeyContent) Equal(rhs *sniCertKeyContent) bool {
	if c == nil || rhs == nil {
		return c == rhs
	}

	if len(c.sniNames) != len(rhs.sniNames) {
		return false
	}

	for i := range c.sniNames {
		if c.sniNames[i] != rhs.sniNames[i] {
			return false
		}
	}

	return c.certKeyContent.Equal(&rhs.certKeyContent)
}
