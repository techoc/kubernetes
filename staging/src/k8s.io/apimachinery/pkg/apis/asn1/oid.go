/*
Copyright 2024 The Kubernetes Authors.

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

// Package asn1 定义 Kubernetes 使用的 ASN.1 对象标识符（OID）。
package asn1

import "encoding/asn1"

// 以下常量是 Kubernetes 在 CNCF 私有企业号下的后缀：
// https://www.iana.org/assignments/enterprise-numbers.txt
//
// 根节点：1.3.6.1.4.1.57683
const (
	// 单值，字符串类型
	x509UIDSuffix = 2
)

// makeOID 在 CNCF 分配的 Kubernetes 根 OID 下拼接给定后缀。
func makeOID(suffix int) asn1.ObjectIdentifier {
	return asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57683, suffix}
}

// X509UID 返回表示用户 UID 的 x509 可分辨名称元素的 OID（1.3.6.1.4.1.57683.2）。
// 该 UID 对特定用户唯一；当用户从系统中删除、再加入同名用户时会发生变化。
//
// 此元素在一个可分辨名称中最多出现一次，且值必须是字符串。
func X509UID() asn1.ObjectIdentifier {
	return makeOID(x509UIDSuffix)
}
