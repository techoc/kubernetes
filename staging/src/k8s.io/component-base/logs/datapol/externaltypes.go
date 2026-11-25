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

package datapol

import (
	"crypto/x509"
	"net/http"
	"reflect"
)

var (
	// 预取标准库类型的 reflect.Type，避免在热路径上重复计算。
	httpHeader      = reflect.TypeOf(http.Header{})
	httpCookie      = reflect.TypeOf(http.Cookie{})
	x509Certificate = reflect.TypeOf(x509.Certificate{})
)

// GlobalDatapolicyMapping 返回非 Kubernetes 原生类型中内嵌的敏感数据类型列表。
//
// 标准库类型无法携带 datapolicy 标签，因此在此集中声明其敏感字段类别，
// 供日志脱敏检查识别。
func GlobalDatapolicyMapping(v interface{}) []string {
	return byType(reflect.TypeOf(v))
}

// byType 按标准库类型返回敏感数据类别；未列出的类型返回 nil。
func byType(t reflect.Type) []string {
	switch t {
	case httpHeader:
		// Header 可能包含 Authorization/Cookie 等凭据。
		return []string{"password", "token"}
	case httpCookie:
		return []string{"token"}
	case x509Certificate:
		// 证书里的公钥本身不是秘密，但私钥材料常与之关联，按 security-key 处理。
		return []string{"security-key"}
	default:
		return nil
	}
}
