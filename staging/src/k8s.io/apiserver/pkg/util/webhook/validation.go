/*
Copyright 2018 The Kubernetes Authors.

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

package webhook

import (
	"fmt"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/transport"
)

// ValidateCABundle 通过实际构建 TLS 配置来校验 CA 证书内容：
// 与逐字节检查相比，这条路径确保 bundle 能被 TLS 栈真正解析。
func ValidateCABundle(fldPath *field.Path, caBundle []byte) field.ErrorList {
	var allErrors field.ErrorList
	_, err := transport.TLSConfigFor(&transport.Config{TLS: transport.TLSConfig{CAData: caBundle}})
	if err != nil {
		allErrors = append(allErrors, field.Invalid(fldPath, caBundle, err.Error()))
	}
	return allErrors
}

// ValidateWebhookURL 校验 webhook 的 URL。
// 安全边界：禁止 URL 中携带用户信息、fragment 与查询参数，
// 避免 webhook 目标被注入额外语义或被客户端凭据混淆。
// forceHttps 为 true 时仅允许 https scheme。
func ValidateWebhookURL(fldPath *field.Path, URL string, forceHttps bool) field.ErrorList {
	var allErrors field.ErrorList
	const form = "; desired format: https://host[/path]"
	if u, err := url.Parse(URL); err != nil {
		allErrors = append(allErrors, field.Required(fldPath, "url must be a valid URL: "+err.Error()+form))
	} else {
		if forceHttps && u.Scheme != "https" {
			allErrors = append(allErrors, field.Invalid(fldPath, u.Scheme, "'https' is the only allowed URL scheme"+form))
		}
		if len(u.Host) == 0 {
			allErrors = append(allErrors, field.Invalid(fldPath, u.Host, "host must be specified"+form))
		}
		if u.User != nil {
			allErrors = append(allErrors, field.Invalid(fldPath, u.User.String(), "user information is not permitted in the URL"))
		}
		if len(u.Fragment) != 0 {
			allErrors = append(allErrors, field.Invalid(fldPath, u.Fragment, "fragments are not permitted in the URL"))
		}
		if len(u.RawQuery) != 0 {
			allErrors = append(allErrors, field.Invalid(fldPath, u.RawQuery, "query parameters are not permitted in the URL"))
		}
	}
	return allErrors
}

// ValidateWebhookService 校验 service 引用与 path。
// 每一段 path 必须是 DNS1123 子域：服务端会把它拼接进转发 URL，
// 宽松字符集可能引入路径穿越或编码歧义。
func ValidateWebhookService(fldPath *field.Path, namespace, name string, path *string, port int32) field.ErrorList {
	var allErrors field.ErrorList

	if len(name) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("name"), ""))
	}

	if len(namespace) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("namespace"), ""))
	}

	if errs := validation.IsValidPortNum(int(port)); errs != nil {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("port"), port, "port is not valid: "+strings.Join(errs, ", ")))
	}

	if path == nil {
		return allErrors
	}

	// TODO：能否用 url.Parse 加校验 host 为空来替代下面这段？

	urlPath := *path
	// "/" 与空路径都表示根路径，无需进一步检查。
	if urlPath == "/" || len(urlPath) == 0 {
		return allErrors
	}
	// "//" 会产生空的第一段，必须显式拒绝（否则后续 split 会得到空元素）。
	if urlPath == "//" {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("path"), urlPath, "segment[0] may not be empty"))
		return allErrors
	}

	if !strings.HasPrefix(urlPath, "/") {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("path"), urlPath, "must start with a '/'"))
	}

	// 去掉前导斜杠与可选的尾随斜杠后再逐段校验，避免把空段误判为非法。
	urlPathToCheck := urlPath[1:]
	if strings.HasSuffix(urlPathToCheck, "/") {
		urlPathToCheck = urlPathToCheck[:len(urlPathToCheck)-1]
	}
	steps := strings.Split(urlPathToCheck, "/")
	for i, step := range steps {
		if len(step) == 0 {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("path"), urlPath, fmt.Sprintf("segment[%d] may not be empty", i)))
			continue
		}
		failures := validation.IsDNS1123Subdomain(step)
		for _, failure := range failures {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("path"), urlPath, fmt.Sprintf("segment[%d]: %v", i, failure)))
		}
	}

	return allErrors
}
