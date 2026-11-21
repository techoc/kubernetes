/*
Copyright 2023 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"strings"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// IsKubernetesSignerName 判断 signerName 是否属于 Kubernetes 项目保留的域名。
func IsKubernetesSignerName(signerName string) bool {
	hostName, _, _ := strings.Cut(signerName, "/")
	return hostName == "kubernetes.io" || strings.HasSuffix(hostName, ".kubernetes.io")
}

// ValidateSignerName 校验 signerName 在语法上是否合法。
//
// 要求 signerName 形如 domain.com/something，且最长 571 个字符。
// 这一长度和格式是为了容纳形如 <fqdn>/<resource-namespace>.<resource-name> 的 signerName：
// FQDN 最长 253 个字符（DNS1123Subdomain 的上限）；
// namespace 名最长 63 个字符（DNS1123Label 的上限）；
// 资源名最长 253 个字符（DNS1123Subdomain 的上限）；
// 再加上一个 '.' 和一个 '/'，合计额外 2 个字符。
func ValidateSignerName(fldPath *field.Path, signerName string) field.ErrorList {
	var el field.ErrorList
	if len(signerName) == 0 {
		el = append(el, field.Required(fldPath, ""))
		return el
	}

	segments := strings.Split(signerName, "/")
	// 校验 signerName 中恰好有一个 '/'。
	// 放在域名段校验之后，是为了给用户提供更有针对性的错误信息。
	if len(segments) != 2 {
		el = append(el, field.Invalid(fldPath, signerName, "must be a fully qualified domain and path of the form 'example.com/signer-name'"))
		// 此处提前返回：路径段缺失或格式错误（含多个或零个 `/`）时不应继续校验。
		return el
	}

	// 校验 segments[0] 总长度不超过 253 个字符。
	maxDomainSegmentLength := validation.DNS1123SubdomainMaxLength
	if len(segments[0]) > maxDomainSegmentLength {
		el = append(el, field.TooLong(fldPath, "" /*unused*/, maxDomainSegmentLength))
	}
	// 校验 segments[0] 由 '.' 分隔的合法 DNS1123 label 组成。
	domainLabels := strings.Split(segments[0], ".")
	for _, lbl := range domainLabels {
		// 使用 IsDNS1123Label 是为了确保域名中单个 label 的长度不超过 63。
		if errs := validation.IsDNS1123Label(lbl); len(errs) > 0 {
			for _, err := range errs {
				el = append(el, field.Invalid(fldPath, segments[0], fmt.Sprintf("validating label %q: %s", lbl, err)))
			}
			// 域名段一旦出错就停止校验：后续错误信息只会重复且难以区分，反而让用户困惑。
			break
		}
	}

	// 校验 segments[0] 至少包含一个 '.'（即至少两个由点分隔的段）。
	if len(domainLabels) < 2 {
		el = append(el, field.Invalid(fldPath, segments[0], "should be a domain with at least two segments separated by dots"))
	}

	// 校验 segments[1] 由 '.' 分隔的合法 DNS1123 subdomain 组成。
	pathLabels := strings.Split(segments[1], ".")
	for _, lbl := range pathLabels {
		// 使用 IsDNS1123Subdomain 是因为它强制 253 字符的长度上限，
		// 这样才能容纳一个完整的资源名作为单个 label。
		if errs := validation.IsDNS1123Subdomain(lbl); len(errs) > 0 {
			for _, err := range errs {
				el = append(el, field.Invalid(fldPath, segments[1], fmt.Sprintf("validating label %q: %s", lbl, err)))
			}
			// 路径段一旦出错就停止校验：后续错误信息只会重复且难以区分，反而让用户困惑。
			break
		}
	}

	// 确保 segments[1] 能容纳 dns label + dns subdomain + '.'。
	maxPathSegmentLength := validation.DNS1123SubdomainMaxLength + validation.DNS1123LabelMaxLength + 1
	maxSignerNameLength := maxDomainSegmentLength + maxPathSegmentLength + 1
	if len(signerName) > maxSignerNameLength {
		el = append(el, field.TooLong(fldPath, "" /*unused*/, maxSignerNameLength))
	}

	return el
}

// ValidateClusterTrustBundleName 校验 ClusterTrustBundle 名称是否符合类型文档中的规则。
// 返回的函数可直接用作 apimachinery 的名称校验器：无 signerName 时按 DNS 子域名校验；
// 有 signerName 时名称必须以 signerName 中的 '/' 换成 ':' 后加 ':' 为前缀。
func ValidateClusterTrustBundleName(signerName string) func(name string, prefix bool) []string {
	return func(name string, isPrefix bool) []string {
		if signerName == "" {
			if strings.Contains(name, ":") {
				return []string{"ClusterTrustBundle without signer name must not have \":\" in its name"}
			}
			return apimachineryvalidation.NameIsDNSSubdomain(name, isPrefix)
		}

		requiredPrefix := strings.ReplaceAll(signerName, "/", ":") + ":"
		if !strings.HasPrefix(name, requiredPrefix) {
			return []string{fmt.Sprintf("ClusterTrustBundle for signerName %s must be named with prefix %s", signerName, requiredPrefix)}
		}
		return apimachineryvalidation.NameIsDNSSubdomain(strings.TrimPrefix(name, requiredPrefix), isPrefix)
	}
}

// extractSignerNameFromClusterTrustBundleName 从 ClusterTrustBundle 名称中提取 signerName。
// 若名称包含 ':'，则将最后一个 ':' 之前的部分中的 ':' 还原为 '/' 作为 signerName。
func extractSignerNameFromClusterTrustBundleName(name string) (string, bool) {
	if splitPoint := strings.LastIndex(name, ":"); splitPoint != -1 {
		// 名称中包含 ':'，说明它是与某个 signerName 绑定的 trustbundle。
		return strings.ReplaceAll(name[:splitPoint], ":", "/"), true
	} else {
		return "", false
	}
}
