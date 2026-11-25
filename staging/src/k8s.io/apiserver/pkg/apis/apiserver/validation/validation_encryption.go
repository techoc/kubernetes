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

// Package validation validates EncryptionConfiguration.
//
// 校验分为两类：结构性校验（每个资源项至少一个资源/提供者、单个提供者只能
// 配置一种算法）和安全性校验（资源不重叠、密钥长度、KMS endpoint 必须为
// unix socket）。错误路径使用 field.Path，便于直接反馈到配置字段。
package validation

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/apis/apiserver"
)

const (
	moreThanOneElementErr          = "more than one provider specified in a single element, should split into different list elements"
	keyLenErrFmt                   = "secret is not of the expected length, got %d, expected one of %v"
	unsupportedSchemeErrFmt        = "unsupported scheme %q for KMS provider, only unix is supported"
	unsupportedKMSAPIVersionErrFmt = "unsupported apiVersion %s for KMS provider, only v1 and v2 are supported"
	atLeastOneRequiredErrFmt       = "at least one %s is required"
	invalidURLErrFmt               = "invalid endpoint for kms provider, error: %v"
	mandatoryFieldErrFmt           = "%s is a mandatory field for a %s"
	base64EncodingErr              = "secrets must be base64 encoded"
	zeroOrNegativeErrFmt           = "%s should be a positive value"
	nonZeroErrFmt                  = "%s should be a positive value, or negative to disable"
	encryptionConfigNilErr         = "EncryptionConfiguration can't be nil"
	invalidKMSConfigNameErrFmt     = "invalid KMS provider name %s, must not contain ':'"
	duplicateKMSConfigNameErrFmt   = "duplicate KMS provider name %s, names must be unique"
	eventsGroupErr                 = "'*.events.k8s.io' objects are stored using the 'events' API group in etcd. Use 'events' instead in the config file"
	extensionsGroupErr             = "'extensions' group has been removed and cannot be used for encryption"
	starResourceErr                = "use '*.' to encrypt all the resources from core API group or *.* to encrypt all resources"
	overlapErr                     = "using overlapping resources such as 'secrets' and '*.' in the same resource list is not allowed as they will be masked"
	nonRESTAPIResourceErr          = "resources which do not have REST API/s cannot be encrypted"
	resourceNameErr                = "resource name should not contain capital letters"
	resourceAcrossGroupErr         = "encrypting the same resource across groups is not supported"
	duplicateResourceErr           = "the same resource cannot be specified multiple times"
)

var (
	// AES 支持的密钥长度详见 https://golang.org/pkg/crypto/aes/#NewCipher；
	// 16/24/32 字节分别对应 AES-128/192/256，禁止其他长度是为了避免
	// 弱密钥被误用。
	aesKeySizes = []int{16, 24, 32}

	// Secretbox 支持的密钥长度详见 https://godoc.org/golang.org/x/crypto/nacl/secretbox#Open；只支持 256 位密钥。
	secretBoxKeySizes = []int{32}
)

// ValidateEncryptionConfiguration 校验一个 v1.EncryptionConfiguration。
// reload 表示配置是否来自热重载路径：非 v1 的 KMS 名称始终要求全局唯一，
// 而 v1 名称只在热重载时要求唯一，以兼容既有静态配置。
func ValidateEncryptionConfiguration(c *apiserver.EncryptionConfiguration, reload bool) field.ErrorList {
	root := field.NewPath("resources")
	allErrs := field.ErrorList{}

	if c == nil {
		allErrs = append(allErrs, field.Required(root, encryptionConfigNilErr))
		return allErrs
	}

	if len(c.Resources) == 0 {
		allErrs = append(allErrs, field.Required(root, fmt.Sprintf(atLeastOneRequiredErrFmt, root)))
		return allErrs
	}

	// kmsProviderNames 用于跟踪各 provider 配置的名称并保证其唯一；
	// 跨资源项共享同一集合，因为 KMS provider 名是整个配置内的逻辑标识。
	kmsProviderNames := sets.New[string]()
	for i, conf := range c.Resources {
		r := root.Index(i).Child("resources")
		p := root.Index(i).Child("providers")

		if len(conf.Resources) == 0 {
			allErrs = append(allErrs, field.Required(r, fmt.Sprintf(atLeastOneRequiredErrFmt, r)))
		}

		allErrs = append(allErrs, validateResourceOverlap(conf.Resources, r)...)
		allErrs = append(allErrs, validateResourceNames(conf.Resources, r)...)

		if len(conf.Providers) == 0 {
			allErrs = append(allErrs, field.Required(p, fmt.Sprintf(atLeastOneRequiredErrFmt, p)))
		}

		for j, provider := range conf.Providers {
			path := p.Index(j)
			allErrs = append(allErrs, validateSingleProvider(provider, path)...)

			// 按提供者类型分发到各自的密钥/配置校验；
			// Identity 提供者无需额外校验，因此不在分支中出现。
			switch {
			case provider.KMS != nil:
				allErrs = append(allErrs, validateKMSConfiguration(provider.KMS, path.Child("kms"), kmsProviderNames, reload)...)
				kmsProviderNames.Insert(provider.KMS.Name)
			case provider.AESGCM != nil:
				allErrs = append(allErrs, validateKeys(provider.AESGCM.Keys, path.Child("aesgcm").Child("keys"), aesKeySizes)...)
			case provider.AESCBC != nil:
				allErrs = append(allErrs, validateKeys(provider.AESCBC.Keys, path.Child("aescbc").Child("keys"), aesKeySizes)...)
			case provider.Secretbox != nil:
				allErrs = append(allErrs, validateKeys(provider.Secretbox.Keys, path.Child("secretbox").Child("keys"), secretBoxKeySizes)...)
			}
		}
	}

	return allErrs
}

// anyGroupAnyResource 表示 “*.*”，即加密所有资源。
var anyGroupAnyResource = schema.GroupResource{
	Group:    "*",
	Resource: "*",
}

// validateResourceOverlap 检查 resource 列表内是否存在重叠或重复。
// 重叠（如 secrets 与 *.）会让后者被前者遮蔽，属于易错配置，
// 因此直接拒绝而不是按顺序取第一个。
func validateResourceOverlap(resources []string, fieldPath *field.Path) field.ErrorList {
	if len(resources) < 2 { // cannot have overlap with a single resource
		return nil
	}

	var allErrs field.ErrorList

	r := make([]schema.GroupResource, 0, len(resources))
	for _, resource := range resources {
		r = append(r, schema.ParseGroupResource(resource))
	}

	var hasOverlap, hasDuplicate bool

	// O(n²) 比较在这里可接受：资源列表通常只有个位数项。
	for i, r1 := range r {
		for j, r2 := range r {
			if i == j {
				continue
			}

			if r1 == r2 && !hasDuplicate {
				hasDuplicate = true
				continue
			}

			if hasOverlap {
				continue
			}

			// "*.*" 覆盖所有资源，必然与任何其他项重叠。
			if r1 == anyGroupAnyResource {
				hasOverlap = true
				continue
			}

			// 不同组之间的同名资源互不影响。
			if r1.Group != r2.Group {
				continue
			}

			// 同组内 "*" 会覆盖该组全部资源。
			if r1.Resource == "*" || r2.Resource == "*" {
				hasOverlap = true
				continue
			}
		}
	}

	if hasDuplicate {
		allErrs = append(
			allErrs,
			field.Invalid(
				fieldPath,
				resources,
				duplicateResourceErr,
			),
		)
	}

	if hasOverlap {
		allErrs = append(
			allErrs,
			field.Invalid(
				fieldPath,
				resources,
				overlapErr,
			),
		)
	}

	return allErrs
}

// validateResourceNames 逐个校验资源名，拒绝大写、裸 "*"、无 REST 端点的
// 内部资源、错误的组名以及跨组通配等历史遗留/不可生效写法。
func validateResourceNames(resources []string, fieldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	for j, res := range resources {
		jj := fieldPath.Index(j)

		// check if resource name has capital letters
		if hasCapital(res) {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					resourceNameErr,
				),
			)
			continue
		}

		// check if resource is '*'
		if res == "*" {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					starResourceErr,
				),
			)
			continue
		}

		// check if resource is:
		// 'apiserveripinfo' OR
		// 'serviceipallocations' OR
		// 'servicenodeportallocations' OR
		if res == "apiserveripinfo" ||
			res == "serviceipallocations" ||
			res == "servicenodeportallocations" {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					nonRESTAPIResourceErr,
				),
			)
			continue
		}

		// check if group is 'events.k8s.io'
		gr := schema.ParseGroupResource(res)
		if gr.Group == "events.k8s.io" {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					eventsGroupErr,
				),
			)
			continue
		}

		// check if group is 'extensions'
		if gr.Group == "extensions" {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					extensionsGroupErr,
				),
			)
			continue
		}

		// disallow resource.* as encrypting the same resource across groups does not make sense
		if gr.Group == "*" && gr.Resource != "*" {
			allErrs = append(
				allErrs,
				field.Invalid(
					jj,
					resources[j],
					resourceAcrossGroupErr,
				),
			)
			continue
		}
	}

	return allErrs
}

// validateSingleProvider 要求每个 provider 元素恰好配置一种算法。
// 允许多种会导致优先级歧义，因此超过一个即报错。
func validateSingleProvider(provider apiserver.ProviderConfiguration, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	found := 0

	if provider.KMS != nil {
		found++
	}
	if provider.AESGCM != nil {
		found++
	}
	if provider.AESCBC != nil {
		found++
	}
	if provider.Secretbox != nil {
		found++
	}
	if provider.Identity != nil {
		found++
	}

	if found == 0 {
		return append(allErrs, field.Invalid(fieldPath, provider, "provider does not contain any of the expected providers: KMS, AESGCM, AESCBC, Secretbox, Identity"))
	}

	if found > 1 {
		return append(allErrs, field.Invalid(fieldPath, provider, moreThanOneElementErr))
	}

	return allErrs
}

// validateKeys 要求至少一个密钥，并逐个校验。
func validateKeys(keys []apiserver.Key, fieldPath *field.Path, expectedLen []int) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(keys) == 0 {
		allErrs = append(allErrs, field.Required(fieldPath, fmt.Sprintf(atLeastOneRequiredErrFmt, "keys")))
		return allErrs
	}

	for i, key := range keys {
		allErrs = append(allErrs, validateKey(key, fieldPath.Index(i), expectedLen)...)
	}

	return allErrs
}

// validateKey 校验密钥名、base64 编码与解码后的长度。
// 错误中回显 "REDACTED" 而非真实 secret，防止校验错误把密钥写进日志。
func validateKey(key apiserver.Key, fieldPath *field.Path, expectedLen []int) field.ErrorList {
	allErrs := field.ErrorList{}

	if key.Name == "" {
		allErrs = append(allErrs, field.Required(fieldPath.Child("name"), fmt.Sprintf(mandatoryFieldErrFmt, "name", "key")))
	}

	if key.Secret == "" {
		allErrs = append(allErrs, field.Required(fieldPath.Child("secret"), fmt.Sprintf(mandatoryFieldErrFmt, "secret", "key")))
		return allErrs
	}

	secret, err := base64.StdEncoding.DecodeString(key.Secret)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("secret"), "REDACTED", base64EncodingErr))
		return allErrs
	}

	lenMatched := false
	for _, l := range expectedLen {
		if len(secret) == l {
			lenMatched = true
			break
		}
	}

	if !lenMatched {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("secret"), "REDACTED", fmt.Sprintf(keyLenErrFmt, len(secret), expectedLen)))
	}

	return allErrs
}

// validateKMSConfiguration 汇总 KMS 提供者的各字段校验。
func validateKMSConfiguration(c *apiserver.KMSConfiguration, fieldPath *field.Path, kmsProviderNames sets.Set[string], reload bool) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateKMSConfigName(c, fieldPath.Child("name"), kmsProviderNames, reload)...)
	allErrs = append(allErrs, validateKMSTimeout(c, fieldPath.Child("timeout"))...)
	allErrs = append(allErrs, validateKMSEndpoint(c, fieldPath.Child("endpoint"))...)
	allErrs = append(allErrs, validateKMSCacheSize(c, fieldPath.Child("cachesize"))...)
	allErrs = append(allErrs, validateKMSAPIVersion(c, fieldPath.Child("apiVersion"))...)
	return allErrs
}

// validateKMSCacheSize 校验 KMS 缓存大小配置。
func validateKMSCacheSize(c *apiserver.KMSConfiguration, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// In defaulting, we set the cache size to the default value only when API version is v1.
	// So, for v2 API version, we expect the cache size field to be nil.
	if c.APIVersion != "v1" && c.CacheSize != nil {
		allErrs = append(allErrs, field.Invalid(fieldPath, *c.CacheSize, "cachesize is not supported in v2"))
	}
	if c.APIVersion == "v1" && *c.CacheSize == 0 {
		allErrs = append(allErrs, field.Invalid(fieldPath, *c.CacheSize, fmt.Sprintf(nonZeroErrFmt, "cachesize")))
	}

	return allErrs
}

// validateKMSTimeout 要求超时为正数；零或负值会让 KMS 调用立即失败。
func validateKMSTimeout(c *apiserver.KMSConfiguration, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if c.Timeout.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(fieldPath, c.Timeout, fmt.Sprintf(zeroOrNegativeErrFmt, "timeout")))
	}

	return allErrs
}

// validateKMSEndpoint 要求 endpoint 为 unix scheme 的合法 URL。
// 只允许 unix socket，是因为 KMS 插件与 apiserver 之间需要本机
// 可信通道，禁止网络传输可避免凭据与明文密钥跨主机流动。
func validateKMSEndpoint(c *apiserver.KMSConfiguration, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(c.Endpoint) == 0 {
		return append(allErrs, field.Invalid(fieldPath, "", fmt.Sprintf(mandatoryFieldErrFmt, "endpoint", "kms")))
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return append(allErrs, field.Invalid(fieldPath, c.Endpoint, fmt.Sprintf(invalidURLErrFmt, err)))
	}

	if u.Scheme != "unix" {
		return append(allErrs, field.Invalid(fieldPath, c.Endpoint, fmt.Sprintf(unsupportedSchemeErrFmt, u.Scheme)))
	}

	return allErrs
}

// validateKMSAPIVersion 限制 KMS 插件协议版本为 v1 或 v2。
func validateKMSAPIVersion(c *apiserver.KMSConfiguration, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if c.APIVersion != "v1" && c.APIVersion != "v2" {
		allErrs = append(allErrs, field.Invalid(fieldPath, c.APIVersion, fmt.Sprintf(unsupportedKMSAPIVersionErrFmt, "apiVersion")))
	}

	return allErrs
}

// validateKMSConfigName 校验 KMS provider 名非空、v2 名不含 ':'、
// 并按 reload 语义检查名称唯一性。名字是 KMS 状态文件中的键；
// 含 ':' 会与其分隔格式冲突，故 v2 禁止。
func validateKMSConfigName(c *apiserver.KMSConfiguration, fieldPath *field.Path, kmsProviderNames sets.Set[string], reload bool) field.ErrorList {
	allErrs := field.ErrorList{}
	if c.Name == "" {
		allErrs = append(allErrs, field.Required(fieldPath, fmt.Sprintf(mandatoryFieldErrFmt, "name", "provider")))
	}

	// kms v2 providers are not allowed to have a ":" in their name
	if c.APIVersion != "v1" && strings.Contains(c.Name, ":") {
		allErrs = append(allErrs, field.Invalid(fieldPath, c.Name, fmt.Sprintf(invalidKMSConfigNameErrFmt, c.Name)))
	}

	// kms v2 providers name must always be unique across all kms providers (v1 and v2)
	// kms v1 provider names must be unique across all kms providers (v1 and v2) when hot reloading of encryption configuration is enabled (reload=true)
	if reload || c.APIVersion != "v1" {
		if kmsProviderNames.Has(c.Name) {
			allErrs = append(allErrs, field.Invalid(fieldPath, c.Name, fmt.Sprintf(duplicateKMSConfigNameErrFmt, c.Name)))
		}
	}

	return allErrs
}

// hasCapital 判断字符串是否包含大写字母（用于资源名规范校验）。
func hasCapital(input string) bool {
	return strings.ToLower(input) != input
}
