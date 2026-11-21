/*
Copyright 2025 The Kubernetes Authors.

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

package pullmanager

import (
	"fmt"
	"strings"

	imageref "github.com/distribution/reference"

	"k8s.io/apimachinery/pkg/util/sets"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// ImagePullPolicyEnforcer 定义镜像拉取凭证校验策略的函数类型。这些函数根据镜像名、
// 本地镜像是否存在以及历史拉取记录，决定当前策略是否要求校验凭证。
//
// `image` 是 Pod 容器 "image" 字段中的镜像名。
// `imagePresent` 表示该镜像是否已存在于节点上。
// `imagePulledByKubelet` 表示节点上存在该镜像的 ImagePulledRecord 或
// ImagePullingIntent 记录，即 kubelet 过去拉取过该镜像。
type ImagePullPolicyEnforcer interface {
	RequireCredentialVerificationForImage(image string, imagePulledByKubelet bool) bool
}

// ImagePullPolicyEnforcerFunc 是实现 ImagePullPolicyEnforcer 接口的函数类型。
type ImagePullPolicyEnforcerFunc func(image string, imagePulledByKubelet bool) bool

// RequireCredentialVerificationForImage 调用底层函数判断是否需要校验镜像拉取凭证。
func (e ImagePullPolicyEnforcerFunc) RequireCredentialVerificationForImage(image string, imagePulledByKubelet bool) bool {
	return e(image, imagePulledByKubelet)
}

// NewImagePullCredentialVerificationPolicy 根据配置的策略创建对应的镜像拉取凭证校验器。
func NewImagePullCredentialVerificationPolicy(policy kubeletconfiginternal.ImagePullCredentialsVerificationPolicy, imageAllowList []string) (ImagePullPolicyEnforcer, error) {
	switch policy {
	case kubeletconfiginternal.NeverVerify:
		return NeverVerifyImagePullPolicy(), nil
	case kubeletconfiginternal.NeverVerifyPreloadedImages:
		return NeverVerifyPreloadedPullPolicy(), nil
	case kubeletconfiginternal.NeverVerifyAllowlistedImages:
		return NewNeverVerifyAllowListedPullPolicy(imageAllowList)
	case kubeletconfiginternal.AlwaysVerify:
		return AlwaysVerifyImagePullPolicy(), nil
	// 未知策略直接报错，避免静默使用不安全的默认行为。
	default:
		return nil, fmt.Errorf("unknown image pull credential verification policy: %v", policy)
	}
}

// NeverVerifyImagePullPolicy 返回永不校验凭证的策略。
func NeverVerifyImagePullPolicy() ImagePullPolicyEnforcerFunc {
	return func(image string, imagePulledByKubelet bool) bool {
		return false
	}
}

// NeverVerifyPreloadedPullPolicy 返回仅对 kubelet 自己拉取过的镜像校验凭证的策略。
func NeverVerifyPreloadedPullPolicy() ImagePullPolicyEnforcerFunc {
	return func(image string, imagePulledByKubelet bool) bool {
		// 由 kubelet 拉取过的镜像需要校验凭证；预加载镜像无需校验。
		return imagePulledByKubelet
	}
}

// AlwaysVerifyImagePullPolicy 返回始终校验凭证的策略。
func AlwaysVerifyImagePullPolicy() ImagePullPolicyEnforcerFunc {
	return func(image string, imagePulledByKubelet bool) bool {
		return true
	}
}

// NeverVerifyAllowlistedImages 对白名单中的镜像跳过凭证校验。
type NeverVerifyAllowlistedImages struct {
	absoluteURLs sets.Set[string]
	prefixes     []string
}

// NewNeverVerifyAllowListedPullPolicy 解析白名单，区分精确 URL 与通配前缀。
func NewNeverVerifyAllowListedPullPolicy(allowList []string) (*NeverVerifyAllowlistedImages, error) {
	policy := &NeverVerifyAllowlistedImages{
		absoluteURLs: sets.New[string](),
	}
	for _, pattern := range allowList {
		normalizedPattern, isWildcard, err := getAllowlistImagePattern(pattern)
		if err != nil {
			return nil, err
		}

		// 通配模式按前缀匹配，精确模式按完整镜像名匹配。
		if isWildcard {
			policy.prefixes = append(policy.prefixes, normalizedPattern)
		} else {
			policy.absoluteURLs.Insert(normalizedPattern)
		}
	}

	return policy, nil
}

// RequireCredentialVerificationForImage 在白名单不匹配时要求校验凭证。
func (p *NeverVerifyAllowlistedImages) RequireCredentialVerificationForImage(image string, imagePulledByKubelet bool) bool {
	return !p.imageMatches(image)
}

// imageMatches 判断镜像是否命中白名单。
func (p *NeverVerifyAllowlistedImages) imageMatches(image string) bool {
	noTagsName, err := trimImageTagDigest(image)
	if err != nil {
		// 解析失败时仍以尽力而为的方式，用用户提供的原始值继续匹配。
		noTagsName = image
	}

	if p.absoluteURLs.Has(noTagsName) {
		return true
	}

	for _, prefix := range p.prefixes {
		if strings.HasPrefix(noTagsName, prefix) {
			return true
		}
	}
	return false
}

// ValidateAllowlistImagesPatterns 校验白名单中的每个模式是否合法。
func ValidateAllowlistImagesPatterns(patterns []string) error {
	for _, p := range patterns {
		if _, _, err := getAllowlistImagePattern(p); err != nil {
			return err
		}
	}
	return nil
}

// getAllowlistImagePattern 规范化白名单模式，并返回是否为通配模式。
func getAllowlistImagePattern(pattern string) (string, bool, error) {
	// 模式首尾不允许空白字符。
	if pattern != strings.TrimSpace(pattern) {
		return "", false, fmt.Errorf("leading/trailing spaces are not allowed: %s", pattern)
	}

	trimmedPattern := pattern
	isWildcard := false
	// 仅支持以 /* 结尾的通配模式。
	if strings.HasSuffix(pattern, "/*") {
		isWildcard = true
		trimmedPattern = strings.TrimSuffix(trimmedPattern, "*")
	}

	if len(trimmedPattern) == 0 {
		return "", false, fmt.Errorf("the supplied pattern is too short: %s", pattern)
	}

	if strings.ContainsRune(trimmedPattern, '*') {
		return "", false, fmt.Errorf("not a valid wildcard pattern, only patterns ending with '/*' are allowed: %s", pattern)
	}

	if isWildcard {
		if len(trimmedPattern) == 1 {
			return "", false, fmt.Errorf("at least registry hostname is required")
		}
	} else { // not a wildcard
		// 非通配模式必须是合法镜像名，且不能带 tag 或 digest。
		image, err := trimImageTagDigest(trimmedPattern)
		if err != nil {
			return "", false, fmt.Errorf("failed to parse as an image name: %w", err)
		}

		if trimmedPattern != image {
			return "", false, fmt.Errorf("neither tag nor digest is accepted in an image reference: %s", pattern)
		}

		return trimmedPattern, false, nil
	}

	return trimmedPattern, true, nil
}

// trimImageTagDigest 从镜像名中去除 digest 与 tag。
func trimImageTagDigest(image string) (string, error) {
	imageParsed, err := imageref.Parse(image)
	if err != nil {
		return "", err
	}
	// 依次剥离 digest 与 tag，得到不带版本信息的镜像名。
	var tag, digest string
	if tagged, ok := imageParsed.(imageref.Tagged); ok {
		tag = tagged.Tag()
	}
	if digested, ok := imageParsed.(imageref.Digested); ok {
		digest = digested.Digest().String()
	}

	// digest 必须位于镜像名末尾。
	if len(digest) > 0 {
		var foundDigest bool
		image, foundDigest = strings.CutSuffix(image, "@"+digest)
		if !foundDigest {
			return "", fmt.Errorf("digest was not specified last, this does not appear to be a valid image spec: %q", image)
		}
	}

	// tag 必须位于末尾（或 digest 之前）。
	if len(tag) > 0 {
		var tagFound bool
		image, tagFound = strings.CutSuffix(image, ":"+tag)
		if !tagFound {
			return "", fmt.Errorf("tag %q was not specified last (or before the digest), this does not appear to be a valid image spec: %q", tag, image)
		}
	}

	return image, nil
}
