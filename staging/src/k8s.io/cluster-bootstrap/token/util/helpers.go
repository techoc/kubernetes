/*
Copyright 2017 The Kubernetes Authors.

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

// Package util 提供 bootstrap token 的生成、格式校验与名称转换工具。
//
// 这里同时保留较旧的辅助函数（token 字符串层面）和更细分的子包
// (util/secrets、util/tokens)。公开 token ID 可用正则匹配；Secret 则单独检查
// 长度和字符集，避免把保密内容交给正则引擎。
package util

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cluster-bootstrap/token/api"
)

// TODO(dixudx): 重构到 util/secrets 与 util/tokens。

// validBootstrapTokenChars 是 bootstrap token 允许使用的字符集。
const validBootstrapTokenChars = "0123456789abcdefghijklmnopqrstuvwxyz"

var (
	// BootstrapTokenRegexp 匹配完整 token 的公开结构。
	// Secret 校验应使用 IsValidBootstrapToken，避免把保密内容交给正则引擎。
	BootstrapTokenRegexp = regexp.MustCompile(api.BootstrapTokenPattern)
	// BootstrapTokenIDRegexp 匹配公开 token ID 的格式。
	BootstrapTokenIDRegexp = regexp.MustCompile(api.BootstrapTokenIDPattern)
	// BootstrapGroupRegexp 匹配 bootstrap token 附加组的允许格式。
	BootstrapGroupRegexp = regexp.MustCompile(api.BootstrapGroupPattern)
)

// GenerateBootstrapToken 生成随机的 "<id>.<secret>" 形式 bootstrap token。
// ID 与 Secret 分别从 crypto/rand 独立采样，公开 ID 不能用于推导 Secret。
func GenerateBootstrapToken() (string, error) {
	tokenID, err := randBytes(api.BootstrapTokenIDBytes)
	if err != nil {
		return "", err
	}

	tokenSecret, err := randBytes(api.BootstrapTokenSecretBytes)
	if err != nil {
		return "", err
	}

	return TokenFromIDAndSecret(tokenID, tokenSecret), nil
}

// randBytes 从 crypto/rand 独立采样，返回指定长度、字符均属于 validBootstrapTokenChars 的字符串。
// 随机整数通过无分支位运算映射到 a-z 或 0-9，避免映射过程产生依赖于随机值的时序差异。
func randBytes(length int) (string, error) {
	var (
		token = make([]byte, length)
		max   = new(big.Int).SetUint64(uint64(len(validBootstrapTokenChars)))
	)

	for i := range token {
		val, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", fmt.Errorf("could not generate random integer: %w", err)
		}
		// 用无分支位运算把随机值映射到 a-z 或 0-9 字符范围。
		x := val.Uint64()
		res := x + 48 + (39 & ((9 - x) >> 8))
		token[i] = byte(res)
	}

	return string(token), nil
}

// TokenFromIDAndSecret 将 ID 与 Secret 拼接为 "{id}.{secret}"，不校验输入内容。
func TokenFromIDAndSecret(id, secret string) string {
	return fmt.Sprintf("%s.%s", id, secret)
}

// IsValidBootstrapToken 检查字符串是否符合 bootstrap token 的完整格式。
// Secret 不交给正则匹配，而是单独校验长度与字符集，避免正则回溯影响保密输入的匹配。
func IsValidBootstrapToken(token string) bool {
	// 必须恰好由一个小数点分成两段。
	t := strings.Split(token, ".")
	if len(t) != 2 {
		return false
	}

	// ID 是公开信息，可直接使用正则。
	if !BootstrapTokenIDRegexp.MatchString(t[0]) {
		return false
	}

	// Secret 必须精确为 16 位，仅允许小写字母与数字。
	secret := t[1]
	if len(secret) != api.BootstrapTokenSecretBytes { // 长度必须精确匹配
		return false
	}
	for i := range secret {
		c := int(secret[i])
		notDigit := (c < 48 || c > 57)   // 字符不在 0-9 范围
		notLetter := (c < 97 || c > 122) // 字符不在 a-z 范围
		if notDigit && notLetter {
			return false
		}
	}
	return true
}

// IsValidBootstrapTokenID 检查字符串是否满足 BootstrapTokenIDRegexp 的完整匹配要求。
func IsValidBootstrapTokenID(tokenID string) bool {
	return BootstrapTokenIDRegexp.MatchString(tokenID)
}

// BootstrapTokenSecretName 返回存放 bootstrap token 的 Kubernetes Secret 名称。
// 名称只包含公开 ID，可安全用于对象路径和日志；本函数不校验 ID 是否合法。
func BootstrapTokenSecretName(tokenID string) string {
	return fmt.Sprintf("%s%s", api.BootstrapTokenSecretPrefix, tokenID)
}

// ValidateBootstrapGroupName 校验 bootstrap 组名，合法时返回 nil，否则返回校验错误。
// TODO(dixudx): 应移到 util/secrets。
func ValidateBootstrapGroupName(name string) error {
	if BootstrapGroupRegexp.Match([]byte(name)) {
		return nil
	}
	return fmt.Errorf("bootstrap group %q is invalid (must match %s)", name, api.BootstrapGroupPattern)
}

// ValidateUsages 检查 bootstrap token 用途是否都在 KnownTokenUsages 白名单中。
// 未知用途去重后一次性报错并全部列出，便于调用方一次修正配置。
func ValidateUsages(usages []string) error {
	validUsages := sets.NewString(api.KnownTokenUsages...)
	invalidUsages := sets.NewString()
	for _, usage := range usages {
		if !validUsages.Has(usage) {
			invalidUsages.Insert(usage)
		}
	}
	if len(invalidUsages) > 0 {
		return fmt.Errorf("invalid bootstrap token usage string: %s, valid usage options: %s", strings.Join(invalidUsages.List(), ","), strings.Join(api.KnownTokenUsages, ","))
	}
	return nil
}
