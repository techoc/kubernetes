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
// (util/secrets、util/tokens)。代码中特意区分公开的 token ID 与保密的
// token Secret：对 ID 可以用正则匹配，对 Secret 则采用常量时间比较，
// 以减少侧信道泄露。
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

// TODO(dixudx): refactor this to util/secrets and util/tokens

// validBootstrapTokenChars defines the characters a bootstrap token can consist of
const validBootstrapTokenChars = "0123456789abcdefghijklmnopqrstuvwxyz"

var (
	// BootstrapTokenRegexp is a compiled regular expression of TokenRegexpString
	//
	// 仅用于匹配公开的完整 token 结构；涉及 Secret 的校验应改用
	// IsValidBootstrapToken 的常量时间实现。
	BootstrapTokenRegexp = regexp.MustCompile(api.BootstrapTokenPattern)
	// BootstrapTokenIDRegexp is a compiled regular expression of TokenIDRegexpString
	BootstrapTokenIDRegexp = regexp.MustCompile(api.BootstrapTokenIDPattern)
	// BootstrapGroupRegexp is a compiled regular expression of BootstrapGroupPattern
	BootstrapGroupRegexp = regexp.MustCompile(api.BootstrapGroupPattern)
)

// GenerateBootstrapToken generates a new, random Bootstrap Token.
//
// 返回 "<id>.<secret>" 形式。ID 与 Secret 分开用 crypto/rand 生成，
// 因此即使 ID 被公开，也无法推导出 Secret。
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

// randBytes returns a random string consisting of the characters in
// validBootstrapTokenChars, with the length customized by the parameter
//
// 逐个字符从 crypto/rand 取随机整数并映射到合法字符集；使用常量时间的
// 位运算完成映射，避免基于时间差异的分支泄露随机值。
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
		// Use simple operations in constant-time to obtain a byte in the a-z,0-9
		// character range
		x := val.Uint64()
		res := x + 48 + (39 & ((9 - x) >> 8))
		token[i] = byte(res)
	}

	return string(token), nil
}

// TokenFromIDAndSecret returns the full token which is of the form "{id}.{secret}"
func TokenFromIDAndSecret(id, secret string) string {
	return fmt.Sprintf("%s.%s", id, secret)
}

// IsValidBootstrapToken returns whether the given string is valid as a Bootstrap Token.
// Avoid using BootstrapTokenRegexp.MatchString(token) and instead perform constant-time
// comparisons on the secret.
//
// 对 Secret 部分不使用正则：正则引擎的比较可能因不匹配位置不同而耗时不同，
// 逐字符常量时间校验可降低通过时间侧信道探测 Secret 的风险。
func IsValidBootstrapToken(token string) bool {
	// Must be exactly two strings separated by "."
	t := strings.Split(token, ".")
	if len(t) != 2 {
		return false
	}

	// Validate the ID: t[0]
	// Using a Regexp for it is safe because the ID is public already
	if !BootstrapTokenIDRegexp.MatchString(t[0]) {
		return false
	}

	// Validate the secret with constant-time: t[1]
	secret := t[1]
	if len(secret) != api.BootstrapTokenSecretBytes { // Must be an exact size
		return false
	}
	for i := range secret {
		c := int(secret[i])
		notDigit := (c < 48 || c > 57)   // Character is not in the 0-9 range
		notLetter := (c < 97 || c > 122) // Character is not in the a-z range
		if notDigit && notLetter {
			return false
		}
	}
	return true
}

// IsValidBootstrapTokenID returns whether the given string is valid as a Bootstrap Token ID and
// in other words satisfies the BootstrapTokenIDRegexp
func IsValidBootstrapTokenID(tokenID string) bool {
	return BootstrapTokenIDRegexp.MatchString(tokenID)
}

// BootstrapTokenSecretName returns the expected name for the Secret storing the
// Bootstrap Token in the Kubernetes API.
//
// 名称只包含公开的 ID，所以可以安全地用于对象路径和日志。
func BootstrapTokenSecretName(tokenID string) string {
	return fmt.Sprintf("%s%s", api.BootstrapTokenSecretPrefix, tokenID)
}

// ValidateBootstrapGroupName checks if the provided group name is a valid
// bootstrap group name. Returns nil if valid or a validation error if invalid.
// TODO(dixudx): should be moved to util/secrets
func ValidateBootstrapGroupName(name string) error {
	if BootstrapGroupRegexp.Match([]byte(name)) {
		return nil
	}
	return fmt.Errorf("bootstrap group %q is invalid (must match %s)", name, api.BootstrapGroupPattern)
}

// ValidateUsages validates that the passed in string are valid usage strings for bootstrap tokens.
//
// 未知用途被收集后一次性报错，错误信息会列出所有非法值，便于调用方
// 直接修正配置而非逐个试错。
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
