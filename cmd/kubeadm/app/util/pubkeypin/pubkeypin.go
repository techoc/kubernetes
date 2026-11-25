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

// Package pubkeypin 提供遵循 RFC7469 的 X509 公钥固定原语。
package pubkeypin

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"strings"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// formatSHA256 是完整长度 SHA-256 哈希（以十六进制编码）的 pin 前缀。
	formatSHA256 = "sha256"
)

var (
	// supportedFormats 列出支持的格式。
	supportedFormats = strings.Join([]string{formatSHA256}, ", ")
)

// Set 是已固定的 X509 公钥集合。
type Set struct {
	sha256Hashes map[string]bool
}

// NewSet 返回新的空 PubKeyPinSet。
func NewSet() *Set {
	return &Set{make(map[string]bool)}
}

// Allow 向集合添加允许的公钥哈希。
func (s *Set) Allow(pubKeyHashes ...string) error {
	for _, pubKeyHash := range pubKeyHashes {
		parts := strings.Split(pubKeyHash, ":")
		if len(parts) != 2 {
			return errors.Errorf("invalid hash, expected \"format:hex-value\". "+
				"Known format(s) are: %s", supportedFormats)
		}
		format, value := parts[0], parts[1]

		switch strings.ToLower(format) {
		case "sha256":
			if err := s.allowSHA256(value); err != nil {
				return errors.Errorf("invalid hash %q, %v", pubKeyHash, err)
			}
		default:
			return errors.Errorf("unknown hash format %q. Known format(s) are: %s", format, supportedFormats)
		}
	}
	return nil
}

// CheckAny 判断证书是否匹配集合中的任一公钥。
func (s *Set) CheckAny(certificates []*x509.Certificate) error {
	var hashes []string

	for _, certificate := range certificates {
		if s.checkSHA256(certificate) {
			return nil
		}

		hashes = append(hashes, Hash(certificate))
	}
	return errors.Errorf("none of the public keys %q are pinned", strings.Join(hashes, ":"))
}

// Empty 判断集合是否未包含已固定的公钥。
func (s *Set) Empty() bool {
	return len(s.sha256Hashes) == 0
}

// Hash 计算 X509 证书中 SPKI 对象（DER 编码）的 SHA-256 哈希，
// 返回可直接传给 Set.Allow 的完整十六进制字符串。
func Hash(certificate *x509.Certificate) string {
	spkiHash := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return formatSHA256 + ":" + strings.ToLower(hex.EncodeToString(spkiHash[:]))
}

// allowSHA256 校验 "sha256" 格式的哈希，并将规范形式加入集合。
func (s *Set) allowSHA256(hash string) error {
	// 校验长度是否为完整 SHA-256 哈希。
	hashLength := hex.DecodedLen(len(hash))
	if hashLength != sha256.Size {
		return errors.Errorf("expected a %d byte SHA-256 hash, found %d bytes", sha256.Size, hashLength)
	}

	// 校验是否为有效十六进制。
	_, err := hex.DecodeString(hash)
	if err != nil {
		return errors.Wrap(err, "could not decode SHA-256 from hex")
	}

	// 最终仅将原始十六进制字符串以小写形式存入内存。
	s.sha256Hashes[strings.ToLower(hash)] = true
	return nil
}

// checkSHA256 判断证书的 "sha256" 哈希是否固定在集合中。
func (s *Set) checkSHA256(certificate *x509.Certificate) bool {
	actualHash := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	actualHashHex := strings.ToLower(hex.EncodeToString(actualHash[:]))
	return s.sha256Hashes[actualHashHex]
}
