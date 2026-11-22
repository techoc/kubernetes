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

// Package flag 提供 Kubernetes 组件共用的 pflag 值类型，
// 用于解析 TLS 密码套件、版本、证书以及 map/slice 形式的命令行参数。

package flag

import (
	"crypto/tls"
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	// ciphers maps strings into tls package cipher constants in
	// https://golang.org/pkg/crypto/tls/#pkg-constants
	//
	// 仅收录 crypto/tls 认为安全、默认允许使用的套件。
	ciphers         = map[string]uint16{}
	insecureCiphers = map[string]uint16{}
)

// init 在包初始化时把标准库的套件清单灌入查找表。
// 使用标准库列表而非硬编码，可随 Go 版本自动获得新增套件。
func init() {
	for _, suite := range tls.CipherSuites() {
		ciphers[suite.Name] = suite.ID
	}
	// keep legacy names for backward compatibility
	// 早期版本接受省略 SHA256 后缀的套件名，当前仍在用；保留别名以免破坏既有配置。
	ciphers["TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305"] = tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
	ciphers["TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305"] = tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256

	for _, suite := range tls.InsecureCipherSuites() {
		insecureCiphers[suite.Name] = suite.ID
	}
}

// InsecureTLSCiphers returns the cipher suites implemented by crypto/tls which have
// security issues.
//
// 返回副本而不是内部 map，避免调用方修改影响包级状态。
func InsecureTLSCiphers() map[string]uint16 {
	cipherKeys := make(map[string]uint16, len(insecureCiphers))
	for k, v := range insecureCiphers {
		cipherKeys[k] = v
	}
	return cipherKeys
}

// InsecureTLSCipherNames returns a list of cipher suite names implemented by crypto/tls
// which have security issues.
//
// 主要用于 --tls-cipher-suites 的帮助文本，向用户展示“不推荐但可显式启用”的取值。
func InsecureTLSCipherNames() []string {
	// sets.NewString 保证结果有序且去重，便于生成稳定的帮助输出。
	cipherKeys := sets.NewString()
	for key := range insecureCiphers {
		cipherKeys.Insert(key)
	}
	return cipherKeys.List()
}

// PreferredTLSCipherNames returns a list of cipher suite names implemented by crypto/tls.
//
// “Preferred” 指 crypto/tls 默认启用、未标记为不安全的套件。
func PreferredTLSCipherNames() []string {
	cipherKeys := sets.NewString()
	for key := range ciphers {
		cipherKeys.Insert(key)
	}
	return cipherKeys.List()
}

// allCiphers 合并安全与不安全套件，供命令行校验接受两者的全部名称。
// 是否真正允许不安全套件由调用方策略决定，这里只负责解析。
func allCiphers() map[string]uint16 {
	acceptedCiphers := make(map[string]uint16, len(ciphers)+len(insecureCiphers))
	for k, v := range ciphers {
		acceptedCiphers[k] = v
	}
	for k, v := range insecureCiphers {
		acceptedCiphers[k] = v
	}
	return acceptedCiphers
}

// TLSCipherPossibleValues returns all acceptable cipher suite names.
// This is a combination of both InsecureTLSCipherNames() and PreferredTLSCipherNames().
//
// 供 pflag 生成 --tls-cipher-suites 的枚举提示。
func TLSCipherPossibleValues() []string {
	cipherKeys := sets.NewString()
	acceptedCiphers := allCiphers()
	for key := range acceptedCiphers {
		cipherKeys.Insert(key)
	}
	return cipherKeys.List()
}

// TLSCipherSuites returns a list of cipher suite IDs from the cipher suite names passed.
//
// 空输入返回 nil，表示“使用 Go 默认值”而非“禁用所有套件”；这一语义被
// TLS 配置调用方依赖，不能改成空切片。
func TLSCipherSuites(cipherNames []string) ([]uint16, error) {
	if len(cipherNames) == 0 {
		return nil, nil
	}
	ciphersIntSlice := make([]uint16, 0)
	possibleCiphers := allCiphers()
	for _, cipher := range cipherNames {
		intValue, ok := possibleCiphers[cipher]
		if !ok {
			return nil, fmt.Errorf("Cipher suite %s not supported or doesn't exist", cipher)
		}
		ciphersIntSlice = append(ciphersIntSlice, intValue)
	}
	return ciphersIntSlice, nil
}

// versions 把命令行版本名映射到 crypto/tls 常量；名称沿用 Go 的 VersionTLSxx 拼写，
// 变更会破坏现存配置文件，故保持稳定。
var versions = map[string]uint16{
	"VersionTLS10": tls.VersionTLS10,
	"VersionTLS11": tls.VersionTLS11,
	"VersionTLS12": tls.VersionTLS12,
	"VersionTLS13": tls.VersionTLS13,
}

// TLSPossibleVersions returns all acceptable values for TLS Version.
//
// 结果有序，便于稳定地出现在帮助文本中。
func TLSPossibleVersions() []string {
	versionsKeys := sets.NewString()
	for key := range versions {
		versionsKeys.Insert(key)
	}
	return versionsKeys.List()
}

// TLSVersion returns the TLS Version ID for the version name passed.
//
// 空字符串视为未指定，回落到默认版本，方便调用方统一处理零值。
func TLSVersion(versionName string) (uint16, error) {
	if len(versionName) == 0 {
		return DefaultTLSVersion(), nil
	}
	if version, ok := versions[versionName]; ok {
		return version, nil
	}
	return 0, fmt.Errorf("unknown tls version %q", versionName)
}

// DefaultTLSVersion defines the default TLS Version.
//
// 默认 TLS 1.2：更新版本可能尚未被所有对端支持，更旧版本存在已知攻击面。
func DefaultTLSVersion() uint16 {
	// Can't use SSLv3 because of POODLE and BEAST
	// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
	// Can't use TLSv1.1 because of RC4 cipher usage
	return tls.VersionTLS12
}
