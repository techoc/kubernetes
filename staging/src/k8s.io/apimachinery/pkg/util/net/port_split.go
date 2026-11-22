/*
Copyright 2015 The Kubernetes Authors.

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

package net

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// validSchemes 是 SplitSchemeNamePort 接受的前缀；空字符串表示省略 scheme。
var validSchemes = sets.NewString("http", "https", "")

// SplitSchemeNamePort takes a string of the following forms:
//   - "<name>",                 returns "",        "<name>","",      true
//   - "<name>:<port>",          returns "",        "<name>","<port>",true
//   - "<scheme>:<name>:<port>", returns "<scheme>","<name>","<port>",true
//
// Name must be non-empty or valid will be returned false.
// Scheme must be "http" or "https" if specified
// Port is returned as a string, and it is not required to be numeric (could be
// used for a named port, for example).
//
// 中文说明：段的个数多于 3 或 name 为空都返回 false；scheme 缺省时只允许
// 两段/一段形式，从而避免与 IPv6 之类的冒号语法混淆。
func SplitSchemeNamePort(id string) (scheme, name, port string, valid bool) {
	parts := strings.Split(id, ":")
	switch len(parts) {
	case 1:
		name = parts[0]
	case 2:
		name = parts[0]
		port = parts[1]
	case 3:
		scheme = parts[0]
		name = parts[1]
		port = parts[2]
	default:
		return "", "", "", false
	}

	if len(name) > 0 && validSchemes.Has(scheme) {
		return scheme, name, port, true
	} else {
		return "", "", "", false
	}
}

// JoinSchemeNamePort returns a string that specifies the scheme, name, and port:
//   - "<name>"
//   - "<name>:<port>"
//   - "<scheme>:<name>:<port>"
//
// None of the parameters may contain a ':' character
// Name is required
// Scheme must be "", "http", or "https"
//
// 中文说明：段数由非空参数个数决定；空 port 与空 scheme 会被省略，因此
// 拼接结果可以被 SplitSchemeNamePort 原样解析。
func JoinSchemeNamePort(scheme, name, port string) string {
	if len(scheme) > 0 {
		// Must include three segments to specify scheme
		return scheme + ":" + name + ":" + port
	}
	if len(port) > 0 {
		// Must include two segments to specify port
		return name + ":" + port
	}
	// Return name alone
	return name
}
