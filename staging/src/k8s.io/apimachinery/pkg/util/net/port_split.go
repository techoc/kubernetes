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

// SplitSchemeNamePort 接受以下形式的字符串：
//   - "<name>",                 returns "",        "<name>","",      true
//   - "<name>:<port>",          returns "",        "<name>","<port>",true
//   - "<scheme>:<name>:<port>", returns "<scheme>","<name>","<port>",true
//
// name 必须非空，否则返回 false。
// 如果指定 scheme，则必须为 "http" 或 "https"。
// port 以字符串返回，不要求为数字，
// 例如可以是命名端口。
//
// 段的个数多于 3 或 name 为空都返回 false；scheme 缺省时只允许
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

// JoinSchemeNamePort 返回指定 scheme、name 和 port 的字符串：
//   - "<name>"
//   - "<name>:<port>"
//   - "<scheme>:<name>:<port>"
//
// 任何参数都不能包含 ':' 字符。
// name 必填。
// scheme 必须为 ""、"http" 或 "https"。
//
// 段数由非空参数个数决定；空 port 与空 scheme 会被省略，因此
// 拼接结果可以被 SplitSchemeNamePort 原样解析。
func JoinSchemeNamePort(scheme, name, port string) string {
	if len(scheme) > 0 {
		//指定 scheme 时必须包含三段。
		return scheme + ":" + name + ":" + port
	}
	if len(port) > 0 {
		//指定 port 时必须包含两段。
		return name + ":" + port
	}
	//仅返回 name。
	return name
}
