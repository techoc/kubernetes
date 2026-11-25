/*
Copyright 2024 The Kubernetes Authors.

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

package api

import (
	"encoding/json"
	"unique"
)

// NullUniqueString 是不包含任何字符串的 UniqueString 零值。
var NullUniqueString UniqueString

// UniqueString 是对 [unique.Handle[string]] 的包装，比较更快，作为 map 键时更高效。
type UniqueString unique.Handle[string]

// String 返回 UniqueString 中保存的字符串；若为零值则返回空字符串。
func (us UniqueString) String() string {
	// 零值直接返回空字符串。
	if us == NullUniqueString {
		return ""
	}
	// 从唯一句柄中取出底层字符串。
	return unique.Handle[string](us).Value()
}

// MarshalJSON 将 UniqueString 编码为其底层字符串，主要用于 JSON/YAML 的美观打印。
func (us UniqueString) MarshalJSON() ([]byte, error) {
	// 委托 json.Marshal 编码 String 得到的字符串。
	return json.Marshal(us.String())
}

// MarshalText 使 UniqueString 可用作 map 键，且在日志中正常显示。
func (us UniqueString) MarshalText() ([]byte, error) {
	// 将底层字符串转换为字节切片作为文本表示。
	return []byte(us.String()), nil
}

// MakeUniqueString 将字符串驻留为唯一句柄并构造一个 UniqueString。
func MakeUniqueString(str string) UniqueString {
	// 通过 unique.Make 驻留字符串后包装为 UniqueString。
	return UniqueString(unique.Make(str))
}
