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

package store

import (
	"fmt"
	"regexp"
)

const (
	// keyMaxLength 是 key 允许的最大长度。
	keyMaxLength = 250

	keyCharFmt      string = "[A-Za-z0-9]"
	keyExtCharFmt   string = "[-A-Za-z0-9_.]"
	qualifiedKeyFmt string = "(" + keyCharFmt + keyExtCharFmt + "*)?" + keyCharFmt
)

var (
	// key 只能由字母数字以及 '-'、'_'、'.' 组成，且必须以字母数字开头和结尾。
	keyRegex = regexp.MustCompile("^" + qualifiedKeyFmt + "$")

	// ErrKeyNotFound 在 Store 中找不到 key 时返回。
	ErrKeyNotFound = fmt.Errorf("key is not found")
)

// Store 定义按键存储数据的接口，实现必须是线程安全的。
type Store interface {
	// Write 使用 key 写入数据；key 必须至少包含一个 [A-Za-z0-9] 字符。
	Write(key string, data []byte) error
	// Read 按键读取数据；key 不存在时必须返回 ErrKeyNotFound。
	Read(key string) ([]byte, error)
	// Delete 按键删除数据；key 不存在时不得返回错误。
	Delete(key string) error
	// List 列出所有已存在的 key。
	List() ([]string, error)
}

// ValidateKey 在 key 不满足格式与长度要求时返回错误。
func ValidateKey(key string) error {
	if len(key) <= keyMaxLength && keyRegex.MatchString(key) {
		return nil
	}
	return fmt.Errorf("invalid key: %q", key)
}
