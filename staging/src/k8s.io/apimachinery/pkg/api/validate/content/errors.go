/*
Copyright 2014 The Kubernetes Authors.

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

// Package content 集中定义校验失败时使用的错误信息构造函数。
// 这些字符串会直接出现在 API 返回的 field.Error 中，属于用户可见的约定。
package content

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/api/validate/constraints"
)

// MinError 返回 “必须大于等于” 校验失败时的说明文本。
func MinError[T constraints.Integer](min T) string {
	return fmt.Sprintf("must be greater than or equal to %d", min)
}

// MaxError 返回 “必须小于等于” 校验失败时的说明文本。
func MaxError[T constraints.Integer](max T) string {
	return fmt.Sprintf("must be less than or equal to %d", max)
}

// MaxLenError 返回 “字符串过长” 校验失败时的说明文本。
// 注意长度按字节计算，而非字符数。
func MaxLenError(length int) string {
	return fmt.Sprintf("must be no more than %d bytes", length)
}

// EmptyError 返回 “字符串为空” 校验失败时的说明文本。
func EmptyError() string {
	return "must be non-empty"
}

// RegexError 返回正则校验失败时的说明文本。
// examples 非空时会先列出合法示例，便于调用方修正；否则只给出所用正则。
func RegexError(msg string, re string, examples ...string) string {
	if len(examples) == 0 {
		return msg + " (regex used for validation is '" + re + "')"
	}
	msg += " (e.g. "
	for i := range examples {
		if i > 0 {
			msg += " or "
		}
		msg += "'" + examples[i] + "', "
	}
	msg += "regex used for validation is '" + re + "')"
	return msg
}

// NEQError 返回 “不得等于某值” 校验失败时的说明文本。
// 字符串值加引号输出，避免与数字/布尔值混淆。
func NEQError[T any](disallowed T) string {
	format := "%v"
	if reflect.ValueOf(disallowed).Kind() == reflect.String {
		format = "%q"
	}
	return fmt.Sprintf("must not be equal to "+format, disallowed)
}
