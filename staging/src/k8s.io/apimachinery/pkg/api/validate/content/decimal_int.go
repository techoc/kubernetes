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

package content

// decimalIntegerErrMsg 统一用于所有非规范形式，避免泄露具体哪一位不合法。
const decimalIntegerErrMsg string = "must be a valid decimal integer in canonical form"

// IsDecimalInteger 校验字符串是否为严格规范形式的十进制整数。
// 所谓规范，即人类书写整数的自然形式：不允许前导零、正号或其它进制前缀。
//
// 合法值："0" 或以 1-9 开头的非零整数（如 "123"、"-456"），其后跟任意 0-9 数字。
//
// 该校验比 strconv.ParseInt 更严格：后者接受前导零（如 "0700"）并当作十进制 700，
// 容易与八进制写法混淆。
func IsDecimalInteger(value string) []string {
	n := len(value)
	if n == 0 {
		return []string{EmptyError()}
	}

	i := 0
	if value[0] == '-' {
		// 只有单独的 "-" 不构成整数。
		if n == 1 {
			return []string{decimalIntegerErrMsg}
		}
		i = 1
	}

	// 首位为 0 时，只有整个值恰为 "0" 才合法；"00"、"-0"、"0700" 均拒绝。
	if value[i] == '0' {
		if n == 1 && i == 0 {
			return nil
		}
		return []string{decimalIntegerErrMsg}
	}

	// 首位必须是 1-9；这也顺带拒绝了 "+"、"a" 等非法字符。
	if value[i] < '1' || value[i] > '9' {
		return []string{decimalIntegerErrMsg}
	}

	// 其余各位必须是纯 ASCII 数字，拒绝空格、下划线等分隔符。
	for i++; i < n; i++ {
		if value[i] < '0' || value[i] > '9' {
			return []string{decimalIntegerErrMsg}
		}
	}

	return nil
}
