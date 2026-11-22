/*
Copyright 2023 The Kubernetes Authors.

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

package cache

import (
	"math/rand"
	"strings"
	"testing"
)

// TestObjectNames 用随机字符串验证 ParseObjectName 与 ObjectName.String 的性质。
//
// 中文说明：字符集包含 '/'，随机长度固定为 10，并循环 100 次（每次字符串不同）。
// 断言三条性质：多于一个 '/' 必须报错；0 或 1 个 '/' 时解析出的 Namespace/Name 正确；
// Parse 后再 String 应保持原串（唯一例外是单个前导 '/'，因为空 namespace 会被省略，
// 测试通过补回前导 '/' 来对齐该历史行为）。
func TestObjectNames(t *testing.T) {
	chars := "abcdefghi/"
	for count := 1; count <= 100; count++ {
		var encodedB strings.Builder
		for index := 0; index < 10; index++ {
			encodedB.WriteByte(chars[rand.Intn(len(chars))])
		}
		encodedS := encodedB.String()
		parts := strings.Split(encodedS, "/")
		on, err := ParseObjectName(encodedS)
		// 中文：两个以上分段（即至少两个 '/'）无法映射为 namespace/name。
		expectError := len(parts) > 2
		if expectError != (err != nil) {
			t.Errorf("Wrong error; expected=%v, got=%v", expectError, err)
		}
		if expectError || err != nil {
			continue
		}
		var expectedObjectName ObjectName
		if len(parts) == 2 {
			expectedObjectName = ObjectName{Namespace: parts[0], Name: parts[1]}
		} else {
			// 中文：无 '/' 时整串都是 name，namespace 为空。
			expectedObjectName = ObjectName{Name: encodedS}
		}
		if on != expectedObjectName {
			t.Errorf("Parse failed, expected=%+v, got=%+v", expectedObjectName, on)
		}
		recoded := on.String()
		// 中文：String 在 namespace 为空时只输出 name，需补回被吃掉的前导 '/'。
		if encodedS[0] == '/' {
			recoded = "/" + recoded
		}
		if encodedS != recoded {
			t.Errorf("Parse().String() was not identity, original=%q, final=%q", encodedS, recoded)
		}
	}
}
