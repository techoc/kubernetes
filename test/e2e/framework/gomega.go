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

// Package framework 提供与 provider 无关的辅助代码，用于通过 Ginkgo 构建和运行 E2E 测试。
// 实际的 Ginkgo 测试套件由该框架、可选的 provider 支持代码以及具体测试组合而成，
// 组合过程在单独的 .go 文件中完成，例如 Kubernetes 的 test/e2e.go。
package framework

import (
	"fmt"
	"strings"

	"github.com/onsi/gomega/format"
	gtypes "github.com/onsi/gomega/types"
)

// GomegaObject 返回一个 matcher，它在所包装 matcher 的失败信息后追加实际值的完整转储。
// 例如 gomega.HaveField 默认只显示被检查的字段，不显示该字段所属的对象，此时它很有用。
func GomegaObject(shouldMatch gtypes.GomegaMatcher) gtypes.GomegaMatcher {
	return &gomegaObjectMatcher{
		shouldMatch: shouldMatch,
	}
}

type gomegaObjectMatcher struct {
	shouldMatch gtypes.GomegaMatcher
}

func (m *gomegaObjectMatcher) Match(actual interface{}) (success bool, err error) {
	return m.shouldMatch.Match(actual)
}

func (m *gomegaObjectMatcher) FailureMessage(actual interface{}) (message string) {
	return m.withDump(actual, m.shouldMatch.FailureMessage(actual))
}

func (m *gomegaObjectMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return m.withDump(actual, m.shouldMatch.NegatedFailureMessage(actual))
}

func (m *gomegaObjectMatcher) withDump(actual any, message string) string {
	dump := format.Object(actual, 1)
	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}
	message += fmt.Sprintf("\nFull object:\n%s", dump)
	return message
}
