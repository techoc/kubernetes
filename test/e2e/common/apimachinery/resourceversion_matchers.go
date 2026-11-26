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

package apimachinery

import (
	"fmt"
	"math/big"

	"github.com/onsi/gomega/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HaveValidResourceVersion 返回一个 Gomega matcher，检查 Kubernetes
// 对象的 resourceVersion 是否为有效的 128 位无符号整数。
func HaveValidResourceVersion() types.GomegaMatcher {
	return &HaveValidResourceVersionMatcher{}
}

type HaveValidResourceVersionMatcher struct {
	failureReason string
}

// Match 是一个 gomega matcher，检查传入的接口是否为
// 带有 uint128 resourceVersion 字符串的 metav1.Object。
func (m *HaveValidResourceVersionMatcher) Match(actual interface{}) (success bool, err error) {
	// 先确认输入是 object。
	o, ok := actual.(metav1.Object)
	if !ok {
		return false, fmt.Errorf("HaveValidResourceVersionMatcher matcher expects an api object, but got %T", actual)
	}

	val, ok := new(big.Int).SetString(o.GetResourceVersion(), 10)

	if !ok {
		m.failureReason = "the resource version is not a valid integer"
		return false, nil
	}
	if val.Sign() < 0 {
		m.failureReason = "the resource version is a negative number"
		return false, nil
	}
	if val.BitLen() > 128 {
		m.failureReason = fmt.Sprintf("resource version requires %d bits (more than 128)", val.BitLen())
		return false, nil
	}
	if val.Cmp(big.NewInt(0)) == 0 {
		m.failureReason = "the resource version is zero which is not valid"
		return false, nil
	}

	return true, nil
}

// 断言写作 `Expect(...).To(...)` 时，使用 FailureMessage
func (m *HaveValidResourceVersionMatcher) FailureMessage(actual interface{}) (message string) {
	o, ok := actual.(metav1.Object)
	if !ok {
		return fmt.Sprintf("HaveValidResourceVersion matcher expects an api object, but got %T", actual)
	}
	return fmt.Sprintf("Expected resource version to be a valid uint128, but got %q: %s", o.GetResourceVersion(), m.failureReason)
}

// 断言写作 `Expect(...).ToNot(...)` 时，使用 NegatedFailureMessage
func (m *HaveValidResourceVersionMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	o, ok := actual.(metav1.Object)
	if !ok {
		return fmt.Sprintf("HaveValidResourceVersion matcher expects an api object, but got %T", actual)
	}
	return fmt.Sprintf("Expected resource version not to be a valid uint128, but got %q", o.GetResourceVersion())
}
