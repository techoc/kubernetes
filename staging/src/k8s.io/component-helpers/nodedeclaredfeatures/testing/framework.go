/*
Copyright The Kubernetes Authors.

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

package testing

import (
	"slices"
	"testing"

	"k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// SetFrameworkDuringTest 在测试期间临时替换全局的默认 nodedeclaredfeatures.Framework，
// 并通过 tb.Cleanup 在测试结束后恢复原值。
func SetFrameworkDuringTest(tb testing.TB, framework nodedeclaredfeatures.Framework) {
	tb.Helper()
	// DefaultFramework 是包级可变全局，测试之间必须互不污染。
	// 用 Cleanup 而不是在测试末尾手动恢复，即使测试失败或提前 Fatal 也能还原现场。
	originalDefaultFramework := nodedeclaredfeatures.DefaultFramework
	nodedeclaredfeatures.DefaultFramework = &framework
	tb.Cleanup(func() {
		tb.Helper()
		nodedeclaredfeatures.DefaultFramework = originalDefaultFramework
	})
}

// NewMockFramework 根据给定名字生成一组 MockFeature，并创建一个注册了这些特性的 Framework。
// 第二个返回值是"名字 -> mock 对象"的映射，便于测试按需配置某个特性的行为。
func NewMockFramework(tb testing.TB, features ...string) (nodedeclaredfeatures.Framework, map[string]*MockFeature) {
	// 先排序，让相同名字集合以固定顺序注册到 Framework 中，避免注册顺序影响测试结果。
	slices.Sort(features)
	var allFeatures []types.Feature
	mockFeatures := map[string]*MockFeature{}

	for _, name := range features {
		m := NewMockFeature(tb)
		m.SetName(name)

		allFeatures = append(allFeatures, m)
		mockFeatures[name] = m
	}

	return *nodedeclaredfeatures.New(allFeatures), mockFeatures
}
